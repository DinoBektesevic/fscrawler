use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use crossbeam::deque::{Injector, Worker};

use clap::Parser;
use fs_crawler::cli::{Cli, OutputMode};
use fs_crawler::types::{WorkItem, next_dir_id};
use fs_crawler::worker::worker_thread;

use fs_crawler::writers::{streaming_writer_thread, buffering_writer_thread};
use fs_crawler::writers::stdout::StdoutWriter;
use fs_crawler::writers::postgres::PostgresWriter;
use fs_crawler::writers::table::{TableWriter, SortOrder, SizeUnit};


fn main() {
    let cli = Cli::parse();

    // validate postgres url is provided when needed
    if cli.create_tables && cli.database_url.is_none() {
        eprintln!("error: --database-url required when --create-tables provided");
        std::process::exit(1);
    }

    if matches!(cli.output, OutputMode::Postgres) && cli.database_url.is_none() {
        eprintln!("error: --database-url required when --output=postgres");
        std::process::exit(1);
    }

    if cli.clear && cli.database_url.is_none() {
        eprintln!("error: --database-url required when --clear provided");
        std::process::exit(1);
    }

    // if we're just creating tables, we can
    if cli.create_tables {
        match fs_crawler::db::run_create(&cli.database_url.unwrap()) {
            Ok(_)  => println!("Database tables created successfully."),
            Err(e) => {
                eprintln!("Failed to create tables: {}", e);
                std::process::exit(1);
            }
        }
        std::process::exit(0);
    }

    if cli.clear {
        match fs_crawler::db::run_clear(&cli.database_url.unwrap()) {
            Ok(_)  => println!("Tables cleared and re-initialised."),
            Err(e) => {
                eprintln!("Failed to clear tables: {}", e);
                std::process::exit(1);
            }
        }
        std::process::exit(0);
    }

    // Set up
    // - the number of workers
    let num_workers = cli.workers
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        });

    let global:       Arc<Injector<WorkItem>> = Arc::new(Injector::new());
    let active_count: Arc<AtomicUsize>        = Arc::new(AtomicUsize::new(0));

    let mut workers  = Vec::new();
    let mut stealers = Vec::new();

    for _ in 0..num_workers {
        let w = Worker::<WorkItem>::new_fifo();
        stealers.push(w.stealer());
        workers.push(w);
    }

    let stealers = Arc::new(stealers);

    // In multiple producers, single consumer, setup the producers can be
    // returning more data than the consumer can manage in the same time. These
    // results get stored in memory and if we don't bounce back and pause the
    // workers if the consumer is struggling, then we would just have a
    // memory-runaway effect. The number of batches that the consumer (writers)
    // are allowed to fall behind the producers (crawler worker threads) is 256.
    // After that the workers will block and stop work, until the writer catches
    // up. How big this number should be depends on the amount of memory we have
    // and how performance sensitive we are on the worker side.
    let (result_tx, result_rx) = std::sync::mpsc::sync_channel(256);

    let root_dir_id = next_dir_id();
    global.push(WorkItem::FullScan{
        path: PathBuf::from(cli.root),
        dir_id: root_dir_id,
        parent_id: None
    });

    let writer_handle = match cli.output {
        OutputMode::Stdout => std::thread::spawn(move || {
            streaming_writer_thread(
                result_rx,
                StdoutWriter::new()
            )
        }),
        OutputMode::Table => std::thread::spawn(move || {
            buffering_writer_thread(
                result_rx,
                TableWriter::new(SortOrder::Path, SizeUnit::Kilobytes),
            )
        }),
        OutputMode::Postgres => {
            let tmpurl = cli.database_url.clone().unwrap();
            std::thread::spawn(move || {
                streaming_writer_thread(
                    result_rx,
                    PostgresWriter::new(tmpurl))
            })
        },
    };

    // Launch workers
    let worker_handles: Vec<_> = workers
        .into_iter()
        .map(|local| {
            let global       = Arc::clone(&global);
            let stealers     = Arc::clone(&stealers);
            let result_tx    = result_tx.clone();
            let active_count = Arc::clone(&active_count);

            std::thread::spawn(move || {
                worker_thread(local, global, stealers, result_tx, active_count);
            })
        })
        .collect();

    for handle in worker_handles {
        handle.join().expect("worker thread panicked");
    }

    // Dropping results drains the workers and releases the pool, but it isn't
    // instantaneous
    drop(result_tx);

    match writer_handle.join() {
        Ok(Ok(_))  => {},
        Ok(Err(e)) => eprintln!("writer error: {}", e),
        Err(_)     => eprintln!("writer thread panicked"),
    };

    // after writer finishes, reconnect for post-crawl
    // In this step we add the primary key constraints removed
    // earlier to gain the sql COPY speedups with no checks
    if let OutputMode::Postgres = cli.output {
        let url = cli.database_url.as_ref().unwrap();
        match fs_crawler::db::run_post_crawl(&url) {
            Ok(_) => println!("Post-crawl successful!"),
            Err(e) => eprintln!("Post-crawl failure: {}", e),
        }

        match fs_crawler::db::run_finish(&url) {
            Ok(_) => println!("Closure and summary tables created!"),
            Err(e) => eprintln!("Failed to create closure and summary tables: {}", e),
        }
    }
}
