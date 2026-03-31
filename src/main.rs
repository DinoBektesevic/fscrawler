use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use crossbeam::deque::{Injector, Worker};

use clap::Parser;
use fscrawler::cli::Cli;
use fscrawler::config::{FilesystemConfig, OutputMode};
use fscrawler::types::{WorkItem, next_dir_id};
use fscrawler::worker::worker_thread;

use fscrawler::writers::{streaming_writer_thread, buffering_writer_thread};
use fscrawler::writers::stdout::StdoutWriter;
use fscrawler::writers::postgres::PostgresWriter;
use fscrawler::writers::table::{TableWriter, SortOrder, SizeUnit};


/// Crawls a single filesystem root and writes all results to the configured output.
///
/// Spawns `fs.workers` worker threads (falling back to available parallelism)
/// around a shared work-stealing [`Injector`] queue, plus one writer thread
/// that drains results via a bounded [`sync_channel`]. Workers block when the
/// channel is full, providing backpressure so memory usage stays bounded.
///
/// Blocks until all workers and the writer have finished. Post-crawl steps
/// (foreign key constraints, closure table) are handled by the caller after
/// all filesystems complete.
fn crawl_filesystem(fs: FilesystemConfig, db_url: Option<String>, output: OutputMode) {
    // Set up workers
    let num_workers = fs.workers
        .map(|w| w as usize)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(8)
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
    global.push(WorkItem::FullScan {
        path:      PathBuf::from(&fs.root),
        dir_id:    root_dir_id,
        parent_id: None,
    });

    let writer_handle = match output {
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
            let url = db_url.expect("postgres output requires a database URL");
            std::thread::spawn(move || {
                streaming_writer_thread(
                    result_rx,
                    PostgresWriter::new(url))
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
}

fn main() {
    let cli = Cli::parse();

    let config = match cli.resolve() {
        Ok(c)  => c,
        Err(e) => { eprintln!("{}", e); std::process::exit(1); }
    };

    // ////////////////////////////////////////////////////////////////////////////
    //                              DB Management
    // ////////////////////////////////////////////////////////////////////////////
    // if --create-tables is the only goal, run it and exit early
    if cli.create_tables {
        let url = config.server.connection_string.as_deref().unwrap_or_else(|| {
            eprintln!("error: --database-url required when --create-tables provided");
            std::process::exit(1);
        });

        match fscrawler::db::run_create(url) {
            Ok(_)  => println!("Database tables created successfully."),
            Err(e) => { eprintln!("Failed to create tables: {}", e); std::process::exit(1); }
        }
        std::process::exit(0);
    }

    // if --clear is the only goal, truncate all tables, re-initialise schema and exit early
    if cli.clear {
        let url = config.server.connection_string.as_deref().unwrap_or_else(|| {
            eprintln!("error: --database-url required when --clear provided");
            std::process::exit(1);
        });
        match fscrawler::db::run_clear(url) {
            Ok(_)  => println!("Tables cleared and re-initialised."),
            Err(e) => { eprintln!("Failed to clear tables: {}", e); std::process::exit(1); }
        }
        std::process::exit(0);
    }

    // Seed ID counters from DB max to avoid primary key conflicts on re-run
    if let Some(url) = &config.server.connection_string {
        match fscrawler::db::run_query_max_ids(url) {
            Ok((file_max, dir_max)) => {
                fscrawler::types::seed_file_id(file_max);
                fscrawler::types::seed_dir_id(dir_max);
            }
            Err(e) => {
                eprintln!("error: failed to query max IDs from database: {}", e);
                std::process::exit(1);
            }
        }
    }

    // ////////////////////////////////////////////////////////////////////////
    //                              Crawling
    // ////////////////////////////////////////////////////////////////////////
    let db_url = config.server.connection_string.clone();
    let fs_handles: Vec<_> = config.filesystems
        .into_iter()
        .map(|fs| {
            let db_url  = db_url.clone();
            let output  = config.output.clone();
            std::thread::spawn(move || crawl_filesystem(fs, db_url, output))
        })
        .collect();

    for handle in fs_handles {
        handle.join().expect("filesystem crawl panicked");
    }

    // after writer finishes, reconnect for post-crawl
    // In this step we add the foreign key constraints omitted during ingestion
    // to gain the COPY throughput benefits of constraint-free bulk inserts
    if let Some(url) = &config.server.connection_string {
        match fscrawler::db::run_post_crawl(url) {
            Ok(_) => println!("Post-crawl successful!"),
            Err(e) => eprintln!("Post-crawl failure: {}", e),
        }

        match fscrawler::db::run_finish(url) {
            Ok(_) => println!("Closure and summary tables created!"),
            Err(e) => eprintln!("Failed to create closure and summary tables: {}", e),
        }
    }
}
