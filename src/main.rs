use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use crossbeam::deque::{Injector, Worker};

use clap::Parser;
use fs_crawler::cli::{Cli, OutputMode};
use fs_crawler::types::WorkItem;
use fs_crawler::worker::worker_thread;

use fs_crawler::writers::{streaming_writer_thread, buffering_writer_thread};
use fs_crawler::writers::stdout::StdoutWriter;
use fs_crawler::writers::postgres::PostgresWriter;
use fs_crawler::writers::table::{TableWriter, SortOrder, SizeUnit};


fn main() {
    let cli = Cli::parse();

    // validate postgres url is provided when needed
    if matches!(cli.output, OutputMode::Postgres) && cli.database_url.is_none() {
        eprintln!("error: --database-url required when --output=postgres");
        std::process::exit(1);
    }

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

    let (result_tx, result_rx) = std::sync::mpsc::sync_channel(256);

    global.push(WorkItem::FullScan(PathBuf::from(cli.root)));

    let writer_handle = match cli.output {
        OutputMode::Stdout => std::thread::spawn(move || {
            streaming_writer_thread(result_rx, StdoutWriter::new())
        }),
        OutputMode::Table => std::thread::spawn(move || {
            buffering_writer_thread(
                result_rx,
                TableWriter::new(SortOrder::Path, SizeUnit::Kilobytes),
            )
        }),
        OutputMode::Postgres => std::thread::spawn(move || {
            streaming_writer_thread(result_rx, PostgresWriter::new(cli.database_url.unwrap()))
        }),
    };

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

    drop(result_tx);
    let _ = writer_handle.join().expect("writer thread panicked");
}
