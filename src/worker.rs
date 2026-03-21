use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam::deque::{Injector, Steal, Stealer, Worker};

use crate::crawler::process_work_item;
use crate::types::{DirResult, WorkItem};

pub fn worker_thread(
    local:        Worker<WorkItem>,
    global:       Arc<Injector<WorkItem>>,
    stealers:     Arc<Vec<Stealer<WorkItem>>>,
    result_tx:    std::sync::mpsc::SyncSender<DirResult>,
    active_count: Arc<AtomicUsize>,
) {
    loop {
        match find_work(&local, &global, &stealers) {
            Some(work) => {
                active_count.fetch_add(1, Ordering::SeqCst);

                let path = match &work {
                    WorkItem::FullScan(p)            => p.as_path(),
                    WorkItem::DeltaScan { path, .. } => path.as_path(),
                    WorkItem::FileRefresh(p)         => p.as_path(),
                };

                let result = process_work_item(path);

                for subdir in result.subdirs.iter() {
                    global.push(subdir.clone());
                }

                result_tx.send(result).expect("writer thread died");
                active_count.fetch_sub(1, Ordering::SeqCst);
            }
            None => {
                if active_count.load(Ordering::SeqCst) == 0 && global.is_empty() {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_micros(100));
            }
        }
    }
}

fn find_work(
    local:    &Worker<WorkItem>,
    global:   &Injector<WorkItem>,
    stealers: &[Stealer<WorkItem>],
) -> Option<WorkItem> {
    local.pop()
        .or_else(|| loop {
            match global.steal_batch_and_pop(local) {
                Steal::Success(item) => break Some(item),
                Steal::Retry        => continue,
                Steal::Empty        => break None,
            }
        })
        .or_else(|| {
            stealers.iter()
                .map(|s| s.steal())
                .find_map(|s| s.success())
        })
}
