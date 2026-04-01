use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam::deque::{Injector, Steal, Stealer, Worker};

use crate::crawler::process_work_item;
use crate::types::{DirResult, WorkItem};


/// ┌─────────────────────────────────────────────────────────────┐
/// │                     Main Thread                             │
/// │  Seeds queue with root path(s)                              │
/// │  Spawns N worker threads                                    │
/// │  Waits on result_rx channel                                 │
/// └──────────────────────────┬──────────────────────────────────┘
///                            │  initial work items
///                            ▼
/// ┌─────────────────────────────────────────────────────────────┐
/// │              crossbeam::deque::Injector<PathBuf>            │
/// │                   (global work queue)                       │
/// └──────┬───────────────────┬──────────────────────┬───────────┘
///        │ steal             │ steal                │ steal
///        ▼                   ▼                      ▼
/// ┌────────────┐      ┌────────────┐        ┌────────────┐
/// │  Worker 1  │      │  Worker 2  │        │  Worker N  │
/// │  local     │◄────►│  local     │◄──────►│  local     │
/// │  deque     │steal │  deque     │steal   │  deque     │
/// └─────┬──────┘      └─────┬──────┘        └─────┬──────┘
///       │                   │                     │
///       │  pushes new dirs back to injector       │
///       │  sends file batches to writer           │
///       └───────────────────┼─────────────────────┘
///                           │  CrawlBatch via mpsc channel
///                           ▼
///                ┌─────────────────────┐
///                │    Writer Thread    │
///                │  accumulates rows   │
///                │  bulk COPY to DB    │
///                └─────────────────────┘

/// Worker thread.
///
/// Continuously pulls [`WorkItem`]s from the local deque.
/// Falls back to stealing from the global injector or other workers' deques when empty.
///
/// For each item, calls [`process_work_item`] and:
/// - Pushes any discovered subdirectories back onto the global queue
/// - Sends the [`DirResult`] to the writer thread via `result_tx`
///
/// Exits when there is no work left and no other worker is active.
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

                let (path, dir_id, parent_id) = match &work {
                    WorkItem::FullScan    { path, dir_id, parent_id }     => (path.as_path(), *dir_id, *parent_id),
                    WorkItem::DeltaScan   { path, dir_id, parent_id, .. } => (path.as_path(), *dir_id, *parent_id),
                    // WorkItem::FileRefresh { path, dir_id }     => (path.as_path(), *dir_id),
                    WorkItem::FileRefresh { .. } => {
                        unimplemented!("FileRefresh requires inotify code that doesn't exist yet.");
                    }
                };

                let result = process_work_item(path, dir_id, parent_id);

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

/// Finds the next [`WorkItem`] to process.
///
/// Checks in order:
/// 1. Local deque
/// 2. Global injector (batch steal)
/// 3. Other workers' deques
///
/// Returns `None` if all sources are empty.
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
