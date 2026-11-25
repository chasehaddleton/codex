//! Watches subscribed files or directories and routes coarse-grained change
//! notifications to the subscribers that own matching watched paths.

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use notify::Event;
use notify::EventKind;
use notify::RecommendedWatcher;
use notify::RecursiveMode;
use notify::Watcher;
use tokio::runtime::Handle;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio::time::sleep_until;
use tracing::warn;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Coalesced file change notification for a subscriber.
pub struct FileWatcherEvent {
    /// Changed paths delivered in sorted order with duplicates removed.
    pub paths: Vec<PathBuf>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
/// Path subscription registered by a [`FileWatcherSubscriber`].
pub struct WatchPath {
    /// Root path to watch.
    pub path: PathBuf,
    /// Whether events below `path` should match recursively.
    pub recursive: bool,
}

#[cfg(target_os = "macos")]
mod vnode;

type SubscriberId = u64;

enum BackendEvent {
    Native(notify::Result<Event>),
    #[cfg(target_os = "macos")]
    VnodeChanged(vnode::Location),
    #[cfg(target_os = "macos")]
    Vanished(PathBuf),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WatchMode {
    Native(RecursiveMode),
    #[cfg(target_os = "macos")]
    Ancestor,
}

impl WatchPath {
    fn mode_for(&self, _requested: &WatchPath) -> WatchMode {
        #[cfg(target_os = "macos")]
        if self.path != _requested.path {
            return WatchMode::Ancestor;
        }
        WatchMode::Native(if self.recursive {
            RecursiveMode::Recursive
        } else {
            RecursiveMode::NonRecursive
        })
    }
}

#[derive(Default)]
struct WatchState {
    next_subscriber_id: SubscriberId,
    path_ref_counts: HashMap<PathBuf, PathWatchCounts>,
    subscribers: HashMap<SubscriberId, SubscriberState>,
}

struct SubscriberState {
    watched_paths: HashMap<SubscriberWatchKey, SubscriberWatchState>,
    tx: WatchSender,
}

/// Immutable per-subscriber watch identity.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct SubscriberWatchKey {
    /// Original path requested by the subscriber. Notifications are reported
    /// in this namespace so clients do not see canonicalization artifacts.
    requested: WatchPath,
    /// Canonical equivalent of `requested` used to match backend events.
    /// Some backends report canonical paths such as `/private/var/...` even
    /// when the watch was registered through `/var/...`.
    matched: WatchPath,
}

/// Mutable per-subscriber watch state.
struct SubscriberWatchState {
    /// A previously missing component can be created as a symlink to another
    /// directory. Keep the registration key stable while matching its new target.
    #[cfg(target_os = "macos")]
    updated_match: Option<WatchPath>,
    /// Existing path passed to the OS watcher and used for ref-counting. This
    /// is usually `requested`, but missing targets use an existing ancestor.
    actual: WatchPath,
    count: usize,
    /// Whether the requested path existed the last time an ancestor event was
    /// handled. This preserves delete notifications for fallback watches.
    last_exists: bool,
    /// Whether this watch started from a missing path. Such watches normalize
    /// ancestor create/delete events back to `requested`.
    fallback: bool,
}

/// Registration-time watch data before it is merged into subscriber state.
///
/// The key is stable for unregistering while `actual` may later move closer
/// to the requested path as missing path components are created.
#[derive(Clone)]
struct SubscriberWatchRegistration {
    /// Immutable subscriber-visible identity for this registration.
    key: SubscriberWatchKey,
    /// Existing path initially passed to the OS watcher.
    actual: WatchPath,
    /// Whether registration started from a missing path fallback.
    fallback: bool,
}

/// Receives coalesced change notifications for a single subscriber.
pub struct Receiver {
    inner: Arc<ReceiverInner>,
}

struct WatchSender {
    inner: Arc<ReceiverInner>,
}

struct ReceiverInner {
    changed_paths: AsyncMutex<BTreeSet<PathBuf>>,
    notify: Notify,
    sender_count: AtomicUsize,
}

impl Receiver {
    /// Waits for the next batch of changed paths, or returns `None` once the
    /// corresponding subscriber has been removed and no more events can arrive.
    pub async fn recv(&mut self) -> Option<FileWatcherEvent> {
        loop {
            let notified = self.inner.notify.notified();
            {
                let mut changed_paths = self.inner.changed_paths.lock().await;
                if !changed_paths.is_empty() {
                    return Some(FileWatcherEvent {
                        paths: std::mem::take(&mut *changed_paths).into_iter().collect(),
                    });
                }
                if self.inner.sender_count.load(Ordering::Acquire) == 0 {
                    return None;
                }
            }
            notified.await;
        }
    }
}

impl WatchSender {
    async fn add_changed_paths(&self, paths: &[PathBuf]) {
        if paths.is_empty() {
            return;
        }

        let mut changed_paths = self.inner.changed_paths.lock().await;
        let previous_len = changed_paths.len();
        changed_paths.extend(paths.iter().cloned());
        if changed_paths.len() != previous_len {
            self.inner.notify.notify_one();
        }
    }
}

impl Clone for WatchSender {
    fn clone(&self) -> Self {
        self.inner.sender_count.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Drop for WatchSender {
    fn drop(&mut self) {
        if self.inner.sender_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.inner.notify.notify_waiters();
        }
    }
}

fn watch_channel() -> (WatchSender, Receiver) {
    let inner = Arc::new(ReceiverInner {
        changed_paths: AsyncMutex::new(BTreeSet::new()),
        notify: Notify::new(),
        sender_count: AtomicUsize::new(1),
    });
    (
        WatchSender {
            inner: Arc::clone(&inner),
        },
        Receiver { inner },
    )
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct PathWatchCounts {
    non_recursive: usize,
    recursive: usize,
    #[cfg(target_os = "macos")]
    ancestors: usize,
}

impl PathWatchCounts {
    fn count_mut(&mut self, mode: WatchMode) -> &mut usize {
        match mode {
            WatchMode::Native(RecursiveMode::Recursive) => &mut self.recursive,
            WatchMode::Native(RecursiveMode::NonRecursive) => &mut self.non_recursive,
            #[cfg(target_os = "macos")]
            WatchMode::Ancestor => &mut self.ancestors,
        }
    }

    fn increment(&mut self, mode: WatchMode, amount: usize) {
        *self.count_mut(mode) += amount;
    }

    fn decrement(&mut self, mode: WatchMode, amount: usize) {
        let count = self.count_mut(mode);
        *count = count.saturating_sub(amount);
    }

    fn effective_mode(self) -> Option<WatchMode> {
        if self.recursive > 0 {
            return Some(WatchMode::Native(RecursiveMode::Recursive));
        }
        if self.non_recursive > 0 {
            return Some(WatchMode::Native(RecursiveMode::NonRecursive));
        }
        #[cfg(target_os = "macos")]
        if self.ancestors > 0 {
            return Some(WatchMode::Ancestor);
        }
        None
    }
}

struct FileWatcherInner {
    #[cfg(not(target_os = "macos"))]
    watcher: RecommendedWatcher,
    #[cfg(target_os = "macos")]
    watcher: MacNativeWatcher,
    watched_paths: HashMap<PathBuf, RecursiveMode>,
    #[cfg(target_os = "macos")]
    ancestors: HashMap<PathBuf, vnode::Watch>,
    #[cfg(target_os = "macos")]
    roots: HashMap<PathBuf, vnode::Watch>,
    #[cfg(target_os = "macos")]
    raw_tx: mpsc::UnboundedSender<BackendEvent>,
}

#[cfg(target_os = "macos")]
struct MacNativeWatcher {
    inner: RecommendedWatcher,
    // A symlink can resolve elsewhere by the time notify is asked to unwatch it.
    // Register canonical paths and retain the spelling owned by each caller.
    bindings: HashMap<PathBuf, (PathBuf, RecursiveMode)>,
}

#[cfg(target_os = "macos")]
impl MacNativeWatcher {
    fn mode(&self, target: &Path) -> Option<RecursiveMode> {
        self.bindings
            .values()
            .filter(|(path, _)| path == target)
            .map(|(_, mode)| *mode)
            .max_by_key(|m| *m == RecursiveMode::Recursive)
    }

    fn set_mode(
        &mut self,
        target: &Path,
        before: Option<RecursiveMode>,
        after: Option<RecursiveMode>,
    ) -> notify::Result<()> {
        if before == after {
            return Ok(());
        }
        if before.is_some() {
            self.inner.unwatch(target)?;
        }
        if let Some(mode) = after
            && let Err(err) = self.inner.watch(target, mode)
        {
            if let Some(previous) = before {
                let _ = self.inner.watch(target, previous);
            }
            return Err(err);
        }
        Ok(())
    }

    fn watch(&mut self, path: &Path, mode: RecursiveMode) -> notify::Result<()> {
        let target = path.canonicalize()?;
        if let Some((previous, previous_mode)) = self.bindings.get(path) {
            if *previous == target && *previous_mode == mode {
                return Ok(());
            }
            self.unwatch(path)?;
        }
        let before = self.mode(&target);
        let after = before
            .filter(|m| *m == RecursiveMode::Recursive)
            .unwrap_or(mode);
        self.set_mode(&target, before, Some(after))?;
        self.bindings.insert(path.to_owned(), (target, mode));
        Ok(())
    }

    fn unwatch(&mut self, path: &Path) -> notify::Result<()> {
        let Some((target, removed)) = self.bindings.remove(path) else {
            return Ok(());
        };
        let after = self.mode(&target);
        let before = after
            .filter(|m| *m == RecursiveMode::Recursive)
            .unwrap_or(removed);
        self.set_mode(&target, Some(before), after)
    }
}

/// Coalesces bursts of watch notifications and emits at most once per interval.
pub struct ThrottledWatchReceiver {
    rx: Receiver,
    interval: Duration,
    next_allowed: Option<Instant>,
}

impl ThrottledWatchReceiver {
    /// Creates a throttling wrapper around a raw watcher [`Receiver`].
    pub fn new(rx: Receiver, interval: Duration) -> Self {
        Self {
            rx,
            interval,
            next_allowed: None,
        }
    }

    /// Receives the next event, enforcing the configured minimum delay after
    /// the previous emission.
    pub async fn recv(&mut self) -> Option<FileWatcherEvent> {
        if let Some(next_allowed) = self.next_allowed {
            sleep_until(next_allowed).await;
        }

        let event = self.rx.recv().await;
        if event.is_some() {
            self.next_allowed = Some(Instant::now() + self.interval);
        }
        event
    }
}

/// Coalesces file watcher notifications that arrive within a fixed debounce
/// window after the first event in each batch.
pub struct DebouncedWatchReceiver {
    rx: Receiver,
    interval: Duration,
    changed_paths: BTreeSet<PathBuf>,
}

impl DebouncedWatchReceiver {
    /// Creates a debouncing wrapper around a raw watcher [`Receiver`].
    pub fn new(rx: Receiver, interval: Duration) -> Self {
        Self {
            rx,
            interval,
            changed_paths: BTreeSet::new(),
        }
    }

    /// Receives the next debounced event batch.
    pub async fn recv(&mut self) -> Option<FileWatcherEvent> {
        while self.changed_paths.is_empty() {
            self.changed_paths.extend(self.rx.recv().await?.paths);
        }
        let deadline = Instant::now() + self.interval;

        loop {
            tokio::select! {
                event = self.rx.recv() => match event {
                    Some(event) => self.changed_paths.extend(event.paths),
                    None => break,
                },
                _ = sleep_until(deadline) => break,
            }
        }

        Some(FileWatcherEvent {
            paths: std::mem::take(&mut self.changed_paths)
                .into_iter()
                .collect(),
        })
    }
}

/// Handle used to register watched paths for one logical consumer.
pub struct FileWatcherSubscriber {
    id: SubscriberId,
    file_watcher: Arc<FileWatcher>,
}

impl FileWatcherSubscriber {
    /// Registers the provided paths for this subscriber and returns an RAII
    /// guard that unregisters them on drop.
    pub fn register_paths(&self, watched_paths: Vec<WatchPath>) -> WatchRegistration {
        let watched_paths = dedupe_watched_paths(watched_paths)
            .into_iter()
            .map(|requested| {
                let (actual, matched, fallback) = actual_watch_path(&requested);
                SubscriberWatchRegistration {
                    key: SubscriberWatchKey { requested, matched },
                    actual,
                    fallback,
                }
            })
            .collect::<Vec<_>>();
        let registration_succeeded = self.file_watcher.register_paths(self.id, &watched_paths);

        WatchRegistration {
            registration_succeeded,
            file_watcher: Arc::downgrade(&self.file_watcher),
            subscriber_id: self.id,
            watched_paths: watched_paths
                .iter()
                .map(|watch| watch.key.clone())
                .collect(),
        }
    }

    #[cfg(test)]
    pub(crate) fn register_path(&self, path: PathBuf, recursive: bool) -> WatchRegistration {
        self.register_paths(vec![WatchPath { path, recursive }])
    }
}

impl Drop for FileWatcherSubscriber {
    fn drop(&mut self) {
        self.file_watcher.remove_subscriber(self.id);
    }
}

/// RAII guard for a set of active path registrations.
pub struct WatchRegistration {
    registration_succeeded: bool,
    file_watcher: std::sync::Weak<FileWatcher>,
    subscriber_id: SubscriberId,
    watched_paths: Vec<SubscriberWatchKey>,
}

impl WatchRegistration {
    /// Whether all required OS watches registered successfully at creation time.
    pub fn registration_succeeded(&self) -> bool {
        self.registration_succeeded
    }
}

impl Default for WatchRegistration {
    fn default() -> Self {
        Self {
            registration_succeeded: false,
            file_watcher: std::sync::Weak::new(),
            subscriber_id: 0,
            watched_paths: Vec::new(),
        }
    }
}

impl Drop for WatchRegistration {
    fn drop(&mut self) {
        if let Some(file_watcher) = self.file_watcher.upgrade() {
            file_watcher.unregister_paths(self.subscriber_id, &self.watched_paths);
        }
    }
}

/// Multi-subscriber file watcher built on top of `notify`.
pub struct FileWatcher {
    inner: Option<Arc<Mutex<FileWatcherInner>>>,
    state: Arc<RwLock<WatchState>>,
}

impl FileWatcher {
    /// Creates a live filesystem watcher and starts its background event loop
    /// on the current Tokio runtime.
    pub fn new() -> notify::Result<Self> {
        let (raw_tx, raw_rx) = mpsc::unbounded_channel();
        let file_watcher = Self::with_event_sender(raw_tx)?;
        file_watcher.spawn_event_loop(raw_rx);
        Ok(file_watcher)
    }

    fn with_event_sender(raw_tx: mpsc::UnboundedSender<BackendEvent>) -> notify::Result<Self> {
        #[cfg(target_os = "macos")]
        let ancestor_tx = raw_tx.clone();
        let watcher = notify::recommended_watcher(move |res| {
            let _ = raw_tx.send(BackendEvent::Native(res));
        })?;
        #[cfg(target_os = "macos")]
        let watcher = MacNativeWatcher {
            inner: watcher,
            bindings: HashMap::new(),
        };
        let inner = FileWatcherInner {
            watcher,
            watched_paths: HashMap::new(),
            #[cfg(target_os = "macos")]
            ancestors: HashMap::new(),
            #[cfg(target_os = "macos")]
            roots: HashMap::new(),
            #[cfg(target_os = "macos")]
            raw_tx: ancestor_tx,
        };
        let state = Arc::new(RwLock::new(WatchState::default()));
        Ok(Self {
            inner: Some(Arc::new(Mutex::new(inner))),
            state,
        })
    }

    /// Creates an inert watcher that only supports test-driven synthetic
    /// notifications.
    pub fn noop() -> Self {
        Self {
            inner: None,
            state: Arc::new(RwLock::new(WatchState::default())),
        }
    }

    /// Adds a new subscriber and returns both its registration handle and its
    /// dedicated event receiver.
    pub fn add_subscriber(self: &Arc<Self>) -> (FileWatcherSubscriber, Receiver) {
        let (tx, rx) = watch_channel();
        let mut state = self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let subscriber_id = state.next_subscriber_id;
        state.next_subscriber_id += 1;
        state.subscribers.insert(
            subscriber_id,
            SubscriberState {
                watched_paths: HashMap::new(),
                tx,
            },
        );

        let subscriber = FileWatcherSubscriber {
            id: subscriber_id,
            file_watcher: self.clone(),
        };
        (subscriber, rx)
    }

    fn register_paths(
        &self,
        subscriber_id: SubscriberId,
        watched_paths: &[SubscriberWatchRegistration],
    ) -> bool {
        let mut state = self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut inner_guard: Option<std::sync::MutexGuard<'_, FileWatcherInner>> = None;

        #[cfg(target_os = "macos")]
        let mut ancestors_to_recheck = BTreeSet::new();
        for registration in watched_paths {
            let actual = {
                let Some(subscriber) = state.subscribers.get_mut(&subscriber_id) else {
                    return false;
                };
                match subscriber.watched_paths.entry(registration.key.clone()) {
                    std::collections::hash_map::Entry::Occupied(mut entry) => {
                        entry.get_mut().count += 1;
                        entry.get().actual.clone()
                    }
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        #[cfg(target_os = "macos")]
                        if registration.fallback {
                            ancestors_to_recheck.insert(&registration.actual.path);
                        }
                        entry.insert(SubscriberWatchState {
                            actual: registration.actual.clone(),
                            #[cfg(target_os = "macos")]
                            updated_match: None,
                            count: 1,
                            last_exists: registration.key.matched.path.exists(),
                            fallback: registration.fallback,
                        });
                        registration.actual.clone()
                    }
                }
            };

            let counts = state
                .path_ref_counts
                .entry(actual.path.clone())
                .or_default();
            let previous_mode = counts.effective_mode();
            counts.increment(
                actual.mode_for(&registration.key.requested),
                /*amount*/ 1,
            );
            let next_mode = counts.effective_mode();
            if previous_mode != next_mode {
                self.reconfigure_watch(&actual.path, next_mode, &mut inner_guard);
            }
        }

        // Registration remains best-effort. Check that every requested path has an OS watch.
        let Some(inner) = &self.inner else {
            return false;
        };
        let guard = inner_guard.get_or_insert_with(|| {
            inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        });
        let Some(subscriber) = state.subscribers.get(&subscriber_id) else {
            return false;
        };
        #[cfg(target_os = "macos")]
        for ancestor in ancestors_to_recheck {
            // An existing source can have delivered the relevant directory change
            // before this registration joined it. Reconcile once under the new state.
            let _ = guard
                .raw_tx
                .send(BackendEvent::VnodeChanged(vnode::Location::Ancestor(
                    ancestor.clone(),
                )));
        }
        watched_paths.iter().all(|registration| {
            let Some(watch) = subscriber.watched_paths.get(&registration.key) else {
                return false;
            };
            #[cfg(target_os = "macos")]
            if watch.actual.mode_for(&registration.key.requested) == WatchMode::Ancestor
                && guard.ancestors.contains_key(&watch.actual.path)
            {
                return true;
            }
            match guard.watched_paths.get(&watch.actual.path) {
                Some(RecursiveMode::Recursive) => true,
                Some(RecursiveMode::NonRecursive) => !watch.actual.recursive,
                None => false,
            }
        })
    }

    fn unregister_paths(&self, subscriber_id: SubscriberId, watched_paths: &[SubscriberWatchKey]) {
        let mut state = self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut inner_guard: Option<std::sync::MutexGuard<'_, FileWatcherInner>> = None;

        for subscriber_watch in watched_paths {
            let actual = {
                let Some(subscriber) = state.subscribers.get_mut(&subscriber_id) else {
                    return;
                };
                let Some(subscriber_watch_state) =
                    subscriber.watched_paths.get_mut(subscriber_watch)
                else {
                    continue;
                };
                let actual = subscriber_watch_state.actual.clone();
                subscriber_watch_state.count = subscriber_watch_state.count.saturating_sub(1);
                if subscriber_watch_state.count == 0 {
                    subscriber.watched_paths.remove(subscriber_watch);
                }
                actual
            };

            let Some(counts) = state.path_ref_counts.get_mut(&actual.path) else {
                continue;
            };
            let previous_mode = counts.effective_mode();
            counts.decrement(
                actual.mode_for(&subscriber_watch.requested),
                /*amount*/ 1,
            );
            let next_mode = counts.effective_mode();
            if next_mode.is_none() {
                state.path_ref_counts.remove(&actual.path);
            }
            if previous_mode != next_mode {
                self.reconfigure_watch(&actual.path, next_mode, &mut inner_guard);
            }
        }
    }

    fn remove_subscriber(&self, subscriber_id: SubscriberId) {
        let mut state = self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(subscriber) = state.subscribers.remove(&subscriber_id) else {
            return;
        };

        let mut inner_guard: Option<std::sync::MutexGuard<'_, FileWatcherInner>> = None;
        for (subscriber_watch, subscriber_watch_state) in subscriber.watched_paths {
            let Some(path_counts) = state
                .path_ref_counts
                .get_mut(&subscriber_watch_state.actual.path)
            else {
                continue;
            };
            let previous_mode = path_counts.effective_mode();
            path_counts.decrement(
                subscriber_watch_state
                    .actual
                    .mode_for(&subscriber_watch.requested),
                subscriber_watch_state.count,
            );
            let next_mode = path_counts.effective_mode();
            if next_mode.is_none() {
                state
                    .path_ref_counts
                    .remove(&subscriber_watch_state.actual.path);
            }
            if previous_mode != next_mode {
                self.reconfigure_watch(
                    &subscriber_watch_state.actual.path,
                    next_mode,
                    &mut inner_guard,
                );
            }
        }
    }

    fn reconfigure_watch<'a>(
        &'a self,
        path: &Path,
        next_mode: Option<WatchMode>,
        inner_guard: &mut Option<std::sync::MutexGuard<'a, FileWatcherInner>>,
    ) {
        Self::reconfigure_watch_inner(self.inner.as_ref(), path, next_mode, inner_guard);
    }

    fn reconfigure_watch_inner<'a>(
        inner: Option<&'a Arc<Mutex<FileWatcherInner>>>,
        path: &Path,
        next_mode: Option<WatchMode>,
        inner_guard: &mut Option<std::sync::MutexGuard<'a, FileWatcherInner>>,
    ) {
        let Some(inner) = inner else {
            return;
        };
        if inner_guard.is_none() {
            let guard = inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *inner_guard = Some(guard);
        }
        let Some(guard) = inner_guard.as_mut() else {
            return;
        };

        #[cfg(target_os = "macos")]
        if !matches!(next_mode, Some(WatchMode::Native(_))) {
            guard.roots.remove(path);
        }
        #[cfg(target_os = "macos")]
        if let Some(ancestor) = guard.ancestors.get(path) {
            if next_mode == Some(WatchMode::Ancestor) && ancestor.still_watches(path) {
                return;
            }
            guard.ancestors.remove(path);
        }
        let existing_mode = guard.watched_paths.get(path).copied();
        if existing_mode.map(WatchMode::Native) == next_mode {
            return;
        }

        if existing_mode.is_some() {
            if let Err(err) = guard.watcher.unwatch(path) {
                warn!("failed to unwatch {}: {err}", path.display());
            }
            guard.watched_paths.remove(path);
        }

        let next_mode = match next_mode {
            Some(WatchMode::Native(mode)) => mode,
            #[cfg(target_os = "macos")]
            Some(WatchMode::Ancestor) => {
                match vnode::Watch::new(
                    vnode::Location::Ancestor(path.to_owned()),
                    guard.raw_tx.clone(),
                ) {
                    Ok(watch) => {
                        guard.ancestors.insert(path.to_path_buf(), watch);
                    }
                    Err(err) => warn!("failed to watch directory {}: {err}", path.display()),
                }
                return;
            }
            None => return,
        };
        if !path.exists() {
            return;
        }

        if let Err(err) = guard.watcher.watch(path, next_mode) {
            warn!("failed to watch {}: {err}", path.display());
            return;
        }
        guard.watched_paths.insert(path.to_path_buf(), next_mode);
    }

    fn apply_actual_watch_move<'a>(
        path_ref_counts: &mut HashMap<PathBuf, PathWatchCounts>,
        old_actual: WatchPath,
        new_actual: WatchPath,
        requested: &WatchPath,
        count: usize,
        inner: Option<&'a Arc<Mutex<FileWatcherInner>>>,
        inner_guard: &mut Option<std::sync::MutexGuard<'a, FileWatcherInner>>,
    ) {
        if old_actual == new_actual {
            return;
        }

        if let Some(counts) = path_ref_counts.get_mut(&old_actual.path) {
            let previous_mode = counts.effective_mode();
            counts.decrement(old_actual.mode_for(requested), count);
            let next_mode = counts.effective_mode();
            if next_mode.is_none() {
                path_ref_counts.remove(&old_actual.path);
            }
            if previous_mode != next_mode {
                Self::reconfigure_watch_inner(inner, &old_actual.path, next_mode, inner_guard);
            }
        }

        let counts = path_ref_counts.entry(new_actual.path.clone()).or_default();
        let previous_mode = counts.effective_mode();
        counts.increment(new_actual.mode_for(requested), count);
        let next_mode = counts.effective_mode();
        if previous_mode != next_mode {
            Self::reconfigure_watch_inner(inner, &new_actual.path, next_mode, inner_guard);
        }
        #[cfg(target_os = "macos")]
        if new_actual.path == requested.path
            && let Some(inner) = inner
        {
            // An FSEvents stream rooted at a directory need not report the root
            // disappearing. Keep one terminal-event vnode source for the lifetime
            // of this shared root so its missing-path subscription can resume.
            let guard = inner_guard.get_or_insert_with(|| {
                inner
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
            });
            Self::watch_root_lifetime(guard, &new_actual.path);
        }
    }

    #[cfg(target_os = "macos")]
    fn watch_root_lifetime(inner: &mut FileWatcherInner, path: &Path) {
        if inner
            .roots
            .get(path)
            .is_some_and(|watch| watch.still_watches(path))
        {
            return;
        }
        inner.roots.remove(path);
        match vnode::Watch::new(vnode::Location::Root(path.to_owned()), inner.raw_tx.clone()) {
            Ok(watch) => {
                inner.roots.insert(path.to_owned(), watch);
            }
            Err(err) => warn!("failed to watch lifetime of {}: {err}", path.display()),
        }
    }

    // Bridge `notify`'s callback-based events into the Tokio runtime and
    // notify the matching subscribers.
    fn spawn_event_loop(&self, mut raw_rx: mpsc::UnboundedReceiver<BackendEvent>) {
        if let Ok(handle) = Handle::try_current() {
            let state = Arc::clone(&self.state);
            let inner = self.inner.as_ref().map(Arc::downgrade);
            handle.spawn(async move {
                loop {
                    match raw_rx.recv().await {
                        Some(BackendEvent::Native(Ok(event))) => {
                            if !is_mutating_event(&event) {
                                continue;
                            }
                            if event.paths.is_empty() {
                                continue;
                            }
                            let inner = inner.as_ref().and_then(std::sync::Weak::upgrade);
                            Self::notify_subscribers(&state, inner.as_ref(), &event.paths, None)
                                .await;
                        }
                        #[cfg(target_os = "macos")]
                        Some(BackendEvent::VnodeChanged(vnode::Location::Ancestor(path))) => {
                            let inner = inner.as_ref().and_then(std::sync::Weak::upgrade);
                            Self::notify_subscribers(
                                &state,
                                inner.as_ref(),
                                std::slice::from_ref(&path),
                                Some(&path),
                            )
                            .await;
                        }
                        #[cfg(target_os = "macos")]
                        Some(BackendEvent::VnodeChanged(vnode::Location::Root(path))) => {
                            let Some(inner) = inner.as_ref().and_then(std::sync::Weak::upgrade)
                            else {
                                break;
                            };
                            {
                                let mut guard = inner
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                                if guard
                                    .roots
                                    .get(&path)
                                    .is_none_or(|watch| watch.still_watches(&path))
                                {
                                    continue;
                                }
                                if path.exists() {
                                    if let Some(mode) = guard.watched_paths.get(&path).copied()
                                        && let Err(err) = guard.watcher.watch(&path, mode)
                                    {
                                        warn!("failed to rebind {}: {err}", path.display());
                                        if !guard.watcher.bindings.contains_key(&path) {
                                            guard.watched_paths.remove(&path);
                                        }
                                    }
                                    Self::watch_root_lifetime(&mut guard, &path);
                                } else {
                                    guard.roots.remove(&path);
                                }
                            }
                            Self::notify_subscribers(
                                &state,
                                Some(&inner),
                                std::slice::from_ref(&path),
                                None,
                            )
                            .await;
                        }
                        #[cfg(target_os = "macos")]
                        Some(BackendEvent::Vanished(path)) => {
                            let inner = inner.as_ref().and_then(std::sync::Weak::upgrade);
                            Self::notify_subscribers(&state, inner.as_ref(), &[path], None).await;
                        }
                        Some(BackendEvent::Native(Err(err))) => {
                            warn!("file watcher error: {err}");
                        }
                        None => break,
                    }
                }
            });
        } else {
            warn!("file watcher loop skipped: no Tokio runtime available");
        }
    }

    async fn notify_subscribers(
        state: &RwLock<WatchState>,
        inner: Option<&Arc<Mutex<FileWatcherInner>>>,
        event_paths: &[PathBuf],
        changed_ancestor: Option<&Path>,
    ) {
        let subscribers_to_notify: Vec<(WatchSender, Vec<PathBuf>)> = {
            let mut state = state
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut actual_watch_moves = Vec::new();
            let mut subscribers_to_notify = Vec::new();

            for subscriber in state.subscribers.values_mut() {
                let mut changed_paths = Vec::new();
                for event_path in event_paths {
                    for (subscriber_watch, subscriber_watch_state) in &mut subscriber.watched_paths
                    {
                        // A vnode event names the directory, not its changed child. Only
                        // registrations waiting on this ancestor need to resolve their path.
                        if changed_ancestor.is_some_and(|ancestor| {
                            subscriber_watch_state.actual.path != ancestor
                                || subscriber_watch.requested.path == ancestor
                        }) {
                            continue;
                        }
                        if let Some(path) = changed_path_for_event(
                            subscriber_watch,
                            subscriber_watch_state,
                            event_path,
                        ) {
                            changed_paths.push(path);
                        }

                        let requested = &subscriber_watch.requested;
                        let (new_actual, _matched, fallback) = actual_watch_path(requested);
                        #[cfg(target_os = "macos")]
                        {
                            subscriber_watch_state.updated_match =
                                (_matched != subscriber_watch.matched).then_some(_matched);
                        }
                        subscriber_watch_state.fallback |= fallback;
                        if subscriber_watch_state.actual != new_actual {
                            #[cfg(target_os = "macos")]
                            if subscriber_watch_state.actual.path == requested.path
                                || new_actual.path == requested.path
                            {
                                // A descendant event can be the first indication of root
                                // removal or creation. Invalidate the root before switching
                                // streams, which can discard its remaining events.
                                changed_paths.push(requested.path.clone());
                                subscriber_watch_state.last_exists = !fallback;
                            }
                            let old_actual = subscriber_watch_state.actual.clone();
                            let count = subscriber_watch_state.count;
                            subscriber_watch_state.actual = new_actual.clone();
                            actual_watch_moves.push((
                                old_actual,
                                new_actual,
                                requested.clone(),
                                count,
                            ));
                        }
                    }
                }
                if !changed_paths.is_empty() {
                    subscribers_to_notify.push((subscriber.tx.clone(), changed_paths));
                }
            }

            let mut inner_guard: Option<std::sync::MutexGuard<'_, FileWatcherInner>> = None;
            for (old_actual, new_actual, requested, count) in actual_watch_moves {
                Self::apply_actual_watch_move(
                    &mut state.path_ref_counts,
                    old_actual,
                    new_actual,
                    &requested,
                    count,
                    inner,
                    &mut inner_guard,
                );
            }

            #[cfg(target_os = "macos")]
            if let Some(path) = changed_ancestor
                && state
                    .path_ref_counts
                    .get(path)
                    .and_then(|counts| counts.effective_mode())
                    == Some(WatchMode::Ancestor)
            {
                // A directory can be replaced at the same pathname. Rebind if the
                // open vnode no longer matches the directory used to resolve the target.
                Self::reconfigure_watch_inner(
                    inner,
                    path,
                    Some(WatchMode::Ancestor),
                    &mut inner_guard,
                );
            }
            subscribers_to_notify
        };

        for (subscriber, changed_paths) in subscribers_to_notify {
            subscriber.add_changed_paths(&changed_paths).await;
        }
    }

    #[cfg(test)]
    pub(crate) async fn send_paths_for_test(&self, paths: Vec<PathBuf>) {
        Self::notify_subscribers(&self.state, self.inner.as_ref(), &paths, None).await;
    }

    #[cfg(test)]
    pub(crate) fn spawn_event_loop_for_test(&self, raw_rx: mpsc::UnboundedReceiver<BackendEvent>) {
        self.spawn_event_loop(raw_rx);
    }

    #[cfg(test)]
    pub(crate) fn watch_counts_for_test(&self, path: &Path) -> Option<(usize, usize)> {
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.path_ref_counts.get(path).map(|counts| {
            let non_recursive = counts.non_recursive;
            #[cfg(target_os = "macos")]
            let non_recursive = non_recursive + counts.ancestors;
            (non_recursive, counts.recursive)
        })
    }
}

fn is_mutating_event(event: &Event) -> bool {
    matches!(
        event.kind,
        EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
    )
}

fn dedupe_watched_paths(mut watched_paths: Vec<WatchPath>) -> Vec<WatchPath> {
    watched_paths.sort_unstable_by(|a, b| {
        a.path
            .as_os_str()
            .cmp(b.path.as_os_str())
            .then(a.recursive.cmp(&b.recursive))
    });
    watched_paths.dedup();
    watched_paths
}

/// Returns the actual OS watch path and canonical match path for a request.
///
/// Missing targets use the nearest existing directory to discover the next
/// component. On macOS a vnode source observes membership changes without
/// subscribing to the ancestor's FSEvents subtree.
fn actual_watch_path(requested: &WatchPath) -> (WatchPath, WatchPath, bool) {
    if requested.path.exists() {
        let matched_path = requested
            .path
            .canonicalize()
            .unwrap_or_else(|_| requested.path.clone());
        let actual = requested.clone();
        let matched = WatchPath {
            path: matched_path,
            recursive: requested.recursive,
        };
        return (actual, matched, false);
    }

    let requested_parent = requested.path.parent();
    let mut ancestor = requested_parent;
    while let Some(path) = ancestor {
        if path.is_dir() {
            let actual_path = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
            let matched_path = requested
                .path
                .strip_prefix(path)
                .map(|suffix| actual_path.join(suffix))
                .unwrap_or_else(|_| requested.path.clone());
            let actual = WatchPath {
                path: path.to_path_buf(),
                recursive: false,
            };
            let matched = WatchPath {
                path: matched_path,
                recursive: requested.recursive,
            };
            return (actual, matched, true);
        }
        ancestor = path.parent();
    }

    (requested.clone(), requested.clone(), false)
}

/// Converts one raw backend event path into the subscriber-visible path.
///
/// Matching first uses the canonical path namespace reported by many OS
/// backends, then falls back to the originally requested namespace for
/// synthetic tests and backends that preserve the input spelling.
fn changed_path_for_event(
    key: &SubscriberWatchKey,
    state: &mut SubscriberWatchState,
    event_path: &Path,
) -> Option<PathBuf> {
    let requested = &key.requested;
    let matched = &key.matched;
    #[cfg(target_os = "macos")]
    let matched = state.updated_match.as_ref().unwrap_or(matched);
    let mut changed_in_namespace = |matched: &WatchPath| {
        if event_path == matched.path {
            state.last_exists = matched.path.exists();
            return Some(requested.path.clone());
        }
        if matched.path.starts_with(event_path) {
            let now_exists = matched.path.exists();
            let previously_existed = std::mem::replace(&mut state.last_exists, now_exists);
            if state.fallback || state.actual.path != matched.path {
                return (now_exists || previously_existed).then(|| requested.path.clone());
            }
            return Some(event_path.to_path_buf());
        }

        let suffix = event_path.strip_prefix(&matched.path).ok()?;
        if !matched.recursive && event_path.parent() != Some(matched.path.as_path()) {
            return None;
        }
        state.last_exists = matched.path.exists();
        Some(requested.path.join(suffix))
    };
    if let Some(path) = changed_in_namespace(matched) {
        return Some(path);
    }
    if matched.path == requested.path {
        return None;
    }
    changed_in_namespace(requested)
}

#[cfg(test)]
#[path = "file_watcher_tests.rs"]
mod tests;
