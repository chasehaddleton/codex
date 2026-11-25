//! Watch directory membership while resolving a missing target. FSEvents
//! delivers the entire subtree before notify applies its nonrecursive filter.

use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::path::PathBuf;

use block2::RcBlock;
use dispatch2::_dispatch_source_type_vnode;
use dispatch2::DispatchObject;
use dispatch2::DispatchRetained;
use dispatch2::DispatchSource;
use dispatch2::dispatch_source_vnode_flags_t as Flags;
use tokio::sync::mpsc;

use super::BackendEvent;

#[derive(Clone)]
pub(super) enum Location {
    Ancestor(PathBuf),
    Root(PathBuf),
}

pub(super) struct Watch {
    root: Source,
    links: Vec<(PathBuf, Source)>,
}

struct Source {
    source: DispatchRetained<DispatchSource>,
    device: u64,
    inode: u64,
}

impl Watch {
    pub(super) fn new(
        location: Location,
        tx: mpsc::UnboundedSender<BackendEvent>,
    ) -> io::Result<Self> {
        let terminal = Flags::DISPATCH_VNODE_RENAME.0
            | Flags::DISPATCH_VNODE_DELETE.0
            | Flags::DISPATCH_VNODE_REVOKE.0;
        let (path, mask, open_flags) = match &location {
            Location::Ancestor(path) => (
                path,
                terminal | Flags::DISPATCH_VNODE_WRITE.0 | Flags::DISPATCH_VNODE_ATTRIB.0,
                libc::O_DIRECTORY,
            ),
            Location::Root(path) => (path, terminal, 0),
        };
        let open = |entry: &Path, mask, flags| {
            Source::new(entry, mask, flags, &location, &tx).inspect_err(|err| {
                // A component can disappear before its descriptor is armed.
                if matches!(
                    err.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
                ) {
                    let _ = tx.send(BackendEvent::Vanished(path.clone()));
                }
            })
        };
        let root = open(path, mask, open_flags)?;
        let mut links = Vec::new();
        for component in path.ancestors() {
            if component.symlink_metadata().is_ok_and(|m| m.is_symlink()) {
                // Removing a symlink does not change the vnode of its destination.
                let link = open(component, terminal, libc::O_SYMLINK)?;
                links.push((component.to_owned(), link));
            }
        }
        Ok(Self { root, links })
    }

    pub(super) fn still_watches(&self, path: &Path) -> bool {
        self.root.matches(path.metadata())
            && self
                .links
                .iter()
                .all(|(p, source)| source.matches(p.symlink_metadata()))
    }
}

impl Source {
    fn new(
        path: &Path,
        mask: libc::c_ulong,
        open_flags: libc::c_int,
        location: &Location,
        tx: &mpsc::UnboundedSender<BackendEvent>,
    ) -> io::Result<Self> {
        let file = File::options()
            .read(true)
            .custom_flags(libc::O_EVTONLY | libc::O_NONBLOCK | open_flags)
            .open(path)?;
        let metadata = file.metadata()?;
        // SAFETY: the vnode type and mask describe an open file or directory. The
        // cancellation handler retains its descriptor until dispatch is done with it.
        let source = unsafe {
            DispatchSource::new(
                (&raw const _dispatch_source_type_vnode).cast_mut(),
                file.as_raw_fd() as usize,
                mask as usize,
                None,
            )
        };
        let location = location.clone();
        let tx = tx.clone();
        let changed = RcBlock::new(move || {
            let _ = tx.send(BackendEvent::VnodeChanged(location.clone()));
        });
        let keep_open = RcBlock::new(move || {
            let _ = file.as_raw_fd();
        });
        // SAFETY: dispatch copies these blocks. Both own their captures and may
        // run on the global queue. The initial recheck closes the race between
        // looking up a missing path and arming its ancestor watch.
        unsafe {
            source.set_event_handler_with_block(RcBlock::as_ptr(&changed));
            source.set_registration_handler_with_block(RcBlock::as_ptr(&changed));
            source.set_cancel_handler_with_block(RcBlock::as_ptr(&keep_open));
        }
        source.activate();
        Ok(Self {
            source,
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }

    fn matches(&self, metadata: io::Result<std::fs::Metadata>) -> bool {
        metadata.is_ok_and(|m| m.dev() == self.device && m.ino() == self.inode)
    }
}

impl Drop for Source {
    fn drop(&mut self) {
        self.source.cancel();
    }
}
