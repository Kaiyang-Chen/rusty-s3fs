use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::ffi::OsStr;
use fuser;
use std::{io, fs};
use std::path::{Path, PathBuf};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, Write, SeekFrom};
use fuser::{
    Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry,
    Request, KernelConfig, FUSE_ROOT_ID, ReplyOpen, ReplyWrite, ReplyCreate, ReplyEmpty
};
use fuser::consts::FOPEN_DIRECT_IO;
use std::sync::atomic::{AtomicU64, Ordering};
use std::os::raw::c_int;
use std::collections::BTreeMap;
use std::os::unix::ffi::OsStrExt;
use log::debug;
use std::cmp::min;
use std::os::unix::fs::FileExt;
use crate::s3util::S3Worker;
use tokio::runtime::Runtime;
use time::OffsetDateTime;
use async_recursion::async_recursion;


const BLOCK_SIZE: u64 = 512;
const MAX_NAME_LENGTH: u32 = 255;
const FMODE_EXEC: i32 = 0x20;
// Top two file handle bits are used to store permissions
// Note: This isn't safe, since the client can modify those bits.
const FILE_HANDLE_READ_BIT: u64 = 1 << 63;
const FILE_HANDLE_WRITE_BIT: u64 = 1 << 62;
type Inode = u64;
type DirectoryDescriptor = BTreeMap<Vec<u8>, (Inode, FileKind)>;

#[derive(Serialize, Deserialize, Copy, Clone, PartialEq)]
enum FileKind {
    File,
    Directory,
}

impl From<FileKind> for fuser::FileType {
    fn from(kind: FileKind) -> Self {
        match kind {
            FileKind::File => fuser::FileType::RegularFile,
            FileKind::Directory => fuser::FileType::Directory,
        }
    }
}

fn system_time_from_time(secs: i64, nsecs: u32) -> SystemTime {
    if secs >= 0 {
        UNIX_EPOCH + Duration::new(secs as u64, nsecs)
    } else {
        UNIX_EPOCH - Duration::new((-secs) as u64, nsecs)
    }
}

fn time_now() -> (i64, u32) {
    time_from_system_time(&SystemTime::now())
}

fn time_from_system_time(system_time: &SystemTime) -> (i64, u32) {
    // Convert to signed 64-bit time with epoch at 0
    match system_time.duration_since(UNIX_EPOCH) {
        Ok(duration) => (duration.as_secs() as i64, duration.subsec_nanos()),
        Err(before_epoch_error) => (
            -(before_epoch_error.duration().as_secs() as i64),
            before_epoch_error.duration().subsec_nanos(),
        ),
    }
}

#[derive(Serialize, Deserialize)]
struct InodeAttributes {
    pub inode: Inode,
    pub open_file_handles: u64, // Ref count of open file handles to this inode
    pub size: u64,
    pub last_accessed: (i64, u32),
    pub last_modified: (i64, u32),
    pub last_metadata_changed: (i64, u32),
    pub kind: FileKind,
    // Permissions and special mode bits
    pub mode: u16,
    pub hardlinks: u32,
    pub uid: u32,
    pub gid: u32,
    pub md5: String
}

impl From<InodeAttributes> for fuser::FileAttr {
    fn from(attrs: InodeAttributes) -> Self {
        fuser::FileAttr {
            ino: attrs.inode,
            size: attrs.size,
            blocks: (attrs.size + BLOCK_SIZE - 1) / BLOCK_SIZE,
            atime: system_time_from_time(attrs.last_accessed.0, attrs.last_accessed.1),
            mtime: system_time_from_time(attrs.last_modified.0, attrs.last_modified.1),
            ctime: system_time_from_time(
                attrs.last_metadata_changed.0,
                attrs.last_metadata_changed.1,
            ),
            crtime: SystemTime::UNIX_EPOCH,
            kind: attrs.kind.into(),
            perm: attrs.mode,
            nlink: attrs.hardlinks,
            uid: attrs.uid,
            gid: attrs.gid,
            rdev: 0,
            blksize: BLOCK_SIZE as u32,
            flags: 0,
        }
    }
}

// Stores inode metadata data in "$data_dir/inodes" and file contents in "$data_dir/contents"
// Directory data is stored in the file's contents, as a serialized DirectoryDescriptor
pub(crate) struct S3FS {
    data_dir: String,
    next_file_handle: AtomicU64,
    direct_io: bool,
    worker: S3Worker,
}

impl S3FS  {
    pub fn new(
        data_dir: String,
        direct_io: bool,
        worker: S3Worker,
    ) -> S3FS {
        S3FS {
            data_dir,
            next_file_handle: AtomicU64::new(1),
            direct_io,
            worker,
        }
    }

    pub fn fuse_allow_other_enabled() -> io::Result<bool> {
        let file = File::open("/etc/fuse.conf")?;
        for line in BufReader::new(file).lines() {
            if line?.trim_start().starts_with("user_allow_other") {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn allocate_next_file_handle(&self, read: bool, write: bool) -> u64 {
        let mut fh = self.next_file_handle.fetch_add(1, Ordering::SeqCst);
        // Assert that we haven't run out of file handles
        assert!(fh < FILE_HANDLE_WRITE_BIT && fh < FILE_HANDLE_READ_BIT);
        if read {
            fh |= FILE_HANDLE_READ_BIT;
        }
        if write {
            fh |= FILE_HANDLE_WRITE_BIT;
        }

        fh
    }

    fn allocate_next_inode(&self) -> Inode {
        let path = Path::new(&self.data_dir).join("superblock");
        let current_inode = if let Ok(file) = File::open(&path) {
            bincode::deserialize_from(file).unwrap()
        } else {
            fuser::FUSE_ROOT_ID
        };

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        bincode::serialize_into(file, &(current_inode + 1)).unwrap();

        current_inode + 1
    }

    fn get_inode(&self, inode: Inode) -> Result<InodeAttributes, c_int> {
        let path = Path::new(&self.data_dir)
            .join("inodes")
            .join(inode.to_string());
        if let Ok(file) = File::open(&path) {
            Ok(bincode::deserialize_from(file).unwrap())
        } else {
            Err(libc::ENOENT)
        }
    }

    fn write_inode(&self, inode: &InodeAttributes) {
        let path = Path::new(&self.data_dir)
            .join("inodes")
            .join(inode.inode.to_string());
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        bincode::serialize_into(file, inode).unwrap();
    }

    fn get_directory_content(&self, inode: Inode) -> Result<DirectoryDescriptor, c_int> {
        let path = Path::new(&self.data_dir)
            .join("contents")
            .join(inode.to_string());
        if let Ok(file) = File::open(&path) {
            Ok(bincode::deserialize_from(file).unwrap())
        } else {
            Err(libc::ENOENT)
        }
    }

    fn write_directory_content(&self, inode: Inode, entries: DirectoryDescriptor) {
        let path = Path::new(&self.data_dir)
            .join("contents")
            .join(inode.to_string());
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        bincode::serialize_into(file, &entries).unwrap();
    }

    fn  lookup_name(&self, parent: u64, name: &OsStr) -> Result<InodeAttributes, c_int> {
        let entries = self.get_directory_content(parent)?;
        if let Some((inode, _)) = entries.get(name.as_bytes()) {
            // TODO: check metadata of the file, if not consistent, update, otherwise, return
            return self.get_inode(*inode);
        } else {
            // let exists = Runtime::new().unwrap().block_on(self.worker.is_exist(name.to_str().unwrap())).unwrap();
            // if exists {
            //     let inode = self.allocate_next_inode();
            //     let metadata = Runtime::new().unwrap().block_on(self.worker.get_stats(name.to_str().unwrap())).unwrap();
            //     let parent_attrs = self.get_inode(parent).unwrap();
            //     let mut attrs = InodeAttributes {
            //         inode,
            //         open_file_handles: 1,
            //         size: 0,
            //         last_accessed: time_now(),
            //         last_modified: time_from_offsetdatatime(metadata.last_modified()),
            //         last_metadata_changed: time_now(),
            //         kind: FileKind::File,
            //         mode: 0o777,
            //         hardlinks: 1,
            //         uid: parent_attrs.uid,
            //         gid: parent_attrs.gid,
            //         // use dummy metadata here
            //         // md5: metadata.content_md5().unwrap().to_string(),
            //         md5: "".to_string(),
            //     };
            //     let path = self.content_path(inode);
            //     File::create(&path).unwrap();
            //     let data = Runtime::new().unwrap().block_on(self.worker.get_data(name.to_str().unwrap())).unwrap();
            //     if let Ok(mut file) = OpenOptions::new().write(true).open(&path) {
            //         file.seek(SeekFrom::Start(0 as u64)).unwrap();
            //         file.write_all(&data).unwrap();

            //         attrs.last_metadata_changed = time_now();
            //         attrs.last_modified = time_now();
            //         if data.len() as usize > attrs.size as usize {
            //             attrs.size = (data.len() as usize) as u64;
            //         }
            //         clear_suid_sgid(&mut attrs);
            //         self.write_inode(&attrs);

            //         let mut entries = self.get_directory_content(parent).unwrap();
            //         entries.insert(name.as_bytes().to_vec(), (inode, attrs.kind));
            //         self.write_directory_content(parent, entries);
            //     } 
                

            //     return self.get_inode(inode);
            // } else {
            //     return Err(libc::ENOENT);
            // }  
            return Err(libc::ENOENT);
        }
    }

    fn check_file_handle_read(&self, file_handle: u64) -> bool {
        (file_handle & FILE_HANDLE_READ_BIT) != 0
    }

    fn check_file_handle_write(&self, file_handle: u64) -> bool {
        (file_handle & FILE_HANDLE_WRITE_BIT) != 0
    }

    fn content_path(&self, inode: Inode) -> PathBuf {
        Path::new(&self.data_dir)
            .join("contents")
            .join(inode.to_string())
    }

    //  TODO: The function is only a toy at the moment that it does not support finding files in directories other than root. If want to find any file path with the inode, one might need to find a way to get its parent dir's inode for any path.
    fn get_filename_from_inode(&self, inode: Inode) -> String {
        let entries = self.get_directory_content(FUSE_ROOT_ID).unwrap();
        let filename = get_key_by_value(&entries, &(inode, FileKind::File)).unwrap();
        let s = String::from_utf8_lossy(filename);
        s.to_string()
    }

    fn creation_mode(&self, mode: u32) -> u16 {
        (mode & !(libc::S_ISUID | libc::S_ISGID) as u32) as u16
    }

    #[async_recursion]
    async fn init_directories(&self, path: &str, parent: Inode)  -> Result<(), Box<dyn std::error::Error>>{
        let entries = self.worker.list_dir(path).await?;
        let mut parent_attrs =self.get_inode(parent).unwrap();
        for file in entries {
            let full_path = format!("{}{}", path, file);
            let is_file = self.worker.is_file(&full_path).await?;

            if is_file {
                // let metadata =  self.worker.get_stats(full_path.as_str()).await?;
                let file_inode = self.allocate_next_inode();
                let attrs = InodeAttributes {
                    inode: file_inode,
                    open_file_handles: 1,
                    size: 0,
                    last_accessed: time_now(),
                    // last_modified: time_from_offsetdatatime(metadata.last_modified()),
                    last_modified: time_now(),
                    last_metadata_changed: time_now(),
                    kind: FileKind::File,
                    mode: 0x777,
                    hardlinks: 1,
                    uid: parent_attrs.uid,
                    gid: parent_attrs.gid,
                    md5: "".to_string()
                };
                self.write_inode(&attrs);
                let mut entries = self.get_directory_content(parent).unwrap();
                entries.insert(file.as_bytes().to_vec(), (file_inode, attrs.kind));
                self.write_directory_content(parent, entries);
                
                parent_attrs.last_modified = time_now();
                parent_attrs.last_metadata_changed = time_now();
                self.write_inode(&parent_attrs);
            } else {
                let dir_path = format!("{}/", full_path);
                let dir_inode = self.allocate_next_inode();
                let attrs = InodeAttributes {
                    inode: dir_inode,
                    open_file_handles: 1,
                    size: 0,
                    last_accessed: time_now(),
                    last_modified: time_now(),
                    last_metadata_changed: time_now(),
                    kind: FileKind::Directory,
                    mode: 0x777,
                    hardlinks: 1,
                    uid: parent_attrs.uid,
                    gid: parent_attrs.gid,
                    md5: "".to_string()
                };
                self.write_inode(&attrs);
                let mut entries = BTreeMap::new();
                entries.insert(b"..".to_vec(), (parent, FileKind::Directory));
                entries.insert(b".".to_vec(), (dir_inode, FileKind::Directory));
                self.write_directory_content(dir_inode, entries);

                let mut parent_entries = self.get_directory_content(parent).unwrap();
                let name = list_directory(dir_path.as_str()).unwrap();
                parent_entries.insert(name.as_bytes().to_vec(), (dir_inode, FileKind::Directory));
                self.write_directory_content(parent, parent_entries);
                parent_attrs.last_modified = time_now();
                parent_attrs.last_metadata_changed = time_now();
                self.write_inode(&parent_attrs);
                self.init_directories(&dir_path, dir_inode).await?;
            }
        }
        Ok(())
    }

    // Check whether a file should be removed from storage. Should be called after decrementing
    // the link count, or closing a file handle
    fn gc_inode(&self, inode: &InodeAttributes) -> bool {
        if inode.hardlinks == 0 && inode.open_file_handles == 0 {
            let inode_path = Path::new(&self.data_dir)
                .join("inodes")
                .join(inode.inode.to_string());
            fs::remove_file(inode_path).unwrap();
            let content_path = Path::new(&self.data_dir)
                .join("contents")
                .join(inode.inode.to_string());
            fs::remove_file(content_path).unwrap();

            return true;
        }

        return false;
    }

}


impl Filesystem for S3FS {
    // Initialize filesystem. Called before any other filesystem method. 
    // The kernel module connection can be configured using the KernelConfig object.
    fn init(
        &mut self,
        _req: &Request,
        #[allow(unused_variables)] config: &mut KernelConfig,
    ) -> Result<(), c_int> {
        fs::create_dir_all(Path::new(&self.data_dir).join("inodes")).unwrap();
        fs::create_dir_all(Path::new(&self.data_dir).join("contents")).unwrap();
        if self.get_inode(FUSE_ROOT_ID).is_err() {
            // Initialize with empty filesystem
            let root = InodeAttributes {
                inode: FUSE_ROOT_ID,
                open_file_handles: 0,
                size: 0,
                last_accessed: time_now(),
                last_modified: time_now(),
                last_metadata_changed: time_now(),
                kind: FileKind::Directory,
                mode: 0o777,
                hardlinks: 2,
                uid: 0,
                gid: 0,
                md5: "".to_string(),
            };
            self.write_inode(&root);
            let mut entries = BTreeMap::new();
            entries.insert(b".".to_vec(), (FUSE_ROOT_ID, FileKind::Directory));
            self.write_directory_content(FUSE_ROOT_ID, entries);
            let rt = Runtime::new().unwrap();
            rt.block_on(self.init_directories("", FUSE_ROOT_ID)).unwrap();
            
        }
        Ok(())
    }

    // Look up a directory entry by name and get its attributes.
    fn lookup(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        if name.len() > MAX_NAME_LENGTH as usize {
            reply.error(libc::ENAMETOOLONG);
            return;
        }
        let parent_attrs = self.get_inode(parent).unwrap();
        if !check_access(
            parent_attrs.uid,
            parent_attrs.gid,
            parent_attrs.mode,
            req.uid(),
            req.gid(),
            libc::X_OK,
        ) {
            reply.error(libc::EACCES);
            return;
        }

        match self.lookup_name(parent, name) {
            Ok(attrs) => reply.entry(&Duration::new(0, 0), &attrs.into(), 0),
            Err(error_code) => reply.error(error_code),
        }
    }

    // Get file attributes.
    fn getattr(&mut self, _req: &Request, inode: u64, reply: ReplyAttr) {
        match self.get_inode(inode) {
            Ok(attrs) => reply.attr(&Duration::new(0, 0), &attrs.into()),
            Err(error_code) => reply.error(error_code),
        }
    }

    // Open a file. Open flags (with the exception of O_CREAT, O_EXCL, O_NOCTTY and O_TRUNC) are available in flags. 
    // Filesystem may store an arbitrary file handle (pointer, index, etc) in fh, and use this in other all other file operations (read, write, flush, release, fsync).
    fn open(&mut self, req: &Request, inode: u64, flags: i32, reply: ReplyOpen) {
        debug!("open() called for {:?}", inode);
        let (access_mask, read, write) = match flags & libc::O_ACCMODE {
            libc::O_RDONLY => {
                // Behavior is undefined, but most filesystems return EACCES
                if flags & libc::O_TRUNC != 0 {
                    reply.error(libc::EACCES);
                    return;
                }
                if flags & FMODE_EXEC != 0 {
                    // Open is from internal exec syscall
                    (libc::X_OK, true, false)
                } else {
                    (libc::R_OK, true, false)
                }
            }
            libc::O_WRONLY => (libc::W_OK, false, true),
            libc::O_RDWR => (libc::R_OK | libc::W_OK, true, true),
            // Exactly one access mode flag must be specified
            _ => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        match self.get_inode(inode) {
            Ok(mut attr) => {
                // check whether the file is newest version, if not, write the newest version to local cache. initial md5 is set to empty string, so when open the file for the first time, it will load the file from the cloud.
                let rt = Runtime::new().unwrap();
                let filename = self.get_filename_from_inode(inode);
                let metadata = rt.block_on(self.worker.get_stats(&filename)).unwrap();
                // if metadata.content_md5().unwrap().to_string() != attr.md5 {
                if time_from_offsetdatatime(metadata.last_modified()) != attr.last_modified {
                    let path = self.content_path(inode);
                    File::create(&path).unwrap();
                    let data = rt.block_on(self.worker.get_data(&filename)).unwrap();
                    if let Ok(mut file) = OpenOptions::new().write(true).open(&path) {
                        file.seek(SeekFrom::Start(0 as u64)).unwrap();
                        file.write_all(&data).unwrap();
                        attr.last_metadata_changed = time_now();
                        attr.last_modified = time_from_offsetdatatime(metadata.last_modified());
                        if data.len() as usize > attr.size as usize {
                            attr.size = (data.len() as usize) as u64;
                        }
                        clear_suid_sgid(&mut attr);
                        self.write_inode(&attr);
                        // TODO: some flaws here, the last change time for parent dir is unmodified, but since the monified_time entry for dir is unused in our case, leave for future to solve.
                    } 
                }
                if check_access(
                    attr.uid,
                    attr.gid,
                    attr.mode,
                    req.uid(),
                    req.gid(),
                    access_mask,
                ) {
                    attr.open_file_handles += 1;
                    self.write_inode(&attr);
                    let open_flags = if self.direct_io { FOPEN_DIRECT_IO } else { 0 };
                    reply.opened(self.allocate_next_file_handle(read, write), open_flags);
                } else {
                    reply.error(libc::EACCES);
                }
                return;
            }
            Err(error_code) => reply.error(error_code),
        }
    }

    // Read data. Read should send exactly the number of bytes requested except on EOF or error, otherwise the rest of the data will be substituted with zeroes. 
    // An exception to this is when the file has been opened in ‘direct_io’ mode, in which case the return value of the read system call will reflect the return value of this operation. 
    // fh will contain the value set by the open method, or will be undefined if the open method didn’t set any value. 
    // flags: these are the file flags, such as O_SYNC.
    fn read(
        &mut self,
        _req: &Request,
        inode: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        debug!(
            "read() called on {:?} offset={:?} size={:?}",
            inode, offset, size
        );
        assert!(offset >= 0);
        if !self.check_file_handle_read(fh) {
            reply.error(libc::EACCES);
            return;
        }

        let path = self.content_path(inode);
        if let Ok(file) = File::open(&path) {
            let file_size = file.metadata().unwrap().len();
            // Could underflow if file length is less than local_start
            let read_size = min(size, file_size.saturating_sub(offset as u64) as u32);

            let mut buffer = vec![0; read_size as usize];
            file.read_exact_at(&mut buffer, offset as u64).unwrap();
            reply.data(&buffer);
        } else {
            reply.error(libc::ENOENT);
        }
    }

    fn write(
        &mut self,
        _req: &Request,
        inode: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        #[allow(unused_variables)] flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        debug!("write() called with {:?} size={:?}", inode, data.len());
        assert!(offset >= 0);
        if !self.check_file_handle_write(fh) {
            reply.error(libc::EACCES);
            return;
        }

        let path = self.content_path(inode);
        if let Ok(mut file) = OpenOptions::new().write(true).open(&path) {
            file.seek(SeekFrom::Start(offset as u64)).unwrap();
            file.write_all(data).unwrap();

            let mut attrs = self.get_inode(inode).unwrap();
            attrs.last_metadata_changed = time_now();
            attrs.last_modified = time_now();
            if data.len() + offset as usize > attrs.size as usize {
                attrs.size = (data.len() + offset as usize) as u64;
            }
            clear_suid_sgid(&mut attrs);
            self.write_inode(&attrs);

            reply.written(data.len() as u32);
        } else {
            reply.error(libc::EBADF);
        }
    }

    fn create(
        &mut self,
        req: &Request,
        parent: u64,
        name: &OsStr,
        mut mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        debug!("create() called with {:?} {:?}", parent, name);
        if self.lookup_name(parent, name).is_ok() {
            reply.error(libc::EEXIST);
            return;
        }

        let (read, write) = match flags & libc::O_ACCMODE {
            libc::O_RDONLY => (true, false),
            libc::O_WRONLY => (false, true),
            libc::O_RDWR => (true, true),
            // Exactly one access mode flag must be specified
            _ => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        let mut parent_attrs = match self.get_inode(parent) {
            Ok(attrs) => attrs,
            Err(error_code) => {
                reply.error(error_code);
                return;
            }
        };

        if !check_access(
            parent_attrs.uid,
            parent_attrs.gid,
            parent_attrs.mode,
            req.uid(),
            req.gid(),
            libc::W_OK,
        ) {
            reply.error(libc::EACCES);
            return;
        }
        parent_attrs.last_modified = time_now();
        parent_attrs.last_metadata_changed = time_now();
        self.write_inode(&parent_attrs);

        if req.uid() != 0 {
            mode &= !(libc::S_ISUID | libc::S_ISGID) as u32;
        }

        let inode = self.allocate_next_inode();
        let attrs = InodeAttributes {
            inode,
            open_file_handles: 1,
            size: 0,
            last_accessed: time_now(),
            last_modified: time_now(),
            last_metadata_changed: time_now(),
            kind: as_file_kind(mode),
            mode: self.creation_mode(mode),
            hardlinks: 1,
            uid: req.uid(),
            gid: creation_gid(&parent_attrs, req.gid()),
            // a dummy md5, will update after writting content to it
            md5: "".to_string()
        };
        self.write_inode(&attrs);
        File::create(self.content_path(inode)).unwrap();

        if as_file_kind(mode) == FileKind::Directory {
            let mut entries = BTreeMap::new();
            entries.insert(b".".to_vec(), (inode, FileKind::Directory));
            entries.insert(b"..".to_vec(), (parent, FileKind::Directory));
            self.write_directory_content(inode, entries);
        }

        let mut entries = self.get_directory_content(parent).unwrap();
        entries.insert(name.as_bytes().to_vec(), (inode, attrs.kind));
        self.write_directory_content(parent, entries);

        reply.created(
            &Duration::new(0, 0),
            &attrs.into(),
            0,
            self.allocate_next_file_handle(read, write),
            0,
        );
    }


    // Open a directory. Filesystem may store an arbitrary file handle (pointer, index, etc) in fh, and use this in other all other directory stream operations (readdir, releasedir, fsyncdir). 
    fn opendir(&mut self, req: &Request, inode: u64, flags: i32, reply: ReplyOpen) {
        debug!("opendir() called on {:?}", inode);
        let (access_mask, read, write) = match flags & libc::O_ACCMODE {
            libc::O_RDONLY => {
                // Behavior is undefined, but most filesystems return EACCES
                if flags & libc::O_TRUNC != 0 {
                    reply.error(libc::EACCES);
                    return;
                }
                (libc::R_OK, true, false)
            }
            libc::O_WRONLY => (libc::W_OK, false, true),
            libc::O_RDWR => (libc::R_OK | libc::W_OK, true, true),
            // Exactly one access mode flag must be specified
            _ => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        match self.get_inode(inode) {
            Ok(mut attr) => {
                if check_access(
                    attr.uid,
                    attr.gid,
                    attr.mode,
                    req.uid(),
                    req.gid(),
                    access_mask,
                ) {
                    attr.open_file_handles += 1;
                    self.write_inode(&attr);
                    let open_flags = if self.direct_io { FOPEN_DIRECT_IO } else { 0 };
                    reply.opened(self.allocate_next_file_handle(read, write), open_flags);
                } else {
                    reply.error(libc::EACCES);
                }
                return;
            }
            Err(error_code) => reply.error(error_code),
        }
    }

    // Read directory. Send a buffer filled using buffer.fill(), with size not exceeding the requested size. 
    // Send an empty buffer on end of stream. fh will contain the value set by the opendir method, or will be undefined if the opendir method didn’t set any value.
    fn readdir(
        &mut self,
        _req: &Request,
        inode: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        debug!("readdir() called with {:?}", inode);
        assert!(offset >= 0);
        let entries = match self.get_directory_content(inode) {
            Ok(entries) => entries,
            Err(error_code) => {
                reply.error(error_code);
                return;
            }
        };

        for (index, entry) in entries.iter().skip(offset as usize).enumerate() {
            let (name, (inode, file_type)) = entry;

            let buffer_full: bool = reply.add(
                *inode,
                offset + index as i64 + 1,
                (*file_type).into(),
                OsStr::from_bytes(name),
            );

            if buffer_full {
                break;
            }
        }

        reply.ok();
    }


    fn unlink(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        debug!("unlink() called with {:?} {:?}", parent, name);
        let mut attrs = match self.lookup_name(parent, name) {
            Ok(attrs) => attrs,
            Err(error_code) => {
                reply.error(error_code);
                return;
            }
        };

        let mut parent_attrs = match self.get_inode(parent) {
            Ok(attrs) => attrs,
            Err(error_code) => {
                reply.error(error_code);
                return;
            }
        };

        if !check_access(
            parent_attrs.uid,
            parent_attrs.gid,
            parent_attrs.mode,
            req.uid(),
            req.gid(),
            libc::W_OK,
        ) {
            reply.error(libc::EACCES);
            return;
        }

        let uid = req.uid();
        // "Sticky bit" handling
        if parent_attrs.mode & libc::S_ISVTX as u16 != 0
            && uid != 0
            && uid != parent_attrs.uid
            && uid != attrs.uid
        {
            reply.error(libc::EACCES);
            return;
        }

        parent_attrs.last_metadata_changed = time_now();
        parent_attrs.last_modified = time_now();
        self.write_inode(&parent_attrs);

        attrs.hardlinks -= 1;
        attrs.last_metadata_changed = time_now();
        self.write_inode(&attrs);
        self.gc_inode(&attrs);

        let mut entries = self.get_directory_content(parent).unwrap();
        entries.remove(name.as_bytes());
        self.write_directory_content(parent, entries);

        reply.ok();
    }



}

fn clear_suid_sgid(attr: &mut InodeAttributes) {
    attr.mode &= !libc::S_ISUID as u16;
    // SGID is only suppose to be cleared if XGRP is set
    if attr.mode & libc::S_IXGRP as u16 != 0 {
        attr.mode &= !libc::S_ISGID as u16;
    }
}

pub fn check_access(
    file_uid: u32,
    file_gid: u32,
    file_mode: u16,
    uid: u32,
    gid: u32,
    mut access_mask: i32,
) -> bool {
    // F_OK tests for existence of file
    if access_mask == libc::F_OK {
        return true;
    }
    let file_mode = i32::from(file_mode);

    // root is allowed to read & write anything
    if uid == 0 {
        // root only allowed to exec if one of the X bits is set
        access_mask &= libc::X_OK;
        access_mask -= access_mask & (file_mode >> 6);
        access_mask -= access_mask & (file_mode >> 3);
        access_mask -= access_mask & file_mode;
        return access_mask == 0;
    }

    if uid == file_uid {
        access_mask -= access_mask & (file_mode >> 6);
    } else if gid == file_gid {
        access_mask -= access_mask & (file_mode >> 3);
    } else {
        access_mask -= access_mask & file_mode;
    }

    return access_mask == 0;
}


fn as_file_kind(mut mode: u32) -> FileKind {
    mode &= libc::S_IFMT as u32;

    if mode == libc::S_IFREG as u32 {
        return FileKind::File;
    } else if mode == libc::S_IFDIR as u32 {
        return FileKind::Directory;
    } else {
        unimplemented!("{}", mode);
    }
}

fn creation_gid(parent: &InodeAttributes, gid: u32) -> u32 {
    if parent.mode & libc::S_ISGID as u16 != 0 {
        return parent.gid;
    }

    gid
}

fn time_from_offsetdatatime(dt: Option<OffsetDateTime>) -> (i64, u32) {
    dt.map(|dt| {
        let timestamp_secs = dt.unix_timestamp();
        let timestamp_millis = dt.millisecond();
        (timestamp_secs, timestamp_millis as u32)
    }).unwrap()
}

fn list_directory(path: &str) -> Option<&str> {
    let mut directories = path.split('/').filter(|s| !s.is_empty());
    let last_directory = directories.next_back();
    last_directory
}

fn get_key_by_value<'a, K, V>(map: &'a BTreeMap<K, V>, value: &V) -> Option<&'a K>
where
    K: Ord,
    V: PartialEq,
{
    map.iter().find(|(_, v)| v == &value).map(|(k, _)| k)
}