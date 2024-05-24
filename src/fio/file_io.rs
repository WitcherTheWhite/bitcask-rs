use std::{
    fs::{File, OpenOptions},
    io::Write,
    os::unix::prelude::FileExt,
    path::PathBuf,
    sync::Arc,
};

use log::error;
use parking_lot::RwLock;

use crate::errors::Errors;

use super::IOManager;

// FileIO 标准系统文件 IO
pub struct FileIO {
    fd: Arc<RwLock<File>>, // 系统文件描述符
}

impl FileIO {
    pub fn new(file_path: PathBuf) -> Result<Self, Errors> {
        match OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .append(true)
            .open(file_path)
        {
            Ok(file) => Ok(Self {
                fd: Arc::new(RwLock::new(file)),
            }),
            Err(err) => {
                error!("open data file error: {}", err);
                Err(Errors::FailedOpenDataFile)
            }
        }
    }
}

impl IOManager for FileIO {
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize, Errors> {
        let read_guard = self.fd.read();
        match read_guard.read_at(buf, offset) {
            Ok(n) => Ok(n),
            Err(err) => {
                error!("read from data file error: {}", err);
                Err(Errors::FailedReadFromDataFile)
            }
        }
    }

    fn write(&self, buf: &[u8]) -> Result<usize, Errors> {
        let mut write_guard = self.fd.write();
        match write_guard.write(buf) {
            Ok(n) => Ok(n),
            Err(err) => {
                error!("write to data file error: {}", err);
                Err(Errors::FailedWriteToDataFile)
            }
        }
    }

    fn sync(&self) -> Result<(), Errors> {
        let read_guard = self.fd.read();
        match read_guard.sync_all() {
            Ok(_) => Ok(()),
            Err(err) => {
                error!("failed to sync data file: {}", err);
                Err(Errors::FailedSyncDataFile)
            }
        }
    }

    fn size(&self) -> u64 {
        let read_guard = self.fd.read();
        let metadata = read_guard.metadata().unwrap();
        metadata.len()
    }
}

#[cfg(test)]
mod tests {

    use std::fs::remove_file;

    use super::*;

    #[test]
    fn test_file_io_write() {
        let path = PathBuf::from("/tmp/a.data");
        let fio_res = FileIO::new(path.clone());
        assert!(fio_res.is_ok());
        let fio = fio_res.unwrap();

        let res1 = fio.write("key-a".as_bytes());
        assert!(res1.is_ok());
        assert_eq!(res1.unwrap(), 5);

        let res2 = fio.write("hsy".as_bytes());
        assert!(res2.is_ok());
        assert_eq!(res2.unwrap(), 3);

        let remove_res = remove_file(path);
        assert!(remove_res.is_ok());
    }

    #[test]
    fn test_file_io_read() {
        let path = PathBuf::from("/tmp/b.data");
        let fio_res = FileIO::new(path.clone());
        assert!(fio_res.is_ok());
        let fio = fio_res.unwrap();

        let res1 = fio.write("key-a".as_bytes());
        assert!(res1.is_ok());
        assert_eq!(res1.unwrap(), 5);

        let res2 = fio.write("hsy".as_bytes());
        assert!(res2.is_ok());
        assert_eq!(res2.unwrap(), 3);

        let mut buf = [0u8; 5];
        let read_res1 = fio.read(&mut buf, 0);
        assert!(read_res1.is_ok());
        assert_eq!(read_res1.unwrap(), 5);

        let read_res2 = fio.read(&mut buf, 5);
        assert!(read_res2.is_ok());
        assert_eq!(read_res2.unwrap(), 3);

        let remove_res = remove_file(path);
        assert!(remove_res.is_ok());
    }

    #[test]
    fn test_file_io_sync() {
        let path = PathBuf::from("/tmp/c.data");
        let fio_res = FileIO::new(path.clone());
        assert!(fio_res.is_ok());
        let fio = fio_res.unwrap();

        let res1 = fio.write("key-a".as_bytes());
        assert!(res1.is_ok());
        assert_eq!(res1.unwrap(), 5);

        let res2 = fio.write("key-b".as_bytes());
        assert!(res2.is_ok());
        assert_eq!(res2.unwrap(), 5);

        let sync_res = fio.sync();
        assert!(sync_res.is_ok());

        let remove_res = remove_file(path);
        assert!(remove_res.is_ok());
    }
}
