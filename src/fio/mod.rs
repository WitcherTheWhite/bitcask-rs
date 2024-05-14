pub mod file_io;

use std::path::PathBuf;

use crate::errors::Errors;

use self::file_io::FileIO;

/// 抽象 IO 管理接口
pub trait IOManager: Sync + Send {
    /// 从 offset 开始读取对应的数据
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize, Errors>;

    /// 写入字节流到文件中
    fn write(&self, buf: &[u8]) -> Result<usize, Errors>;

    /// 持久化数据
    fn sync(&self) -> Result<(), Errors>;
}

/// 根据数据文件路径初始化 IOManager
pub fn new_io_manager(file_path: PathBuf) -> Result<impl IOManager, Errors> {
    FileIO::new(file_path)
}