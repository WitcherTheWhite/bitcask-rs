use std::path::PathBuf;

pub struct Options {
    pub dir_path: PathBuf,   // 数据库目录
    pub data_file_size: u64, // 数据文件大小
    pub sync_writes: bool,   // 是否在写入数据后持久化
}
