use std::path::PathBuf;

#[derive(Clone)]
pub struct Options {
    pub dir_path: PathBuf,     // 数据库目录
    pub data_file_size: u64,   // 数据文件大小
    pub sync_writes: bool,     // 是否在写入数据后持久化
    pub index_type: IndexType, // 索引类型
}

#[derive(Clone)]
pub enum IndexType {
    BTree,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            dir_path: std::env::temp_dir().join("bitcask"),
            data_file_size: 256 * 1024 * 1024,
            sync_writes: false,
            index_type: IndexType::BTree,
        }
    }
}
