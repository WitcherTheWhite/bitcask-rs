use std::path::PathBuf;

#[derive(Clone)]
pub struct Options {
    pub dir_path: PathBuf,     // 数据库目录
    pub data_file_size: u64,   // 数据文件大小
    pub sync_writes: bool,     // 是否在写入数据后持久化
    pub bytes_per_sync: usize, // 累计字节后持久化
    pub index_type: IndexType, // 索引类型
    pub mmap_at_startup: bool, // 是否使用 mmap 读取数据文件
}

#[derive(Clone, PartialEq)]
pub enum IndexType {
    BTree,
    SkipList,
    BPlusTree,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            dir_path: std::env::temp_dir().join("bitcask"),
            data_file_size: 256 * 1024 * 1024,
            sync_writes: false,
            bytes_per_sync: 0,
            index_type: IndexType::SkipList,
            mmap_at_startup: true,
        }
    }
}

/// 索引迭代器配置项
pub struct IteratorOptions {
    pub prefix: Vec<u8>,
    pub reverse: bool,
}

impl Default for IteratorOptions {
    fn default() -> Self {
        Self {
            prefix: Default::default(),
            reverse: false,
        }
    }
}

/// 批量写数据配置项
pub struct WriteBatchOptions {
    pub max_batch_num: usize, // 一个批次中最大数据量
    pub sync_writes: bool,    // 提交时是否持久化
}

impl Default for WriteBatchOptions {
    fn default() -> Self {
        Self {
            max_batch_num: 1000,
            sync_writes: true,
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum IOType {
    FileIO,
    MMapIO,
}
