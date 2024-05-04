pub mod btree;

use crate::data::log_record::LogRecordPos;

/// Indexer 抽象索引接口，可以用不同的数据结构实现该接口
pub trait Indexer {
    /// 在索引中存储 key 对应的数据位置信息
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool;

    /// 根据 key 取出对应的数据位置信息
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;

    /// 根据 key 删除对应的数据位置信息
    fn delete(&self, key: Vec<u8>) -> bool;
}
