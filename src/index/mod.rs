pub mod btree;
pub mod skiplist;
pub mod bptree;

use std::path::PathBuf;

use bytes::Bytes;

use crate::{
    data::log_record::LogRecordPos,
    options::{IndexType, IteratorOptions},
};

use self::{bptree::BPlusTree, btree::BTree, skiplist::SkipList};

/// Indexer 抽象索引接口，可以用不同的数据结构实现该接口
pub trait Indexer: Sync + Send {
    /// 在索引中存储 key 对应的数据位置信息
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> Option<LogRecordPos>;

    /// 根据 key 取出对应的数据位置信息
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;

    /// 根据 key 删除对应的数据位置信息
    fn delete(&self, key: Vec<u8>) -> Option<LogRecordPos>;

    /// 返回索引迭代器
    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator>;

    /// 返回索引中所有的 key
    fn list_keys(&self) -> Vec<Bytes>;
}

/// 根据类型打开内存索引
pub fn new_indexer(index_type: IndexType, dir_path: PathBuf) -> Box<dyn Indexer> {
    match index_type {
        IndexType::BTree => Box::new(BTree::new()),
        IndexType::SkipList => Box::new(SkipList::new()),
        IndexType::BPlusTree => Box::new(BPlusTree::new(dir_path))
    }
}

/// 抽象索引迭代器
pub trait IndexIterator: Sync + Send {
    // 回到迭代器起点，即第一条数据
    fn rewind(&mut self);

    // 根据 key 寻找第一个大于（或小于）等于的目标 key，从它开始遍历
    fn seek(&mut self, key: Vec<u8>);

    // 跳转到下一个 key，返回 None 说明迭代完毕
    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)>;
}
