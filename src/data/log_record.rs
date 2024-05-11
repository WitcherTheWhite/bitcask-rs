// 数据位置索引信息，描述数据存储的位置
#[derive(Clone, Copy, Debug)]
pub struct LogRecordPos {
    pub(crate) file_id: u32,
    pub(crate) offset: u64,
}

// LogRecord 写入到数据文件的记录
pub struct LogRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
    pub(crate) rec_type: LogRecordType,
}

impl LogRecord {
    pub(crate) fn encode(&self) -> Vec<u8> {
        todo!()
    }
}

pub enum LogRecordType {
    NOAMAL = 1,  // 正常写入的数据
    DELETED = 2, // 删除数据的标记，墓碑值
}

// 读取 LogRecord 的信息，包括数据大小
pub struct ReadLogRecord {
    pub(crate) record: LogRecord,
    pub(crate) size: u64
}