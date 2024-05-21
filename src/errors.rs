// use std::result;

use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum Errors {
    #[error("failed to read from data file")]
    FailedReadFromDataFile,

    #[error("failed to write to data file")]
    FailedWriteToDataFile,

    #[error("failed to sync data file")]
    FailedSyncDataFile,

    #[error("failed to open data file")]
    FailedOpenDataFile,

    #[error("the key is empty")]
    KeyIsEmpty,

    #[error("memory index update failed")]
    FailedIndexUpdate,

    #[error("key is not found")]
    KeyIsNotFound,

    #[error("datafile is not found")]
    DataFileIsNotFound,

    #[error("database directory path can not be empty")]
    DirPathIsEmpty,

    #[error("data file size must be greater than 0")]
    DataFileSizeInvalid,

    #[error("failed to create database directory")]
    FailedCreateDatabaseDir,

    #[error("failed to open database directory")]
    FailedOpenDatabaseDir,

    #[error("database directory is corrupted")]
    DataDirCorrupted,

    #[error("read data file EOF")]
    ReadDataFileEOF,

    #[error("invalid crc value, log record maybe corrupted")]
    InvalidLogRecordCrc,

    #[error("exceed max batch num size")]
    ExceedMaxBatchNum,

    #[error("merge is in processing")]
    MergeInProcess,
}

// pub type Result<T> = result::Result<T, Errors>;
