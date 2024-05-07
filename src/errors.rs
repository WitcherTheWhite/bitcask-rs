// use std::result;

use thiserror::Error;

#[derive(Error, Debug)]
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
}

// pub type Result<T> = result::Result<T, Errors>;
