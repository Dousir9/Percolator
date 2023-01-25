use futures::executor::block_on;
use labrpc::Error;
use labrpc::*;

use crate::msg::{self, CommitRequest, TimestampRequest};
use crate::service::{TSOClient, TransactionClient};
use std::collections::HashMap;
use std::{thread, time};

// BACKOFF_TIME_MS is the wait time before retrying to send the request.
// It should be exponential growth. e.g.
//|  retry time  |  backoff time  |
//|--------------|----------------|
//|      1       |       100      |
//|      2       |       200      |
//|      3       |       400      |
const BACKOFF_TIME_MS: u64 = 100;
// RETRY_TIMES is the maximum number of times a client attempts to send a request.
const RETRY_TIMES: usize = 3;

/// Client mainly has two purposes:
/// One is getting a monotonically increasing timestamp from TSO (Timestamp Oracle).
/// The other is do the transaction logic.
#[derive(Clone)]
pub struct Client {
    // Your definitions here.
    tso_client: TSOClient,
    txn_client: TransactionClient,
    start_ts: u64,
    mem_buffer: HashMap<Vec<u8>, Vec<u8>>,
}

impl Client {
    /// Creates a new Client.
    pub fn new(tso_client: TSOClient, txn_client: TransactionClient) -> Client {
        // Your code here.
        Client {
            tso_client,
            txn_client,
            start_ts: 0,
            mem_buffer: HashMap::new(),
        }
    }

    /// Gets a timestamp from a TSO.
    pub fn get_timestamp(&self) -> Result<u64> {
        // Your code here.
        for i in 0..RETRY_TIMES {
            match block_on(async {
                self.tso_client
                    .get_timestamp(&msg::TimestampRequest {})
                    .await
            }) {
                Ok(response) => return Ok(response.timestamp),
                Err(error) => {
                    warn!("get_timestamp resquest failed because {}", error);
                    match error {
                        Error::Timeout => thread::sleep(time::Duration::from_millis(
                            ((1 << i) * BACKOFF_TIME_MS) as u64,
                        )),
                        other => return Err(other),
                    }
                }
            }
        }
        Err(Error::Timeout)
    }

    /// Begins a new transaction.
    pub fn begin(&mut self) {
        self.start_ts = self
            .get_timestamp()
            .expect("Unexpected error occurs when get_timestamp");
        info!("begin with start_ts {}", self.start_ts);
    }

    /// Gets the value for a given key.
    pub fn get(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        // Your code here.
        let get_request = msg::GetRequest {
            key,
            start_ts: self.start_ts,
        };
        for i in 0..RETRY_TIMES {
            match block_on(async { self.txn_client.get(&get_request).await }) {
                Ok(reponse) => return Ok(reponse.value),
                Err(error) => {
                    warn!("get resquest failed because {}", error);
                    match error {
                        Error::Timeout => thread::sleep(time::Duration::from_millis(
                            ((1 << i) * BACKOFF_TIME_MS) as u64,
                        )),
                        other => return Err(other),
                    }
                }
            }
        }
        Err(Error::Timeout)
    }

    /// Sets keys in a buffer until commit time.
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        // Your code here.
        self.mem_buffer.insert(key, value);
    }

    fn inner_prewrite(
        &self,
        start_ts: u64,
        key: Vec<u8>,
        value: Vec<u8>,
        primary: Vec<u8>,
    ) -> Result<bool> {
        let prewrite_request = msg::PrewriteRequest {
            start_ts,
            key,
            value,
            primary,
        };
        for i in 0..RETRY_TIMES {
            match block_on(async { self.txn_client.prewrite(&prewrite_request).await }) {
                Ok(response) => return Ok(response.success),
                Err(error) => match error {
                    Error::Timeout => thread::sleep(time::Duration::from_millis(
                        ((1 << i) * BACKOFF_TIME_MS) as u64,
                    )),
                    other => return Err(other),
                },
            }
        }
        Err(Error::Timeout)
    }

    fn inner_commit(
        &self,
        start_ts: u64,
        commit_ts: u64,
        key: Vec<u8>,
        is_primary: bool,
    ) -> Result<bool> {
        let commit_request = CommitRequest {
            start_ts,
            commit_ts,
            key,
            is_primary,
        };
        for i in 0..RETRY_TIMES {
            match block_on(async { self.txn_client.commit(&commit_request).await }) {
                Ok(response) => {
                    return Ok(response.success);
                }
                Err(error) => match error {
                    Error::Timeout => thread::sleep(time::Duration::from_millis(
                        ((1 << i) * BACKOFF_TIME_MS) as u64,
                    )),
                    Error::Other(s) => {
                        if s == "reqhook" {
                            return Ok(false);
                        }
                        return Err(Error::Other(s));
                    }
                    other => return Err(other),
                },
            }
        }
        Err(Error::Timeout)
    }

    /// Commits a transaction.
    pub fn commit(&self) -> Result<bool> {
        // Your code here.
        if self.mem_buffer.is_empty() {
            return Ok(true);
        }

        let start_ts = self.start_ts;
        let mutations: Vec<(&Vec<u8>, &Vec<u8>)> = self.mem_buffer.iter().collect();
        let primary = &mutations[0];
        let secondaries = &mutations[1..];

        if !self.inner_prewrite(
            start_ts,
            primary.0.to_vec(),
            primary.1.to_vec(),
            primary.0.to_vec(),
        )? {
            return Ok(false);
        }

        for secondary in secondaries {
            if !self.inner_prewrite(
                start_ts,
                secondary.0.to_vec(),
                secondary.1.to_vec(),
                primary.0.to_vec(),
            )? {
                return Ok(false);
            }
        }

        let commit_ts = self.get_timestamp()?;
        if !self.inner_commit(start_ts, commit_ts, primary.0.to_vec(), true)? {
            return Ok(false);
        }

        // primary commit 成功则事务提交成功，无需检查 secondary 成功与否
        for secondary in secondaries {
            self.inner_commit(start_ts, commit_ts, secondary.0.to_vec(), false)?;
        }

        Ok(true)
    }
}
