use std::collections::BTreeMap;
use std::ops::Bound::Included;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::msg::*;
use crate::service::*;
use crate::*;

// TTL is used for a lock key.
// If the key's lifetime exceeds this value, it should be cleaned up.
// Otherwise, the operation should back off.
const TTL: u64 = Duration::from_millis(100).as_nanos() as u64;

#[derive(Clone, Default)]
pub struct TimestampOracle {
    // You definitions here if needed.
    timestamp_gemerator: Arc<AtomicU64>,
}

#[async_trait::async_trait]
impl timestamp::Service for TimestampOracle {
    // example get_timestamp RPC handler.
    async fn get_timestamp(&self, _: TimestampRequest) -> labrpc::Result<TimestampResponse> {
        // Your code here.
        Ok(TimestampResponse {
            timestamp: self.timestamp_gemerator.fetch_add(1, Ordering::Relaxed),
        })
    }
}

// Key is a tuple (raw key, timestamp).
pub type Key = (Vec<u8>, u64);

#[derive(Clone, PartialEq, Debug)]
pub enum Value {
    Timestamp(u64),
    Vector(Vec<u8>),
}

#[derive(Debug, Clone)]
pub struct Write(Vec<u8>, Vec<u8>);

pub enum Column {
    Write,
    Data,
    Lock,
}

// KvTable is used to simulate Google's Bigtable.
// It provides three columns: Write, Data, and Lock.
#[derive(Clone, Default, Debug)]
pub struct KvTable {
    write: BTreeMap<Key, Value>, // {key, commit_ts} => {start_ts}
    data: BTreeMap<Key, Value>,  // {key, start_ts} => {value}
    lock: BTreeMap<Key, Value>,  // {key, start_ts} => {primary}
}

impl KvTable {
    // Reads the latest key-value record from a specified column
    // in MemoryStorage with a given key and a timestamp range.
    #[inline]
    fn read(
        &self,
        key: Vec<u8>,
        column: Column,
        ts_start_inclusive: Option<u64>,
        ts_end_inclusive: Option<u64>,
    ) -> Option<(&Key, &Value)> {
        // Your code here.
        let ts_start_inclusive = ts_start_inclusive.unwrap_or(0);
        let ts_end_inclusive = ts_end_inclusive.unwrap_or(u64::MAX);
        let map = match column {
            Column::Write => &self.write,
            Column::Data => &self.data,
            Column::Lock => &self.lock,
        };
        map.range((
            Included((key.clone(), ts_start_inclusive)),
            Included((key, ts_end_inclusive)),
        ))
        .last()
    }

    // Writes a record to a specified column in MemoryStorage.
    #[inline]
    fn write(&mut self, key: Vec<u8>, column: Column, ts: u64, value: Value) {
        // Your code here.
        let mut map = match column {
            Column::Write => &mut self.write,
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
        };
        map.insert((key, ts), value);
    }

    #[inline]
    // Erases a record from a specified column in MemoryStorage.
    fn erase(&mut self, key: Vec<u8>, column: Column, commit_ts: u64) {
        // Your code here.
        let mut map = match column {
            Column::Write => &mut self.write,
            Column::Data => &mut self.data,
            Column::Lock => &mut self.lock,
        };
        map.remove(&(key, commit_ts));
    }
}

// MemoryStorage is used to wrap a KvTable.
// You may need to get a snapshot from it.
#[derive(Clone, Default)]
pub struct MemoryStorage {
    data: Arc<Mutex<KvTable>>,
}

#[async_trait::async_trait]
impl transaction::Service for MemoryStorage {
    // example get RPC handler.
    async fn get(&self, req: GetRequest) -> labrpc::Result<GetResponse> {
        // Your code here.
        loop {
            let kvtable = self.data.lock().unwrap();
            // 检查 [0, start_ts] 是否有有锁
            if kvtable
                .read(req.key.to_vec(), Column::Lock, Some(0), Some(req.start_ts))
                .is_some()
            {
                continue;
            }
            // 通过 write 列找到 start_ts 可以看到的 key 的最新提交版本
            match kvtable.read(req.key.to_vec(), Column::Write, None, Some(req.start_ts)) {
                Some(write) => {
                    // 根据版本读取数据
                    if let Value::Timestamp(data_ts) = write.1 {
                        if let Value::Vector(value) = kvtable
                            .read(
                                req.key.to_vec(),
                                Column::Data,
                                Some(*data_ts),
                                Some(data_ts.clone()),
                            )
                            .unwrap()
                            .1
                        {
                            return Ok(GetResponse {
                                value: value.to_vec(),
                            });
                        }
                    }
                }
                None => return Ok(GetResponse { value: vec![] }),
            }
        }
    }

    // example prewrite RPC handler.
    async fn prewrite(&self, req: PrewriteRequest) -> labrpc::Result<PrewriteResponse> {
        // Your code here.
        // 检查 write-write 冲突
        let mut kvtable = self.data.lock().unwrap();
        if kvtable
            .read(req.key.to_vec(), Column::Write, Some(req.start_ts), None)
            .is_some()
        {
            return Ok(PrewriteResponse { success: false });
        }

        // 检查 key 是否被加锁
        if kvtable
            .read(req.key.to_vec(), Column::Lock, None, None)
            .is_some()
        {
            return Ok(PrewriteResponse { success: false });
        }

        // 为 key 加锁
        kvtable.write(
            req.key.to_vec(),
            Column::Lock,
            req.start_ts,
            Value::Vector(req.primary),
        );

        // 写 data 列
        kvtable.write(
            req.key,
            Column::Data,
            req.start_ts,
            Value::Vector(req.value),
        );

        Ok(PrewriteResponse { success: true })
    }

    // example commit RPC handler.
    async fn commit(&self, req: CommitRequest) -> labrpc::Result<CommitResponse> {
        // Your code here.
        let mut kvtable = self.data.lock().unwrap();
        // 检查 lock 的合法性
        match kvtable.read(req.key.to_vec(), Column::Lock, None, None) {
            Some(lock) => {
                let key_and_start_ts = lock.0;
                if key_and_start_ts.1 != req.start_ts {
                    return Ok(CommitResponse { success: false });
                }
                if req.is_primary {
                    if let Value::Vector(primary) = lock.1 {
                        if primary != &req.key {
                            return Ok(CommitResponse { success: false });
                        }
                    }
                }
            }
            None => return Ok(CommitResponse { success: false }),
        }

        // 写 write 列
        kvtable.write(
            req.key.to_vec(),
            Column::Write,
            req.commit_ts,
            Value::Timestamp(req.start_ts),
        );

        // 删除 key 的 lock
        kvtable.erase(req.key, Column::Lock, req.start_ts);

        Ok(CommitResponse { success: true })
    }
}

impl MemoryStorage {
    fn back_off_maybe_clean_up_lock(&self, start_ts: u64, key: Vec<u8>) {
        // Your code here.
        unimplemented!()
    }
}
