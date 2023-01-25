use std::collections::BTreeMap;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use std::fmt::Display;

use itertools::Itertools;

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

// 将 match 逻辑搬到这里
impl Value {
    fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Vector(bytes) => bytes,
            _ => panic!("expect vector"),
        }
    }

    fn as_ts(&self) -> u64 {
        match self {
            Self::Timestamp(ts) => *ts,
            _ => panic!("expect timestamp"),
        }
    }
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
        ts_range: impl RangeBounds<u64>,
    ) -> Option<(u64, &Value)> {
        let map = match column {
            Column::Write => &self.write,
            Column::Data => &self.data,
            Column::Lock => &self.lock,
        };
        let start = (
            key.clone(),
            match ts_range.start_bound() {
                Bound::Included(ts) => *ts,
                Bound::Excluded(ts) => *ts + 1,
                Bound::Unbounded => 0,
            },
        );
        let end = (
            key.clone(),
            match ts_range.end_bound() {
                Bound::Included(ts) => *ts,
                Bound::Excluded(ts) => *ts - 1,
                Bound::Unbounded => u64::MAX,
            },
        );
        map.range(start..=end)
            .next_back()
            .map(|((_, ts), value)| (*ts, value))
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

    #[inline]
    fn find_write(&self, key: Vec<u8>, start_ts: u64) -> Option<u64> {
        self.write
            .range(..)
            .find(|(_, ts)| ts.as_ts() == start_ts)
            .map(|((_, ts), _)| *ts)
    }
}

impl Display for KvTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut map = BTreeMap::<&[u8], BTreeMap<u64, (_, _, _)>>::new();
        for ((key, ts), value) in &self.data {
            map.entry(key).or_default().entry(*ts).or_default().0 = Some(value);
        }
        for ((key, ts), value) in &self.lock {
            map.entry(key).or_default().entry(*ts).or_default().1 = Some(value);
        }
        for ((key, ts), value) in &self.write {
            map.entry(key).or_default().entry(*ts).or_default().2 = Some(value);
        }

        let mut table = comfy_table::Table::new();
        table.set_header(vec!["Key", "Data", "Lock", "Write"]);
        for (key, map) in map {
            let value_to_string = |ts: u64, v: Option<&Value>| match v {
                Some(Value::Timestamp(t)) => format!("{ts}: data@{t}"),
                Some(Value::Vector(v)) => format!("{ts}: {}", String::from_utf8_lossy(v)),
                None => String::new(),
            };
            table.add_row(vec![
                String::from_utf8_lossy(key).to_string(),
                map.iter()
                    .rev()
                    .map(|(ts, (v, _, _))| value_to_string(*ts, *v))
                    .join("\n"),
                map.iter()
                    .rev()
                    .map(|(ts, (_, v, _))| value_to_string(*ts, *v))
                    .join("\n"),
                map.iter()
                    .rev()
                    .map(|(ts, (_, _, v))| value_to_string(*ts, *v))
                    .join("\n"),
            ]);
        }
        write!(f, "{table}")
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
            let table = self.data.lock().unwrap();
            // 检查 [0, start_ts] 是否有有锁
            if let Some((start_ts, primary)) =
                table.read(req.key.to_vec(), Column::Lock, ..=req.start_ts)
            {
                // Err IsLocked
                info!(
                    "[IsLocked] start_ts = {}, primary = {:?}",
                    start_ts, primary
                );
                let primary = primary.clone();
                drop(table);
                self.back_off_maybe_clean_up_lock(
                    start_ts,
                    req.key.clone(),
                    primary.as_bytes().to_vec(),
                );
                continue;
            }
            // 通过 write 列找到 start_ts 可以看到的 key 的最新提交版本
            let data_ts = match table.read(req.key.to_vec(), Column::Write, 0..=req.start_ts) {
                Some(write) => write.1.as_ts(),
                None => return Ok(GetResponse { value: vec![] }),
            };
            // 根据版本读取数据
            let value = table
                .read(req.key.to_vec(), Column::Data, data_ts..=data_ts.clone())
                .unwrap()
                .1
                .as_bytes();
            return Ok(GetResponse {
                value: value.to_vec(),
            });
        }
    }

    // example prewrite RPC handler.
    async fn prewrite(&self, req: PrewriteRequest) -> labrpc::Result<PrewriteResponse> {
        // 检查 write-write 冲突
        let mut table = self.data.lock().unwrap();
        if let Some((commit_ts, _)) = table.read(req.key.to_vec(), Column::Write, req.start_ts..) {
            info!("[WriteConflict] commit_ts = {}", commit_ts);
            return Ok(PrewriteResponse { success: false });
        }

        // 检查 key 是否被加锁
        if let Some((start_ts, _)) = table.read(req.key.to_vec(), Column::Lock, ..) {
            info!("[IsLocked] start_ts = {}", start_ts);
            return Ok(PrewriteResponse { success: false });
        }

        // 为 key 加锁
        table.write(
            req.key.to_vec(),
            Column::Lock,
            req.start_ts,
            Value::Vector(req.primary),
        );

        // 写 data 列
        table.write(
            req.key,
            Column::Data,
            req.start_ts,
            Value::Vector(req.value),
        );

        // print!("{}\n", table);
        Ok(PrewriteResponse { success: true })
    }

    // example commit RPC handler.
    async fn commit(&self, req: CommitRequest) -> labrpc::Result<CommitResponse> {
        let mut table = self.data.lock().unwrap();
        // 检查 lock 的合法性
        if let Some((start_ts, primary)) = table.read(req.key.to_vec(), Column::Lock, ..) {
            if start_ts != req.start_ts || (req.is_primary && primary.as_bytes() != &req.key) {
                info!(
                    "[InvalidLock] start_ts = {}, primary = {:?}",
                    start_ts, primary
                );
                return Ok(CommitResponse { success: false });
            }
        } else {
            info!(
                "[LockNotExist] start_ts = {}, key = {:?}",
                req.start_ts, req.key
            );
            return Ok(CommitResponse { success: false });
        }

        // 写 write 列
        table.write(
            req.key.to_vec(),
            Column::Write,
            req.commit_ts,
            Value::Timestamp(req.start_ts),
        );

        // 删除 key 的 lock
        table.erase(req.key, Column::Lock, req.start_ts);

        // print!("{}\n", table);
        Ok(CommitResponse { success: true })
    }
}

impl MemoryStorage {
    fn back_off_maybe_clean_up_lock(&self, start_ts: u64, key: Vec<u8>, primary: Vec<u8>) {
        thread::sleep(Duration::from_nanos(TTL));
        let mut table = self.data.lock().unwrap();
        if let None = table.read(key.clone(), Column::Lock, ..) {
            // lock 被拥有者清除了
            return;
        }
        // 检查 primary 是否 commit 成功来决定 roll back 还是 roll forward
        match table.find_write(primary.clone(), start_ts) {
            Some(commit_ts) => {
                // commit 成功，roll forward
                table.write(
                    key.clone(),
                    Column::Write,
                    commit_ts,
                    Value::Timestamp(start_ts),
                );
                table.erase(key, Column::Lock, start_ts);
            }
            None => {
                // commit 失败，roll back
                table.erase(key, Column::Lock, start_ts);
            }
        }
    }
}
