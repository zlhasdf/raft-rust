//! KV 状态机的本地存储实现。
//!
//! 这个文件故意把“业务接口”和“具体数据结构”分开：
//! - `StorageEngine` 是 KVServer 需要的最小能力；
//! - `KvStore` 是状态机对外使用的包装类型；
//! - `SkipListStore` 是贴近原 C++ skipList 的 Rust 跳表实现；
//! - 默认仍保留 `BTreeMap`，方便学习和调试时先看简单版本。

use std::cmp::Ordering;
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::Result;

/// 状态机存储引擎 trait。
///
/// KVServer 只关心这三个操作，不关心底层到底是 BTreeMap、跳表，还是以后替换成 RocksDB。
pub trait StorageEngine {
    fn get(&self, key: &str) -> Option<String>;
    fn put(&mut self, key: String, value: String);
    fn append(&mut self, key: String, value: String);
}

/// KVStore 当前使用的后端。
///
/// 这里用 enum 而不是 trait object，是因为状态机快照需要 `serde + bincode`
/// 直接序列化。`Box<dyn StorageEngine>` 这种动态分发对象不容易直接持久化。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum KvStoreBackend {
    BTree(BTreeMap<String, String>),
    SkipList(SkipListStore),
}

/// KVServer 持有的状态机类型。
///
/// 默认构造函数使用 BTreeMap；如果希望使用跳表，可以调用 `KvStore::new_skip_list()`。
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct KvStore {
    backend: KvStoreBackend,
}

impl Default for KvStoreBackend {
    fn default() -> Self {
        Self::BTree(BTreeMap::new())
    }
}

impl KvStore {
    /// 创建默认状态机。默认使用 BTreeMap，保持第一版重构的简单性。
    pub fn new() -> Self {
        Self::default()
    }

    /// 创建使用跳表作为底层数据结构的状态机。
    pub fn new_skip_list() -> Self {
        Self {
            backend: KvStoreBackend::SkipList(SkipListStore::new()),
        }
    }

    /// 返回当前后端名称，主要用于测试、日志和调试。
    pub fn backend_name(&self) -> &'static str {
        match &self.backend {
            KvStoreBackend::BTree(_) => "btree",
            KvStoreBackend::SkipList(_) => "skiplist",
        }
    }

    /// 把状态机完整编码成快照字节。
    ///
    /// Raft 做 snapshot 时不关心里面是什么结构，只保存这段字节。
    pub fn to_snapshot(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(self)?)
    }

    /// 从快照字节恢复状态机。
    pub fn from_snapshot(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() {
            return Ok(Self::new());
        }
        Ok(bincode::deserialize(bytes)?)
    }

    pub fn len(&self) -> usize {
        match &self.backend {
            KvStoreBackend::BTree(data) => data.len(),
            KvStoreBackend::SkipList(data) => data.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl StorageEngine for KvStore {
    fn get(&self, key: &str) -> Option<String> {
        match &self.backend {
            KvStoreBackend::BTree(data) => data.get(key).cloned(),
            KvStoreBackend::SkipList(data) => data.get(key),
        }
    }

    fn put(&mut self, key: String, value: String) {
        match &mut self.backend {
            KvStoreBackend::BTree(data) => {
                data.insert(key, value);
            }
            KvStoreBackend::SkipList(data) => data.put(key, value),
        }
    }

    fn append(&mut self, key: String, value: String) {
        // The source C++ code routes Append through SkipList::insert_set_element,
        // whose active behavior is upsert instead of string concatenation.
        self.put(key, value);
    }
}

const MAX_SKIP_LIST_LEVEL: usize = 16;
const DEFAULT_SKIP_LIST_SEED: u64 = 0x9E37_79B9_7F4A_7C15;

/// 跳表节点。
///
/// C++ 版本使用裸指针连接节点。Rust 里直接用指针会牵涉生命周期和 unsafe，
/// 所以这里用 `Vec<SkipListNode>` 保存所有节点，再用节点下标模拟“指针”。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct SkipListNode {
    key: String,
    value: String,
    forwards: Vec<Option<usize>>,
}

/// 一个可序列化的跳表 KV 存储引擎。
///
/// 这是原项目 skipList 思想的 Rust 版：多层索引加速查找，底层链表保持有序。
/// 为了快照简单可靠，节点之间用数组下标连接，而不是用引用或裸指针。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SkipListStore {
    nodes: Vec<SkipListNode>,
    level: usize,
    len: usize,
    rng_state: u64,
}

impl Default for SkipListStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SkipListStore {
    /// 创建空跳表。第 0 个节点是哨兵 head，不保存真实 key/value。
    pub fn new() -> Self {
        Self {
            nodes: vec![SkipListNode {
                key: String::new(),
                value: String::new(),
                forwards: vec![None; MAX_SKIP_LIST_LEVEL],
            }],
            level: 1,
            len: 0,
            rng_state: DEFAULT_SKIP_LIST_SEED,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// 查找每一层里“小于目标 key 的最后一个节点”。
    ///
    /// 插入时需要把这些节点的 forward 指针改到新节点，所以单独提取出来。
    fn find_update_path(&self, key: &str) -> [usize; MAX_SKIP_LIST_LEVEL] {
        let mut update = [0; MAX_SKIP_LIST_LEVEL];
        let mut current = 0;

        for level in (0..self.level).rev() {
            while let Some(next) = self.nodes[current].forwards[level] {
                match self.nodes[next].key.as_str().cmp(key) {
                    Ordering::Less => current = next,
                    Ordering::Equal | Ordering::Greater => break,
                }
            }
            update[level] = current;
        }

        update
    }

    /// 简单的 xorshift 伪随机数。
    ///
    /// 这样 `SkipListStore` 不需要额外依赖随机数对象，快照时也能保存当前随机状态。
    fn next_random(&mut self) -> u64 {
        let mut x = self.rng_state;
        x ^= x << 7;
        x ^= x >> 9;
        x ^= x << 8;
        self.rng_state = x;
        x
    }

    /// 随机生成新节点高度。
    ///
    /// 跳表靠“少量节点升到更高层”来减少查找步数，概率大致是每升一层折半。
    fn random_level(&mut self) -> usize {
        let mut level = 1;
        while level < MAX_SKIP_LIST_LEVEL && self.next_random() & 1 == 0 {
            level += 1;
        }
        level
    }
}

impl StorageEngine for SkipListStore {
    fn get(&self, key: &str) -> Option<String> {
        let update = self.find_update_path(key);
        let candidate = self.nodes[update[0]].forwards[0]?;
        (self.nodes[candidate].key == key).then(|| self.nodes[candidate].value.clone())
    }

    fn put(&mut self, key: String, value: String) {
        let mut update = self.find_update_path(&key);

        if let Some(candidate) = self.nodes[update[0]].forwards[0] {
            if self.nodes[candidate].key == key {
                self.nodes[candidate].value = value;
                return;
            }
        }

        let new_level = self.random_level();
        if new_level > self.level {
            for slot in update.iter_mut().take(new_level).skip(self.level) {
                *slot = 0;
            }
            self.level = new_level;
        }

        let mut forwards = vec![None; new_level];
        for (level, forward) in forwards.iter_mut().enumerate() {
            *forward = self.nodes[update[level]].forwards[level];
        }

        let new_index = self.nodes.len();
        self.nodes.push(SkipListNode {
            key,
            value,
            forwards,
        });

        for (level, previous_index) in update.iter().copied().enumerate().take(new_level) {
            self.nodes[previous_index].forwards[level] = Some(new_index);
        }
        self.len += 1;
    }

    fn append(&mut self, key: String, value: String) {
        // 和原 C++ 当前实现保持一致：Append 走 insert_set_element，实际表现为 upsert。
        self.put(key, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_get() {
        let mut store = KvStore::new();
        store.put("x".to_owned(), "1".to_owned());
        assert_eq!(store.get("x").as_deref(), Some("1"));
    }

    #[test]
    fn test_append() {
        let mut store = KvStore::new();
        store.put("x".to_owned(), "hello".to_owned());
        store.append("x".to_owned(), " world".to_owned());
        assert_eq!(store.get("x").as_deref(), Some(" world"));
    }

    #[test]
    fn test_snapshot_restore() {
        let mut store = KvStore::new();
        store.put("x".to_owned(), "1".to_owned());
        let snapshot = store.to_snapshot().unwrap();
        let restored = KvStore::from_snapshot(&snapshot).unwrap();
        assert_eq!(restored.get("x").as_deref(), Some("1"));
    }

    #[test]
    fn skip_list_put_get() {
        let mut store = SkipListStore::new();
        store.put("b".to_owned(), "2".to_owned());
        store.put("a".to_owned(), "1".to_owned());
        store.put("c".to_owned(), "3".to_owned());
        assert_eq!(store.get("a").as_deref(), Some("1"));
        assert_eq!(store.get("b").as_deref(), Some("2"));
        assert_eq!(store.get("c").as_deref(), Some("3"));
        assert_eq!(store.get("missing"), None);
    }

    #[test]
    fn skip_list_updates_existing_key() {
        let mut store = SkipListStore::new();
        store.put("x".to_owned(), "1".to_owned());
        store.put("x".to_owned(), "2".to_owned());
        assert_eq!(store.len(), 1);
        assert_eq!(store.get("x").as_deref(), Some("2"));
    }

    #[test]
    fn kv_store_can_use_skip_list_backend() {
        let mut store = KvStore::new_skip_list();
        assert_eq!(store.backend_name(), "skiplist");
        store.put("x".to_owned(), "1".to_owned());
        store.append("x".to_owned(), "2".to_owned());
        assert_eq!(store.get("x").as_deref(), Some("2"));
    }

    #[test]
    fn skip_list_backend_snapshot_restore() {
        let mut store = KvStore::new_skip_list();
        store.put("x".to_owned(), "1".to_owned());
        let snapshot = store.to_snapshot().unwrap();
        let restored = KvStore::from_snapshot(&snapshot).unwrap();
        assert_eq!(restored.backend_name(), "skiplist");
        assert_eq!(restored.get("x").as_deref(), Some("1"));
    }
}
