use std::collections::BTreeMap;
use std::hash::DefaultHasher;
use crate::maat_ring::Serializable;

pub trait RingBuffer<T: Serializable + Clone + Eq> {
    fn add(&mut self, data: T);

    fn remove(&mut self, data: T);

    fn find_nearest(&self, hash: usize) -> Option<&Vec<T>>;

    fn get_hash_fn(&self) -> Box<dyn Fn(&dyn Serializable) -> usize>;
}

pub struct InMemoryRingBuffer<T> {
    storage: BTreeMap<usize, Vec<T>>,
    size: usize,
}

impl<T> InMemoryRingBuffer<T> {
    fn new(capacity: usize) -> Self {
        let mut storage = BTreeMap::new();
        for i in 0..capacity {
            storage.insert(i, Vec::new())
        }
        InMemoryRingBuffer {
            storage,
            size: capacity,
        }
    }
}

impl<T: Serializable + Clone + Eq> RingBuffer<T> for InMemoryRingBuffer<T> {
    fn add(&mut self, data: T) {
        let idx = self.get_hash_fn()(&data);
        self.storage.get(&idx).unwrap().push(data);
    }

    fn remove(&mut self, data: T) {
        let idx = self.get_hash_fn()(&data);
        let found = self.storage.get_mut(&idx).unwrap();
        if let Some(pos) = found.iter()
            .position(|node| { *node == data }) {
            found.remove(pos)
        }
    }

    fn find_nearest(&self, hash: usize) -> Option<&Vec<T>> {
        if let Some(vec) = self.storage.get(&hash) {
            return Some(vec);
        }

        if let Some((_, vec)) = self.storage.range(hash + 1..).next() {
            return Some(vec);
        }

        return None;
    }

    fn get_hash_fn(&self) -> Box<dyn Fn(&dyn Serializable) -> usize> {
        let f = |obj: &dyn Serializable| {
            let s = obj.serialize();
            let mut hasher = DefaultHasher::new();
            s.hash(&mut hasher);
            let i = hasher.finish() as usize;
            i % self.size
        };

        Box::new(f)
    }
}