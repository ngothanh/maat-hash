use std::collections::BTreeMap;
use crate::maat_ring::Serializable;

pub trait RingBuffer<T: Serializable + Clone> {
    fn add(&mut self, data: T);

    fn remove(&mut self, data: T);

    fn find_nearest(&self, hash: usize) -> T;

    fn get_hash_fn(&self) -> Box<dyn Fn(&dyn Serializable) -> usize>;
}

pub struct InMemoryRingBuffer<T> {
    storage: BTreeMap<usize, Vec<T>>,
    size: usize,
}

impl<T: Serializable + Clone> InMemoryRingBuffer<T> {
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

impl<T: Serializable + Clone> RingBuffer<T> for InMemoryRingBuffer<T> {
    fn add(&mut self, data: T) {
        todo!()
    }

    fn remove(&mut self, data: T) {
        todo!()
    }

    fn find_nearest(&self, hash: usize) -> T {
        todo!()
    }

    fn get_hash_fn(&self) -> Box<dyn Fn(&dyn Serializable) -> usize> {
        todo!()
    }
}