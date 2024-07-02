use crate::maat_ring::Serializable;

pub trait RingBuffer<T: Serializable> {
    fn add(&mut self, data: T);

    fn remove(&mut self, data: T);

    fn find_nearest(&self, hash: u64) -> T;

    fn get_hash_fn(&self) -> Box<dyn Fn(&dyn Serializable) -> u64>;
}