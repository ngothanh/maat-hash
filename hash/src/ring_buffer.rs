use std::collections::{BTreeMap, HashSet};
use std::hash::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::maat_ring::Serializable;

pub trait RingBuffer<T: Serializable + Clone + Eq + Hash> {
    fn add(&mut self, data: &T);

    fn remove(&mut self, data: &T);

    fn find_nearest(&self, hash: usize) -> Option<&HashSet<T>>;

    fn get_hash_fn(&self) -> Box<dyn Fn(&(dyn Serializable + '_)) -> usize + '_>;
}

pub struct InMemoryRingBuffer<T> {
    storage: BTreeMap<usize, HashSet<T>>,
    size: usize,
}

impl<T> InMemoryRingBuffer<T> {
    fn new(capacity: usize) -> Self {
        let mut storage = BTreeMap::new();
        for i in 0..capacity {
            storage.insert(i, HashSet::new());
        }
        InMemoryRingBuffer {
            storage,
            size: capacity,
        }
    }
}

impl<T: Serializable + Eq + Clone + Hash> RingBuffer<T> for InMemoryRingBuffer<T> {
    fn add(&mut self, data: &T) {
        let idx = self.get_hash_fn()(data);
        self.storage.get_mut(&idx).unwrap().insert(data.clone());
    }

    fn remove(&mut self, data: &T) {
        let idx = self.get_hash_fn()(data);
        if let Some(found) = self.storage.get_mut(&idx) {
            found.remove(data);
        }
    }

    fn find_nearest(&self, hash: usize) -> Option<&HashSet<T>> {
        if let Some(vec) = self.storage.get(&hash) {
            if !vec.is_empty() {
                return Some(vec);
            }
        }

        if let Some((_, vec)) = self.storage.range(hash + 1..).next() {
            return Some(vec);
        }

        return None;
    }

    fn get_hash_fn(&self) -> Box<dyn Fn(&(dyn Serializable + '_)) -> usize + '_> {
        let f = |obj: &(dyn Serializable + '_)| {
            let s = obj.serialize();
            let mut hasher = DefaultHasher::new();
            s.hash(&mut hasher);
            let i = hasher.finish() as usize;
            i % self.size
        };

        Box::new(f)
    }
}

#[cfg(test)]
mod tests {
    use crate::maat_ring::Serializable;
    use crate::ring_buffer::{InMemoryRingBuffer, RingBuffer};

    #[derive(Hash, Eq, PartialEq)]
    struct TestData {
        content: String,
    }

    impl Serializable for TestData {
        fn serialize(&self) -> String {
            self.content.clone()
        }
    }

    impl TestData {
        fn new(content: String) -> TestData {
            TestData { content }
        }
    }
    impl Clone for TestData {
        fn clone(&self) -> Self {
            TestData::new(self.content.clone())
        }
    }

    #[test]
    fn given_ring_buffer_when_adding_new_item_then_item_was_added_to_the_correct_index() {
        // Given
        let mut ring_buffer: InMemoryRingBuffer<TestData> = InMemoryRingBuffer::new(1000);

        let data = TestData::new(String::from("I'm good"));

        // When
        ring_buffer.add(&data);

        // Then
        let hash = ring_buffer.get_hash_fn()(&data);
        let found_data = ring_buffer.find_nearest(hash).unwrap();
        assert!(found_data.contains(&data));
    }

    #[test]
    fn given_ring_buffer_when_adding_same_item_twice_then_item_was_added_to_the_correct_index() {
        // Given
        let mut ring_buffer: InMemoryRingBuffer<TestData> = InMemoryRingBuffer::new(1000);

        let data = TestData::new(String::from("I'm good"));

        // When
        ring_buffer.add(&data);
        ring_buffer.add(&data);

        // Then
        let hash = ring_buffer.get_hash_fn()(&data);
        let found_data = ring_buffer.find_nearest(hash).unwrap();
        assert_eq!(found_data.len(), 1);
        assert!(found_data.contains(&data));
    }

    #[test]
    fn given_ring_buffer_when_adding_item_and_then_remove_it_then_item_was_removed_correctly() {
        // Given
        let mut ring_buffer: InMemoryRingBuffer<TestData> = InMemoryRingBuffer::new(1000);

        let data = TestData::new(String::from("To be deleted"));
        ring_buffer.add(&data);

        // When
        ring_buffer.remove(&data);

        // Then
        let hash = ring_buffer.get_hash_fn()(&data);
        let found_data = ring_buffer.find_nearest(hash).unwrap();
        assert_eq!(found_data.len(), 0);
    }

    #[test]
    fn given_ring_buffer_when_adding_items_and_then_find_nearest_then_item_was_find_correctly() {
        // Given
        let mut ring_buffer: InMemoryRingBuffer<TestData> = InMemoryRingBuffer::new(1000);

        let data1 = TestData::new(String::from("To be deleted"));
        let data2 = TestData::new(String::from("To be fucking deleted"));

        // When
        let found = ring_buffer.find_nearest(735).unwrap().clone();

        // Then
        assert!(found.contains(&data2));
    }
}