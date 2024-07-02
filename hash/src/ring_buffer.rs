pub trait RingBuffer<T> {

    fn add(&mut self, data: T);

    fn remove(&mut self, data: T);

    fn findNearest(&self, data: T) -> T;
}