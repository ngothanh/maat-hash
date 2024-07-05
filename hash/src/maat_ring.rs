use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use rand::seq::SliceRandom;
use rand::thread_rng;

use crate::maat_node::{MaatNode, Server};
use crate::ring_buffer::{InMemoryRingBuffer, RingBuffer};

pub trait MaatRing {
    fn accept(&mut self, node: &Server);

    fn remove(&mut self, node: &Server);

    fn route<T: Serializable>(&self, request: &Request<T>) -> Result<Server, NotFound>;

    fn hash<T: Serializable>(&self, data: &T) -> usize;
}

#[derive(Debug)]
pub struct NotFound;

pub trait Serializable {
    fn serialize(&self) -> String;
}

pub trait Wrapper<T>: Serializable {
    fn of(data: T) -> Self
    where
        Self: Sized;
}

pub struct Request<T> {
    data: T,
}

impl<T: Serializable> Serializable for Request<T> {
    fn serialize(&self) -> String {
        let serialized_data = self.data.serialize();

        format!("[{}]", serialized_data)
    }
}

impl<T: Serializable> Wrapper<T> for Request<T> {
    fn of(data: T) -> Self
    where
        Self: Sized,
    {
        Request { data }
    }
}


struct DefaultMaatRing {
    ring: Box<dyn RingBuffer<Server>>,
    replicas: usize,
    node_replicas_indices: HashMap<String, HashSet<String>>,
    replica_node_indices: HashMap<String, String>,
    node_indices: HashMap<String, Server>,
}

impl DefaultMaatRing {
    fn new(capacity: usize, replicas: usize) -> DefaultMaatRing {
        DefaultMaatRing {
            ring: Box::new(InMemoryRingBuffer::new(capacity)),
            replicas,
            node_replicas_indices: HashMap::new(),
            replica_node_indices: HashMap::new(),
            node_indices: HashMap::new(),
        }
    }

    fn pick(&self, nodes: &HashSet<Server>) -> Server {
        let mut physical_node_ids = HashSet::new();
        for node in nodes {
            if node.is_physical() {
                physical_node_ids.insert(node.get_id());
                continue;
            }

            let virtual_node_id = node.get_id();
            let physical_node_id = self.replica_node_indices[&virtual_node_id].clone();
            physical_node_ids.insert(physical_node_id);
        }

        if physical_node_ids.len() == 1 {
            let physical_node_id = physical_node_ids.into_iter().next().unwrap();
            return self.node_indices[&physical_node_id].clone();
        }

        let random_physical_node_id = Self::shuffle_set(&physical_node_ids)
            .into_iter()
            .next()
            .unwrap();
        return self.node_indices[&random_physical_node_id].clone();
    }

    fn shuffle_set(input_set: &HashSet<String>) -> Vec<String> {
        let mut vec: Vec<String> = input_set.iter().cloned().collect();

        let mut rng = thread_rng();
        vec.shuffle(&mut rng);

        vec
    }
}

impl MaatRing for DefaultMaatRing {
    fn accept(&mut self, node: &Server) {
        let node_id = node.get_id();
        for _ in 0..self.replicas {
            let replicated_node = node.replicate();
            let replica_id = replicated_node.get_id();

            self.ring.add(&replicated_node);

            self.node_replicas_indices
                .entry(node_id.clone())
                .or_insert_with(HashSet::new)
                .insert(replica_id.clone());
            self.replica_node_indices.insert(replica_id.clone(), node_id.clone());
            self.node_indices.insert(replica_id.clone(), replicated_node.clone());
        }

        self.ring.add(&node);
        self.node_indices.insert(node_id.clone(), node.clone());
    }

    fn remove(&mut self, node: &Server) {
        let node_id = node.get_id();
        let replica_ids = self.node_replicas_indices.get(&node_id).unwrap().clone();
        let replicas: Vec<Server> = replica_ids.into_iter()
            .map(|id| { self.node_indices.get(&id).unwrap() }.clone())
            .collect();
        self.ring.remove(&node);
        self.node_replicas_indices.remove(&node_id);
        self.node_indices.remove(&node_id);
        replicas.iter()
            .for_each(
                |replica| {
                    let replica_id = replica.get_id();
                    self.ring.remove(replica);
                    self.replica_node_indices.remove(&replica_id.clone());
                    self.node_indices.remove(&replica_id.clone());
                }
            );
    }

    fn route<T: Serializable>(&self, request: &Request<T>) -> Result<Server, NotFound> {
        let hash = self.hash(request);
        if let Some(available_nodes) = self.ring.find_nearest(hash) {
            return Ok(self.pick(available_nodes));
        }

        return Err(NotFound);
    }

    fn hash<T: Serializable>(&self, data: &T) -> usize {
        self.ring.get_hash_fn()(data)
    }
}

#[cfg(test)]
mod tests {
    use crate::maat_node::Server;
    use crate::maat_ring::{DefaultMaatRing, MaatRing, Request, Serializable, Wrapper};

    struct Payload {
        data: String,
    }

    impl Payload {
        fn new(data: String) -> Payload {
            Payload {
                data
            }
        }
    }

    impl Serializable for Payload {
        fn serialize(&self) -> String {
            self.data.clone()
        }
    }

    #[test]
    fn give_maat_ring_with_single_node_when_new_node_join_then_the_request_was_correctly_routed_to_this_node() {
        //given
        let mut ring = DefaultMaatRing::new(100, 0);
        let server = Server::new(
            String::from("1.1.1.1"),
            61,
            true,
        );
        ring.accept(&server);

        let payload = Payload::new(String::from("test"));
        let request = Request::of(payload);

        //when
        let result = ring.route(&request);

        //then
        assert!(result.is_ok());
        match result {
            Ok(found) => { assert!(found == server) }
            Err(_) => {}
        }
    }

    #[test]
    fn give_maat_ring_with_single_node_and_replicas_when_new_node_join_then_the_request_was_correctly_routed_to_this_node() {
        //given
        let mut ring = DefaultMaatRing::new(100, 10);
        let server = Server::new(
            String::from("1.10.11.12"),
            61,
            true,
        );
        ring.accept(&server);

        let payload = Payload::new(String::from("test"));
        let request = Request::of(payload);

        //when
        let result = ring.route(&request);

        //then
        assert!(result.is_ok());
        match result {
            Ok(found) => { assert!(found == server) }
            Err(_) => {}
        }
    }

    #[test]
    fn give_maat_ring_with_two_nodes_and_replicas_when_one_node_left_then_the_request_was_correctly_routed_to_another_node() {
        //given
        let mut ring = DefaultMaatRing::new(10, 1);
        let server1 = Server::new(
            String::from("1.10.11.12"),
            61,
            true,
        );
        let server2 = Server::new(
            String::from("1.10.11.14"),
            61,
            true,
        );
        ring.accept(&server1);
        ring.accept(&server2);

        let payload = Payload::new(String::from("test"));
        let request = Request::of(payload);

        //when
        ring.remove(&server2);
        let result = ring.route(&request);

        //then
        assert!(result.is_ok());
        match result {
            Ok(found) => { assert!(found == server1) }
            Err(_) => {}
        }
    }
}
