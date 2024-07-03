use std::collections::{HashMap, HashSet};
use rand::thread_rng;
use crate::maat_node::MaatNode;
use crate::ring_buffer::RingBuffer;

pub trait MaatRing {
    fn accept(&mut self, node: Box<dyn MaatNode>);

    fn remove(&mut self, node: Box<dyn MaatNode>);

    fn route(&self, request: &dyn Request) -> Box<dyn MaatNode>;

    fn hash<T: Serializable>(&self, data: &T) -> usize;
}

pub trait Serializable {
    fn serialize(&self) -> String;
}

pub trait Request: Serializable {
    fn of<T>(data: T) -> Self
    where
        Self: Sized;
}


struct DefaultMaatRing {
    ring: Box<dyn RingBuffer<Box<dyn MaatNode>>>,
    replicas: usize,
    node_replicas_indices: HashMap<String, HashSet<String>>,
    replica_node_indices: HashMap<String, String>,
    node_indices: HashMap<String, Box<dyn MaatNode>>,
}

impl DefaultMaatRing {
    fn pick(&self, nodes: Vec<Box<dyn MaatNode>>) -> Box<dyn MaatNode> {
        if nodes.len() == 1 {
            return nodes.into_iter().next().unwrap();
        }

        let mut physical_node_ids = HashSet::new();
        for node in nodes {
            if node.is_physical() {
                physical_node_ids.insert(node.get_id());
                continue;
            }

            let virtual_node_id = node.get_id();
            let physical_node_id = self.replica_node_indices[virtual_node_id];
            physical_node_ids.insert(physical_node_id);
        }

        if physical_node_ids.len() == 1 {
            let physical_node_id = physical_node_ids.into_iter().next().unwrap();
            return self.node_indices[physical_node_id];
        }

        let random_physical_node_id = Self::shuffle_set(&physical_node_ids)
            .into_iter()
            .next()
            .unwrap();
        return self.node_indices[random_physical_node_id];
    }

    fn shuffle_set(input_set: &HashSet<String>) -> Vec<String> {
        let mut vec: Vec<String> = input_set.iter().cloned().collect();

        let mut rng = thread_rng();
        vec.shuffle(&mut rng);

        vec
    }
}

impl MaatRing for DefaultMaatRing {
    fn accept(&mut self, node: Box<dyn MaatNode>) {
        let node_id = node.get_id();
        for _ in 0..self.replicas {
            let replicated_node = node.replicate();
            let id = replicated_node.get_id();

            self.ring.add(replicated_node.clone());
            self.node_indices.insert(id.clone(), replicated_node.clone());

            self.node_replicas_indices
                .entry(id.clone())
                .or_insert_with(HashSet::new)
                .insert(id.clone());
            self.replica_node_indices.insert(id.clone(), node_id.clone())
        }

        self.node_indices.insert(node_id.clone(), node.clone());
        self.ring.add(node);
    }

    fn remove(&mut self, node: Box<dyn MaatNode>) {
        let node_id = node.get_id();
        let replica_ids = self.node_replicas_indices.get(&node_id).unwrap().clone();
        let replicas = replica_ids.iter()
            .map(|id| { self.node_indices.get(id) }
            )
            .collect();
        self.ring.remove(node);
        self.node_replicas_indices.remove(&node_id);
        self.node_indices.remove(&node_id);
        replicas.iter()
            .for_each(
                |replica| {
                    let replica_id = replica.get_id();
                    self.ring.remove(replica);
                    self.replica_node_indices.remove(replica_id.clone());
                    self.node_indices.remove(replica_id.clone());
                }
            );
    }

    fn route(&self, request: &dyn Request) -> Box<dyn MaatNode> {
        let hash = self.hash(request);
        let available_nodes = self.ring.find_nearest(hash);
        self.pick(available_nodes)
    }

    fn hash<T: Serializable>(&self, data: &T) -> usize {
        self.ring.get_hash_fn()(data)
    }
}
