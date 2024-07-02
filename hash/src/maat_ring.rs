use std::collections::{HashMap, HashSet};

use crate::maat_node::MaatNode;
use crate::ring_buffer::RingBuffer;

pub trait MaatRing {
    fn accept(&mut self, node: Box<dyn MaatNode>);

    fn remove(&mut self, node: Box<dyn MaatNode>);

    fn route(&self, request: &dyn Request) -> Box<dyn MaatNode>;

    fn hash(&self, data: &dyn Serializable) -> usize;
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
    replicas_trace: HashMap<String, HashSet<String>>,
    node_indices: HashMap<String, Box<dyn MaatNode>>,
}

impl MaatRing for DefaultMaatRing {
    fn accept(&mut self, node: Box<dyn MaatNode>) {
        for _ in 0..self.replicas {
            let replicated_node = node.replicate();
            let id = replicated_node.get_id();

            self.ring.add(replicated_node.clone());
            self.node_indices.insert(id.clone(), replicated_node.clone());

            self.replicas_trace
                .entry(id.clone())
                .or_insert_with(HashSet::new)
                .insert(id.clone());
        }

        self.node_indices.insert(node.get_id(), node.clone());
        self.ring.add(node);
    }

    fn remove(&mut self, node: Box<dyn MaatNode>) {
        let node_id = node.get_id();
        let replica_ids = self.replicas_trace.get(&node_id).unwrap().clone();
        let replicas = replica_ids.iter()
            .map(|id| { self.node_indices.get(id) })
            .collect();

        self.ring.remove(node);
        replicas.iter().for_each(|replica| { self.ring.remove(replica) })
    }

    fn route(&self, request: &dyn Request) -> Box<dyn MaatNode> {
        todo!()
    }

    fn hash(&self, data: &dyn Serializable) -> usize {
        todo!()
    }
}
