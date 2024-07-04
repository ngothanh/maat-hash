use uuid::Uuid;
use crate::maat_ring::Serializable;

pub trait MaatNode: Serializable {
    fn replicate(&self) -> Box<dyn MaatNode>;

    fn get_id(&self) -> String;

    fn is_physical(&self) -> bool;

    fn clone_box(&self) -> Box<dyn MaatNode>;
}

#[derive(Clone)]
pub struct Server {
    ip: String,
    port: usize,
    id: String,
    is_physical: bool,
}

impl Server {
    fn new(ip: String, port: usize, is_physical: bool) -> Server {
        Server {
            ip,
            port,
            is_physical,
            id: Uuid::new_v4().to_string(),
        }
    }
}

impl Serializable for Server {
    fn serialize(&self) -> String {
        format!("{}@{}", self.ip, self.port)
    }
}

impl Clone for Box<dyn MaatNode> {
    fn clone(&self) -> Box<dyn MaatNode> {
        self.clone_box()
    }
}

impl MaatNode for Server {
    fn replicate(&self) -> Box<dyn MaatNode> {
        Box::new(
            Server::new(
                self.ip.clone(),
                self.port,
                false,
            )
        )
    }

    fn get_id(&self) -> String {
        self.id.clone()
    }

    fn is_physical(&self) -> bool {
        self.is_physical
    }

    fn clone_box(&self) -> Box<dyn MaatNode> {
        Box::new(self.clone())
    }
}