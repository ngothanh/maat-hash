use uuid::Uuid;
use crate::maat_ring::Serializable;

pub trait MaatNode: Serializable {
    fn replicate(&self) -> impl MaatNode;

    fn get_id(&self) -> String;

    fn is_physical(&self) -> bool;
}

#[derive(Clone, Eq, PartialEq, Hash)]
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

impl MaatNode for Server {
    fn replicate(&self) -> Server {
        Server::new(
            self.ip.clone(),
            self.port,
            false,
        )
    }

    fn get_id(&self) -> String {
        self.id.clone()
    }

    fn is_physical(&self) -> bool {
        self.is_physical
    }
}