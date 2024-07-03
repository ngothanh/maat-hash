use crate::maat_ring::Serializable;

pub trait MaatNode: Serializable {
    fn replicate(&self) -> Box<dyn MaatNode>;

    fn get_id(&self) -> String;

    fn is_physical(&self) -> String;
}