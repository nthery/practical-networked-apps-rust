use serde::{Deserialize, Serialize};

// TODO: Use &str instead of String
#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Get(String),
    Set(String, String),
    Rm(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Reply(pub Result<Option<String>, String>);
