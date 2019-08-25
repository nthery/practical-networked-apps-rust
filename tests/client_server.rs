use kvs::{KvStore, KvsClient, KvsEngine, KvsServer, SharedQueueThreadPool, ThreadPool};
use std::net::SocketAddr;
use tempfile::TempDir;

#[test]
fn shutdown() {
    let tmpdir = TempDir::new().unwrap();
    let engine = KvStore::open(&tmpdir).unwrap();
    let pool = SharedQueueThreadPool::new(1).unwrap();
    let addr = "127.0.0.1:5000".parse::<SocketAddr>().unwrap();
    let mut server = KvsServer::new(engine, pool, addr).unwrap();
    let mut client = KvsClient::new(addr).unwrap();
    let server_thread = std::thread::spawn(move || server.run());
    client.shutdown().unwrap();
    assert!(server_thread.join().is_ok());
}

#[test]
fn several_operations_from_single_client() {
    let tmpdir = TempDir::new().unwrap();
    let engine = KvStore::open(&tmpdir).unwrap();
    let pool = SharedQueueThreadPool::new(1).unwrap();
    let addr = "127.0.0.1:5001".parse::<SocketAddr>().unwrap();
    let mut server = KvsServer::new(engine, pool, addr).unwrap();
    let mut client = KvsClient::new(addr).unwrap();
    let server_thread = std::thread::spawn(move || server.run());
    client.set("K1", "V1").unwrap();
    assert_eq!(client.get("K1").unwrap(), Some("V1".to_string()));
    client.shutdown().unwrap();
    assert!(server_thread.join().is_ok());
}
