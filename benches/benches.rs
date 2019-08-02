use criterion::{criterion_group, criterion_main, Criterion};
use kvs::{KvStore, KvsEngine, SledKvsEngine};
use rand::{rngs::SmallRng, Rng, SeedableRng};
use tempfile::TempDir;

// TODO: The spec requires to write and read 100 times but this takes several minutes with the sled engine.
const WRITE_COUNT: usize = 10;
const READ_COUNT: usize = 10;

fn random_ascii_string(rng: &mut impl Rng) -> String {
    let len = rng.gen_range(1, 100000);
    let mut s = String::with_capacity(len);
    for _ in 0..len {
        // TODO: Track down deserialization error when re-opening existing store when allowing all
        // printable ascii characters.  Escape bug?  serde_json bug?
        // s.push(rng.gen_range(32, 127) as u8 as char);
        s.push(rng.gen_range('a' as u8, 'z' as u8) as u8 as char);
    }
    s
}

fn key_val_pairs(n: usize) -> Vec<(String, String)> {
    let mut rng = SmallRng::seed_from_u64(0x0DDB1A5E5BAD5EEDu64);
    let mut pairs = Vec::with_capacity(n);
    for _ in 0..n {
        pairs.push((random_ascii_string(&mut rng), random_ascii_string(&mut rng)));
    }
    pairs
}

fn engine_write(engine: &mut impl KvsEngine, pairs: &Vec<(String, String)>) {
    for (k, v) in pairs {
        engine.set(k.to_string(), v.to_string()).unwrap();
    }
}

fn generic_write<T>(c: &mut Criterion, name: &str)
where
    T: 'static + KvsEngine + Sized,
{
    let tmpdir = TempDir::new().unwrap();
    let pairs = key_val_pairs(WRITE_COUNT);
    let mut engine = T::open(&tmpdir).unwrap();
    c.bench_function(name, move |b| b.iter(|| engine_write(&mut engine, &pairs)));
}

fn kvs_write(c: &mut Criterion) {
    generic_write::<KvStore>(c, "kvs_write")
}

fn sled_write(c: &mut Criterion) {
    generic_write::<SledKvsEngine>(c, "sled_write")
}

fn engine_read(engine: &impl KvsEngine, pairs: &Vec<(String, String)>) {
    let mut rng = SmallRng::seed_from_u64(0x0DDB1A5E5BAD5EEDu64);
    for _ in 0..READ_COUNT {
        let i = rng.gen_range(0, pairs.len());
        assert_eq!(
            engine.get(pairs[i].0.to_string()).unwrap(),
            Some(pairs[i].1.to_string())
        );
    }
}

fn generic_read<T>(c: &mut Criterion, name: &str)
where
    T: KvsEngine + 'static + Sized,
{
    let tmpdir = TempDir::new().unwrap();
    let pairs = key_val_pairs(WRITE_COUNT);
    {
        let mut engine = T::open(&tmpdir).unwrap();
        engine_write(&mut engine, &pairs);
    }
    c.bench_function(name, move |b| {
        b.iter(|| {
            let engine = T::open(&tmpdir).unwrap();
            engine_read(&engine, &pairs);
        })
    });
}

fn kvs_read(c: &mut Criterion) {
    generic_read::<KvStore>(c, "kvs_read");
}

fn sled_read(c: &mut Criterion) {
    generic_read::<SledKvsEngine>(c, "sled_read");
}

criterion_group!(benches, kvs_write, sled_write, kvs_read, sled_read);
criterion_main!(benches);
