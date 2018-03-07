extern crate bincode;
extern crate byteorder;
extern crate itertools;
extern crate rocksdb;
extern crate serde;
#[macro_use]
extern crate serde_derive;

extern crate rand;

use std::thread;
use std::time::Duration;

use bincode::{deserialize, serialize};
use rocksdb::{BlockBasedOptions, MergeOperands, Options, DB};
use rocksdb::Error;
use std::ops::Deref;
use std::string::String;
use byteorder::{BigEndian, ByteOrder};

use itertools::kmerge;
use rand::Rng;

struct DocId(u32);

impl DocId {
  fn parse(data: &[u8]) -> DocId {
    DocId(BigEndian::read_u32(data))
  }

  fn write(&self) -> Vec<u8> {
    let mut data: Vec<u8> = vec![0, 0, 0, 0];
    BigEndian::write_u32(&mut data[..], self.0);
    data
  }
}
struct Store {
  db:         DB,
  max_doc_id: DocId,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Msg {
  name: String,
}

#[derive(Debug, Clone, PartialEq)]
enum StoreError {
  DbError(String),
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct DocIds(Vec<u32>, Vec<u32>);

impl DocIds {
  fn serialize(&self) -> Vec<u8> {
    serialize(&self).unwrap()
  }

  fn deserialize(data: &[u8]) -> DocIds {
    // FIXME slow as hell ( for 400k )
    deserialize(data).unwrap()
  }
}

impl From<rocksdb::Error> for StoreError {
  fn from(e: Error) -> StoreError {
    StoreError::DbError(e.into())
  }
}

fn concat_merge(_new_key: &[u8], existing_val: Option<&[u8]>, operands: &mut MergeOperands) -> Option<Vec<u8>> {
  use std::time::Instant;
  let now = Instant::now();

  let mut add: Vec<Vec<u32>> = Vec::new();
  let mut remove: Vec<Vec<u32>> = Vec::new();
  let ops: Vec<&[u8]> = operands.collect();

  existing_val.map(|v| {
    let docs = DocIds::deserialize(v);
    add.push(docs.0);
    remove.push(docs.1)
  });

  if ops.len() == 1 && add.len() == 0 && remove.len() == 0 {
    return Some(ops[0].into());
  }

  for op in ops.into_iter() {
    let docs = DocIds::deserialize(op);
    add.push(docs.0);
    remove.push(docs.1)
  }

  let elapsed = now.elapsed();
  let sec = (elapsed.as_secs() as f64) + (elapsed.subsec_nanos() as f64 / 1000_000.0);
  println!("merge {}ms", sec);

  let sr = DocIds(kmerge(add).collect(), kmerge(remove).collect()).serialize();
  Some(sr)
}

impl Store {
  fn open() -> Result<Store, StoreError> {
    let mut opts = Options::default();
    opts.set_merge_operator("test", concat_merge, None);
    opts.create_if_missing(true);
    opts.set_report_bg_io_stats(true);
    opts.enable_statistics();
    opts.set_stats_dump_period_sec(1);
    let mut bb_opts = BlockBasedOptions::default();
    bb_opts.set_lru_cache(100_000_000);
    bb_opts.set_cache_index_and_filter_blocks(true);
    opts.set_block_based_table_factory(&bb_opts);
    let db = DB::open(&opts, "/tmp/teststorage")?;
    let max = match db.get(b"max_doc_id")? {
      Some(x) => DocId::parse(x.deref()),
      None => DocId(1),
    };
    println!("max value {}", max.0);
    Ok(Store {
      db:         db,
      max_doc_id: max,
    })
  }
  fn next_doc(&mut self) -> Result<DocId, StoreError> {
    let max = DocId(self.max_doc_id.0 + 1);

    self.db.put(b"max_doc_id", &max.write()[..])?;
    self.max_doc_id = max;
    Ok(DocId(self.max_doc_id.0))
  }

  fn put(&mut self, msg: &Msg) -> Result<(), StoreError> {
    let doc_id = self.next_doc()?;
    let mut key = Vec::new();
    key.extend(b"msg#name#".iter());
    key.extend(msg.name.as_bytes());

    let docs = DocIds(vec![doc_id.0], vec![]);
    self.db.merge(&key[..], &docs.serialize()[..])?;
    Ok(())
  }

  fn compact(&self) {
    self.db.compact_range(None, None);
  }

  fn find(&self, name: &str) -> Result<Option<DocIds>, StoreError> {
    let mut key = Vec::new();
    key.extend(b"msg#name#".iter());
    key.extend(name.as_bytes());
    use std::time::Instant;
    let now = Instant::now();

    let res = self.db.get(&key[..])?;
    let elapsed = now.elapsed();
    let sec = (elapsed.as_secs() as f64) + (elapsed.subsec_nanos() as f64 / 1000_000.0);
    println!("find rocks: {} ms", sec);

    match res {
      Some(r) => {
        let docs: DocIds = DocIds::deserialize(r.deref());
        let elapsed = now.elapsed();
        let sec = (elapsed.as_secs() as f64) + (elapsed.subsec_nanos() as f64 / 1000_000.0);
        println!("fin: {} ms", sec);
        Ok(Some(docs))
      }
      None => Ok(None),
    }
  }
}
fn main() {
  let mut store = Store::open().unwrap();
  store
    .put(&Msg {
      name: String::from("test"),
    })
    .unwrap();
  for _i in 0..1000000 {
    let name = rand::thread_rng()
      .gen_ascii_chars()
      .take(10)
      .collect::<String>();
    store
      .put(&Msg {
        name: String::from(name),
      })
      .unwrap();
  }

  store.compact();
  for _i in 0..10000 {
    let name = rand::thread_rng()
      .gen_ascii_chars()
      .take(10)
      .collect::<String>();
    store.find(name.as_str()).unwrap();
  }

  let res = store.find("test").unwrap();
  match res {
    Some(docs) => println!("doc len {:?}", docs.0.len()),
    None => println!("none"),
  };

  println!("sleep");
  thread::sleep(Duration::from_secs(10));
}
