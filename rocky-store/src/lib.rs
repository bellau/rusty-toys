extern crate bincode;
extern crate byteorder;
extern crate itertools;
extern crate rocksdb;
extern crate serde;

#[macro_use]
extern crate serde_derive;

extern crate rand;

use bincode::{deserialize, serialize};
use rocksdb::{BlockBasedOptions, ColumnFamily, ColumnFamilyDescriptor, MergeOperands, Options, WriteBatch, DB};
use rocksdb::Error;
use std::ops::Deref;
use std::string::String;
use byteorder::{BigEndian, ByteOrder};

use itertools::kmerge;

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

pub struct Store {
  db:         DB,
  index_cf:   ColumnFamily,
  max_doc_id: DocId,
}

#[derive(PartialEq, Debug)]
pub struct Address(pub Option<String>, pub String);

#[derive(PartialEq, Debug)]
pub struct Msg {
  pub subject: Option<String>,
  pub from:    Option<Address>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum StoreError {
  DbError(String),
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct DocIds(pub Vec<u32>, pub Vec<u32>);

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
  if let Some(existing_val) = existing_val {
    if ops.len() == 0 {
      return Some(existing_val.into());
    }
    let docs = DocIds::deserialize(existing_val);
    add.push(docs.0);
    remove.push(docs.1);
  }

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
  fn default_options() -> Options {
    let mut dopts = Options::default();
    dopts.set_merge_operator("test", concat_merge, None);
    dopts.create_if_missing(true);
    dopts.set_report_bg_io_stats(true);
    dopts.enable_statistics();
    dopts.set_stats_dump_period_sec(10);
    let mut bb_opts = BlockBasedOptions::default();
    bb_opts.set_lru_cache(10_000);
    bb_opts.set_cache_index_and_filter_blocks(true);
    dopts.set_block_based_table_factory(&bb_opts);
    dopts
  }

  fn index_options() -> Options {
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
    opts
  }

  pub fn open(path : &str) -> Result<Store, StoreError> {
    let mut gopts = Options::default();

    gopts.set_merge_operator("test", concat_merge, None);
    gopts.create_if_missing(true);
    gopts.create_missing_column_families(true);
    gopts.set_report_bg_io_stats(true);
    gopts.enable_statistics();
    gopts.set_stats_dump_period_sec(1);
    let mut bb_opts = BlockBasedOptions::default();
    bb_opts.set_lru_cache(1_000);
    bb_opts.set_cache_index_and_filter_blocks(true);
    gopts.set_block_based_table_factory(&bb_opts);

    let default_cf = ColumnFamilyDescriptor::new("default", Store::default_options());
    let index_cf = ColumnFamilyDescriptor::new("index", Store::index_options());
    let db = DB::open_cf_descriptors(&gopts, path, vec![default_cf, index_cf])?;
    let max = match db.get(b"max_doc_id")? {
      Some(x) => DocId::parse(x.deref()),
      None => DocId(1),
    };
    println!("max value {}", max.0);
    Ok(Store {
      index_cf:   match db.cf_handle("index") {
        Some(i) => i,
        None => panic!(""),
      },
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

  fn shred_string(&self, batch: &mut WriteBatch, doc_id: &DocId, name: &str, value: &str) -> Result<(), StoreError> {
    let mut key: Vec<u8> = Vec::new();
    key.extend(format!("msg#{}#", name).as_bytes());
    key.extend(value.as_bytes());
    let docs = DocIds(vec![doc_id.0], vec![]);
    batch.merge_cf(self.index_cf, &key[..], &docs.serialize()[..])?;
    Ok(())
  }

  fn shred(&self, batch: &mut WriteBatch, doc_id: &DocId, msg: &Msg) -> Result<(), StoreError> {
    let from = msg.from.as_ref();
    if let Some(from) = from {
      match from.0 {
        Some(ref user) => {
          self.shred_string(batch, doc_id, "from", user)?;
        }
        _ => {}
      };

      self.shred_string(batch, doc_id, "from", from.1.as_ref())?;
    }

    let subject = msg.subject.as_ref();
    if let Some(subject) = subject {
      self.shred_string(batch, doc_id, "subject", subject)
    } else {
      Ok(())
    }
  }

  pub fn put(&mut self, msg: &Msg) -> Result<(), StoreError> {
    let doc_id = self.next_doc()?;
    let mut batch = WriteBatch::default();
    self.shred(&mut batch, &doc_id, msg)?;
    self.db.write(batch)?;
    Ok(())
  }

  pub fn compact(&self) {
    self.db.compact_range_cf(self.index_cf, None, None);
  }

  pub fn find_by_name(&self, name: &str) -> Result<Option<DocIds>, StoreError> {
    let mut key = Vec::new();
    key.extend(b"msg#from#".iter());
    key.extend(name.as_bytes());
    use std::time::Instant;
    let now = Instant::now();

    let res = self.db.get_cf(self.index_cf, &key[..])?;
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
