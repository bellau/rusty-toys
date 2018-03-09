extern crate bincode;
extern crate byteorder;
extern crate itertools;
extern crate rocksdb;
extern crate serde;
extern crate unicode_segmentation;

extern crate rand;

use bincode::{deserialize, serialize};
use rocksdb::{BlockBasedOptions, ColumnFamily, ColumnFamilyDescriptor, MergeOperands, Options, WriteBatch, DB};
use rocksdb::Error;
use std::ops::Deref;
use std::string::String;
use byteorder::{BigEndian, ByteOrder};

use itertools::kmerge;
use unicode_segmentation::UnicodeSegmentation;
use roaring::bitmap::RoaringBitmap;
use std::str;

struct DocId(u32);

impl DocId {
  fn parse(data: &[u8]) -> DocId {
    DocId(BigEndian::read_u32(data))
  }

  fn write(&self) -> Vec<u8> {
    let mut data = vec![0; 4];
    BigEndian::write_u32(&mut data[..], self.0);
    data
  }
}

pub struct Store {
  db: DB,
  index_cf: ColumnFamily,
  max_doc_id: DocId,
}

#[derive(PartialEq, Debug)]
pub struct Address(pub Option<String>, pub String);

#[derive(PartialEq, Debug)]
pub struct Msg {
  pub subject: Option<String>,
  pub from: Option<String>,
  pub text: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum StoreError {
  DbError(String),
}

struct DocIdsMsg(RoaringBitmap, RoaringBitmap);

impl DocIdsMsg {
  fn deserialize(data: &[u8]) -> DocIdsMsg {
    let a = BigEndian::read_u32(&data[0..4]) as usize;
    let r = BigEndian::read_u32(&data[4..8]) as usize;
    let add_buf = &data[8..(a + 8)];
    let remove_buf = &data[(a + 8)..(a + 8 + r)];
    DocIdsMsg(
      RoaringBitmap::deserialize_from(add_buf).unwrap(),
      RoaringBitmap::deserialize_from(remove_buf).unwrap(),
    )
  }

  fn serialize(&self) -> Vec<u8> {
    let a_size = self.0.serialized_size();
    let b_size = self.1.serialized_size();

    let mut data: Vec<u8> = Vec::with_capacity(a_size + b_size + 4 + 4);
    let mut aa: Vec<u8> = vec![0; 4];
    let mut bb: Vec<u8> = vec![0; 4];
    BigEndian::write_u32(&mut aa[..], a_size as u32);
    BigEndian::write_u32(&mut bb[..], b_size as u32);

    data.extend(aa);
    data.extend(bb);

    self.0.serialize_into(&mut data).unwrap();
    self.1.serialize_into(&mut data).unwrap();
    data
  }

  fn one(doc_id: &DocId) -> DocIdsMsg {
    let mut add = RoaringBitmap::default();
    add.insert(doc_id.0);
    DocIdsMsg(add, RoaringBitmap::default())
  }
}

pub type DocIds = Vec<u32>;

impl From<rocksdb::Error> for StoreError {
  fn from(e: Error) -> StoreError {
    StoreError::DbError(e.into())
  }
}

fn concat_merge(_new_key: &[u8], existing_val: Option<&[u8]>, operands: &mut MergeOperands) -> Option<Vec<u8>> {
  use std::time::Instant;
  let now = Instant::now();

  let mut add: Vec<RoaringBitmap> = Vec::new();
  let mut remove: Vec<RoaringBitmap> = Vec::new();
  let ops: Vec<&[u8]> = operands.collect();
  if let Some(existing_val) = existing_val {
    if ops.len() == 0 {
      return Some(existing_val.into());
    }
    let docs = DocIdsMsg::deserialize(existing_val);
    add.push(docs.0);
    remove.push(docs.1);
  }

  if ops.len() == 1 && add.len() == 0 && remove.len() == 0 {
    return Some(ops[0].into());
  }

  for op in ops.into_iter() {
    let docs = DocIdsMsg::deserialize(op);
    add.push(docs.0);
    remove.push(docs.1)
  }

  let elapsed = now.elapsed();
  let sec = (elapsed.as_secs() as f64) + (elapsed.subsec_nanos() as f64 / 1000_000.0);
 // println!("merge {}ms", sec);

  let mut ret = RoaringBitmap::default();
  for a in add {
    ret.union_with(&a);
  }
  for r in remove {
    ret.difference_with(&r);
  }
  let sr = DocIdsMsg(ret, RoaringBitmap::default()).serialize();
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

  pub fn open(path: &str) -> Result<Store, StoreError> {
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
      index_cf: match db.cf_handle("index") {
        Some(i) => i,
        None => panic!(""),
      },
      db: db,
      max_doc_id: max,
    })
  }

  fn next_doc(&mut self) -> Result<DocId, StoreError> {
    let max = DocId(self.max_doc_id.0 + 1);

    self.db.put(b"max_doc_id", &max.write()[..])?;
    self.max_doc_id = max;
    Ok(DocId(self.max_doc_id.0))
  }

  fn shred_text(&self, batch: &mut WriteBatch, doc_id: &DocId, name: &str, value: &str) -> Result<(), StoreError> {
    let base_key = format!("msg#{}#", name);
    for s in value.unicode_words() {
      let mut key: Vec<u8> = Vec::with_capacity(base_key.len() + s.len());
      key.extend(base_key.as_bytes());
      key.extend(s.as_bytes());
      batch.merge_cf(
        self.index_cf,
        &key[..],
        &DocIdsMsg::one(doc_id).serialize()[..],
      )?;
    }
    Ok(())
  }

  fn shred_string(&self, batch: &mut WriteBatch, doc_id: &DocId, name: &str, value: &str) -> Result<(), StoreError> {
    let base_key = format!("msg#{}#", name);
    let mut key: Vec<u8> = Vec::with_capacity(base_key.len() + value.len());
    key.extend(base_key.as_bytes());
    key.extend(value.as_bytes());
    batch.merge_cf(
      self.index_cf,
      &key[..],
      &DocIdsMsg::one(doc_id).serialize()[..],
    )?;

    Ok(())
  }

  fn shred(&self, batch: &mut WriteBatch, doc_id: &DocId, msg: &Msg) -> Result<(), StoreError> {
    let from = msg.from.as_ref();
    if let Some(from) = from {
      self.shred_text(batch, doc_id, "from", from)?;
    }

    self.shred_text(batch, doc_id, "body", &msg.text)?;
    let subject = msg.subject.as_ref();
    if let Some(subject) = subject {
      self.shred_text(batch, doc_id, "subject", subject)
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
    key.extend(b"msg#body#".iter());
    key.extend(name.as_bytes());
    use std::time::Instant;
    let now = Instant::now();

    let res = self.db.get_cf(self.index_cf, &key[..])?;
    let elapsed = now.elapsed();
    let sec = (elapsed.as_secs() as f64) + (elapsed.subsec_nanos() as f64 / 1000_000.0);
    println!("find rocks: {} ms", sec);

    match res {
      Some(r) => {
        let docs: DocIdsMsg = DocIdsMsg::deserialize(r.deref());

        let elapsed = now.elapsed();
        let sec = (elapsed.as_secs() as f64) + (elapsed.subsec_nanos() as f64 / 1000_000.0);
        println!("fin: {} ms", sec);
        Ok(Some(docs.0.iter().collect()))
      }
      None => Ok(None),
    }
  }
}
