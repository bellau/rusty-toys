extern crate byteorder;
extern crate rocksdb;
extern crate unicode_segmentation;

extern crate rand;

use rocksdb::{BlockBasedOptions, ColumnFamily, ColumnFamilyDescriptor, MergeOperands, Options, WriteBatch, DB};
use rocksdb::Error;
use std::ops::Deref;
use std::string::String;
use byteorder::{BigEndian, ByteOrder};

use unicode_segmentation::UnicodeSegmentation;
use roaring::bitmap::RoaringBitmap;
use std::str;

pub type DocIdSet = RoaringBitmap;

struct DocId(u32);
pub struct Collection(pub u32, pub String);

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

impl Iterator for StoreIt {
    type Item = (i64, DocIdSet);

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.0.next();
        if next.is_none() {
            return None;
        }

        let next = next.unwrap();
        if next.0.len() < "msg#date#".len() {
            return None;
        }
        let f = &next.0[.."msg#date#".len()];
        if f != &b"msg#date#"[..] {
            return None;
        }
        let date = &next.0["msg#date#".len()..];
        let d = BigEndian::read_i64(date);
        let docs: DocIdsMsg = DocIdsMsg::deserialize(&next.1);

        Some((d, docs.0))
    }
}

use std::sync::RwLock;
use std::sync::atomic::{AtomicIsize, Ordering};
use std::collections::HashMap;

unsafe impl Send for Store {}

pub struct Store {
    db: DB,
    /*
    index_cf: ColumnFamily,
    eml_cf: ColumnFamily,
    mod_cf: ColumnFamily,*/
    max_doc_id: AtomicIsize,
    modseq_max: AtomicIsize,
    cols: RwLock<(HashMap<u32, String>, HashMap<String, u32>)>,
}

#[derive(PartialEq, Debug)]
pub struct Address(pub Option<String>, pub String);

#[derive(PartialEq, Debug)]
pub struct Msg {
    pub subject: Option<String>,
    pub from: Option<String>,
    pub text: String,
    pub date: i64,
    pub eml: Vec<u8>,
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


#[derive(Debug, Clone, PartialEq)]
pub enum StoreError {
    DbError(String),
}

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

pub struct StoreIt(rocksdb::DBIterator);

use std::fmt::{Debug, Formatter, Result as FmtResult};
impl Debug for Store {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "hell")
    }
}
impl Store {
    fn default_options() -> Options {
        let mut dopts = Options::default();
        dopts.set_merge_operator("test", concat_merge, None);
        let mut bb_opts = BlockBasedOptions::default();
        bb_opts.set_lru_cache(10_000);
        bb_opts.set_cache_index_and_filter_blocks(true);
        dopts.set_block_based_table_factory(&bb_opts);
        dopts
    }

    fn eml_options() -> Options {
        let mut dopts = Options::default();
        dopts.set_merge_operator("test", concat_merge, None);
        let mut bb_opts = BlockBasedOptions::default();
        bb_opts.set_lru_cache(10_000);
        dopts.set_block_based_table_factory(&bb_opts);
        dopts.set_compression_per_level(&[
            rocksdb::DBCompressionType::None,
            rocksdb::DBCompressionType::Snappy,
            rocksdb::DBCompressionType::Snappy,
        ]);

        dopts
    }

    fn index_options() -> Options {
        let mut dopts = Options::default();
        dopts.set_merge_operator("test", concat_merge, None);
        let mut bb_opts = BlockBasedOptions::default();
        bb_opts.set_lru_cache(100_000_000);
        bb_opts.set_cache_index_and_filter_blocks(true);
        dopts.set_block_based_table_factory(&bb_opts);
        dopts
    }

    pub fn open(path: &str) -> Result<Store, StoreError> {
        let mut gopts = Options::default();

        gopts.set_merge_operator("test", concat_merge, None);
        gopts.create_if_missing(true);
        gopts.create_missing_column_families(true);
        gopts.set_report_bg_io_stats(true);
        gopts.enable_statistics();
        gopts.set_stats_dump_period_sec(1);

        let default_cf = ColumnFamilyDescriptor::new("default", Store::default_options());
        let index_cf = ColumnFamilyDescriptor::new("index", Store::index_options());
        let col_cf = ColumnFamilyDescriptor::new("col", Store::index_options());
        let eml_cf = ColumnFamilyDescriptor::new("eml", Store::eml_options());

        let mod_cf = ColumnFamilyDescriptor::new("mod", Store::index_options());
        let db = DB::open_cf_descriptors(
            &gopts,
            path,
            vec![default_cf, index_cf, col_cf, mod_cf, eml_cf],
        )?;
        let mod_cf = match db.cf_handle("mod") {
            Some(i) => i,
            None => panic!(""),
        };
        let max = match db.get(b"max_doc_id")? {
            Some(x) => DocId::parse(x.deref()),
            None => DocId(1),
        };

        let modseq_max = match db.get_cf(mod_cf, b"modseq_max")? {
            Some(x) => BigEndian::read_u64(x.deref()),
            None => 1,
        };

        let cols = Store::collections_internal(&db)?;
        let mut id_name = HashMap::new();
        let mut name_id = HashMap::new();
        for col in cols {
            id_name.insert(col.0, col.1.clone());
            name_id.insert(col.1.clone(), col.0);
        }
        println!("max value {}", max.0);
        Ok(Store {
            /*
            index_cf: match db.cf_handle("index") {
                Some(i) => i,
                None => panic!(""),
            },
            mod_cf: mod_cf,
            eml_cf: match db.cf_handle("eml") {
                Some(i) => i,
                None => panic!(""),
            },*/
            db: db,
            max_doc_id: AtomicIsize::new(max.0 as isize),
            modseq_max: AtomicIsize::new(modseq_max as isize),
            cols: RwLock::new((id_name, name_id)),
        })
    }

    fn next_modseq(&self) -> Result<u64, StoreError> {
        let max = self.modseq_max.fetch_add(1, Ordering::SeqCst) as u64;
        let mut data = vec![0; 8];
        BigEndian::write_u64(&mut data[..], max);

        ;
        self.db
            .put_cf(self.db.cf_handle("mod").unwrap(), b"modseq_max", &data[..])?;
        Ok(max)
    }

    fn next_doc(&self) -> Result<DocId, StoreError> {
        let max = DocId(self.max_doc_id.fetch_add(1, Ordering::SeqCst) as u32);

        self.db.put(b"max_doc_id", &max.write()[..])?;
        Ok(max)
    }

    fn shred_text(&self, batch: &mut WriteBatch, doc_id: &DocId, name: &str, value: &str) -> Result<(), StoreError> {
        let base_key = format!("msg#{}#", name);
        for s in value.unicode_words() {
            let mut key: Vec<u8> = Vec::with_capacity(base_key.len() + s.len());
            key.extend(base_key.as_bytes());
            key.extend(s.as_bytes());
            batch.merge_cf(
                self.db.cf_handle("index").unwrap(),
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
            self.db.cf_handle("index").unwrap(),
            &key[..],
            &DocIdsMsg::one(doc_id).serialize()[..],
        )?;

        Ok(())
    }

    fn shred_date(&self, batch: &mut WriteBatch, doc_id: &DocId, name: &str, value: i64) -> Result<(), StoreError> {
        let base_key = format!("msg#{}#", name);
        let mut key: Vec<u8> = Vec::with_capacity(base_key.len() + 8);
        key.extend(base_key.as_bytes());

        let mut v: Vec<u8> = vec![0; 8];
        BigEndian::write_i64(&mut v, value);
        key.extend(&v[..]);
        batch.merge_cf(
            self.db.cf_handle("index").unwrap(),
            &key[..],
            &DocIdsMsg::one(doc_id).serialize()[..],
        )?;

        Ok(())
    }

    fn shred_collections(&self, batch: &mut WriteBatch, doc_id: &DocId, collections: &Vec<u32>) -> Result<(), StoreError> {
        let base_key = "msg#cols#";
        for col in collections {
            let mut key: Vec<u8> = Vec::with_capacity(base_key.len() + 4);
            key.extend(base_key.as_bytes());

            let mut v: Vec<u8> = vec![0; 4];
            BigEndian::write_u32(&mut v, *col);
            key.extend(&v[..]);
            batch.merge_cf(
                self.db.cf_handle("index").unwrap(),
                &key[..],
                &DocIdsMsg::one(doc_id).serialize()[..],
            )?;
        }
        Ok(())
    }

    fn shred(&self, batch: &mut WriteBatch, doc_id: &DocId, msg: &Msg) -> Result<(), StoreError> {
        let from = msg.from.as_ref();
        if let Some(from) = from {
            self.shred_text(batch, doc_id, "from", from)?;
        }

        self.shred_text(batch, doc_id, "body", &msg.text)?;

        self.shred_date(batch, doc_id, "date", msg.date)?;
        let subject = msg.subject.as_ref();
        if let Some(subject) = subject {
            self.shred_text(batch, doc_id, "subject", subject)
        } else {
            Ok(())
        }
    }

    pub fn modify(&self, msgs: Vec<u8>, added_collections: Vec<u32>, removed_collections: Vec<u32>) -> Result<(), StoreError> {
        for msg in msgs {}
        Ok(())
    }

    fn next_col_id(&self, col: u32) -> Result<u32, StoreError> {
        let mut key = Vec::new();
        key.extend(b"col_seq#".iter());
        let mut v: Vec<u8> = vec![0; 4];
        BigEndian::write_u32(&mut v, col);
        key.extend(&v[..]);
        let value = self.db.get_cf(self.db.cf_handle("col").unwrap(),&key[..])?;
        let next_col_id = if let Some(value) = value {
            BigEndian::read_u32(&value) + 1
        } else {
            1
        };
        Ok(next_col_id)
    }

    pub fn put(&self, collections: &Vec<u32>, msg: &Msg) -> Result<(), StoreError> {
        let doc_id = self.next_doc()?;

        let mut batch = WriteBatch::default();

        // mod log
        let base_mod_key = "mod#";
        for col in collections {
            let col_id = self.next_col_id(*col)?;
            let modseq = self.next_modseq()?;
            let mut key: Vec<u8> = Vec::with_capacity(base_mod_key.len() + 8);
            key.extend(base_mod_key.as_bytes());

            let mut v: Vec<u8> = vec![0; 8];
            BigEndian::write_u64(&mut v, modseq);
            key.extend(&v[..]);

            let mut v: Vec<u8> = vec![0; 4];
            BigEndian::write_u32(&mut v, *col);
            key.extend(&v[..]);
            batch.put_cf(self.db.cf_handle("mod").unwrap(), &key[..], b"add")?;
        }
        self.shred_collections(&mut batch, &doc_id, collections)?;
        self.shred(&mut batch, &doc_id, msg)?;

        {
            let base_eml_key = "eml#";
            let mut key: Vec<u8> = Vec::with_capacity(base_eml_key.len() + 4);
            key.extend(&doc_id.write()[..]);
            batch.put_cf(self.db.cf_handle("eml").unwrap(), &key[..], &msg.eml[..])?;
        }
        self.db.write(batch)?;
        Ok(())
    }

    pub fn compact(&self) {
        self.db
            .compact_range_cf(self.db.cf_handle("index").unwrap(), None, None);
    }

    pub fn iterate_date(&self) -> Result<StoreIt, StoreError> {
        let mut key = Vec::new();
        key.extend(b"msg#date#".iter());

        use rocksdb::DBIterator;
        let it: DBIterator = self.db
            .prefix_iterator_cf(self.db.cf_handle("index").unwrap(), &key[..])?;
        Ok(StoreIt(it))
    }

    pub fn create_collection(&self, name: String) -> Result<Collection, StoreError> {
        let doc_id = self.next_doc()?;
        let mut key = Vec::new();
        key.extend(b"collections#".iter());

        let mut batch = WriteBatch::default();

        let v = doc_id.write();
        key.extend(&v[..]);

        batch.put_cf(
            self.db.cf_handle("col").unwrap(),
            &key[..],
            &name.as_bytes(),
        )?;
        self.db.write(batch)?;
        Ok(Collection(doc_id.0, name))
    }

    fn collections_internal(db: &DB) -> Result<Vec<Collection>, StoreError> {
        let mut key = Vec::new();
        key.extend(b"collections#".iter());

        let mut ret = vec![];
        use rocksdb::DBIterator;
        let it: DBIterator = db.prefix_iterator_cf(db.cf_handle("col").unwrap(), &key[..])?;
        for v in it {
            let k = v.0;
            if k.len() < b"collections#".len() || &k[0..b"collections#".len()] != b"collections#" {
                break;
            }

            ret.push(Collection(
                DocId::parse(&k[b"collections#".len()..]).0,
                str::from_utf8(&v.1).unwrap().to_string(),
            ))
        }
        Ok(ret)
    }

    pub fn collections(&self) -> Result<Vec<Collection>, StoreError> {
        Store::collections_internal(&self.db)
    }

    pub fn find_by_name(&self, name: &str) -> Result<Option<DocIdSet>, StoreError> {
        let mut key = Vec::new();
        key.extend(b"msg#body#".iter());
        key.extend(name.as_bytes());
        use std::time::Instant;
        let now = Instant::now();

        let res = self.db
            .get_cf(self.db.cf_handle("index").unwrap(), &key[..])?;
        let elapsed = now.elapsed();
        let sec = (elapsed.as_secs() as f64) + (elapsed.subsec_nanos() as f64 / 1000_000.0);
        println!("find rocks: {} ms", sec);

        match res {
            Some(r) => {
                let docs: DocIdsMsg = DocIdsMsg::deserialize(r.deref());

                let elapsed = now.elapsed();
                let sec = (elapsed.as_secs() as f64) + (elapsed.subsec_nanos() as f64 / 1000_000.0);
                println!("fin: {} ms", sec);
                Ok(Some(docs.0))
            }
            None => Ok(None),
        }
    }

    pub fn find_by_col(&self, col_id: u32) -> Result<Option<DocIdSet>, StoreError> {
        let mut key = Vec::new();
        key.extend(b"msg#cols#".iter());
        let mut v: Vec<u8> = vec![0; 4];
        BigEndian::write_u32(&mut v, col_id);
        key.extend(&v[..]);
        use std::time::Instant;
        let now = Instant::now();

        let res = self.db
            .get_cf(self.db.cf_handle("index").unwrap(), &key[..])?;
        let elapsed = now.elapsed();
        let sec = (elapsed.as_secs() as f64) + (elapsed.subsec_nanos() as f64 / 1000_000.0);
        println!("find rocks: {} ms", sec);

        match res {
            Some(r) => {
                let docs: DocIdsMsg = DocIdsMsg::deserialize(r.deref());

                let elapsed = now.elapsed();
                let sec = (elapsed.as_secs() as f64) + (elapsed.subsec_nanos() as f64 / 1000_000.0);
                println!("fin: {} ms", sec);
                Ok(Some(docs.0))
            }
            None => Ok(None),
        }
    }
}
