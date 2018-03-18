extern crate rocky;

extern crate futures;
extern crate grpc;
extern crate mailparse;
extern crate protobuf;
extern crate rocky_server;

use std::string::String;
use std::fs::File;
use rocky::store::*;
mod mail;
use mailparse::MailHeaderMap;

use futures::Future;

use futures::future::{loop_fn, Loop, LoopFn};
use rocky_server::rockyproto::*;
use rocky_server::rockyproto_grpc::*;
use grpc::RequestOptions;

use protobuf::repeated::RepeatedField;

struct MailIterator {
    it: mail::iter::Iter<File>,
}

impl Iterator for MailIterator {
    type Item = Message;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buf: Vec<u8> = vec![];
        loop {
            let entry = self.it.next();
            if let Some(entry) = entry {
                match entry {
                    Err(error) => {
                        println!("{:?}", error);
                    }
                    Ok(e) => match e {
                        mail::iter::Entry::From(_) => {}
                        mail::iter::Entry::Body(b) => {
                            buf.extend(b);
                            buf.push(b'\r');
                            buf.push(b'\n');
                        }
                        mail::iter::Entry::End => {
                            let mail = mailparse::parse_mail(&buf[..]);
                            match mail {
                                Ok(m) => {
                                    let mut text = String::new();
                                    if m.ctype.mimetype == "text/plain" {
                                        text.push_str(&m.get_body().unwrap());
                                    }
                                    for p in m.subparts {
                                        if p.ctype.mimetype == "text/plain" {
                                            text.push_str(&p.get_body().unwrap());
                                        }
                                    }

                                    let mut t = Message::new();
                                    let mut headers = vec![];
                                    for h in m.headers {
                                        let mut mh = MessageHeader::new();
                                        mh.set_name(h.get_key().unwrap());
                                        mh.set_value(h.get_value().unwrap());
                                        headers.push(mh);
                                    }
                                    t.set_headers(RepeatedField::from_vec(headers));
                                    return Some(t);
                                }
                                Err(_) => {
                                    println!("err mail {}", std::str::from_utf8(&buf[..]).unwrap());
                                }
                            }
                        }
                    },
                }
            } else {
                return None;
            }
        }
    }
}

fn main() {
    use grpc::Client;

    use rocky_server::rockyproto::*;
    use rocky_server::rockyproto_grpc::*;
    use std::collections::HashMap;
    let c = Client::new_plain("localhost", 50051, Default::default()).unwrap();
    let client = MessageStoreClient::with_client(c.clone());
    let res = client
        .collections(RequestOptions::new(), CollectionsRequest::new())
        .wait_drop_metadata();
    //.unwrap();
    let cols = if let Ok(ref res) = res {
        res.get_collections()
    } else {
        &[]
    };
    let mut collections: HashMap<String, u32> = HashMap::new();
    for col in cols {
        collections.insert(col.get_name().to_string(), col.get_id());
        println!("existing coll {}", col.get_id());
    }
    let it = MailIterator {
        it: mail::iter::Iter::new(File::open("../test.mbox").unwrap()),
    };

    for mut msg in it {
        let mut collection_ids = vec![];
        for h in msg.clone().get_headers() {
            if h.name == "X-Gmail-Labels" {
                let slabels = h.value.split(",");
                for l in slabels {
                    if !collections.contains_key(l) {
                        let mut cc = CreateCollectionRequest::new();
                        cc.set_name(l.to_string());
                        let r = client
                            .create_collection(RequestOptions::new(), cc)
                            .wait_drop_metadata()
                            .unwrap();
                        let col = r.get_collection();
                        collections.insert(col.get_name().to_string(), col.get_id());
                    }
                    collection_ids.push(collections[l]);
                }
            }
            let mut putr = PutRequest::new();
            putr.set_collections(collection_ids.clone());
            putr.set_msg(msg.clone());
            client
                .put(RequestOptions::new(), putr)
                .wait_drop_metadata()
                .unwrap();
        }
    }
}
