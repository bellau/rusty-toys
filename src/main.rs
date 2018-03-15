extern crate rocky;

extern crate mailparse;
extern crate rand;

use std::string::String;
use std::fs::File;
use rocky::store::*;
mod mail;
use mailparse::MailHeaderMap;

fn main() {
    use std::collections::HashMap;
    let mut collections: HashMap<String, u32> = HashMap::new();

    {
        let mut store = Store::open("/tmp/teststorage").unwrap();
        store.compact();

        let colls = store.collections().unwrap();
        for col in colls {
            collections.insert(col.1.clone(), col.0);
            println!("existing coll {}", col.1);
        }

        println!("read some mails");
        let it = mail::iter::Iter::new(File::open("../test.mbox").unwrap());

        let mut buf: Vec<u8> = vec![];
        for entry in it {
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
                        {
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
                                    let subject = m.headers.get_first_value("Subject").unwrap();
                                    let from = m.headers.get_first_value("From").unwrap();
                                    let date = m.headers.get_first_value("Date").unwrap();
                                    let labels = m.headers.get_first_value("X-Gmail-Labels").unwrap();
                                    let mut collection_ids = vec![];
                                    if let Some(l) = labels {
                                        let slabels = l.split(",");
                                        for l in slabels {
                                            if !collections.contains_key(l) {
                                                let col = store.create_collection(l.to_string()).unwrap();
                                                collections.insert(col.1, col.0);
                                            }
                                            collection_ids.push(collections[l]);
                                        }
                                    }

                                    let date = if let Some(ds) = date {
                                        mailparse::dateparse(&ds).unwrap_or_else(|_| 0)
                                    } else {
                                        0
                                    };

                                    let msg = Msg {
                                        subject: subject,
                                        from: from,
                                        text: text,
                                        date: date,
                                        collections: collection_ids,
                                        eml : buf[..].to_vec()
                                    };

                                    store.put(&msg).unwrap();
                                }
                                Err(_) => {
                                    println!("err mail {}", std::str::from_utf8(&buf[..]).unwrap());
                                }
                            }
                        }
                        buf.clear();
                    }
                },
                _ => {}
            }
        }
        store.compact();
    }

    let store = Store::open("/tmp/teststorage").unwrap();
    store.compact();
    for _i in 0..100 {
        let res = store.find_by_name("test").unwrap();
        match res {
            Some(docs) => {
                println!("doc len {:?}", docs.len());
            }
            None => println!("none"),
        };
    }

    let res = store.find_by_name("test").unwrap();
    let docs = if let Some(res) = res {
        res
    } else {
        DocIdSet::default()
    };

    let res = store.find_by_name("of").unwrap();

    let docs = if let Some(res) = res {
        docs & res
    } else {
        docs
    };

    let res = store.find_by_name("mine").unwrap();

    let docs = if let Some(res) = res {
        docs & res
    } else {
        docs
    };

    println!("doc len {:?}", docs.len());
    for d in store.iterate_date().unwrap() {
        let t = d.1 & &docs;
        if t.len() > 0 {
            println!("date {}", d.0);
        }
    }

    let res = store
        .find_by_col(collections[&"Non lus".to_string()])
        .unwrap();
    if let Some(res) = res {
        println!("doc len {:?} unread", docs.len());
    } else {
        println!("no unread");
    }
}
