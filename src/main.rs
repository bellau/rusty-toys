extern crate rocky;

extern crate mailparse;
extern crate rand;

use std::string::String;
use std::fs::File;
use rocky::store::*;
mod mail;
use mailparse::MailHeaderMap;

fn main() {
    {
        let mut store = Store::open("/tmp/teststorage").unwrap();
        store.compact();

        println!("read some mails");

        let it = mail::iter::Iter::new(File::open("../test.mbox").unwrap());

        let mut buf: Vec<u8> = vec![];
        for entry in it {
            match entry {
                Err(error) => {
                    println!("{:?}", error);
                }
                Ok(e) => match e {
                    mail::iter::Entry::From(v) => {}
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

                                    let msg = Msg {
                                        subject: subject,
                                        from: from,
                                        text: text,
                                    };

                                    store.put(&msg).unwrap();
                                }
                                Err(e) => {
                                    println!("err mail {}", std::str::from_utf8(&buf[..]).unwrap());
                                }
                            }
                        }
                        //  println!("end {}", i);
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
}
