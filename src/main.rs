extern crate mailbox;
extern crate rocky;

extern crate rand;

use std::thread;
use std::time::Duration;

use std::ops::Deref;
use std::string::String;
use rand::Rng;
use std::fs::File;
use rocky::*;

fn main() {
  let mut store = Store::open("/tmp/teststorage").unwrap();
  store.compact();
  for _i in 0..1 {
    let name = rand::thread_rng()
      .gen_ascii_chars()
      .take(10)
      .collect::<String>();
    store.find_by_name(name.as_str()).unwrap();
  }

  let res = store.find_by_name("test").unwrap();
  match res {
    Some(docs) => println!("doc len {:?}", docs.0.len()),
    None => println!("none"),
  };

  println!("read some mails");
  let mbox = mailbox::read(File::open("../test.mbox").unwrap());
  let mut i = 0;
    
  for mail in mbox {
    if i > 10000 {
      break;
    }
    match mail {
      Ok(m) => {
        let h = m.headers();
        let msg = Msg {
          subject: match h.get::<mailbox::header::Subject>() {
            Some(Ok(sub)) => {
              Some(String::from(sub.deref()))
            }
            _ => None,
          },
          from:    match h.get::<mailbox::header::From>() {
            Some(Ok(from)) => Some(Address(
              from.name().map(String::from),
              format!("{}@{}", from.user(), from.host().unwrap_or_else(|| "xxx")),
            )),
            _ => None,
          },
        };
        store.put(&msg).unwrap();
        i+=1;
      }
      _ => {}
    }
  }
  store.compact();

 // for _i in 0..1 {
    let res = store.find_by_name("toto@test.com").unwrap();
    match res {
      Some(docs) => {
        println!("doc len {:?}", docs.0.len());
        for d in docs.0 {
          println!("t {:?}",d);
        };
      },
      None => println!("none"),
    };

//  }
  println!("sleep");

  thread::sleep(Duration::from_secs(10));
}
