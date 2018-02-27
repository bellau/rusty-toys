extern crate sgf_parser;
use sgf_parser::node_parser;

fn main() {
  let t = &b";AB[1][2] CD[4][3]\0"[..];
  let res = node_parser(t);
  match res {
    Ok((_, v)) => println!("hello {:?}",v),
    Err(error) => println!("bad format {:?}", error),
  }
}