
#[macro_use]
extern crate nom;


use nom::{alphanumeric, space};



named!(name_parser<&[u8],&str>,
    do_parse!(
        tag_s!("i am") >>
        opt!(space) >>
        v : map_res!(alphanumeric, std::str::from_utf8) >>
        (v)
    )
);

fn main() {
    let t = &b"i am world
    "[..];
    let res = name_parser(t);
    match res {
        Ok(name) => println!("hello {}", name.1),
        Err(error) => println!("bad format {:?}", error),
    }

}
