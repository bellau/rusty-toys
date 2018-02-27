
#![feature(slice_patterns, try_from, test)]
#[macro_use]
extern crate nom;

extern crate test;

mod types;

use types::*;

named!(propident_parser<&[u8],PropertyIdent>,
    map!(map_res!(take_while1!(|ch| ch >= b'A' && ch <= b'Z'), std::str::from_utf8), PropertyIdent::from)
);

named!(propvalue_parser<&[u8],String>,
    ws!(delimited!(
            char!('['),
            map_res!(
                escaped!(take_while1!(|chr| chr != b'"' && chr != b']' && chr != b'\\'), '\\', one_of!("]\\:")),
                |s:&[u8]| (String::from_utf8(s.to_vec()))
            ),
            char!(']')
        )
    )
);

named!(property_parser<&[u8],Property>,
    ws!(
        map_res!(
             do_parse!(
        id  :   propident_parser >>
        v   :   many1!(propvalue_parser) >>
        (id,v)), 
        |(id,v)| Property::try_new(id,v))
    )
);

named!(node_parser<&[u8],Node>,
    ws!(do_parse!(
        tag!(";") >>
        v : many1!(property_parser) >>
        (v)
    ))
);

named!(gametree_parser<&[u8],GameTree>,
    ws!(do_parse!(
     tag!("(") >>
      nodes : many0!(node_parser) >>
      gametrees : many0!(gametree_parser) >>
      tag!(")") >>
      (GameTree{ nodes : nodes ,gametrees : gametrees})
    ))
);

fn main() {
  let t = &b";AB[1][2] CD[4][3]\0"[..];
  let res = node_parser(t);
  match res {
    Ok((_, v)) => println!("hello {:?}",v),
    Err(error) => println!("bad format {:?}", error),
  }
}

#[cfg(test)]
mod tests {

  #[test]
  fn test_propident_parser() {
    let propident_res = ::propident_parser(&b"AB\0"[..]);
    assert_eq!(propident_res.unwrap().1, ::PropertyIdent::from("AB"));
  }

  #[test]
  fn test_propvalue_parser() {
    let propvalue_res = ::propvalue_parser(&b"[AB \\]3]\0"[..]);
    assert_eq!(propvalue_res.unwrap().1, "AB \\]3");
  }

  #[test]
  fn test_property_parser() {
    let prop_res = ::property_parser(&b"AB[12] [33] [f\\]jfj]\0"[..]);
    assert_eq!(prop_res.unwrap().1,
    ::Property::Unknown(String::from("AB"),vec!(String::from("12"),String::from("33"),String::from("f\\]jfj"))));
  }

  #[test]
  fn test_node_parser() {
    let prop_res = ::node_parser(&b";AB[12][bb]CD[aa]\0"[..]);
    assert_eq!(prop_res.unwrap().1,
        vec!( 
            ::Property::Unknown(String::from("AB"),vec!(String::from("12"),String::from("bb"))),
            ::Property::Unknown(String::from("CD"),vec!(String::from("aa")))));
  }

  #[test]
  fn test_gametree_parser() {
    let gametree_res = ::gametree_parser(&b"(;AB[12][bb]CD[aa])\0"[..]);
    assert_eq!(gametree_res.unwrap().1,
        ::GameTree { nodes : 
         vec!(vec!( 
            ::Property::Unknown(String::from("AB"),vec!(String::from("12"),String::from("bb"))),
            ::Property::Unknown(String::from("CD"),vec!(String::from("aa")))))
        , gametrees : vec!()});
  }

}
