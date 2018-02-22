
#[macro_use]
extern crate nom;

named!(propident_parser<&[u8],&str>,
    do_parse!(
      v : take_while!(|ch| ch >= b'A' && ch <= b'Z') >>
          (std::str::from_utf8(v).unwrap())
    )
);

named!(propvalue_parser<&[u8],&str>,
    do_parse!(
        v : delimited!(
            char!('['),
            map_res!(
                escaped!(take_while1!(|chr| chr != b'"' && chr != b']' && chr != b'\\'), '\\', one_of!("]\\:")),
                std::str::from_utf8
            ),
            char!(']')
        ) >>
        (v)
    )
);

named!(property_parser<&[u8], (&str, Vec<&str>)>,
    ws!(do_parse!(
        id : propident_parser >>
        v :   many0!(propvalue_parser) >>
        ((id,v))
    ))
);

named!(node_parser<&[u8],Vec<(&str,Vec<&str>)>>,
    ws!(do_parse!(
        tag!(";") >>
        v : many0!(property_parser) >>
        (v)
    ))
);

#[derive(Debug)]
struct GameTree<'a> {
  nodes: Vec<Vec<(&'a str, Vec<&'a str>)>>,
  gametrees: Vec<GameTree<'a>>,
}


named!(sequence_parser<&[u8],Vec<Vec<(&str,Vec<&str>)>>>,
    many0!(node_parser)
);

named!(gametree_parser<&[u8],GameTree>,
    do_parse!(
     tag!("(") >>
      nodes : many0!(node_parser) >>
      gametrees : many0!(gametree_parser) >>
      tag!(")") >>
      (GameTree{ nodes : nodes ,gametrees : gametrees})
    )
);

fn main() {
  let t = &b";AB[1][2] CD[4][3]\0"[..];
  let res = sequence_parser(t);
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
    assert_eq!(propident_res.unwrap().1, "AB");
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
        ("AB",vec!("12","33","f\\]jfj")));
  }

  #[test]
  fn test_node_parser() {
    let prop_res = ::node_parser(&b";AB[12][bb]CD[aa]\0"[..]);
    assert_eq!(prop_res.unwrap().1,
        vec!( ("AB",vec!("12","bb")), ("CD", vec!("aa"))));
  }

}
