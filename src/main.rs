
#[macro_use]
extern crate nom;

use nom::{alphanumeric, multispace};

#[inline(always)]
pub fn is_uppercase(ch: u8) -> bool {
  ch >= b'A' && ch <= b'Z'
}

named!(propident_parser<&[u8],&str>,
    do_parse!(
    v : take_while!(is_uppercase) >>
    (std::str::from_utf8(v).unwrap())
    )
);

named!(propvalue_parser<&[u8],&str>,
    do_parse!(
        opt!(complete!(multispace)) >>
        v : delimited!(
            char!('['),
            map_res!(
                escaped!(call!(alphanumeric), '\\', one_of!("]")),
                std::str::from_utf8
            ),
            char!(']')
        ) >>
        (v)
    )
);

named!(property_parser<&[u8], (&str, Vec<&str>)>,
    do_parse!(
        opt!(complete!(multispace)) >>
        id : propident_parser >>
        v :   many0!(propvalue_parser) >>
        ((id,v))
    )
);

named!(node_parser<&[u8],Vec<(&str,Vec<&str>)>>,
    do_parse!(
        opt!(complete!(multispace)) >>
        tag!(";") >>
        v : many0!(property_parser) >>
        (v)
    )
);


named!(sequence_parser<&[u8],Vec<Vec<(&str,Vec<&str>)>>>,
    many0!(node_parser)
);

fn main() {
  let t = &b" ;AB[1][2] CD[4][3]\0"[..];
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
    let propvalue_res = ::propvalue_parser(&b"[AB]"[..]);
    assert_eq!(propvalue_res.unwrap().1, "AB");
  }

  #[test]
  fn test_property_parser() {
    let prop_res = ::property_parser(&b"AB[12] [33] [fjfj]\0"[..]);
    assert_eq!(prop_res.unwrap().1,
        ("AB",vec!("12","33","fjfj")));
  }

}
