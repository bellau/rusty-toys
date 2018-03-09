use std::convert::TryFrom;
use std::convert::TryInto;

// borrowed code from https://gitlab.com/goboom/sgf
#[derive(Debug)]
pub enum SgfParseError {
    GeneralParseError,
    IncompleteInput,
    InvalidMoveCoordinate(String),
}

#[derive(PartialEq, Debug)]
pub struct GameTree {
    pub nodes: Vec<Node>,
    pub gametrees: Vec<GameTree>,
}

pub type Node = Vec<Property>;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Color {
    Black,
    White,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct Coordinate(pub usize, pub usize);

#[derive(Debug, PartialEq)]
pub enum Property {
    Move(Color, Coordinate),
    Pass(Color),
    Comment(String),
    Unknown(String, Vec<String>),
}

impl Property {
    pub(crate) fn try_new(id: PropertyIdent, args: Vec<String>) -> Result<Self, SgfParseError> {
        match id {
            PropertyIdent::Black => {
                let color = Color::Black;
                // TODO: Coordinates tt are also considered a pass on boards <= 19x19
                if args.len() != 1 {
                    return Err(SgfParseError::GeneralParseError);
                }
                let coord = &args[0][..];
                if coord.is_empty() {
                    Ok(Property::Pass(color))
                } else {
                    Ok(Property::Move(color, coord.try_into()?))
                }
            }
            PropertyIdent::White => {
                let color = Color::White;
                if args.len() != 1 {
                    return Err(SgfParseError::GeneralParseError);
                }
                let coord = &args[0][..];
                if coord.is_empty() {
                    Ok(Property::Pass(color))
                } else {
                    Ok(Property::Move(color, coord.try_into()?))
                }
            }
            PropertyIdent::Comment => Ok(Property::Comment(args[0].to_owned())),
            PropertyIdent::Unknown(id_str) => Ok(Property::Unknown(id_str, args)),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum PropertyIdent {
    White,
    Black,
    Comment,
    Unknown(String),
}

impl<S> From<S> for PropertyIdent
where
    S: Into<String>,
{
    fn from(s: S) -> PropertyIdent {
        let s = s.into();
        match s.as_ref() {
            "W" => PropertyIdent::White,
            "B" => PropertyIdent::Black,
            "C" => PropertyIdent::Comment,
            _ => PropertyIdent::Unknown(s.to_owned()),
        }
    }
}

fn char_code_to_coordinate(code: u8) -> Option<usize> {
    if (b'a' <= code) && (b'z' >= code) {
        Some((code - b'a') as usize)
    } else {
        None
    }
}

impl<'a> TryFrom<&'a str> for Coordinate {
    type Error = SgfParseError;
    fn try_from(s: &'a str) -> Result<Coordinate, SgfParseError> {
        match *s.as_bytes() {
            [x, y] => {
                if let (Some(x), Some(y)) = (char_code_to_coordinate(x), char_code_to_coordinate(y)) {
                    Ok(Coordinate(x, y))
                } else {
                    Err(SgfParseError::InvalidMoveCoordinate(s.to_owned()))
                }
            }
            [] | [_..] => Err(SgfParseError::InvalidMoveCoordinate(s.to_owned())),
        }
    }
}
