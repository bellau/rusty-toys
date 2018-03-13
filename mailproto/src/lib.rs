extern crate prost;
#[macro_use]
extern crate prost_derive;

pub mod mail {
    include!(concat!(env!("OUT_DIR"), "/mail.rs"));
}
