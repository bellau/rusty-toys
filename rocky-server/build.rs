extern crate protoc_rust_grpc;
use std::env;

fn main() {

    let args: Vec<_> = env::args().collect();
    println!("{:?}",args);
    protoc_rust_grpc::run(protoc_rust_grpc::Args {
        out_dir: "src",
        includes: &[],
        input: &["proto/data.proto"],
        rust_protobuf: true,
    }).expect("protoc-rust-grpc");
}