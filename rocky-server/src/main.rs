extern crate futures;
extern crate futures_cpupool;
extern crate grpc;
extern crate protobuf;
extern crate rocky;
extern crate tls_api;

pub mod rockyproto;
pub mod rockyproto_grpc;

use std::sync::Arc;

use rocky::store::Store;

use rockyproto::*;
use rockyproto_grpc::*;
use std::cell::Cell;
use std::sync::Mutex;
use grpc::RequestOptions;

struct MessageStoreImpl {
    store: Store,
}

impl MessageStore for MessageStoreImpl {
    fn put(&self, o: ::grpc::RequestOptions, p: PutRequest) -> ::grpc::SingleResponse<PutResponse> {
        let cols = p.get_collections().to_vec();
        let sm = rocky::store::Msg {
            subject: None,
            from: None,
            text: "test".to_string(),
            date: 0,
            eml: b"".to_vec(),
        };

        self.store.put(&cols, &sm).unwrap();

        grpc::SingleResponse::completed(PutResponse::new())
    }

    fn collections(&self, o: ::grpc::RequestOptions, p: CollectionsRequest) -> ::grpc::SingleResponse<CollectionsResponse> {
        let cols = self.store.collections().unwrap();
        let ret = cols.iter()
            .map(|c| {
                let mut col = Collection::new();
                col.set_id(c.0);
                col.set_name(c.1.clone());
                col
            })
            .collect();

        let mut r = CollectionsResponse::new();
        r.set_collections(ret);
        grpc::SingleResponse::completed(r)
    }

    fn create_collection(&self, o: ::grpc::RequestOptions, p: CreateCollectionRequest) -> ::grpc::SingleResponse<CreateCollectionResponse> {
        let name = p.get_name();
        let c = self.store.create_collection(name.to_string()).unwrap();

        let mut col = Collection::new();
        col.set_id(c.0);
        col.set_name(c.1);

        let mut r = CreateCollectionResponse::new();
        r.set_collection(col);
        grpc::SingleResponse::completed(r)
    }
}

pub fn main() {
    use std::thread;

    let store = Store::open("/tmp/teststorage").unwrap();
    let storeServer = MessageStoreImpl { store: store };
    let mut server = grpc::ServerBuilder::new_plain();
    server.http.set_port(50051);
    server.add_service(MessageStoreServer::new_service_def(storeServer));
    server.http.set_cpu_pool_threads(4);
    let _server = server.build().expect("server");

    loop {
        thread::park();
    }
}
