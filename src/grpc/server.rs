use crate::db_store;
use grpc_minkv::store_server::Store;
use grpc_minkv::{
    DelRequest, DelResponse, ExistsRequest, ExistsResponse, GetRequest, GetResponse, GetSetRequest,
    GetSetResponse, Item, MGetRequest, MGetResponse, MSetRequest, MSetResponse, SetRequest,
    SetResponse,
};
use std::sync::{Arc, RwLock};
use tonic::{Request, Response, Status};
// use google::protobuf::Empty;
use log::*;

// pub mod google {
//     pub mod protobuf {
//         tonic::include_proto!("google.protobuf.empty");
//     }
// }

pub mod grpc_minkv {
    tonic::include_proto!("minkv");
}

pub struct StoreImpl {
    store: Arc<RwLock<dyn db_store::Op>>,
}

impl StoreImpl {
    pub fn new(store: Arc<RwLock<dyn db_store::Op>>) -> StoreImpl {
        StoreImpl { store }
    }
}

#[tonic::async_trait]
impl Store for StoreImpl {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        debug!("gRPC Got a request: {:?}", request);

        let key = request.into_inner().key.as_bytes().to_vec();
        let store = self.store.read().unwrap();
        match store.get(&key) {
            Ok(val) => {
                let resp = GetResponse {
                    // result: None,
                    value: String::from_utf8(val).unwrap(),
                };
                Ok(Response::new(resp))
            }
            Err(e) => {
                debug!("the Key NotFound {:?}", e);
                Err(Status::new(tonic::Code::NotFound, "Key NotFound"))
            }
        }
    }

    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        debug!("gRPC Got a request: {:?}", request);
        let req = request.into_inner();

        let key = req.key.as_bytes().to_vec();
        let value = req.value.as_bytes().to_vec();

        let mut store = self.store.write().unwrap();
        store.set(&key, &value, 0);

        Ok(Response::new(SetResponse {}))
    }

    async fn del(&self, request: Request<DelRequest>) -> Result<Response<DelResponse>, Status> {
        debug!("gRPC Got a request: {:?}", request);

        let req = request.into_inner();
        if req.keys.len() < 1 {
            return Err(Status::new(tonic::Code::DataLoss, "key loss"));
        }

        let mut count = 0;
        let mut keys = Vec::new();

        {
            let store = self.store.read().unwrap();
            for key in req.keys {
                let key = key.as_bytes().to_vec();
                if store.get(&key).is_ok() {
                    count += 1;
                    keys.push(key);
                }
            }
        }

        if keys.len() > 0 {
            let mut store = self.store.write().unwrap();
            for k in keys {
                store.delete(&k);
            }
        }

        Ok(Response::new(DelResponse { num: count }))
    }

    async fn exists(
        &self,
        request: Request<ExistsRequest>,
    ) -> Result<Response<ExistsResponse>, Status> {
        debug!("gRPC Got a request: {:?}", request);

        let req = request.into_inner();
        if req.keys.len() < 1 {
            return Err(Status::new(tonic::Code::DataLoss, "key loss"));
        }

        let mut count = 0;
        let store = self.store.read().unwrap();
        for key in req.keys {
            let key = key.as_bytes().to_vec();
            if store.get(&key).is_ok() {
                count += 1;
            }
        }

        Ok(Response::new(ExistsResponse { result: count }))
    }

    async fn get_set(
        &self,
        request: Request<GetSetRequest>,
    ) -> Result<Response<GetSetResponse>, Status> {
        debug!("gRPC Got a request: {:?}", request);
        let req = request.into_inner();

        let key = req.key.as_bytes().to_vec();
        let value = req.value.as_bytes().to_vec();

        let mut store = self.store.write().unwrap();
        let resp = match store.get(&key) {
            Ok(val) => Ok(Response::new(GetSetResponse {
                result: String::from_utf8(val).unwrap(),
            })),
            Err(e) => {
                debug!("getset {:?}", e);
                Err(Status::new(tonic::Code::NotFound, "NotFound"))
            }
        };
        store.set(&key, &value, 0);

        resp
    }

    async fn m_set(&self, request: Request<MSetRequest>) -> Result<Response<MSetResponse>, Status> {
        debug!("gRPC Got a request: {:?}", request);
        let req = request.into_inner();
        if req.items.len() == 0 {
            return Err(Status::new(
                tonic::Code::InvalidArgument,
                "argument invalid",
            ));
        }

        let mut store = self.store.write().unwrap();
        for item in req.items {
            let key = item.key.as_bytes().to_vec();
            let value = item.value.as_bytes().to_vec();

            // set
            store.set(&key, &value, 0);
        }

        Ok(Response::new(MSetResponse {}))
    }

    async fn m_get(&self, request: Request<MGetRequest>) -> Result<Response<MGetResponse>, Status> {
        debug!("gRPC Got a request: {:?}", request);

        let req = request.into_inner();
        if req.keys.len() < 1 {
            return Err(Status::new(tonic::Code::DataLoss, "key loss"));
        }

        let mut items = Vec::new();
        {
            let store = self.store.read().unwrap();
            for key in req.keys {
                let k = key.as_bytes().to_vec();
                if let Ok(value) = store.get(&k) {
                    items.push(Item {
                        key: key,
                        value: String::from_utf8(value).unwrap(),
                    });
                }
            }
        }

        Ok(Response::new(MGetResponse { items: items }))
    }
}
