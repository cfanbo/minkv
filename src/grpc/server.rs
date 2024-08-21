use crate::db_store;
use crate::util;
use grpc_minkv::store_server::Store;
use grpc_minkv::{
    AppendRequest, AppendResponse, DecrRequest, DecrResponse, DelRequest, DelResponse,
    ExistsRequest, ExistsResponse, ExpireAtRequest, ExpireAtResponse, ExpireRequest,
    ExpireResponse, GetRequest, GetResponse, GetSetRequest, GetSetResponse, IncrRequest,
    IncrResponse, Item, MGetRequest, MGetResponse, MSetRequest, MSetResponse, PExpireAtRequest,
    PExpireAtResponse, PExpireRequest, PExpireResponse, PTtlRequest, PTtlResponse, PersistRequest,
    PersistResponse, SetRequest, SetResponse, TtlRequest, TtlResponse,
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

    async fn append(
        &self,
        request: Request<AppendRequest>,
    ) -> Result<Response<AppendResponse>, Status> {
        debug!("gRPC Got a request: {:?}", request);

        let req = request.into_inner();
        let key = req.key.as_bytes().to_vec();
        let value = req.value.as_bytes().to_vec();

        let mut store = self.store.write().unwrap();
        let resp = match store.get(&key) {
            Ok(mut val) => {
                val.extend(value);
                store.set(&key, &val, 0);
                AppendResponse {
                    len: val.len() as i32,
                }
            }
            Err(_) => {
                store.set(&key, &value, 0);
                AppendResponse {
                    len: value.len() as i32,
                }
            }
        };

        Ok(Response::new(resp))
    }

    async fn incr(&self, request: Request<IncrRequest>) -> Result<Response<IncrResponse>, Status> {
        debug!("gRPC Got a request: {:?}", request);

        let req = request.into_inner();
        let key = req.key.as_bytes().to_vec();

        let mut store = self.store.write().unwrap();
        match store.get(&key) {
            Ok(val) => {
                // convert to a number
                let s = String::from_utf8_lossy(&val);
                match s.parse::<i64>() {
                    Ok(mut n) => {
                        n = n + 1;
                        store.set(&key, &n.to_string().into_bytes(), 0);
                        Ok(Response::new(IncrResponse { num: n as i32 }))
                    }
                    Err(_) => Err(Status::new(tonic::Code::Unavailable, "Unavailable")),
                }
            }
            Err(_) => {
                let n: i32 = 1;
                store.set(&key, &n.to_string().into_bytes(), 0);
                Ok(Response::new(IncrResponse { num: n }))
            }
        }
    }

    async fn decr(&self, request: Request<DecrRequest>) -> Result<Response<DecrResponse>, Status> {
        debug!("gRPC Got a request: {:?}", request);

        let req = request.into_inner();
        let key = req.key.as_bytes().to_vec();

        let mut store = self.store.write().unwrap();
        match store.get(&key) {
            Ok(val) => {
                // convert to a number
                let s = String::from_utf8_lossy(&val);
                match s.parse::<i64>() {
                    Ok(mut n) => {
                        n = n - 1;
                        store.set(&key, &n.to_string().into_bytes(), 0);
                        Ok(Response::new(DecrResponse { num: n as i32 }))
                    }
                    Err(_) => Err(Status::new(tonic::Code::Unavailable, "Unavailable")),
                }
            }
            Err(_) => {
                let n: i32 = -1;
                store.set(&key, &n.to_string().into_bytes(), 0);
                Ok(Response::new(DecrResponse { num: n }))
            }
        }
    }

    async fn expire(
        &self,
        request: Request<ExpireRequest>,
    ) -> Result<Response<ExpireResponse>, Status> {
        debug!("gRPC Got a request: {:?}", request);

        let req = request.into_inner();
        let key = req.key.as_bytes().to_vec();
        let value = req.secs as u64;

        if value > u64::MAX / 1000 {
            return Err(Status::new(tonic::Code::InvalidArgument, "InvalidArgument"));
        }

        let millisec = util::time::get_millisec_from_sec(value);
        let mut store = self.store.write().unwrap();
        match store.get(&key) {
            Ok(val) => {
                store.set(&key, &val, millisec);
                Ok(Response::new(ExpireResponse { result: 1 }))
            }
            Err(_) => Err(Status::new(tonic::Code::InvalidArgument, "InvalidArgument")),
        }
    }

    async fn expire_at(
        &self,
        request: Request<ExpireAtRequest>,
    ) -> Result<Response<ExpireAtResponse>, Status> {
        debug!("gRPC Got a request: {:?}", request);

        let req = request.into_inner();
        let key = req.key.as_bytes().to_vec();
        let value = req.secs as u64;

        if value > u64::MAX / 1000 {
            return Err(Status::new(tonic::Code::InvalidArgument, "InvalidArgument"));
        }

        let millisec = util::time::sec_to_millisec(value);
        let mut store = self.store.write().unwrap();
        match store.get(&key) {
            Ok(val) => {
                store.set(&key, &val, millisec);
                Ok(Response::new(ExpireAtResponse { result: 1 }))
            }
            Err(_) => Err(Status::new(tonic::Code::InvalidArgument, "InvalidArgument")),
        }
    }

    async fn p_expire(
        &self,
        request: Request<PExpireRequest>,
    ) -> Result<Response<PExpireResponse>, Status> {
        debug!("gRPC Got a request: {:?}", request);

        let req = request.into_inner();
        let key = req.key.as_bytes().to_vec();
        let value = req.secs as u64;

        if util::time::current_milliseconds() + value > u64::MAX / 1000 {
            return Err(Status::new(tonic::Code::InvalidArgument, "InvalidArgument"));
        }

        let millisec = util::time::get_millisec(value);
        let mut store = self.store.write().unwrap();
        match store.get(&key) {
            Ok(val) => {
                store.set(&key, &val, millisec);
                Ok(Response::new(PExpireResponse { result: 1 }))
            }
            Err(_) => Err(Status::new(tonic::Code::InvalidArgument, "InvalidArgument")),
        }
    }

    async fn p_expire_at(
        &self,
        request: Request<PExpireAtRequest>,
    ) -> Result<Response<PExpireAtResponse>, Status> {
        debug!("gRPC Got a request: {:?}", request);

        let req = request.into_inner();
        let key = req.key.as_bytes().to_vec();
        let value = req.secs as u64;

        if value > u64::MAX / 1000 {
            return Err(Status::new(tonic::Code::InvalidArgument, "InvalidArgument"));
        }

        let mut store = self.store.write().unwrap();
        match store.get(&key) {
            Ok(val) => {
                store.set(&key, &val, value);
                Ok(Response::new(PExpireAtResponse { result: 1 }))
            }
            Err(_) => Err(Status::new(tonic::Code::InvalidArgument, "InvalidArgument")),
        }
    }

    async fn ttl(&self, request: Request<TtlRequest>) -> Result<Response<TtlResponse>, Status> {
        debug!("gRPC Got a request: {:?}", request);

        let req = request.into_inner();
        let key = req.key.as_bytes().to_vec();

        let store = self.store.write().unwrap();
        match store.get_entry(&key) {
            Ok(entry) => {
                if entry.timestamp == 0 {
                    // 未设置过期时间
                    Ok(Response::new(TtlResponse { result: -1 }))
                } else {
                    if entry.is_expired() {
                        Err(Status::new(tonic::Code::NotFound, "NotFound"))
                    } else {
                        Ok(Response::new(TtlResponse {
                            result: util::time::get_lifetime_sec(entry.timestamp) as i64,
                        }))
                    }
                }

                // Ok(Response::new(TtlResponse{result: 1 }))
            }
            Err(_) => Err(Status::new(tonic::Code::NotFound, "NotFound")),
        }
    }

    async fn p_ttl(&self, request: Request<PTtlRequest>) -> Result<Response<PTtlResponse>, Status> {
        debug!("gRPC Got a request: {:?}", request);

        let req = request.into_inner();
        let key = req.key.as_bytes().to_vec();

        let store = self.store.write().unwrap();
        match store.get_entry(&key) {
            Ok(entry) => {
                if entry.timestamp == 0 {
                    // 未设置过期时间
                    Ok(Response::new(PTtlResponse { result: -1 }))
                } else {
                    if entry.is_expired() {
                        Err(Status::new(tonic::Code::NotFound, "NotFound"))
                    } else {
                        Ok(Response::new(PTtlResponse {
                            result: util::time::get_lifetime_millisec(entry.timestamp) as i64,
                        }))
                    }
                }
            }
            Err(_) => Err(Status::new(tonic::Code::NotFound, "NotFound")),
        }
    }

    async fn persist(
        &self,
        request: Request<PersistRequest>,
    ) -> Result<Response<PersistResponse>, Status> {
        debug!("gRPC Got a request: {:?}", request);

        let req = request.into_inner();
        let key = req.key.as_bytes().to_vec();

        let mut store = self.store.write().unwrap();
        match store.get_entry(&key) {
            Ok(entry) => {
                if entry.timestamp == 0 {
                    // 未设置过期时间
                    Ok(Response::new(PersistResponse { result: -1 }))
                } else {
                    store.set(&key, &entry.value, 0);
                    Ok(Response::new(PersistResponse { result: 1 }))
                }
            }
            Err(_) => Err(Status::new(tonic::Code::NotFound, "NotFound")),
        }
    }
}
