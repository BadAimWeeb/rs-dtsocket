use futures_util::{stream::FusedStream, FutureExt, Sink, SinkExt, Stream, StreamExt};
use std::{
    collections::HashMap,
    pin::Pin,
    task::{Context, Poll},
};

pub struct DTSocketClient {
    protov2d: Box<rs_protov2d::client::Client>,

    nonce_counter: u64,
    t0_callbacks: HashMap<u64, tokio::sync::oneshot::Sender<(bool, Vec<u8>)>>,
}

pub enum DTPacketType {

}

pub struct DTPacket {
    pub packet_type: DTPacketType,
    pub data: Vec<u8>,
}

impl Stream for DTSocketClient {
    type Item = ();

    fn poll_next(
        mut self: Pin<&mut DTSocketClient>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match futures_util::ready!(self.protov2d.next().poll_unpin(cx)) {
                Some(msg) => {
                    if msg.qos != 1 {
                        continue;
                    }

                    let data = msg.data;
                    let t = data[0];
                    let mut data = &data[1..];
                    match t {
                        0 => {
                            // handle packet type 0
                            let d = rmpv::decode::read_value(&mut data);
                            if d.is_err() {
                                continue;
                            }

                            let d = d.unwrap();
                            let d = d.as_array();
                            if d.is_none() {
                                continue;
                            }
                            let d = d.unwrap();
                            if d.len() != 3 {
                                continue;
                            }

                            let nonce = d[0].as_u64();
                            if nonce.is_none() {
                                continue;
                            }
                            let nonce = nonce.unwrap();
                            
                            if !self.t0_callbacks.contains_key(&nonce) {
                                continue;
                            }

                            let success = d[1].as_bool();
                            if success.is_none() {
                                continue;
                            }
                            let success = success.unwrap();

                            let mut v: Vec<u8> = Vec::new();
                            let result = rmpv::encode::write_value(&mut v, &d[2]);
                            if result.is_err() {
                                continue;
                            }

                            let result = self.t0_callbacks.remove(&nonce);
                            if result.is_none() {
                                continue;
                            }

                            let result = result.unwrap();
                            let _ = result.send((success, v));
                        }

                        _ => {
                            continue;
                        }
                    }
                }
                None => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl DTSocketClient {
    pub fn new(protov2d: Box<rs_protov2d::client::Client>) -> Self {
        Self {
            protov2d,
            nonce_counter: 0,
            t0_callbacks: HashMap::new(),
        }
    }

    pub async fn call_procedure<'a, O, I>(&mut self, procedure: &str, data: I) -> Result<O, String>
    where
        I: serde::Serialize,
        O: serde::de::DeserializeOwned,
    {
        let nonce = self.nonce_counter;
        self.nonce_counter += 1;

        let serialized = rmp_serde::encode::to_vec(&(0, nonce, procedure, &data));
        if serialized.is_err() {
            return Err("internal_client_error: failed to serialize data".to_string());
        }

        let serialized = serialized.unwrap();

        let _ = self.protov2d.send(1, serialized);

        let (tx, rx) = tokio::sync::oneshot::channel();
        self.t0_callbacks.insert(nonce, tx);

        let result = rx.await;
        self.t0_callbacks.remove(&nonce);
        if result.is_err() {
            return Err("internal_client_error: failed to receive data".to_string());
        }

        let result = result.unwrap();
        if !result.0 {
            let err: Result<String, _> = rmp_serde::from_slice(&result.1);
            if err.is_err() {
                return Err("internal_client_error: failed to deserialize".to_string());
            }

            return Err(err.unwrap());
        }

        let result: Result<O, _> = rmp_serde::from_slice(&result.1);
        if result.is_err() {
            return Err("internal_client_error: failed to deserialize".to_string());
        }

        Ok(result.unwrap())
    }
}
