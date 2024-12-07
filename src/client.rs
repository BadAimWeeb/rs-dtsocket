use futures_util::{stream::FusedStream, FutureExt, Sink, SinkExt, Stream, StreamExt, task::Poll::{Ready, Pending}};
use std::{
    collections::{HashMap, HashSet, LinkedList},
    pin::Pin,
    task::{Context, Poll},
};

pub struct DTSocketClient {
    protov2d: rs_protov2d::client::Client,

    nonce_counter: u64,

    t0_register: HashSet<u64>,
    backfeed: LinkedList<DTPacketType>
}

pub enum DTPacketType {
    Type0 {
        nonce: u64,
        success: bool,
        data: Vec<u8>
    }
}

pub struct DTPacket {
    pub packet_type: DTPacketType,
    pub data: Vec<u8>
}

impl Stream for DTSocketClient {
    type Item = DTPacketType;

    fn poll_next(
        mut self: Pin<&mut DTSocketClient>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match (self.protov2d.next().poll_unpin(cx)) {
                Ready(t) => {
                    match t {
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
                                    
                                    if !self.t0_register.contains(&nonce) {
                                        continue;
                                    }
                                    self.t0_register.remove(&nonce);
        
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
        
                                    return Poll::Ready(Some(DTPacketType::Type0 { nonce, success, data: v }));
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
                
                Pending => {
                    if !self.backfeed.is_empty() {
                        return Poll::Ready(Some(self.backfeed.pop_front().unwrap()));
                    }

                    return Poll::Pending;
                }
            }
        }
    }
}

impl DTSocketClient {
    pub fn new(protov2d: rs_protov2d::client::Client) -> Self {
        Self {
            protov2d,
            nonce_counter: 0,
            t0_register: HashSet::new(),
            backfeed: LinkedList::new()
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

        let _ = self.protov2d.send_packet(1, serialized).await;

        self.t0_register.insert(nonce);
        let r_packet;
        loop {
            let packet = self.next().await;
            if packet.is_some() {
                let up = packet.unwrap();
                match up {
                    DTPacketType::Type0 { nonce: p_nonce, success, data } => {
                        if nonce != p_nonce {
                            self.backfeed.push_back(DTPacketType::Type0 { nonce: p_nonce, success, data });
                            continue;
                        }

                        r_packet = (success, data);
                        break;
                    }
                    _ => {
                        self.backfeed.push_back(up);
                    }
                }
            }

            panic!("failure");
        }

        if !r_packet.0 {
            let err: Result<String, _> = rmp_serde::from_slice(&r_packet.1);
            if err.is_err() {
                return Err("internal_client_error: failed to deserialize".to_string());
            }

            return Err(err.unwrap());
        }

        let result: Result<O, _> = rmp_serde::from_slice(&r_packet.1);
        if result.is_err() {
            return Err("internal_client_error: failed to deserialize".to_string());
        }

        Ok(result.unwrap())
    }
}
