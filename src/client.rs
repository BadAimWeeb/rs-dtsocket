use futures_util::{
    task::Poll::{Pending, Ready},
    FutureExt, Stream, StreamExt
};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use std::{
    collections::{HashMap, HashSet, LinkedList},
    pin::Pin,
    task::{Context, Poll},
};
use tokio_stream::wrappers::BroadcastStream;

pub struct DTSocketClient<R>
where
    R: IntoClientRequest + Unpin + Copy
{
    protov2d: rs_protov2d::client::Client<R>,

    t0_nonce_counter: u64,
    t2_nonce_recv_counter: HashMap<String, u64>,
    t2_nonce_data_storage: HashMap<(String, u64), Vec<u8>>,
    t2_broadcast_sender: HashMap<String, tokio::sync::broadcast::Sender<Vec<u8>>>,

    t0_register: HashSet<u64>,
    backfeed: LinkedList<DTPacketType>,
}

pub enum DTPacketType {
    Type0 {
        nonce: u64,
        success: bool,
        data: Vec<u8>,
    },
}

pub struct DTPacket {
    pub packet_type: DTPacketType,
    pub data: Vec<u8>,
}

impl<R> Stream for DTSocketClient<R> 
where
    R: IntoClientRequest + Unpin + Copy
{
    type Item = DTPacketType;

    fn poll_next(
        mut self: Pin<&mut DTSocketClient<R>>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match self.protov2d.next().poll_unpin(cx) {
                Ready(t) => {
                    match t {
                        Some(msg) => {
                            if msg.qos != 1 {
                                continue;
                            }

                            let mut data = &msg.data[..];
                            let data = rmpv::decode::read_value(&mut data);
                            if data.is_err() {
                                continue;
                            }
                            let data = data.unwrap();
                            let data = data.as_array();
                            if data.is_none() {
                                continue;
                            }
                            let data = data.unwrap();

                            let t = data[0].as_u64();
                            if t.is_none() {
                                continue;
                            }
                            let t = t.unwrap();

                            match t {
                                0 => {
                                    // handle packet type 0
                                    if data.len() != 4 {
                                        continue;
                                    }

                                    let nonce = data[1].as_u64();
                                    if nonce.is_none() {
                                        continue;
                                    }
                                    let nonce = nonce.unwrap();

                                    if !self.t0_register.contains(&nonce) {
                                        continue;
                                    }
                                    self.t0_register.remove(&nonce);

                                    let success = data[2].as_bool();
                                    if success.is_none() {
                                        continue;
                                    }
                                    let success = success.unwrap();

                                    let mut v: Vec<u8> = Vec::new();
                                    let result = rmpv::encode::write_value(&mut v, &data[3]);
                                    if result.is_err() {
                                        continue;
                                    }

                                    return Poll::Ready(Some(DTPacketType::Type0 {
                                        nonce,
                                        success,
                                        data: v,
                                    }));
                                }

                                2 => {
                                    // handle packet type 2
                                    if data.len() < 3 {
                                        continue;
                                    }

                                    let event = data[1].as_str();
                                    if event.is_none() {
                                        continue;
                                    }
                                    let event = event.unwrap().to_string();

                                    let nonce = data[2].as_u64();
                                    if nonce.is_none() {
                                        continue;
                                    }
                                    let nonce = nonce.unwrap();

                                    let mut v: Vec<u8> = Vec::new();
                                    let slice = &data[2..];
                                    let result = rmpv::encode::write_value(
                                        &mut v,
                                        &rmpv::Value::Array(slice.to_vec()),
                                    );
                                    if result.is_err() {
                                        continue;
                                    }

                                    if !self.t2_nonce_recv_counter.contains_key(&event) {
                                        self.t2_nonce_recv_counter.insert(event.clone(), 0);
                                    }
                                    self.create_broadcast_channel_if_not_exist(event.as_str());

                                    let sender =
                                        self.t2_broadcast_sender.get(&event).unwrap().clone();

                                    let counter = *self.t2_nonce_recv_counter.get(&event).unwrap();

                                    if nonce == counter {
                                        let _ = sender.send(v);

                                        let mut loop_counter = counter + 1;
                                        loop {
                                            if self
                                                .t2_nonce_data_storage
                                                .contains_key(&(event.clone(), loop_counter))
                                            {
                                                let v = self
                                                    .t2_nonce_data_storage
                                                    .remove(&(event.clone(), loop_counter))
                                                    .unwrap();
                                                let _ = sender.send(v);
                                                loop_counter += 1;
                                            } else {
                                                break;
                                            }
                                        }

                                        self.t2_nonce_recv_counter
                                            .insert(event.clone(), loop_counter);
                                    } else {
                                        self.t2_nonce_data_storage
                                            .insert((event.clone(), nonce), v);
                                    }

                                    continue;
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

impl<R> DTSocketClient<R>
where 
    R: IntoClientRequest + Unpin + Copy
{
    pub fn new(protov2d: rs_protov2d::client::Client<R>) -> Self {
        Self {
            protov2d,
            t0_nonce_counter: 0,
            t0_register: HashSet::new(),
            t2_nonce_recv_counter: HashMap::new(),
            t2_nonce_data_storage: HashMap::new(),
            t2_broadcast_sender: HashMap::new(),
            backfeed: LinkedList::new(),
        }
    }

    fn create_broadcast_channel_if_not_exist(&mut self, event: &str) {
        if self.t2_broadcast_sender.contains_key(event) {
            return;
        }

        let (tx, _) = tokio::sync::broadcast::channel(100);
        self.t2_broadcast_sender.insert(event.to_string(), tx);
    }

    async fn call_procedure_wait<O>(&mut self, nonce: u64) -> Result<O, String>
    where
        O: serde::de::DeserializeOwned,
    {
        self.t0_register.insert(nonce);
        let r_packet;
        loop {
            let packet = self.next().await;
            if packet.is_some() {
                let up = packet.unwrap();
                match up {
                    DTPacketType::Type0 {
                        nonce: p_nonce,
                        success,
                        data,
                    } => {
                        if nonce != p_nonce {
                            self.backfeed.push_back(DTPacketType::Type0 {
                                nonce: p_nonce,
                                success,
                                data,
                            });
                            continue;
                        }

                        r_packet = (success, data);
                        break;
                    }
                    // _ => {
                    //     self.backfeed.push_back(up);
                    // }
                }
            }

            let new_connect = self.protov2d.try_reconnect().await.map_err(|_| "internal_client_error: reconnect failed")?;
            if new_connect {
                return Err("internal_client_error: old connection closed".to_string());
            }
        }

        if !r_packet.0 {
            let err: Result<String, _> = rmp_serde::from_slice(&r_packet.1);
            if err.is_err() {
                return Err("internal_client_error: failed to deserialize error".to_string());
            }

            return Err(err.unwrap());
        }

        let result: Result<O, _> = rmp_serde::from_slice(&r_packet.1);
        if result.is_err() {
            return Err("internal_client_error: failed to deserialize result".to_string());
        }

        Ok(result.unwrap())
    }

    pub async fn call_procedure<O, I>(&mut self, procedure: &str, data: I) -> Result<O, String>
    where
        I: serde::Serialize,
        O: serde::de::DeserializeOwned,
    {
        let nonce = self.t0_nonce_counter;
        self.t0_nonce_counter += 1;

        let serialized = rmp_serde::encode::to_vec(&(0, nonce, procedure, &data));
        if serialized.is_err() {
            return Err("internal_client_error: failed to serialize data".to_string());
        }

        let serialized = serialized.unwrap();

        let _ = self.protov2d.send_packet(1, serialized).await;

        self.call_procedure_wait(nonce).await
    }

    pub async fn call_procedure_void_input<O>(&mut self, procedure: &str) -> Result<O, String>
    where
        O: serde::de::DeserializeOwned,
    {
        let nonce = self.t0_nonce_counter;
        self.t0_nonce_counter += 1;

        let serialized = rmp_serde::encode::to_vec(&(0, nonce, procedure));
        if serialized.is_err() {
            return Err("internal_client_error: failed to serialize data".to_string());
        }

        let serialized = serialized.unwrap();

        let _ = self.protov2d.send_packet(1, serialized).await;

        self.call_procedure_wait(nonce).await
    }

    pub fn hook_event<T>(&mut self, event: &str) -> DTSocketClientEventReceiverStream<T, R>
    where
        T: serde::de::DeserializeOwned,
    {
        self.create_broadcast_channel_if_not_exist(event);
        let rec = self.t2_broadcast_sender.get_mut(event).unwrap().subscribe();

        DTSocketClientEventReceiverStream {
            dt: self,
            receiver: Box::new(BroadcastStream::new(rec)),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn poll_keep_alive(&mut self) -> bool {
        let waker = futures::task::noop_waker_ref();
        let mut cx = std::task::Context::from_waker(waker);

        let d = self.poll_next_unpin(&mut cx);
        match d {
            Ready(Some(d)) => {
                self.backfeed.push_back(d);
                false
            }
            Ready(None) => {
                true
            }
            _ => {
                false
            }
        }
    }
}

pub struct DTSocketClientEventReceiverStream<T, R>
where
    T: serde::de::DeserializeOwned,
    R: IntoClientRequest + Unpin + Copy
{
    dt: *mut DTSocketClient<R>,
    receiver: Box<BroadcastStream<Vec<u8>>>,
    _marker: std::marker::PhantomData<T>,
}

impl<T, R> Unpin for DTSocketClientEventReceiverStream<T, R> 
where 
    T: serde::de::DeserializeOwned,
    R: IntoClientRequest + Unpin + Copy
{}

impl<T, R> Stream for DTSocketClientEventReceiverStream<T, R>
where
    T: serde::de::DeserializeOwned,
    R: IntoClientRequest + Unpin + Copy
{
    type Item = T;

    fn poll_next(
        mut self: Pin<&mut DTSocketClientEventReceiverStream<T, R>>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // i know what i'm doing (sort of)
        unsafe {
            match self.dt.as_mut() {
                Some(dt) => {
                    let should_reconnect = dt.poll_keep_alive();
                    if should_reconnect {
                        return Poll::Ready(None);
                    }
                }
                None => {}
            }
        }

        match futures_util::ready!(self.receiver.next().poll_unpin(cx)) {
            Some(data) => {
                if data.is_err() {
                    return Poll::Ready(None);
                }

                let result: Result<T, _> = rmp_serde::from_slice(&data.unwrap());
                if result.is_err() {
                    return Poll::Ready(None);
                }

                Poll::Ready(Some(result.unwrap()))
            }

            None => Poll::Ready(None),
        }
    }
}
