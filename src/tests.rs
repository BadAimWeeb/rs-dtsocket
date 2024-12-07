use std::{process::exit, sync::Arc};

use client::DTSocketClient;
use futures_util::{StreamExt as _};
use rs_protov2d::client::{ClientHandshakeConfig, PublicKey, PublicKeyType};
use async_mutex::Mutex;

use crate::*;

#[derive(serde::Deserialize)]
struct VersionDeserialize {
    pub version: String,
    pub branch: String,
    pub commit: String
}

#[tokio::test]
async fn connect() {
    rustls::crypto::aws_lc_rs::default_provider().install_default().expect("Failed to install rustls crypto provider");

    let result = rs_protov2d::client::Client::connect("wss://id-backend.badaimweeb.me/", ClientHandshakeConfig {
        public_keys: vec![PublicKey {
            key_type: PublicKeyType::Hash,
            data:Some("357093b00d1d5640aae631dd62519a88b29233274df4a23fd960432d8004ecb9".to_string())
        }],
        ping_interval: None
    }).await;

    if result.is_err() {
        println!("{:?}", result.err().unwrap());
        panic!("connection should be successful");
    }

    let client = result.unwrap();
    println!("connected to BAW#ID backend");

    let dt = DTSocketClient::new(client);
    let rc = Arc::new(Mutex::new(dt));
    let rc2 = rc.clone();

    let t = tokio::spawn(async move {
        let mut dt = rc.lock().await;
        println!("locked lol");
        let response = dt.call_procedure("serverVersion", 0);
        let response: Result<VersionDeserialize, _> = response.await;

        if response.is_err() {
            println!("{:?}", response.err().unwrap());
            panic!("serverVersion should be successful");
        }

        let response = response.unwrap();
        println!("BAW#ID server version is {} ({}@{})", response.version, response.branch, response.commit);
    });

    tokio::spawn(async move {
        loop {
            let mut dt = rc2.lock().await;
            let _ = dt.next().await;
        }
    });

    t.await.unwrap();
    exit(0);
}

