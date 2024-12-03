# rs-protov2d

> ⚠️ This package is in a VERY EARLY stage of development and IS NOT USABLE.

Rust implementation of original JavaScript/TypeScript [DTSocket client](https://github.com/BadAimWeeb/js-dtsocket).

This is used to send and receive data from an JS/TS DTSocket server. Due to it being a different language, you cannot bring exported types from TS version to here. 

No server implementation will be provided, unless I can figure out TypeScript and Rust types interoperability (otherwise it doesn't make any sense).

## Technical notes

This package relies heavily on `tokio` (and ProtoV2d relies on `websocket`) crates.
