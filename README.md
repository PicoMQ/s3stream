# s3stream

A stream engine on object storage, in Rust. The host provides stream and object metadata through the `StreamManager` / `ObjectManager` traits.

Wire formats live in `specification/` and are pinned by golden fixtures in `conformance/`.

```
s3stream (facade: Config, builder)
  └── s3stream-core   (S3Storage pipeline, cache, compaction)
        ├── s3stream-wal     (object WAL, memory WAL)
        └── s3stream-object  (object format, ObjectStorage over object_store)
              └── s3stream-codec  (record codec, WAL framing, CRC)
```

## Use

```toml
[dependencies]
s3stream = { git = "https://github.com/PicoMQ/s3stream", tag = "v0.1.0" }
```

## Build

```
cargo test --workspace
```

## License

[Apache-2.0](LICENSE). Derived from [AutoMQ S3Stream](https://github.com/AutoMQ/automq/tree/main/s3stream), Copyright 2023-2025 AutoMQ HK Limited. See [NOTICE](NOTICE).
