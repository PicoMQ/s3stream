//! ObjectReader: reads data blocks out of committed objects.
//!
//! `BasicObjectInfo`, `DataBlockGroup`, footer parsing).
//! Specification: `specification/object-format.md`.

use std::sync::Arc;

use bytes::{Buf, Bytes};
use tokio::sync::OnceCell;

use s3stream_codec::StreamRecordBatch;

use crate::composite::{ObjectIndex, component_of, parse_composite};
use crate::error::ObjectError;
use crate::index::{DataBlockIndex, FindIndexResult, IndexBlock};
use crate::metadata::{S3ObjectMetadata, gen_object_key};
use crate::storage::{ObjectStorage, ReadOptions, ThrottleStrategy};
use crate::writer::{BLOCK_HEADER_SIZE, DATA_BLOCK_MAGIC, FOOTER_MAGIC, FOOTER_SIZE};

/// Parsed footer + index of one object. Cheap to keep cached per object.
#[derive(Debug, Clone)]
pub struct BasicObjectInfo {
    /// Byte size of the data section (== index block start position). 0 for a
    /// composite object, whose blocks live in its component objects.
    pub data_block_size: u64,
    pub index_block: IndexBlock,
}

/// Reader over one committed object. Interior caching replaces manual
/// retain/release.
///
/// A composite object (attributes COMPOSITE bit) is read through its linked
/// components: its index entries keep the positions of the component objects,
/// and `read_block` fetches each block from the component that owns it.
pub struct ObjectReader {
    metadata: S3ObjectMetadata,
    storage: Arc<dyn ObjectStorage>,
    read_options: ReadOptions,
    info: OnceCell<ParsedObject>,
}

struct ParsedObject {
    info: BasicObjectInfo,
    /// Components of a composite object; empty for a plain object.
    components: Vec<ObjectIndex>,
}

impl ObjectReader {
    /// Open a reader (no I/O yet). Works for plain and composite objects.
    pub fn new(metadata: S3ObjectMetadata, storage: Arc<dyn ObjectStorage>) -> Self {
        let read_options = ReadOptions {
            bucket_id: Some(metadata.attributes.bucket_id()),
            ..Default::default()
        };
        Self {
            metadata,
            storage,
            read_options,
            info: OnceCell::new(),
        }
    }

    /// Fetch and parse footer + index block (cached after first call). Two
    /// ranged GETs: footer, then index. (An over-read of the tail could
    /// usually save the second GET. That optimization is deferred.)
    ///
    /// A composite object is small (index only) and is read in one GET.
    pub async fn basic_object_info(&self) -> Result<&BasicObjectInfo, ObjectError> {
        Ok(&self.parsed().await?.info)
    }

    async fn parsed(&self) -> Result<&ParsedObject, ObjectError> {
        self.info
            .get_or_try_init(|| async {
                if self.metadata.attributes.is_composite() {
                    let buf = self
                        .storage
                        .range_read(&self.read_options, &self.metadata.key(), 0, None)
                        .await?;
                    let composite = parse_composite(&buf)?;
                    return Ok(ParsedObject {
                        info: BasicObjectInfo {
                            data_block_size: 0,
                            index_block: composite.index_block,
                        },
                        components: composite.objects,
                    });
                }
                let object_size = self.metadata.object_size;
                if object_size < FOOTER_SIZE as u64 {
                    return Err(ObjectError::InvalidFormat {
                        reason: format!("object size {object_size} smaller than footer"),
                    });
                }
                let key = self.metadata.key();
                let footer = self
                    .storage
                    .range_read(
                        &self.read_options,
                        &key,
                        object_size - FOOTER_SIZE as u64,
                        Some(object_size),
                    )
                    .await?;
                if footer.len() != FOOTER_SIZE {
                    return Err(ObjectError::InvalidFormat {
                        reason: format!("footer read returned {} bytes", footer.len()),
                    });
                }
                let magic = u64::from_be_bytes(footer[40..48].try_into().unwrap());
                if magic != FOOTER_MAGIC {
                    return Err(ObjectError::InvalidFormat {
                        reason: format!("footer magic mismatch: {magic:#x}"),
                    });
                }
                let index_position = u64::from_be_bytes(footer[0..8].try_into().unwrap());
                let index_size = u32::from_be_bytes(footer[8..12].try_into().unwrap()) as u64;
                if index_position + index_size + FOOTER_SIZE as u64 != object_size {
                    return Err(ObjectError::InvalidFormat {
                        reason: format!(
                            "footer geometry: index at {index_position}+{index_size} + footer != size {object_size}"
                        ),
                    });
                }
                let index_bytes = self
                    .storage
                    .range_read(
                        &self.read_options,
                        &key,
                        index_position,
                        Some(index_position + index_size),
                    )
                    .await?;
                Ok(ParsedObject {
                    info: BasicObjectInfo {
                        data_block_size: index_position,
                        index_block: IndexBlock::parse(&index_bytes)?,
                    },
                    components: Vec::new(),
                })
            })
            .await
    }

    /// Find index entries covering the requested range.
    pub async fn find(
        &self,
        stream_id: u64,
        start_offset: u64,
        end_offset: u64,
        max_bytes: usize,
    ) -> Result<FindIndexResult, ObjectError> {
        let info = self.basic_object_info().await?;
        Ok(info
            .index_block
            .find(stream_id, start_offset, end_offset, max_bytes))
    }

    /// Range-GET one data block's bytes, with `ThrottleStrategy::Bypass`.
    pub async fn read_block(&self, index: &DataBlockIndex) -> Result<Bytes, ObjectError> {
        self.read_block_throttled(index, ThrottleStrategy::Bypass)
            .await
    }

    /// Range-GET one data block's bytes with an explicit throttle class.
    ///
    /// `DataBlockCache` passes CATCH_UP for readahead loads, BYPASS for sync reads.
    pub async fn read_block_throttled(
        &self,
        index: &DataBlockIndex,
        throttle: ThrottleStrategy,
    ) -> Result<Bytes, ObjectError> {
        let mut options = ReadOptions {
            throttle,
            ..self.read_options.clone()
        };
        let key = if self.metadata.attributes.is_composite() {
            let parsed = self.parsed().await?;
            let component = component_of(&parsed.components, index.block_id as u32)?;
            options.bucket_id = Some(component.bucket_id);
            gen_object_key(0, component.object_id)
        } else {
            self.metadata.key()
        };
        self.storage
            .range_read(
                &options,
                &key,
                index.start_position,
                Some(index.end_position()),
            )
            .await
    }

    pub fn metadata(&self) -> &S3ObjectMetadata {
        &self.metadata
    }

    /// Bytes held by the cached parsed index (0 before `basic_object_info`
    /// runs). Drives reader-cache eviction accounting.
    pub fn cached_index_bytes(&self) -> usize {
        self.info
            .get()
            .map(|parsed| parsed.info.index_block.entries().len() * crate::index::BLOCK_INDEX_SIZE)
            .unwrap_or(0)
    }

    /// Read options used for block fetches (throttle class set by callers:
    /// tail vs catch-up vs compaction).
    pub fn read_options(&self) -> &ReadOptions {
        &self.read_options
    }
}

/// Decode a fetched data block region into its records.
///
/// A fetched region may contain SEVERAL consecutive data blocks (each with
/// its own header). Validate each header's magic and parse its
/// `record_count` batches, zero-copy.
pub fn decode_data_block(block: &Bytes) -> Result<Vec<StreamRecordBatch>, ObjectError> {
    let mut records = Vec::new();
    let mut buf = block.clone();
    while !buf.is_empty() {
        if buf.len() < BLOCK_HEADER_SIZE {
            return Err(ObjectError::InvalidFormat {
                reason: format!(
                    "data block header needs {BLOCK_HEADER_SIZE} bytes, have {}",
                    buf.len()
                ),
            });
        }
        let magic = buf[0];
        if magic != DATA_BLOCK_MAGIC {
            return Err(ObjectError::InvalidFormat {
                reason: format!("data block magic mismatch: {magic:#x}"),
            });
        }
        let record_count = u32::from_be_bytes(buf[2..6].try_into().unwrap()) as usize;
        let data_length = u32::from_be_bytes(buf[6..10].try_into().unwrap()) as usize;
        if buf.len() < BLOCK_HEADER_SIZE + data_length {
            return Err(ObjectError::InvalidFormat {
                reason: format!(
                    "data block truncated: need {} bytes, have {}",
                    BLOCK_HEADER_SIZE + data_length,
                    buf.len()
                ),
            });
        }
        buf.advance(BLOCK_HEADER_SIZE);
        let mut data = buf.split_to(data_length);
        for _ in 0..record_count {
            records.push(StreamRecordBatch::parse(&mut data)?);
        }
        if !data.is_empty() {
            return Err(ObjectError::InvalidFormat {
                reason: format!("{} trailing bytes after records in data block", data.len()),
            });
        }
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryObjectStorage;
    use crate::metadata::{NOOP_OFFSET, ObjectAttributes, S3ObjectType, gen_object_key};
    use crate::storage::{ObjectStorage, WriteOptions};
    use crate::writer::ObjectWriter;

    fn record(stream_id: u64, base_offset: u64, payload: &[u8]) -> StreamRecordBatch {
        StreamRecordBatch::new(stream_id, 1, base_offset, 1, payload.to_vec().into())
    }

    fn object_metadata(object_id: u64, object_size: u64) -> S3ObjectMetadata {
        S3ObjectMetadata {
            object_id,
            object_type: S3ObjectType::StreamSet,
            offset_ranges: vec![],
            object_size,
            attributes: ObjectAttributes::new(0, false, false),
            committed_timestamp_ms: 0,
            data_timestamp_ms: 0,
        }
    }

    /// Full round trip against in-memory storage.
    #[tokio::test]
    async fn write_read_round_trip() {
        let storage = Arc::new(MemoryObjectStorage::new(0));
        let inputs: Vec<StreamRecordBatch> = (0..20)
            .map(|i| record(7, i, format!("payload-{i}").as_bytes()))
            .collect();

        let mut writer =
            ObjectWriter::open(42, storage.as_ref(), 64, 16 << 20, WriteOptions::default())
                .await
                .unwrap();
        writer.write(7, &inputs).await.unwrap();
        let size = writer.close().await.unwrap();
        assert!(
            writer.block_indexes().len() > 1,
            "test should produce multiple blocks"
        );

        let reader = ObjectReader::new(object_metadata(42, size), storage.clone());
        let found = reader.find(7, 0, 20, usize::MAX).await.unwrap();
        assert!(found.fulfilled);

        let mut read_back = Vec::new();
        for index in &found.blocks {
            let block = reader.read_block(index).await.unwrap();
            read_back.extend(decode_data_block(&block).unwrap());
        }
        assert_eq!(read_back, inputs);
    }

    #[tokio::test]
    async fn reads_java_written_objects() {
        let dir =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance/fixtures/object");
        let manifest = std::fs::read_to_string(dir.join("manifest.json"))
            .expect("run conformance/generator first");
        let cases: serde_json::Value = serde_json::from_str(&manifest).unwrap();
        for case in cases.as_array().unwrap() {
            let name = case["name"].as_str().unwrap();
            let golden = std::fs::read(dir.join(format!("{name}.bin"))).unwrap();
            let object_id = case["object_id"].as_u64().unwrap();

            let storage = Arc::new(MemoryObjectStorage::new(0));
            storage
                .write(
                    &WriteOptions::default(),
                    &gen_object_key(0, object_id),
                    golden.clone().into(),
                )
                .await
                .unwrap();
            let reader = ObjectReader::new(
                object_metadata(object_id, golden.len() as u64),
                storage.clone(),
            );

            // Decode every block of every stream and compare to manifest records.
            let expected: Vec<(u64, u64, u64, i64, Vec<u8>)> = case["records"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| {
                    (
                        r["stream_id"].as_u64().unwrap(),
                        r["epoch"].as_u64().unwrap(),
                        r["base_offset"].as_u64().unwrap(),
                        r["count"].as_i64().unwrap(),
                        hex::decode(r["payload_hex"].as_str().unwrap()).unwrap(),
                    )
                })
                .collect();

            let info = reader.basic_object_info().await.unwrap();
            let mut actual = Vec::new();
            for index in info.index_block.entries().to_vec() {
                let block = reader.read_block(&index).await.unwrap();
                let records = decode_data_block(&block).unwrap();
                assert_eq!(records.len() as u32, index.record_count, "{name}");
                for r in records {
                    actual.push((
                        r.stream_id(),
                        r.epoch(),
                        r.base_offset(),
                        r.count() as i64,
                        r.payload().to_vec(),
                    ));
                }
            }
            assert_eq!(actual, expected, "record mismatch: {name}");

            for range in case["stream_ranges"].as_array().unwrap() {
                let stream_id = range["stream_id"].as_u64().unwrap();
                let start = range["start_offset"].as_u64().unwrap();
                let end = range["end_offset"].as_u64().unwrap();
                let found = reader
                    .find(stream_id, start, end, usize::MAX)
                    .await
                    .unwrap();
                assert!(found.fulfilled, "{name} stream {stream_id}");
                assert_eq!(found.next_start_offset, end, "{name} stream {stream_id}");
            }
            let _ = NOOP_OFFSET;
        }
    }

    /// A composite object reads through its components: its index keeps the
    /// component positions and `read_block` fetches from the owning component.
    #[tokio::test]
    async fn reads_composite_object_through_components() {
        use crate::composite::CompositeObjectWriter;

        let storage = Arc::new(MemoryObjectStorage::new(0));
        let inputs: Vec<StreamRecordBatch> = (0..20)
            .map(|i| record(7, i, format!("payload-{i}").as_bytes()))
            .collect();
        let mut composite = CompositeObjectWriter::new(3, WriteOptions::default());
        for (object_id, records) in [(1, &inputs[..10]), (2, &inputs[10..])] {
            let mut writer = ObjectWriter::open(
                object_id,
                storage.as_ref(),
                64,
                16 << 20,
                WriteOptions::default(),
            )
            .await
            .unwrap();
            writer.write(7, records).await.unwrap();
            let size = writer.close().await.unwrap();
            let component = object_metadata(object_id, size);
            let info = ObjectReader::new(component.clone(), storage.clone())
                .basic_object_info()
                .await
                .unwrap()
                .clone();
            composite
                .add_component(&component, info.index_block.entries().to_vec())
                .unwrap();
        }
        composite.close(storage.as_ref()).await.unwrap();

        // Committed as compaction does: COMPOSITE bit, retained (not physical) size.
        let metadata = S3ObjectMetadata {
            attributes: ObjectAttributes::new(0, true, false),
            ..object_metadata(3, composite.size())
        };
        let reader = ObjectReader::new(metadata, storage.clone());
        let found = reader.find(7, 0, 20, usize::MAX).await.unwrap();
        assert!(found.fulfilled);
        let mut read_back = Vec::new();
        for index in &found.blocks {
            let block = reader.read_block(index).await.unwrap();
            read_back.extend(decode_data_block(&block).unwrap());
        }
        assert_eq!(read_back, inputs);
    }

    /// Corrupt footer magic must fail with InvalidFormat, not panic.
    #[tokio::test]
    async fn corrupt_footer_rejected() {
        let storage = Arc::new(MemoryObjectStorage::new(0));
        let mut writer =
            ObjectWriter::open(1, storage.as_ref(), 1024, 16 << 20, WriteOptions::default())
                .await
                .unwrap();
        writer.write(1, &[record(1, 0, b"x")]).await.unwrap();
        let size = writer.close().await.unwrap();

        // Corrupt the footer magic (last 8 bytes).
        let key = gen_object_key(0, 1);
        let mut bytes = storage
            .read(&ReadOptions::default(), &key)
            .await
            .unwrap()
            .to_vec();
        let n = bytes.len();
        bytes[n - 1] ^= 0xFF;
        storage
            .write(&WriteOptions::default(), &key, bytes.into())
            .await
            .unwrap();

        let reader = ObjectReader::new(object_metadata(1, size), storage.clone());
        let err = reader.basic_object_info().await.unwrap_err();
        assert!(matches!(err, ObjectError::InvalidFormat { .. }));
    }
}
