use super::*;
use std::path::Path;

/// Reframes a test archive with a different progress byte stream and consistent
/// storage lengths. The outer wire remains verified; no live history is edited.
pub(super) fn rewrite(
    source: &Path,
    target: &Path,
    id: &str,
    payload: &[u8],
    first_ordinal: usize,
) {
    let mut reader = v3::Reader::new(
        std::fs::File::open(source).unwrap(),
        v3::Projection::PrivateRecovery,
        files::stream::budget(),
    )
    .unwrap();
    let mut writer = files::stream::writer(
        target.to_str().unwrap(),
        v3::Projection::PrivateRecovery,
        files::stream::budget(),
    )
    .unwrap();
    let chunks = payload.chunks(v3::CELL_BYTES).collect::<Vec<_>>();
    let mut ordinals = (first_ordinal..first_ordinal + chunks.len())
        .map(|n| n.to_string())
        .collect::<Vec<_>>();
    ordinals.sort();
    let mut emitted = false;
    let mut skip = false;
    let mut root = false;
    let mut replace = false;
    while let Some(event) = reader.next_frame().unwrap() {
        match event {
            v3::Event::Frame(v3::Frame::Trailer { .. }) => {}
            v3::Event::Frame(frame) => {
                if let v3::Frame::Row { key, .. } = &frame {
                    skip = key.table == "pin_management_archive_chunks" && key.values[0] == id;
                    root = key.table == "pin_management_archive" && key.values[0] == id;
                    replace = false;
                    if skip && !emitted {
                        for ordinal in &ordinals {
                            let key = NativeKey {
                                table: "pin_management_archive_chunks".into(),
                                values: vec![id.into(), ordinal.clone()],
                            };
                            writer
                                .frame(&v3::Frame::Row {
                                    section: v3::Section::Native,
                                    key,
                                    cells: 3,
                                })
                                .unwrap();
                            writer
                                .complete_cell("archive_id", id.as_bytes(), v3::CellValue::Text)
                                .unwrap();
                            writer
                                .complete_cell("ordinal", ordinal.as_bytes(), v3::CellValue::Text)
                                .unwrap();
                            writer
                                .frame(&v3::Frame::Cell {
                                    column: "payload".into(),
                                })
                                .unwrap();
                            writer
                                .cell_output()
                                .write_all(
                                    hex::encode(
                                        chunks[ordinal.parse::<usize>().unwrap() - first_ordinal],
                                    )
                                    .as_bytes(),
                                )
                                .unwrap();
                            writer
                                .frame(&v3::Frame::CellEnd {
                                    value: v3::CellValue::Text,
                                })
                                .unwrap();
                            writer.frame(&v3::Frame::RowEnd).unwrap();
                        }
                        emitted = true;
                    }
                }
                if skip {
                    continue;
                }
                writer.frame(&frame).unwrap();
                if let v3::Frame::Cell { column } = frame {
                    replace = root && matches!(column.as_str(), "bytes" | "chunks");
                    if replace {
                        let value = if column == "bytes" {
                            payload.len()
                        } else {
                            chunks.len()
                        };
                        writer
                            .cell_output()
                            .write_all(value.to_string().as_bytes())
                            .unwrap();
                    }
                }
            }
            v3::Event::Chunk(bytes) if !skip && !replace => {
                writer.cell_output().write_all(&bytes).unwrap()
            }
            v3::Event::Chunk(_) => {}
        }
    }
    assert!(reader.verified() && emitted);
    files::stream::finish(writer).unwrap();
}

pub(super) fn values(payload: &[u8]) -> Vec<serde_json::Value> {
    serde_json::Deserializer::from_slice(payload)
        .into_iter()
        .map(|value| value.unwrap())
        .collect()
}
pub(super) fn encode(values: &[serde_json::Value]) -> Vec<u8> {
    let mut raw = Vec::new();
    for value in values {
        serde_json::to_writer(&mut raw, value).unwrap();
    }
    raw
}
pub(super) fn flat(values: &[serde_json::Value]) -> Vec<serde_json::Value> {
    values.iter().cloned().map(|mut value| {
        if let Some(page) = value.as_object_mut() {
            assert_eq!(page.remove("format").unwrap(),"S75-source-rows-1");
            let mut parts = Vec::new();
            for row in page["parts"].as_array().unwrap() {
                for cell in row[2].as_array().unwrap() {
                    parts.push(serde_json::json!({"table":row[0],"key":row[1],"column":cell[0],"offset":cell[1],"length":cell[2],"bytes":cell[3]}));
                }
            }
            page.insert("parts".into(),parts.into());
        }
        value
    }).collect()
}

pub(super) async fn assert_chunks(recovered: &mut history::Recovery, id: &str, payload: &[u8]) {
    for (ordinal, expected) in payload.chunks(v3::CELL_BYTES).enumerate() {
        let key = NativeKey {
            table: "pin_management_archive_chunks".into(),
            values: vec![id.into(), ordinal.to_string()],
        };
        let mut raw = Vec::new();
        recovered.cell(&key, "payload", &mut raw).await.unwrap();
        assert_eq!(hex::decode(raw).unwrap(), expected);
    }
}
