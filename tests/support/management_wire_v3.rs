#![allow(dead_code)]

use ipfs_s3_gateway::{
    pinning::management::{PlanRequest, SafeError, wire::v3},
    store::pinning::management::{ManagementError, Result, records},
};
use sea_orm::DatabaseConnection;
use std::io::Write;

pub fn rewrite(
    bytes: &[u8],
    mut change: impl FnMut(&mut v3::Frame),
) -> std::result::Result<Vec<u8>, SafeError> {
    let mut reader = v3::Reader::new(bytes, v3::Projection::Review, v3::files::stream::budget())?;
    let mut writer = v3::Writer::new(
        Vec::new(),
        v3::Projection::Review,
        v3::files::stream::budget(),
    )?;
    while let Some(event) = reader.next_frame()? {
        match event {
            v3::Event::Frame(mut frame) => {
                change(&mut frame);
                writer.frame(&frame)?;
            }
            v3::Event::Chunk(bytes) => writer.cell_output().write_all(&bytes).unwrap(),
        }
    }
    reader.into_verified()?;
    writer.finish()
}

pub fn export(binding: &records::ConfirmedBinding) -> Vec<u8> {
    let mut writer = v3::Writer::new(
        Vec::new(),
        v3::Projection::Review,
        v3::files::stream::budget(),
    )
    .unwrap();
    v3::planning::local_confirmed(&mut writer, binding, v3::Projection::Review).unwrap();
    writer.finish().unwrap()
}

pub async fn restore(
    db: &DatabaseConnection,
    auth: &records::LocalAuthorization,
    request: &PlanRequest,
    bytes: &[u8],
) -> Result<records::ConfirmedBinding> {
    let imported = v3::archive_cursor::stage_import(
        std::io::Cursor::new(bytes),
        v3::Projection::Review,
        v3::files::stream::budget(),
    )
    .map_err(|_| ManagementError::Confirmation)?;
    let binding = records::load_confirmed_authority(
        db,
        auth,
        request,
        records::SharedDecodeBudget::default(),
    )
    .await?;
    v3::matching::confirmed_database(db, imported.into_cursor(), &binding)
        .await
        .map_err(|_| ManagementError::Confirmation)?;
    Ok(binding)
}
