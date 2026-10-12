use std::io;

use anyhow::{Result, anyhow};
use ipfs_s3_gateway::pinning::management::wire::MAX_WIRE_BYTES;
use serde::Serialize;
use tokio::io::AsyncWriteExt;

fn unavailable() -> anyhow::Error { anyhow!("management artifact unavailable or incomplete (details withheld)") }

pub async fn reserve(path: &str) -> Result<tokio::fs::File> {
    let mut options = tokio::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    options.mode(0o600);
    // No mkdir, overwrite or error-path deletion of a possibly replaced file.
    options.open(path).await.map_err(|_| unavailable())
}

pub async fn write(file: &mut tokio::fs::File, bytes: &[u8]) -> Result<()> {
    if bytes.len() > MAX_WIRE_BYTES { return Err(unavailable()); }
    file.write_all(bytes).await.map_err(|_| unavailable())?;
    file.sync_all().await.map_err(|_| unavailable())
}

struct ReportBuffer(Vec<u8>);

impl io::Write for ReportBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > (1024 * 1024usize).saturating_sub(self.0.len()) {
            return Err(io::Error::other("management report limit"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}

pub async fn print(report: &impl Serialize) -> Result<()> {
    let mut buffer = ReportBuffer(Vec::new());
    serde_json::to_writer_pretty(&mut buffer, report).map_err(|_| anyhow!("management report budget exceeded"))?;
    buffer.0.push(b'\n');
    let mut stdout = tokio::io::stdout();
    stdout.write_all(&buffer.0).await.map_err(|_| anyhow!("management report output unavailable"))?;
    stdout.flush().await.map_err(|_| anyhow!("management report output unavailable"))
}
