//! CLI report output; protected archive IO is shared with the v3 library.
use anyhow::{Result, anyhow};
pub use ipfs_s3_gateway::pinning::management::wire::v3::files::{reserve, stream};
use serde::Serialize;
use std::io;
use tokio::io::AsyncWriteExt;

struct ReportBuffer(Vec<u8>);
impl io::Write for ReportBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > (1024 * 1024usize).saturating_sub(self.0.len()) {
            return Err(io::Error::other("management report limit"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
pub async fn print(report: &impl Serialize) -> Result<()> {
    let mut buffer = ReportBuffer(Vec::new());
    serde_json::to_writer_pretty(&mut buffer, report)
        .map_err(|_| anyhow!("management report budget exceeded"))?;
    buffer.0.push(b'\n');
    let mut stdout = tokio::io::stdout();
    stdout
        .write_all(&buffer.0)
        .await
        .map_err(|_| anyhow!("management report output unavailable"))?;
    stdout
        .flush()
        .await
        .map_err(|_| anyhow!("management report output unavailable"))
}
