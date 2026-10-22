use super::{Budget, ManagementError, Result};

impl Budget {
    /// Extra raw IO must fit every ceiling after the logical read is prepaid.
    pub(crate) fn remaining_reader_raw(&self) -> Result<u64> {
        self.check()?;
        let mut remaining = self
            .limits
            .bytes
            .checked_sub(self.work.raw_bytes)
            .ok_or(ManagementError::Budget)?;
        let transferred = self
            .work
            .raw_bytes
            .checked_add(self.work.copied_bytes)
            .ok_or(ManagementError::Budget)?;
        if let Some(limit) = self.transfer_bytes {
            remaining = remaining.min(
                limit
                    .checked_sub(transferred)
                    .ok_or(ManagementError::Budget)?,
            );
        }
        if let Some(limit) = self.task_bytes {
            let used = transferred
                .checked_add(self.work.output_bytes)
                .ok_or(ManagementError::Budget)?;
            remaining = remaining.min(limit.checked_sub(used).ok_or(ManagementError::Budget)?);
        }
        Ok(remaining)
    }

    pub(crate) fn prepay_reader(&mut self, raw: u64, copied: u64) -> Result<()> {
        self.check()?;
        self.record_reader(raw, copied)
    }

    pub(crate) fn prepay_reader_frame_header(&mut self, raw: u64, copied: u64) -> Result<()> {
        self.charge(0, 0, 1, 0)?;
        self.record_reader(raw, copied)
    }

    /// Settle successful read-ahead before checking cancellation or the deadline.
    pub(crate) fn record_reader(&mut self, raw: u64, copied: u64) -> Result<()> {
        let result = (|| {
            self.work.raw_bytes = self
                .work
                .raw_bytes
                .checked_add(raw)
                .ok_or(ManagementError::Budget)?;
            self.work.copied_bytes = self
                .work
                .copied_bytes
                .checked_add(copied)
                .ok_or(ManagementError::Budget)?;
            if self.work.raw_bytes > self.limits.bytes {
                return Err(ManagementError::Budget);
            }
            self.check()
        })();
        self.failed |= result.is_err();
        result
    }

    pub(crate) fn poison_reader(&mut self) {
        self.failed = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::atomic::Ordering, time::Instant};

    #[test]
    fn frame_header_overflows_keep_cell_raw_and_copy_debt_in_order() {
        let mut cells = Budget::default();
        cells.work.cells = u64::MAX;
        assert!(cells.prepay_reader_frame_header(4, 4).is_err());
        assert_eq!(cells.work.cells, u64::MAX);
        assert_eq!(cells.work.raw_bytes, 0);
        assert_eq!(cells.work.copied_bytes, 0);
        assert!(cells.failed);

        let mut raw = Budget::new(super::super::Limits {
            bytes: u64::MAX,
            ..Default::default()
        });
        raw.work.raw_bytes = u64::MAX - 2;
        assert!(raw.prepay_reader_frame_header(4, 4).is_err());
        assert_eq!(raw.work.cells, 1);
        assert_eq!(raw.work.raw_bytes, u64::MAX - 2);
        assert_eq!(raw.work.copied_bytes, 0);
        assert!(raw.failed);

        let mut copied = Budget::default();
        copied.work.copied_bytes = u64::MAX - 2;
        assert!(copied.prepay_reader_frame_header(4, 4).is_err());
        assert_eq!(copied.work.cells, 1);
        assert_eq!(copied.work.raw_bytes, 4);
        assert_eq!(copied.work.copied_bytes, u64::MAX - 2);
        assert!(copied.failed);
    }

    #[test]
    fn frame_header_checks_ordinary_and_transfer_ceilings_before_io() {
        let mut ordinary = Budget::default();
        ordinary.work.rows = ordinary.limits.rows + 1;
        assert!(ordinary.prepay_reader_frame_header(4, 4).is_err());
        assert_eq!(ordinary.work.cells, 1);
        assert_eq!(ordinary.work.raw_bytes, 0);
        assert_eq!(ordinary.work.copied_bytes, 0);
        assert!(ordinary.failed);

        let mut transfer = Budget::default();
        transfer.restrict_transfer_bytes(7).unwrap();
        assert!(transfer.prepay_reader_frame_header(4, 4).is_err());
        assert_eq!(transfer.work.cells, 1);
        assert_eq!(transfer.work.raw_bytes, 4);
        assert_eq!(transfer.work.copied_bytes, 4);
        assert!(transfer.failed);
    }

    #[test]
    fn cancelled_frame_header_does_not_acquire_cell_or_reader_debt() {
        let mut budget = Budget::default();
        budget.cancellation().store(true, Ordering::Relaxed);
        assert!(budget.prepay_reader_frame_header(4, 4).is_err());
        assert_eq!(budget.work.cells, 0);
        assert_eq!(budget.work.raw_bytes, 0);
        assert_eq!(budget.work.copied_bytes, 0);
        assert!(budget.check().is_err());
    }

    #[test]
    fn successful_io_debt_is_settled_before_cancellation_or_deadline_checks() {
        let mut cancelled = Budget::default();
        cancelled.prepay_reader(4, 4).unwrap();
        cancelled.cancellation().store(true, Ordering::Relaxed);
        assert!(cancelled.record_reader(13, 0).is_err());
        assert_eq!(cancelled.work.raw_bytes, 17);
        assert_eq!(cancelled.work.copied_bytes, 4);
        assert!(cancelled.check().is_err());

        let mut expired = Budget::default();
        expired.prepay_reader(4, 4).unwrap();
        expired.deadline = Instant::now();
        assert!(expired.record_reader(13, 0).is_err());
        assert_eq!(expired.work.raw_bytes, 17);
        assert_eq!(expired.work.copied_bytes, 4);
        assert!(expired.check().is_err());
    }
}
