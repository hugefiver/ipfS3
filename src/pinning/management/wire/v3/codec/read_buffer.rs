use super::{Budget, Read, ResidentGuard, Result, SafeError, invalid, io, limit};

pub(super) const READ_BUFFER_BYTES: usize = 64 * 1024;

pub(super) struct ReadBuffer {
    bytes: Vec<u8>,
    start: usize,
    end: usize,
    _memory: ResidentGuard,
}

impl ReadBuffer {
    pub(super) fn new(budget: &mut Budget) -> Result<Self> {
        let memory = limit(budget.reserve(READ_BUFFER_BYTES))?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(READ_BUFFER_BYTES)
            .map_err(|_| SafeError::LimitExceeded)?;
        bytes.resize(READ_BUFFER_BYTES, 0);
        Ok(Self {
            bytes,
            start: 0,
            end: 0,
            _memory: memory,
        })
    }

    pub(super) fn available(&self) -> usize {
        self.end - self.start
    }

    pub(super) fn read_prepaid_exact(
        &mut self,
        input: &mut impl Read,
        budget: &mut Budget,
        mut output: &mut [u8],
    ) -> Result<()> {
        while !output.is_empty() {
            limit(budget.check())?;
            let cached = self.available().min(output.len());
            if cached != 0 {
                output[..cached].copy_from_slice(&self.bytes[self.start..self.start + cached]);
                self.start += cached;
                output = &mut output[cached..];
                continue;
            }
            // The still-missing logical bytes are prepaid, only speculative
            // bytes borrow the remaining ordinary/transfer/task raw allowance.
            let missing = output.len();
            let extra = limit(budget.remaining_reader_raw())?;
            let request =
                (READ_BUFFER_BYTES as u64).min((missing as u64).saturating_add(extra)) as usize;
            match input.read(&mut self.bytes[..request]) {
                Ok(n) => {
                    self.start = 0;
                    self.end = n;
                    limit(budget.record_reader(n.saturating_sub(missing) as u64, 0))?;
                    if n == 0 {
                        return Err(SafeError::InvalidInput);
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(invalid(error)),
            }
        }
        Ok(())
    }
}

pub(super) fn read_direct_exact(
    input: &mut impl Read,
    budget: &Budget,
    mut output: &mut [u8],
) -> Result<()> {
    while !output.is_empty() {
        limit(budget.check())?;
        match input.read(output) {
            Ok(n) => {
                limit(budget.check())?;
                if n == 0 {
                    return Err(SafeError::InvalidInput);
                }
                output = &mut output[n..];
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(invalid(error)),
        }
    }
    Ok(())
}

pub(super) fn probe_eof(input: &mut impl Read, budget: &Budget) -> Result<()> {
    loop {
        limit(budget.check())?;
        match input.read(&mut [0]) {
            Ok(n) => {
                limit(budget.check())?;
                return if n == 0 {
                    Ok(())
                } else {
                    Err(SafeError::InvalidInput)
                };
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(invalid(error)),
        }
    }
}
