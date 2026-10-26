//! Container states for the declaration prefix only. Deep opaque JSON may be
//! larger than a confirmation header; its stack spills without collecting text.
use super::*;
use std::io::{Read, Seek, SeekFrom, Write};

const BLOCK: usize = 4096;
pub(super) struct Stack {
    states: [u8; BLOCK],
    used: usize,
    pages: u64,
    file: Option<std::fs::File>,
}
impl Stack {
    pub fn new() -> Self {
        Self {
            states: [0; BLOCK],
            used: 0,
            pages: 0,
            file: None,
        }
    }
    pub fn len(&self) -> u64 {
        self.pages * BLOCK as u64 + self.used as u64
    }
    pub fn top(&self) -> Result<u8> {
        self.used
            .checked_sub(1)
            .map(|index| self.states[index])
            .ok_or(SafeError::InvalidInput)
    }
    pub fn set(&mut self, state: u8) -> Result<()> {
        let index = self.used.checked_sub(1).ok_or(SafeError::InvalidInput)?;
        self.states[index] = state;
        Ok(())
    }
    pub fn push(&mut self, state: u8, budget: &mut Budget) -> Result<()> {
        if self.used == BLOCK {
            if self.file.is_none() {
                self.file = Some(files::stream::temporary().map_err(invalid)?);
            }
            budget.charge(0, 0, 0, BLOCK as u64).map_err(storage)?;
            let offset = self
                .pages
                .checked_mul(BLOCK as u64)
                .ok_or(SafeError::LimitExceeded)?;
            let file = self.file.as_mut().ok_or(SafeError::InvalidInput)?;
            file.seek(SeekFrom::Start(offset)).map_err(invalid)?;
            file.write_all(&self.states).map_err(invalid)?;
            self.pages = self.pages.checked_add(1).ok_or(SafeError::LimitExceeded)?;
            self.used = 0;
        }
        self.states[self.used] = state;
        self.used += 1;
        Ok(())
    }
    pub fn pop(&mut self, budget: &mut Budget) -> Result<()> {
        self.used = self.used.checked_sub(1).ok_or(SafeError::InvalidInput)?;
        if self.used == 0 && self.pages != 0 {
            self.pages -= 1;
            budget.charge(0, 0, 0, BLOCK as u64).map_err(storage)?;
            let file = self.file.as_mut().ok_or(SafeError::InvalidInput)?;
            let offset = self
                .pages
                .checked_mul(BLOCK as u64)
                .ok_or(SafeError::LimitExceeded)?;
            file.seek(SeekFrom::Start(offset)).map_err(invalid)?;
            file.read_exact(&mut self.states).map_err(invalid)?;
            self.used = BLOCK;
        }
        Ok(())
    }
}
