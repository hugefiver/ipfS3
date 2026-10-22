//! Synchronous work hooks for the shared live receipt resolver.
use crate::{
    error::{AppError, AppResult},
    store::pinning::management::{self, ManagementError, records::DecodeWork},
};
use serde::{Serialize, de::DeserializeOwned};

pub(crate) trait Work {
    type Error;
    fn application(error: AppError) -> Self::Error;
    fn decode(&self) -> Option<DecodeWork<'_>>;
    fn copy<T: Serialize + ?Sized>(&self, value: &T) -> std::result::Result<(), Self::Error>;
    fn cells(&self, count: usize, bytes: usize) -> std::result::Result<(), Self::Error>;
    fn parse<T: DeserializeOwned>(
        &self,
        input: &str,
        reason: &str,
    ) -> std::result::Result<T, Self::Error>;
}
impl Work for () {
    type Error = AppError;
    fn application(error: AppError) -> AppError {
        error
    }
    fn decode(&self) -> Option<DecodeWork<'_>> {
        None
    }
    fn copy<T: Serialize + ?Sized>(&self, _: &T) -> AppResult<()> {
        Ok(())
    }
    fn cells(&self, _: usize, _: usize) -> AppResult<()> {
        Ok(())
    }
    fn parse<T: DeserializeOwned>(&self, input: &str, reason: &str) -> AppResult<T> {
        serde_json::from_str(input).map_err(|_| super::super::refused(reason))
    }
}
impl Work for DecodeWork<'_> {
    type Error = ManagementError;
    fn decode(&self) -> Option<DecodeWork<'_>> {
        Some(*self)
    }
    fn application(error: AppError) -> ManagementError {
        match error {
            AppError::ManagementBudget => ManagementError::Budget,
            AppError::InvalidPinningRequest(_) => ManagementError::Changed,
            _ => ManagementError::Database,
        }
    }
    fn copy<T: Serialize + ?Sized>(&self, value: &T) -> management::Result<()> {
        DecodeWork::copy(*self, value)
    }
    fn cells(&self, count: usize, bytes: usize) -> management::Result<()> {
        self.index_cells(count, bytes)
    }
    fn parse<T: DeserializeOwned>(&self, input: &str, _: &str) -> management::Result<T> {
        // Preserve the public resolver's serde semantics. Only synchronous work
        // holds the traversal budget; the caller's DB/transaction owns all SQL.
        self.with(|work| {
            work.charge_json_owned(input).map_err(|error| {
                if error == ManagementError::Budget {
                    error
                } else {
                    ManagementError::Changed
                }
            })?;
            serde_json::from_str(input).map_err(|_| ManagementError::Changed)
        })
    }
}
