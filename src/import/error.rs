use crate::error::AppError;
use crate::import::model::ImportExecutionError;

impl TryFrom<AppError> for ImportExecutionError {
    type Error = AppError;

    fn try_from(error: AppError) -> Result<Self, Self::Error> {
        match error {
            AppError::StaleImportOwnership => Ok(Self::Superseded),
            error => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use crate::error::AppError;
    use crate::import::model::ImportExecutionError;

    #[test]
    fn stale_import_ownership_becomes_superseded_execution() {
        assert!(matches!(
            ImportExecutionError::try_from(AppError::StaleImportOwnership),
            Ok(ImportExecutionError::Superseded)
        ));
    }
}
