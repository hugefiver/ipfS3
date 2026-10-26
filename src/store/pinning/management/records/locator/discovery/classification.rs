use super::*;
use tokio::task::JoinHandle;

struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(super) async fn classify_row(
    key: NativeKey,
    row: NativeRow,
    resident: Vec<ResidentGuard>,
    budget: &Budget,
) -> Result<(
    NativeKey,
    NativeRow,
    Vec<ResidentGuard>,
    SourceClassification,
)> {
    let check = budget.remaining_budget()?;
    #[cfg(test)]
    let hook = tests::HOOK.try_with(std::sync::Arc::clone).ok();
    #[cfg(test)]
    let blocking_hook = hook.clone();
    budget.check()?;
    let mut task = AbortOnDrop(tokio::task::spawn_blocking(move || {
        #[cfg(test)]
        tests::at(&blocking_hook, tests::Phase::Entered);
        let classification = check.check().and_then(|()| {
            #[cfg(test)]
            tests::at(&blocking_hook, tests::Phase::Classifying);
            classify::classify_source(&key, &row)
        });
        #[cfg(test)]
        tests::at(&blocking_hook, tests::Phase::Classified);
        let classification = check.check().and(classification);
        #[cfg(test)]
        tests::at(
            &blocking_hook,
            tests::Phase::Finished(classification.as_ref().err().copied()),
        );
        // Running CPU work cannot be aborted; its row stays charged until it returns.
        (key, row, resident, classification)
    }));
    let joined = (&mut task.0).await;
    #[cfg(test)]
    tests::at(&hook, tests::Phase::Joined);
    budget.check()?;
    let (key, row, resident, classification) =
        joined.map_err(|_| ManagementError::InvalidEvidence)?;
    Ok((key, row, resident, classification?))
}

#[cfg(test)]
mod tests;
