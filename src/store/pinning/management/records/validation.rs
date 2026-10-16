//! One bounded, uncached validation traversal over exact private predecessors.
use super::*;
use std::collections::BTreeSet;

#[derive(Default)]
pub(crate) struct ValidationContext {
    active: BTreeSet<native::NativeKey>,
    points: usize,
    pub(super) decoding: std::sync::Arc<std::sync::Mutex<evidence::Budget>>,
}
impl ValidationContext {
    pub(crate) fn with_work(decoding: SharedDecodeBudget) -> Self {
        Self {
            active: BTreeSet::new(),
            points: 0,
            decoding,
        }
    }
    pub(crate) fn with_record<T>(
        &mut self,
        key: native::NativeKey,
        validate: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        {
            let mut work = self
                .decoding
                .lock()
                .map_err(|_| ManagementError::InvalidEvidence)?;
            if work.authority_path.contains(&key) {
                return Err(ManagementError::InvalidEvidence);
            }
            work.copy(&key)?;
            work.authority_path.insert(key.clone());
        }
        self.active.insert(key.clone());
        let result = validate(self);
        self.active.remove(&key);
        self.decoding
            .lock()
            .map_err(|_| ManagementError::InvalidEvidence)?
            .authority_path
            .remove(&key);
        result
    }
    pub(crate) fn point(&mut self) -> Result<()> {
        self.points = self.points.checked_add(1).ok_or(ManagementError::Budget)?;
        if self.points > native::MAX_ROWS {
            return Err(ManagementError::Budget);
        }
        Ok(())
    }
    pub(crate) fn with_predecessor<T>(
        &mut self,
        validate: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        {
            let mut work = self
                .decoding
                .lock()
                .map_err(|_| ManagementError::InvalidEvidence)?;
            if work.predecessor_depth >= 32 {
                return Err(ManagementError::Budget);
            }
            work.predecessor_depth += 1;
        }
        let result = validate(self);
        self.decoding
            .lock()
            .map_err(|_| ManagementError::InvalidEvidence)?
            .predecessor_depth -= 1;
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(item: usize) -> native::NativeKey {
        native::NativeKey {
            table: "pin_management_records".into(),
            values: vec![
                "scope".into(),
                "plan".into(),
                item.to_string(),
                "action".into(),
            ],
        }
    }

    fn predecessor_path(context: &mut ValidationContext, remaining: usize) -> Result<()> {
        if remaining == 0 {
            return Ok(());
        }
        context.with_record(key(remaining), |context| {
            context.with_predecessor(|context| predecessor_path(context, remaining - 1))
        })
    }

    #[test]
    fn predecessor_depth_is_bounded_across_the_whole_path_and_cycles_fail_closed() {
        let mut context = ValidationContext::default();
        predecessor_path(&mut context, 32).unwrap();
        assert_eq!(
            predecessor_path(&mut context, 33),
            Err(ManagementError::Budget)
        );
        assert_eq!(
            context.with_record(key(1), |context| context
                .with_record(key(2), |context| context.with_record(key(1), |_| Ok(())))),
            Err(ManagementError::InvalidEvidence)
        );
        predecessor_path(&mut context, 32).unwrap();
    }
    #[test]
    fn ordinary_dependency_members_do_not_consume_migration_depth() {
        fn members(context: &mut ValidationContext, n: usize) -> Result<()> {
            if n == 0 {
                return Ok(());
            }
            context.with_record(key(n), |context| members(context, n - 1))
        }
        let mut context = ValidationContext::default();
        members(&mut context, 40).unwrap();
        predecessor_path(&mut context, 32).unwrap();
    }

    #[test]
    fn independent_paths_revalidate_their_authority_instead_of_reusing_a_cached_permission() {
        let mut context = ValidationContext::default();
        let mut checks = 0;
        context
            .with_record(key(1), |_| {
                checks += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(
            context.with_record(key(1), |_| {
                checks += 1;
                Err::<(), _>(ManagementError::Unauthorized)
            }),
            Err(ManagementError::Unauthorized)
        );
        assert_eq!(checks, 2);
    }
}
