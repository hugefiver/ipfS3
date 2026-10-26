//! Reservation dispatch tied to the actual publication transaction owner.
use super::super::{legacy_migration::ReceiptCatalog, management::records::DecodeWork};
use super::*;

#[derive(Clone, Copy)]
pub(super) enum PublicationDatabase<'a> {
    Full(&'a DatabaseConnection),
    Store(&'a crate::store::Store),
}

impl std::ops::Deref for PublicationDatabase<'_> {
    type Target = DatabaseConnection;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Full(db) => db,
            Self::Store(store) => store.db(),
        }
    }
}

impl<'a> From<&'a DatabaseConnection> for PublicationDatabase<'a> {
    fn from(db: &'a DatabaseConnection) -> Self {
        Self::Full(db)
    }
}

#[async_trait::async_trait]
pub(super) trait Reservations: Send {
    type Connection: ConnectionTrait;
    fn db(&self) -> &Self::Connection;
    fn runtime(&self) -> Option<&crate::store::RuntimeTransaction> {
        None
    }
    fn runtime_work(&self) -> Option<&super::super::management::records::SharedDecodeBudget> {
        None
    }
    fn catalog<'a>(&'a self, work: DecodeWork<'a>) -> ReceiptCatalog<'a> {
        match self.runtime() {
            Some(tx) => tx.catalog(work),
            None => ReceiptCatalog::full(self.db()),
        }
    }
    async fn reserve(
        &mut self,
        provider: &str,
        target: &PinTargetSpec,
        limits: &ProviderLimitMap,
        now: DateTime<Utc>,
        work: super::super::management::records::DecodeWork<'_>,
    ) -> AppResult<quota::ReservationOutcome>;
}

pub(super) struct Borrowed<'a, C>(pub(super) &'a C);

#[async_trait::async_trait]
impl<C: ConnectionTrait> Reservations for Borrowed<'_, C> {
    type Connection = C;
    fn db(&self) -> &C {
        self.0
    }
    async fn reserve(
        &mut self,
        provider: &str,
        target: &PinTargetSpec,
        limits: &ProviderLimitMap,
        now: DateTime<Utc>,
        work: super::super::management::records::DecodeWork<'_>,
    ) -> AppResult<quota::ReservationOutcome> {
        quota::reserve_unique_with_work(
            self.0,
            provider,
            &target.cid,
            target.logical_size,
            limits,
            now,
            work,
        )
        .await
    }
}

pub(in crate::store::pinning) enum PublicationTransaction {
    Full(DatabaseTransaction),
    Runtime(
        crate::store::RuntimeTransaction,
        super::super::management::records::SharedDecodeBudget,
    ),
}

impl PublicationTransaction {
    pub(in crate::store::pinning) fn db(&self) -> &DatabaseTransaction {
        match self {
            Self::Full(tx) => tx,
            Self::Runtime(tx, _) => tx.db(),
        }
    }
    pub(super) fn into_db(self) -> DatabaseTransaction {
        match self {
            Self::Full(tx) => tx,
            Self::Runtime(tx, _) => tx.into_db(),
        }
    }
}

#[async_trait::async_trait]
impl Reservations for PublicationTransaction {
    type Connection = DatabaseTransaction;
    fn db(&self) -> &DatabaseTransaction {
        self.db()
    }
    fn runtime(&self) -> Option<&crate::store::RuntimeTransaction> {
        match self {
            Self::Runtime(tx, _) => Some(tx),
            Self::Full(_) => None,
        }
    }
    fn runtime_work(&self) -> Option<&super::super::management::records::SharedDecodeBudget> {
        match self {
            Self::Runtime(_, work) => Some(work),
            Self::Full(_) => None,
        }
    }
    async fn reserve(
        &mut self,
        provider: &str,
        target: &PinTargetSpec,
        limits: &ProviderLimitMap,
        now: DateTime<Utc>,
        work: super::super::management::records::DecodeWork<'_>,
    ) -> AppResult<quota::ReservationOutcome> {
        if let Self::Runtime(tx, session) = self {
            return quota::reserve_runtime_publication(
                tx,
                provider,
                &target.cid,
                target.logical_size,
                limits,
                now,
                DecodeWork::shared(session),
            )
            .await;
        }
        quota::production::reserve(
            self,
            provider,
            &target.cid,
            target.logical_size,
            limits,
            now,
            work,
        )
        .await
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn attempt(
    db: PublicationDatabase<'_>,
    request: PublicationRequest,
    entries: Vec<PublicationObject>,
    batch: Option<ZipBatchPublication>,
    upload_target: Option<MultipartUploadTargetIdentity>,
    standard_guard: Option<StandardMutationGuard>,
    import_guard: Option<ImportPublicationGuard>,
    result_rows: Vec<import_job_result::ActiveModel>,
    import_now: Option<DateTime<Utc>>,
    hot_receipt: Option<HotPublicationReceipt>,
    limits: ProviderLimitMap,
    decision: Option<DecidedSnapshot>,
    replay: Option<ZipCompletedReplay>,
    session: &super::super::management::records::SharedDecodeBudget,
) -> Result<PublicationResult, TransactionError<AppError>> {
    let mut owner = match db {
        PublicationDatabase::Store(store) => PublicationTransaction::Runtime(
            store
                .begin_publication_runtime(DecodeWork::shared(session))
                .await?,
            session.clone(),
        ),
        PublicationDatabase::Full(db) => {
            PublicationTransaction::Full(db.begin().await.map_err(TransactionError::Connection)?)
        }
    };
    let result = Box::pin(publish_with_reservations(
        &mut owner,
        request,
        entries,
        batch.as_ref(),
        upload_target.as_ref(),
        standard_guard.as_ref(),
        import_guard.as_ref(),
        result_rows,
        import_now,
        hot_receipt.as_ref(),
        &limits,
        decision.as_ref(),
        replay.as_ref(),
    ))
    .await;
    match result {
        Ok(value) => {
            owner
                .into_db()
                .commit()
                .await
                .map_err(TransactionError::Connection)?;
            Ok(value)
        }
        Err(error) => {
            owner
                .into_db()
                .rollback()
                .await
                .map_err(TransactionError::Connection)?;
            Err(TransactionError::Transaction(error))
        }
    }
}
