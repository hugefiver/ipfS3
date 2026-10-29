//! Read-only receipt access bound to its connection owner.
use crate::store::pinning::management::{self, admission, records};
use records::locator::{AuthoritySelector, AuthorityView};
use sea_orm::{ConnectionTrait, DatabaseBackend, DbErr, ExecResult, QueryResult, Statement};

#[derive(Clone, Copy)]
pub(crate) enum ReceiptCatalog<'a> {
    Full(&'a dyn FullReceiptConnection),
    Runtime(
        &'a crate::store::RuntimeTransaction,
        records::DecodeWork<'a>,
    ),
    Observed(&'a admission::worker_observation::WorkerReadWindow),
    Quota(&'a crate::store::pinning::quota::production::QuotaReadWindow<'a>),
}

/// Full reads retain the concrete connection owner, including transaction-bound
/// maintenance read capabilities. The receipt wrapper itself remains read-only.
#[async_trait::async_trait]
pub(crate) trait FullReceiptConnection: ConnectionTrait {
    async fn upgrade_read(&self) -> management::Result<bool>;
    async fn scope(
        &self,
        spec: &management::native::scope::ScopeSpec,
        budget: &mut management::native::meter::Budget,
    ) -> management::Result<management::native::scope::ScopedRead>;
}

#[async_trait::async_trait]
impl<C: ConnectionTrait> FullReceiptConnection for C {
    async fn upgrade_read(&self) -> management::Result<bool> {
        crate::store::initialization::read_context::active(self).await
    }
    async fn scope(
        &self,
        spec: &management::native::scope::ScopeSpec,
        budget: &mut management::native::meter::Budget,
    ) -> management::Result<management::native::scope::ScopedRead> {
        management::native::scope::collect_scope(self, spec, budget).await
    }
}

impl<'a> ReceiptCatalog<'a> {
    pub(crate) fn full<C: ConnectionTrait>(db: &'a C) -> Self {
        Self::Full(db)
    }

    pub(crate) async fn upgrade_read(self) -> management::Result<bool> {
        match self {
            Self::Full(db) => db.upgrade_read().await,
            _ => Ok(false),
        }
    }

    fn connection(&self) -> &dyn ConnectionTrait {
        match self {
            Self::Full(db) => *db,
            Self::Runtime(tx, _) => tx.db(),
            Self::Observed(window) => window.db(),
            Self::Quota(window) => window.db(),
        }
    }

    pub(crate) async fn classify(self) -> crate::error::AppResult<Vec<String>> {
        match self {
            Self::Full(_) => super::super::admission_schema::receipt_schema(&self).await,
            Self::Runtime(tx, work) => tx.schema_versions(work).await,
            Self::Observed(window) => management::schema::validate_observed(window)
                .await
                .map_err(admission::work_error),
            Self::Quota(window) => management::schema::validate_quota(window)
                .await
                .map_err(admission::work_error),
        }
    }

    pub(crate) async fn load(
        self,
        selectors: &[AuthoritySelector],
        work: records::DecodeWork<'_>,
    ) -> management::Result<AuthorityView> {
        match self {
            Self::Full(_) => admission::authority::load(&self, selectors, work).await,
            Self::Runtime(tx, _) => admission::authority::load_runtime(tx, selectors, work).await,
            Self::Observed(window) => {
                admission::authority::load_observed(window, selectors, work).await
            }
            Self::Quota(window) => admission::authority::load_quota(window, selectors, work).await,
        }
    }

    // Keep full-layout and qualified reads in separate futures on the normal worker stack.
    pub(crate) fn collect_scope<'b>(
        self,
        spec: &'b management::native::scope::ScopeSpec,
        budget: &'b mut management::native::meter::Budget,
        work: records::DecodeWork<'b>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = management::Result<management::native::scope::ScopedRead>,
                > + Send
                + 'b,
        >,
    >
    where
        'a: 'b,
    {
        match self {
            Self::Full(db) => Box::pin(db.scope(spec, budget)),
            Self::Runtime(tx, _) => Box::pin(management::native::scope::collect_runtime_scope(
                tx, spec, budget, work,
            )),
            _ => Box::pin(async move {
                management::native::scope::collect_scope(&self, spec, budget).await
            }),
        }
    }
}

#[async_trait::async_trait]
impl ConnectionTrait for ReceiptCatalog<'_> {
    fn get_database_backend(&self) -> DatabaseBackend {
        self.connection().get_database_backend()
    }

    async fn execute(&self, _statement: Statement) -> Result<ExecResult, DbErr> {
        Err(DbErr::Custom("receipt catalog is read-only".into()))
    }

    async fn execute_unprepared(&self, _sql: &str) -> Result<ExecResult, DbErr> {
        Err(DbErr::Custom("receipt catalog is read-only".into()))
    }

    async fn query_one(&self, statement: Statement) -> Result<Option<QueryResult>, DbErr> {
        self.connection().query_one(statement).await
    }

    async fn query_all(&self, statement: Statement) -> Result<Vec<QueryResult>, DbErr> {
        self.connection().query_all(statement).await
    }
}
