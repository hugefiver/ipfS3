use super::*;

pub(super) type WorkerPhase<'a> =
    Pin<Box<dyn Future<Output = AppResult<WorkerContinuation>> + Send + 'a>>;

pub(super) enum WorkerContinuation {
    Done,
    RunJob(Box<ClaimedPinJob>),
    RecordSubmit(Box<SubmitDispatch>),
    RecoverSubmit(Box<ClaimedPinJob>),
    PersistObservation(Box<PendingObservation>),
    CoordinateObservation(Box<CommittedObservation>),
}

pub(super) struct PendingObservation {
    pub claimed: ClaimedPinJob,
    pub remote: RemotePin,
    pub origin: RemoteStatusOrigin,
    pub source: ObservationSource,
    pub typed: Option<crate::pinning::provider::PinObservation>,
}

pub(super) struct CommittedObservation {
    pub claimed: ClaimedPinJob,
    pub remote: RemotePin,
    pub cluster: bool,
    pub now: DateTime<Utc>,
    pub result: RemoteStatusApplyResult,
}

pub(super) fn admission_future<'a>(
    store: &'a Store,
    coordinator: &'a PinningCoordinator,
    global: &'a Arc<Semaphore>,
    claimed: ClaimedPinJob,
    cancellation: &'a CancellationToken,
) -> WorkerPhase<'a> {
    Box::pin(admit_claimed_job(
        store,
        coordinator,
        global,
        claimed,
        cancellation,
    ))
}

fn phase_future<'a>(
    store: &'a Store,
    coordinator: &'a PinningCoordinator,
    execution: Option<(&'a Arc<Semaphore>, &'a CancellationToken)>,
    continuation: WorkerContinuation,
) -> WorkerPhase<'a> {
    match continuation {
        WorkerContinuation::RunJob(claimed) => {
            let (global, cancellation) = execution.expect("job phase has an execution context");
            operation_future(store, coordinator, global, *claimed, cancellation)
        }
        WorkerContinuation::RecoverSubmit(claimed) => {
            let (global, cancellation) =
                execution.expect("recovery phase has an execution context");
            Box::pin(recover_submit(
                store,
                coordinator,
                global,
                *claimed,
                cancellation,
            ))
        }
        WorkerContinuation::RecordSubmit(observed) => record_future(store, coordinator, *observed),
        WorkerContinuation::PersistObservation(pending) => {
            Box::pin(persist_observation_phase(store, coordinator, *pending))
        }
        WorkerContinuation::CoordinateObservation(committed) => Box::pin(async move {
            coordinate_observation_phase(store, coordinator, *committed).await?;
            Ok(WorkerContinuation::Done)
        }),
        WorkerContinuation::Done => unreachable!("completed phases are not constructed"),
    }
}

pub(super) fn record_future<'a>(
    store: &'a Store,
    coordinator: &'a PinningCoordinator,
    observed: SubmitDispatch,
) -> WorkerPhase<'a> {
    Box::pin(record_submit_phase(
        store,
        coordinator,
        observed.claimed,
        observed.observation,
        observed.capture,
        observed.rpc_invocation,
    ))
}

// No phase-owned future or its native poll frame survives construction of the next phase.
// The finish adapter stops at recovery; a worker driver executes that continuation.
pub(super) async fn drive<'a>(
    store: &'a Store,
    coordinator: &'a PinningCoordinator,
    execution: Option<(&'a Arc<Semaphore>, &'a CancellationToken)>,
    mut phase: WorkerPhase<'a>,
) -> AppResult<Option<ClaimedPinJob>> {
    loop {
        let continuation = {
            let continuation = phase.as_mut().await?;
            drop(phase);
            continuation
        };
        match continuation {
            WorkerContinuation::Done => return Ok(None),
            WorkerContinuation::RecoverSubmit(claimed) if execution.is_none() => {
                return Ok(Some(*claimed));
            }
            continuation => phase = phase_future(store, coordinator, execution, continuation),
        }
    }
}
