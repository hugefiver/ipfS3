use super::invalid;
use crate::{
    error::AppResult,
    pinning::{
        config::ProviderLimitMap,
        identity::ProviderRouteSnapshot,
        zip_policy::{CapturedZipIntent, ZipIntentSelection},
    },
};
use std::collections::{BTreeMap, BTreeSet};

use crate::store::pinning::management::{
    ManagementError, Result as MeasurementResult, records::ReadSession,
};
use serde::{Serialize, ser};

#[derive(Default)]
pub(super) struct CaptureBounds {
    pub capture_bytes: usize,
    policy: usize,
    providers: usize,
    source_providers: usize,
    historical: usize,
    provider_count: usize,
    provider_total: usize,
    intent_count: usize,
}

fn copied<T: Serialize + ?Sized>(session: &ReadSession, value: &T) -> MeasurementResult<usize> {
    let (nodes, bytes) = session.measure(value)?;
    // JSON keys/scalars also contribute, covering struct headers and Vec spare capacity.
    nodes
        .checked_mul(32)
        .and_then(|n| n.checked_add(bytes))
        .ok_or(ManagementError::Budget)
}

impl CaptureBounds {
    pub(super) fn measure(
        capture: &CapturedZipIntent,
        session: &ReadSession,
    ) -> MeasurementResult<Self> {
        let (nodes, bytes) = session.measure(capture)?;
        if nodes > crate::pinning::management::wire::MAX_WIRE_NODES
            || bytes > crate::pinning::management::wire::MAX_WIRE_BYTES
        {
            return Err(ManagementError::Budget);
        }
        let mut bounds = Self {
            capture_bytes: copied(session, capture)?,
            intent_count: 1,
            ..Default::default()
        };
        capture
            .serialize(Probe {
                bounds: &mut bounds,
                session,
                kind: "",
                list: "",
            })
            .map_err(|_| ManagementError::Budget)?;
        Ok(bounds)
    }

    pub(super) fn selection_bytes(
        &self,
        capture: &CapturedZipIntent,
        source: Option<&crate::pinning::zip_policy::ZipBoundOutput>,
        outputs: &[crate::pinning::zip_policy::ZipBoundOutput],
        session: &ReadSession,
    ) -> MeasurementResult<usize> {
        let add = |a: usize, b: usize| a.checked_add(b).ok_or(ManagementError::Budget);
        let mul = |a: usize, b: usize| a.checked_mul(b).ok_or(ManagementError::Budget);
        let output_bytes = outputs
            .iter()
            .try_fold(0usize, |n, output| add(n, copied(session, output)?))?;
        let source_bytes = source.map(|s| copied(session, s)).transpose()?.unwrap_or(0);
        let mut bytes = add(self.capture_bytes, mul(outputs.len(), 288)?)?;
        if source.is_some() {
            bytes = add(bytes, copied(session, capture.source_decision())?)?;
            for intent in &capture.source_decision().effective_intents {
                let targets =
                    if intent.content_mode == crate::pinning::tags::ContentMode::Decompressed {
                        output_bytes
                    } else {
                        source_bytes
                    };
                bytes = add(
                    bytes,
                    add(
                        add(copied(session, intent)?, self.source_providers)?,
                        add(source_bytes, targets)?,
                    )?,
                )?;
            }
        }
        let header = copied(
            session,
            &(
                capture.origin(),
                &capture.source_decision().control_revision,
                capture.policy_revision(),
            ),
        )?;
        // planned temporaries coexist with the persistent lease and decision intent copies.
        let planned = mul(2, add(self.policy, self.providers)?.max(self.historical))?;
        for (kind, output) in source
            .into_iter()
            .filter(|_| capture.controls().targets.source)
            .map(|output| (crate::pinning::zip_policy::ZipOutputKind::Source, output))
            .chain(
                outputs
                    .iter()
                    .map(|output| (crate::pinning::zip_policy::ZipOutputKind::Extracted, output)),
            )
        {
            let binding = copied(session, output)?;
            bytes = add(bytes, add(mul(binding, 2)?, header)?)?;
            if capture.automatic_output_eligible(kind, &output.key) {
                bytes = add(
                    bytes,
                    add(planned, mul(mul(binding, 2)?, self.intent_count)?)?,
                )?;
            }
        }
        session.check()?;
        Ok(bytes)
    }

    pub(super) fn shape(
        &self,
        capture: &CapturedZipIntent,
        outputs: usize,
        source: bool,
    ) -> AppResult<usize> {
        let budget = || crate::error::AppError::ManagementBudget;
        let manual = capture.source_decision().effective_intents.len();
        let objects = outputs
            .checked_add(usize::from(source))
            .ok_or_else(budget)?;
        let automatic = objects.checked_mul(self.intent_count).ok_or_else(budget)?;
        let leases = manual.checked_add(automatic).ok_or_else(budget)?;
        let logical = manual
            .checked_mul(objects)
            .and_then(|n| n.checked_add(automatic))
            .and_then(|n| n.checked_mul(self.provider_count))
            .ok_or_else(budget)?;
        objects
            .checked_add(leases)
            .and_then(|n| n.checked_add(logical.checked_mul(2)?))
            .and_then(|n| n.checked_add(self.provider_total))
            .ok_or_else(budget)
    }
}

// Borrowed traversal keeps private capture fields private and never builds JSON or a Value tree.
struct Probe<'a> {
    bounds: &'a mut CaptureBounds,
    session: &'a ReadSession,
    kind: &'static str,
    list: &'static str,
}

macro_rules! scalar {
    ($($method:ident($ty:ty)),* $(,)?) => {$(
        fn $method(self, _: $ty) -> Result<(), serde_json::Error> { Ok(()) }
    )*};
}
impl<'a> ser::Serializer for Probe<'a> {
    type Ok = ();
    type Error = serde_json::Error;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;
    scalar!(
        serialize_bool(bool),
        serialize_i8(i8),
        serialize_i16(i16),
        serialize_i32(i32),
        serialize_i64(i64),
        serialize_u8(u8),
        serialize_u16(u16),
        serialize_u32(u32),
        serialize_u64(u64),
        serialize_f32(f32),
        serialize_f64(f64),
        serialize_char(char),
        serialize_str(&str),
        serialize_bytes(&[u8])
    );
    fn serialize_none(self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn serialize_unit(self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn serialize_unit_struct(self, _: &'static str) -> Result<(), Self::Error> {
        Ok(())
    }
    fn serialize_unit_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<(), Self::Error> {
        value.serialize(self)
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        value.serialize(self)
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        value.serialize(self)
    }
    fn serialize_seq(mut self, len: Option<usize>) -> Result<Self, Self::Error> {
        match self.list {
            "providers" | "source_providers" => {
                let count = len.unwrap_or(0);
                self.bounds.provider_count = self.bounds.provider_count.max(count);
                self.bounds.provider_total = self
                    .bounds
                    .provider_total
                    .checked_add(count)
                    .ok_or_else(|| {
                        <serde_json::Error as ser::Error>::custom("publication shape budget")
                    })?;
            }
            "intents" => self.bounds.intent_count = self.bounds.intent_count.max(len.unwrap_or(0)),
            _ => {}
        }
        self.list = "";
        Ok(self)
    }
    fn serialize_tuple(self, _: usize) -> Result<Self, Self::Error> {
        Ok(self)
    }
    fn serialize_tuple_struct(self, _: &'static str, _: usize) -> Result<Self, Self::Error> {
        Ok(self)
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self, Self::Error> {
        Ok(self)
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Self, Self::Error> {
        Ok(self)
    }
    fn serialize_struct(mut self, name: &'static str, _: usize) -> Result<Self, Self::Error> {
        self.kind = name;
        Ok(self)
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self, Self::Error> {
        Ok(self)
    }
}

impl Probe<'_> {
    fn child<T: Serialize + ?Sized>(
        &mut self,
        value: &T,
        list: &'static str,
    ) -> Result<(), serde_json::Error> {
        value.serialize(Probe {
            bounds: self.bounds,
            session: self.session,
            kind: "",
            list,
        })
    }
    fn field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), serde_json::Error> {
        let slot = match (self.kind, key) {
            ("CapturedPolicy", "policy") => Some(&mut self.bounds.policy),
            ("CapturedPolicy", "providers") => Some(&mut self.bounds.providers),
            ("CapturedZipIntent", "source_providers") => Some(&mut self.bounds.source_providers),
            ("ZipOutputDecision", "intents") => Some(&mut self.bounds.historical),
            _ => None,
        };
        if let Some(slot) = slot {
            let measured = copied(self.session, value).map_err(|_| {
                <serde_json::Error as ser::Error>::custom("publication measurement budget")
            })?;
            *slot = (*slot).max(measured);
        }
        self.child(value, key)
    }
}

macro_rules! sequence {
    ($trait:ident, $method:ident) => {
        impl ser::$trait for Probe<'_> {
            type Ok = ();
            type Error = serde_json::Error;
            fn $method<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
                self.child(value, "")
            }
            fn end(self) -> Result<(), Self::Error> {
                Ok(())
            }
        }
    };
}
sequence!(SerializeSeq, serialize_element);
sequence!(SerializeTuple, serialize_element);
sequence!(SerializeTupleStruct, serialize_field);
sequence!(SerializeTupleVariant, serialize_field);
impl ser::SerializeMap for Probe<'_> {
    type Ok = ();
    type Error = serde_json::Error;
    fn serialize_key<T: Serialize + ?Sized>(&mut self, _: &T) -> Result<(), Self::Error> {
        Ok(())
    }
    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
        self.child(value, "")
    }
    fn end(self) -> Result<(), Self::Error> {
        Ok(())
    }
}
impl ser::SerializeStruct for Probe<'_> {
    type Ok = ();
    type Error = serde_json::Error;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        self.field(key, value)
    }
    fn end(self) -> Result<(), Self::Error> {
        Ok(())
    }
}
impl ser::SerializeStructVariant for Probe<'_> {
    type Ok = ();
    type Error = serde_json::Error;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        self.field(key, value)
    }
    fn end(self) -> Result<(), Self::Error> {
        Ok(())
    }
}

type FrozenAllocation = (
    ProviderLimitMap,
    BTreeMap<String, ProviderRouteSnapshot>,
    BTreeSet<String>,
);

pub(super) fn frozen_allocation(
    capture: &CapturedZipIntent,
    selected: &ZipIntentSelection,
) -> AppResult<FrozenAllocation> {
    let mut limits = ProviderLimitMap::new();
    let mut routes = BTreeMap::new();
    let mut enabled = BTreeSet::new();
    for lease in &selected.leases {
        for provider in &lease.providers {
            let limit = capture
                .provider_limits(&provider.route.provider_id)
                .ok_or_else(invalid)?
                .clone();
            if limit.enabled != provider.enabled
                || !lease.intent.providers.contains(&provider.config_name)
                || limits
                    .insert(provider.config_name.clone(), limit.clone())
                    .is_some_and(|old| old != limit)
                || routes
                    .insert(provider.config_name.clone(), provider.route.clone())
                    .is_some_and(|old| old != provider.route)
            {
                return Err(invalid());
            }
            if provider.enabled {
                enabled.insert(provider.config_name.clone());
            }
        }
        if lease
            .intent
            .providers
            .iter()
            .any(|p| !limits.contains_key(p))
        {
            return Err(invalid());
        }
    }
    Ok((limits, routes, enabled))
}

pub(super) type SelectionShape = Vec<(
    String,
    crate::pinning::policy::LeaseIntent,
    Vec<(String, bool)>,
    Vec<(String, String, i64)>,
)>;

pub(super) fn selection_shape(selected: &ZipIntentSelection) -> SelectionShape {
    selected
        .leases
        .iter()
        .map(|lease| {
            (
                lease.owner.object_id.clone(),
                lease.intent.clone(),
                lease
                    .providers
                    .iter()
                    .map(|provider| (provider.config_name.clone(), provider.enabled))
                    .collect(),
                lease
                    .targets
                    .iter()
                    .map(|target| {
                        (
                            target.object_id.clone(),
                            target.cid.clone(),
                            target.logical_size,
                        )
                    })
                    .collect(),
            )
        })
        .collect()
}

pub(super) fn selection_shape_bytes(
    selected: &ZipIntentSelection,
    session: &mut ReadSession,
) -> MeasurementResult<usize> {
    session.publication_preflight(|session| {
        selected.leases.iter().try_fold(0usize, |total, lease| {
            let mut bytes = copied(session, &(&lease.owner.object_id, &lease.intent))?;
            for provider in &lease.providers {
                bytes = bytes
                    .checked_add(copied(session, &(&provider.config_name, provider.enabled))?)
                    .ok_or(ManagementError::Budget)?;
            }
            for target in &lease.targets {
                bytes = bytes
                    .checked_add(copied(
                        session,
                        &(&target.object_id, &target.cid, target.logical_size),
                    )?)
                    .ok_or(ManagementError::Budget)?;
            }
            total.checked_add(bytes).ok_or(ManagementError::Budget)
        })
    })
}

pub(super) fn verify_candidate(
    candidate: &SelectionShape,
    exact: &ZipIntentSelection,
) -> AppResult<()> {
    if candidate.len() != exact.leases.len()
        || !candidate.iter().zip(&exact.leases).all(
            |((owner, intent, providers, targets), lease)| {
                owner == &lease.owner.object_id
                    && intent == &lease.intent
                    && providers.len() == lease.providers.len()
                    && providers
                        .iter()
                        .zip(&lease.providers)
                        .all(|((name, enabled), provider)| {
                            name == &provider.config_name && *enabled == provider.enabled
                        })
                    && targets.len() == lease.targets.len()
                    && targets
                        .iter()
                        .zip(&lease.targets)
                        .all(|((owner, cid, size), target)| {
                            owner == &target.object_id
                                && cid == &target.cid
                                && *size == target.logical_size
                        })
            },
        )
    {
        return Err(invalid());
    }
    Ok(())
}

#[cfg(test)]
pub(in crate::store::pinning::publication) async fn attachment_pairs<
    R: super::super::reservations::Reservations,
>(
    reservations: &R,
    selected: &ZipIntentSelection,
    limits: &ProviderLimitMap,
    maintenance: &mut crate::store::zip::publication_lease::PublicationLease<'_>,
    work: crate::store::pinning::management::records::DecodeWork<'_>,
) -> AppResult<Vec<(String, String)>> {
    let mut pairs = BTreeSet::new();
    for lease in &selected.leases {
        for provider in super::super::ordered_enabled_providers(&lease.intent, limits)? {
            for target in &lease.targets {
                maintenance.checkpoint(reservations.db()).await?;
                let cid = match reservations.runtime() {
                    Some(_) => {
                        super::ledger::allocation_cid_with_work(
                            &reservations.catalog(work),
                            &provider,
                            &target.cid,
                            work,
                        )
                        .await?
                    }
                    None => {
                        super::ledger::allocation_cid(reservations.db(), &provider, &target.cid)
                            .await?
                    }
                };
                pairs.insert((provider.clone(), cid));
            }
        }
    }
    Ok(pairs.into_iter().collect())
}
