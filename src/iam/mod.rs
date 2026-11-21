mod action;

pub use action::Action;

use std::collections::{BTreeMap, HashMap};

use anyhow::ensure;
use serde::Deserialize;

use crate::{pinning::tags::ObjectTag, store::object_version::PublicVersionId};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rule {
    effect: Effect,
    user: Option<String>,
    bucket: Option<String>,
    actions: Vec<Action>,
    prefix: Option<String>,
    version: Option<String>,
    request_tags: Option<BTreeMap<String, String>>,
    existing_tags: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Effect {
    Allow,
    Deny,
}

#[derive(Debug, Clone, Copy)]
pub enum Resource<'a> {
    Bucket,
    Object(&'a str),
    Prefix(&'a str),
}

#[derive(Debug, Clone, Copy)]
pub struct Context<'a> {
    pub action: Action,
    pub resource: Resource<'a>,
    pub version: Option<&'a str>,
    pub request_tags: Option<&'a [ObjectTag]>,
    /// `None` means not loaded; a completed lookup with no tags uses an empty slice.
    pub existing_tags: Option<&'a [ObjectTag]>,
}

#[derive(Debug, Clone, Default)]
pub struct Policies {
    users: HashMap<String, UserRules>,
    buckets: HashMap<String, Vec<Rule>>,
}

#[derive(Debug, Clone, Default)]
struct UserRules {
    any_bucket: Vec<Rule>,
    buckets: HashMap<String, Vec<Rule>>,
}

pub struct Scope<'a> {
    user: Option<&'a UserRules>,
    user_bucket: Option<&'a Vec<Rule>>,
    bucket: Option<&'a Vec<Rule>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Decision {
    Allow,
    Deny,
    NeedsExistingTags,
}

impl Policies {
    pub fn new(rules: &[Rule]) -> anyhow::Result<Self> {
        use anyhow::Context as _;

        let mut policies = Self::default();
        for (i, rule) in rules.iter().enumerate() {
            rule.validate()
                .with_context(|| format!("auth.policy rule {}", i + 1))?;
            let rules = if let Some(user) = &rule.user {
                let user = policies.users.entry(user.clone()).or_default();
                match &rule.bucket {
                    Some(bucket) => user.buckets.entry(bucket.clone()).or_default(),
                    None => &mut user.any_bucket,
                }
            } else {
                let bucket = rule.bucket.as_ref().expect("validated rule scope");
                policies.buckets.entry(bucket.clone()).or_default()
            };
            rules.push(rule.clone());
        }
        Ok(policies)
    }

    pub fn is_empty(&self) -> bool {
        self.users.is_empty() && self.buckets.is_empty()
    }

    pub fn has_scope(&self, user: &str, bucket: Option<&str>) -> bool {
        self.users.contains_key(user)
            || bucket.is_some_and(|bucket| self.buckets.contains_key(bucket))
    }

    pub fn scope(&self, user: &str, bucket: Option<&str>) -> Scope<'_> {
        let user = self.users.get(user);
        Scope {
            user,
            user_bucket: user.and_then(|user| bucket.and_then(|bucket| user.buckets.get(bucket))),
            bucket: bucket.and_then(|bucket| self.buckets.get(bucket)),
        }
    }
}

impl Scope<'_> {
    pub fn is_unrestricted(&self) -> bool {
        self.user.is_none() && self.bucket.is_none()
    }

    pub fn needs_existing_tags(&self, context: &Context<'_>) -> bool {
        self.decision(context) == Decision::NeedsExistingTags
    }

    pub fn allows(&self, context: &Context<'_>) -> bool {
        self.decision(context) == Decision::Allow
    }

    pub(crate) fn decision(&self, context: &Context<'_>) -> Decision {
        if self.is_unrestricted() {
            return Decision::Allow;
        }
        if !context.action.accepts_resource(context.resource) {
            return Decision::Deny;
        }
        let rules = self
            .user
            .into_iter()
            .flat_map(|user| &user.any_bucket)
            .chain(self.user_bucket.into_iter().flatten())
            .chain(self.bucket.into_iter().flatten());

        let mut allow = false;
        let mut pending_allow = false;
        let mut pending_deny = false;
        for rule in rules.filter(|rule| rule.matches_request(context)) {
            match (
                rule.effect,
                rule.existing_tags.as_ref(),
                context.existing_tags,
            ) {
                (Effect::Allow, Some(_), None) => pending_allow = true,
                (Effect::Deny, Some(_), None) => pending_deny = true,
                (effect, expected, tags) if tags_match(expected, tags) => match effect {
                    Effect::Allow => allow = true,
                    Effect::Deny => return Decision::Deny,
                },
                _ => {}
            }
        }

        // A lookup matters only if it can change the authorization decision.
        match (allow, pending_allow, pending_deny) {
            (true, _, true) | (false, true, _) => Decision::NeedsExistingTags,
            (true, _, false) => Decision::Allow,
            (false, false, _) => Decision::Deny,
        }
    }
}

impl Rule {
    pub(crate) fn user(&self) -> Option<&str> {
        self.user.as_deref()
    }

    fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.user.is_some() || self.bucket.is_some(),
            "user or bucket is required"
        );
        ensure!(
            self.user
                .as_ref()
                .is_none_or(|user| !user.trim().is_empty()),
            "user cannot be empty"
        );
        ensure!(
            self.bucket
                .as_ref()
                .is_none_or(|bucket| !bucket.trim().is_empty()),
            "bucket cannot be empty"
        );
        ensure!(!self.actions.is_empty(), "actions cannot be empty");
        ensure!(
            self.prefix.as_ref().is_none_or(|prefix| !prefix.is_empty()),
            "omit prefix to match all keys"
        );
        if let Some(tags) = &self.request_tags {
            ensure!(
                !tags.is_empty() && tags.keys().all(|key| !key.is_empty()),
                "request_tags must contain non-empty tag keys"
            );
        }
        if let Some(tags) = &self.existing_tags {
            ensure!(
                !tags.is_empty() && tags.keys().all(|key| !key.is_empty()),
                "existing_tags must contain non-empty tag keys"
            );
        }
        if let Some(version) = &self.version {
            PublicVersionId::parse_s3(version)?;
        }
        for &action in &self.actions {
            ensure!(
                action != Action::ListBuckets || self.bucket.is_none(),
                "ListBuckets cannot be bucket-scoped"
            );
            ensure!(
                self.prefix.is_none() || !action.accepts_resource(Resource::Bucket),
                "prefix is unavailable for {action:?}"
            );
            ensure!(
                self.version.is_none() || action.has_version(),
                "version is unavailable for {action:?}"
            );
            ensure!(
                self.request_tags.is_none() || action.has_request_tags(),
                "request_tags are unavailable for {action:?}"
            );
            ensure!(
                self.existing_tags.is_none() || action.has_existing_tags(),
                "existing_tags are unavailable for {action:?}"
            );
        }
        Ok(())
    }

    fn matches_request(&self, context: &Context<'_>) -> bool {
        self.actions.contains(&context.action)
            && self
                .version
                .as_deref()
                .is_none_or(|version| context.version == Some(version))
            && self
                .prefix
                .as_deref()
                .is_none_or(|prefix| match context.resource {
                    Resource::Bucket => false,
                    Resource::Object(key) => key.starts_with(prefix),
                    // A single rule must cover every key. Even a deeper deny overlaps this request.
                    Resource::Prefix(requested) => {
                        requested.starts_with(prefix)
                            || matches!(self.effect, Effect::Deny) && prefix.starts_with(requested)
                    }
                })
            && tags_match(self.request_tags.as_ref(), context.request_tags)
    }
}

fn tags_match(required: Option<&BTreeMap<String, String>>, actual: Option<&[ObjectTag]>) -> bool {
    required.is_none_or(|required| {
        actual.is_some_and(|tags| {
            required.iter().all(|(key, value)| {
                tags.iter()
                    .any(|tag| &tag.key == key && &tag.value == value)
            })
        })
    })
}

#[cfg(test)]
mod tests;
