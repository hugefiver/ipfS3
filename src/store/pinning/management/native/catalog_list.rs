//! Shared fixed schema metadata. Editing a captured layout never edits its catalog.
use serde::{Deserialize, Serialize};
use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

#[derive(Clone, PartialEq, Eq)]
pub struct CatalogList<T>(Arc<Vec<T>>);
impl<T> From<Vec<T>> for CatalogList<T> {
    fn from(value: Vec<T>) -> Self {
        Self(Arc::new(value))
    }
}
impl<T> Deref for CatalogList<T> {
    type Target = Vec<T>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<T: Clone> DerefMut for CatalogList<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        Arc::make_mut(&mut self.0)
    }
}
impl<'a, T> IntoIterator for &'a CatalogList<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
impl<T: Serialize> Serialize for CatalogList<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.as_ref().serialize(serializer)
    }
}
impl<'de, T: Deserialize<'de>> Deserialize<'de> for CatalogList<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Vec::deserialize(deserializer).map(Into::into)
    }
}
