use std::ops::{Deref, DerefMut};

use super::*;

/// A builder to create data pages.
pub struct IndexPageBuilder(SortedPageBuilder);

impl Default for IndexPageBuilder {
    fn default() -> Self {
        Self::new(PageKind::Delta, false)
    }
}

impl IndexPageBuilder {
    pub fn new(kind: PageKind, is_data: bool) -> Self {
        Self(SortedPageBuilder::new(kind, is_data))
    }

    /// Builds an empty data page.
    pub fn build<A>(self, alloc: &A) -> Result<IndexPageBuf, A::Error>
    where
        A: PageAlloc,
    {
        self.0.build(alloc).map(IndexPageBuf)
    }

    /// Builds a data page with entries from the given iterator.
    pub fn build_from_iter<'a, A, I>(
        mut self,
        alloc: &A,
        iter: &mut I,
    ) -> Result<IndexPageBuf, A::Error>
    where
        A: PageAlloc,
        I: RewindableIter<Key = &'a [u8], Value = Index>,
    {
        self.0.build_from_iter(alloc, iter).map(IndexPageBuf)
    }
}

pub struct IndexPageBuf(SortedPageBuf);

impl IndexPageBuf {
    pub fn as_ptr(&self) -> PagePtr {
        self.0.as_ptr()
    }

    pub fn as_ref<'a>(&self) -> IndexPageRef<'a> {
        IndexPageRef(self.0.as_ref())
    }
}

impl Deref for IndexPageBuf {
    type Target = PagePtr;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl DerefMut for IndexPageBuf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.deref_mut()
    }
}

/// An immutable reference to an index page.
#[derive(Clone)]
pub struct IndexPageRef<'a>(SortedPageRef<'a, &'a [u8], Index>);

impl<'a> IndexPageRef<'a> {
    pub fn new(ptr: PagePtr) -> Self {
        // FIXME
        // assert_eq!(ptr.kind(), PageKind::Delta);
        // assert_eq!(ptr.is_data(), false);
        Self(unsafe { SortedPageRef::new(ptr) })
    }

    pub fn find(&self, target: &[u8]) -> Option<(&'a [u8], Index)> {
        self.0.seek_back(&target)
    }

    pub fn iter(&self) -> IndexPageIter<'a> {
        IndexPageIter::new(self.clone())
    }

    pub fn as_ptr(&self) -> PagePtr {
        self.0.as_ptr()
    }

    pub fn clone_with<A>(
        &self,
        alloc: &A,
        kind: PageKind,
        is_data: bool,
    ) -> Result<PagePtr, A::Error>
    where
        A: PageAlloc,
    {
        let mut ptr = self.0.clone_in(alloc)?;
        ptr.set_kind(kind);
        ptr.set_data(is_data);
        Ok(ptr)
    }
}

impl<'a> Deref for IndexPageRef<'a> {
    type Target = PagePtr;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl<'a> From<PagePtr> for IndexPageRef<'a> {
    fn from(ptr: PagePtr) -> Self {
        Self::new(ptr)
    }
}

/// An iterator over the entries of an index page.
pub struct IndexPageIter<'a>(SortedPageIter<'a, &'a [u8], Index>);

impl<'a> IndexPageIter<'a> {
    pub fn new(page: IndexPageRef<'a>) -> Self {
        Self(SortedPageIter::new(page.0))
    }
}

impl<'a> From<PagePtr> for IndexPageIter<'a> {
    fn from(ptr: PagePtr) -> Self {
        Self::new(ptr.into())
    }
}

impl<'a> From<IndexPageRef<'a>> for IndexPageIter<'a> {
    fn from(page: IndexPageRef<'a>) -> Self {
        Self::new(page)
    }
}

impl<'a> ForwardIter for IndexPageIter<'a> {
    type Key = &'a [u8];
    type Value = Index;

    fn last(&self) -> Option<&(Self::Key, Self::Value)> {
        self.0.last()
    }

    fn next(&mut self) -> Option<&(Self::Key, Self::Value)> {
        self.0.next()
    }
}

impl<'a> SeekableIter for IndexPageIter<'a> {
    fn seek<T>(&mut self, target: &T)
    where
        T: Comparable<Self::Key>,
    {
        self.0.seek(target);
    }
}

impl<'a> RewindableIter for IndexPageIter<'a> {
    fn rewind(&mut self) {
        self.0.rewind();
    }
}