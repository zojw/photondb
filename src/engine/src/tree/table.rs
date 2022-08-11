use std::sync::Arc;

use bumpalo::Bump;
use crossbeam_epoch::{pin, Guard};

use super::{
    node::*,
    page::*,
    pagecache::PageCache,
    pagestore::PageStore,
    pagetable::PageTable,
    stats::{AtomicStats, Stats},
    Error, Result,
};

#[derive(Clone, Debug)]
pub struct Options {
    pub cache_size: usize,
    pub data_node_size: usize,
    pub data_delta_length: usize,
    pub index_node_size: usize,
    pub index_delta_length: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            cache_size: usize::MAX,
            data_node_size: 8 * 1024,
            data_delta_length: 8,
            index_node_size: 4 * 1024,
            index_delta_length: 4,
        }
    }
}

#[derive(Clone)]
pub struct Table {
    inner: Arc<Inner>,
}

impl Table {
    pub fn open(opts: Options) -> Result<Self> {
        let inner = Inner::open(opts)?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    pub fn get<F>(&self, key: &[u8], lsn: u64, f: F) -> Result<()>
    where
        F: FnMut(Option<&[u8]>),
    {
        let guard = &pin();
        let key = Key::new(key, lsn);
        self.inner.get(key, guard).map(f)
    }

    pub fn iter(&self) -> Iter {
        Iter::new(self.inner.clone())
    }

    pub fn put(&self, key: &[u8], lsn: u64, value: &[u8]) -> Result<()> {
        let guard = &pin();
        let key = Key::new(key, lsn);
        let value = Value::Put(value);
        self.inner.insert(key, value, guard)
    }

    pub fn delete(&self, key: &[u8], lsn: u64) -> Result<()> {
        let guard = &pin();
        let key = Key::new(key, lsn);
        let value = Value::Delete;
        self.inner.insert(key, value, guard)
    }

    pub fn stats(&self) -> Stats {
        self.inner.stats()
    }
}

struct Inner {
    opts: Options,
    table: PageTable,
    cache: PageCache,
    store: PageStore,
    stats: AtomicStats,
}

impl Inner {
    fn open(opts: Options) -> Result<Self> {
        let table = PageTable::default();
        let cache = PageCache::default();
        let store = PageStore::open()?;
        let inner = Self {
            opts,
            table,
            cache,
            store,
            stats: AtomicStats::default(),
        };
        inner.init()
    }

    fn init(self) -> Result<Self> {
        // Initializes the tree as root -> leaf.
        let root_id = self.table.alloc().unwrap();
        let leaf_id = self.table.alloc().unwrap();
        let leaf_page = DataPageBuilder::default().build(&self.cache)?;
        self.table.set(leaf_id, leaf_page.into());
        let mut root_iter = OptionIter::from(([].as_slice(), Index::new(leaf_id, 0)));
        let root_page = IndexPageBuilder::default().build_from_iter(&self.cache, &mut root_iter)?;
        self.table.set(root_id, root_page.into());
        Ok(self)
    }

    fn get<'a: 'g, 'g>(&'a self, key: Key<'_>, guard: &'g Guard) -> Result<Option<&'g [u8]>> {
        loop {
            match self.try_get(key, guard) {
                Ok(value) => {
                    self.stats.op_succeeded.num_gets.inc();
                    return Ok(value);
                }
                Err(Error::Again) => {
                    self.stats.op_conflicted.num_gets.inc();
                    continue;
                }
                Err(err) => return Err(err),
            }
        }
    }

    fn try_get<'a: 'g, 'g>(&'a self, key: Key<'_>, guard: &'g Guard) -> Result<Option<&'g [u8]>> {
        let (node, _) = self.find_leaf(key.raw, guard)?;
        self.lookup_value(key, &node, guard)
    }

    fn insert<'g>(&self, key: Key<'_>, value: Value<'_>, guard: &'g Guard) -> Result<()> {
        let mut iter = OptionIter::from((key, value));
        let page = DataPageBuilder::default().build_from_iter(&self.cache, &mut iter)?;
        loop {
            match self.try_insert(key.raw, page, guard) {
                Ok(_) => {
                    self.stats.op_succeeded.num_inserts.inc();
                    return Ok(());
                }
                Err(Error::Again) => {
                    self.stats.op_conflicted.num_inserts.inc();
                    continue;
                }
                Err(err) => {
                    unsafe {
                        self.cache.dealloc_page(page);
                    }
                    return Err(err);
                }
            }
        }
    }

    fn try_insert<'g>(&self, key: &[u8], mut page: PagePtr, guard: &'g Guard) -> Result<()> {
        let (mut node, _) = self.find_leaf(key, guard)?;
        loop {
            // TODO: This is a bit of a hack.
            if node.page.len() >= u8::MAX / 2 {
                if !node.page.is_locked() {
                    let _ = self.consolidate_data_node(&mut node, guard);
                }
                self.stats.op_conflicted.num_inserts.inc();
                return Err(Error::Again);
            }
            page.set_ver(node.page.ver());
            page.set_len(node.page.len() + 1);
            page.set_next(node.page.as_addr().into());
            page.set_locked(node.page.is_locked());
            let mut should_consolidate = false;
            if !page.is_locked() && page.len() as usize >= self.opts.data_delta_length {
                page.set_locked(true);
                should_consolidate = true;
            }
            match self.table.cas(node.id, page.next(), page.into()) {
                Ok(_) => {
                    if should_consolidate {
                        node.page = page.into();
                        let _ = self.consolidate_data_node(&mut node, guard);
                    }
                    return Ok(());
                }
                Err(addr) => {
                    if let Some(page) = self.page_view(addr.into(), guard) {
                        // We can keep retrying as long as the page version doesn't change.
                        if page.ver() == node.page.ver() {
                            node.page = page;
                            continue;
                        }
                    }
                    return Err(Error::Again);
                }
            }
        }
    }

    fn stats(&self) -> Stats {
        Stats {
            cache_size: self.cache.size() as u64,
            op_succeeded: self.stats.op_succeeded.snapshot(),
            op_conflicted: self.stats.op_conflicted.snapshot(),
            smo_succeeded: self.stats.smo_succeeded.snapshot(),
            smo_conflicted: self.stats.smo_conflicted.snapshot(),
        }
    }
}

impl Inner {
    fn page_addr(&self, id: u64) -> PageAddr {
        self.table.get(id).into()
    }

    fn page_view<'a: 'g, 'g>(&'a self, addr: PageAddr, _: &'g Guard) -> Option<PageView<'g>> {
        match addr {
            PageAddr::Mem(addr) => {
                let page = unsafe { PageRef::new(addr as *mut u8) };
                page.map(PageView::Mem)
            }
            PageAddr::Disk(addr) => self
                .store
                .page_info(addr)
                .map(|info| PageView::Disk(info, addr)),
        }
    }

    fn update_node<'a: 'g, 'g>(
        &'a self,
        id: u64,
        old: impl Into<u64>,
        new: PagePtr,
        _: &'g Guard,
    ) -> Result<PageRef<'g>> {
        self.table
            .cas(id, old.into(), new.into())
            .map(|_| new.into())
            .map_err(|_| unsafe {
                self.cache.dealloc_page(new);
                Error::Again
            })
    }

    fn install_node<'a: 'g, 'g>(&'a self, new: impl Into<u64>) -> Result<u64> {
        let id = self.table.alloc().ok_or(Error::Alloc)?;
        self.table.set(id, new.into());
        Ok(id)
    }

    fn load_page_with_view<'a: 'g, 'g>(
        &'a self,
        _: u64,
        view: PageView<'g>,
        _: &'g Guard,
    ) -> Result<PageRef<'g>> {
        match view {
            PageView::Mem(page) => Ok(page),
            PageView::Disk(_, addr) => {
                let ptr = self.store.load_page(addr)?;
                Ok(ptr.into())
            }
        }
    }

    fn load_page_with_addr<'a: 'g, 'g>(
        &'a self,
        _: u64,
        addr: PageAddr,
        _: &'g Guard,
    ) -> Result<Option<PageRef<'g>>> {
        match addr {
            PageAddr::Mem(addr) => Ok(unsafe { PageRef::new(addr as *mut u8) }),
            PageAddr::Disk(addr) => {
                let ptr = self.store.load_page(addr)?;
                Ok(Some(ptr.into()))
            }
        }
    }
}

impl Inner {
    fn find_leaf<'a: 'g, 'g>(
        &'a self,
        key: &[u8],
        guard: &'g Guard,
    ) -> Result<(Node<'g>, Option<Node<'g>>)> {
        let mut index = ROOT_INDEX;
        let mut range = Range::default();
        let mut parent = None;
        loop {
            let addr = self.page_addr(index.id);
            let page = self.page_view(addr, guard).expect("the node must be valid");
            let node = Node {
                id: index.id,
                page,
                range,
            };
            if node.page.ver() != index.ver {
                self.reconcile_node(node, parent, guard)?;
                return Err(Error::Again);
            }
            if node.page.is_leaf() {
                return Ok((node, parent));
            }
            let (child, right) = self.lookup_index(key, &node, guard)?;
            let child = child.expect("the index must exists");
            index = child.1;
            range.start = child.0;
            if let Some(right) = right {
                range.end = Some(right.0);
            }
            parent = Some(node);
        }
    }

    fn walk_node<'a: 'g, 'g, F>(&'a self, node: &Node<'g>, guard: &'g Guard, mut f: F) -> Result<()>
    where
        F: FnMut(TypedPageRef<'g>) -> bool,
    {
        let mut page = self.load_page_with_view(node.id, node.page, guard)?;
        let mut old_addr = 0;
        let mut new_addr = 0;
        loop {
            let typed_page = TypedPageRef::from(page);
            if let TypedPageRef::Switch(page) = typed_page {
                old_addr = page.old_addr();
                new_addr = page.new_addr();
            } else if f(typed_page) {
                break;
            }
            let mut next_addr = page.next();
            if next_addr == old_addr {
                next_addr = new_addr;
            }
            match self.load_page_with_addr(node.id, next_addr.into(), guard)? {
                Some(next) => page = next,
                None => break,
            }
        }
        Ok(())
    }

    fn lookup_value<'a: 'g, 'g>(
        &'a self,
        key: Key<'_>,
        node: &Node<'g>,
        guard: &'g Guard,
    ) -> Result<Option<&'g [u8]>> {
        let mut value = None;
        self.walk_node(node, guard, |page| {
            if let TypedPageRef::Data(page) = page.into() {
                if let Some((_, v)) = page.find(key) {
                    value = v.into();
                    return true;
                }
            }
            false
        })?;
        Ok(value)
    }

    fn lookup_index<'a: 'g, 'g>(
        &'a self,
        key: &[u8],
        node: &Node<'g>,
        guard: &'g Guard,
    ) -> Result<(Option<IndexItem<'g>>, Option<IndexItem<'g>>)> {
        let mut left_index = None;
        let mut right_index = None;
        self.walk_node(node, guard, |page| {
            if let TypedPageRef::Index(page) = page.into() {
                let (left, right) = page.find(key);
                if let Some(left) = left {
                    if left.1 != NULL_INDEX {
                        left_index = Some(left);
                        right_index = right;
                        return true;
                    }
                }
            }
            false
        })?;
        Ok((left_index, right_index))
    }

    fn data_node_iter<'a: 'g, 'b, 'g>(
        &'a self,
        node: &Node<'g>,
        bump: &'b Bump,
        guard: &'g Guard,
    ) -> Result<DataNodeIter<'g, 'b>> {
        let mut limit = None;
        let mut merger = MergingIterBuilder::with_len(node.page.len() as usize + 1);
        self.walk_node(node, guard, |page| {
            match TypedPageRef::from(page) {
                TypedPageRef::Data(page) => {
                    merger.add(bump.alloc(page.into()));
                }
                TypedPageRef::Split(page) => {
                    if limit == None {
                        let index = page.split_index();
                        limit = Some(index.0);
                    }
                }
                _ => unreachable!(),
            }
            false
        })?;
        Ok(DataNodeIter::new(merger.build(), limit))
    }

    fn delta_data_iter<'a: 'g, 'b, 'g>(
        &'a self,
        node: &Node<'g>,
        bump: &'b Bump,
        guard: &'g Guard,
    ) -> Result<(DataNodeIter<'g, 'b>, Option<PageRef<'g>>)> {
        let mut size = 0;
        let mut base = None;
        let mut limit = None;
        let mut merger = MergingIterBuilder::with_len(node.page.len() as usize + 1);
        self.walk_node(node, guard, |page| {
            match TypedPageRef::from(page) {
                TypedPageRef::Data(page) => {
                    if size < page.content_size() / 2 && limit.is_none() && merger.len() >= 2 {
                        base = Some(page.base());
                        return true;
                    }
                    size += page.content_size();
                    merger.add(bump.alloc(page.into()));
                }
                TypedPageRef::Split(page) => {
                    if limit == None {
                        let index = page.split_index();
                        limit = Some(index.0);
                    }
                }
                _ => unreachable!(),
            }
            false
        })?;
        Ok((DataNodeIter::new(merger.build(), limit), base))
    }

    fn index_node_iter<'a: 'g, 'b, 'g>(
        &'a self,
        node: &Node<'g>,
        bump: &'b Bump,
        guard: &'g Guard,
    ) -> Result<IndexNodeIter<'g, 'b>> {
        let mut limit = None;
        let mut merger = MergingIterBuilder::with_len(node.page.len() as usize + 1);
        self.walk_node(node, guard, |page| {
            match TypedPageRef::from(page) {
                TypedPageRef::Index(page) => {
                    merger.add(bump.alloc(page.into()));
                }
                TypedPageRef::Split(page) => {
                    if limit == None {
                        let index = page.split_index();
                        limit = Some(index.0);
                    }
                }
                _ => unreachable!(),
            }
            false
        })?;
        let iter = merger.build();
        Ok(IndexNodeIter::new(iter, limit))
    }

    fn split_data_node<'a: 'g, 'g>(
        &'a self,
        id: u64,
        page: PageRef<'g>,
        guard: &'g Guard,
    ) -> Result<PageRef<'g>> {
        assert_eq!(page.next(), 0);
        let data_page = DataPageRef::from(page);
        if let Some((sep, mut iter)) = data_page.split() {
            let right_page = DataPageBuilder::default().build_from_iter(&self.cache, &mut iter)?;
            self.install_split_page(id, page, sep, right_page, guard)
                .map(|page| {
                    self.stats.smo_succeeded.num_data_splits.inc();
                    page.into()
                })
                .map_err(|err| {
                    self.stats.smo_conflicted.num_data_splits.inc();
                    err
                })
        } else {
            Ok(page)
        }
    }

    fn split_index_node<'a: 'g, 'g>(
        &'a self,
        id: u64,
        page: PageRef<'g>,
        guard: &'g Guard,
    ) -> Result<PageRef<'g>> {
        assert_eq!(page.next(), 0);
        let index_page = IndexPageRef::from(page);
        if let Some((sep, mut iter)) = index_page.split() {
            let right_page = IndexPageBuilder::default().build_from_iter(&self.cache, &mut iter)?;
            self.install_split_page(id, page, sep, right_page, guard)
                .map(|page| {
                    self.stats.smo_succeeded.num_index_splits.inc();
                    page.into()
                })
                .map_err(|err| {
                    self.stats.smo_conflicted.num_index_splits.inc();
                    err
                })
        } else {
            Ok(page)
        }
    }

    fn install_split_page<'a: 'g, 'g>(
        &'a self,
        left_id: u64,
        left_page: PageRef<'g>,
        split_key: &[u8],
        right_page: PagePtr,
        guard: &'g Guard,
    ) -> Result<SplitPageRef<'g>> {
        let right_id = self.install_node(right_page)?;
        let split = || -> Result<SplitPageRef<'_>> {
            let mut split_page = SplitPageBuilder::default().build_with_index(
                &self.cache,
                split_key,
                Index::new(right_id, 0),
            )?;
            split_page.set_ver(left_page.ver() + 1);
            split_page.set_len(left_page.len() + 1);
            split_page.set_next(left_page.into());
            split_page.set_leaf(left_page.is_leaf());
            self.update_node(left_id, left_page, split_page, guard)
                .map(SplitPageRef::from)
        };
        split().map_err(|err| unsafe {
            self.table.dealloc(right_id, guard);
            self.cache.dealloc_page(right_page);
            err
        })
    }

    fn reconcile_node<'a: 'g, 'g>(
        &'a self,
        node: Node<'a>,
        parent: Option<Node<'a>>,
        guard: &'g Guard,
    ) -> Result<()> {
        let page = self.load_page_with_view(node.id, node.page, guard)?;
        if let TypedPageRef::Split(page) = page.into() {
            let split_index = page.split_index();
            if let Some(mut parent) = parent {
                self.reconcile_split_node(&node, &mut parent, split_index, guard)?;
            } else {
                self.reconcile_split_root(&node, split_index, guard)?;
            }
        }
        Ok(())
    }

    fn reconcile_split_node<'a: 'g, 'g>(
        &'a self,
        node: &Node<'g>,
        parent: &mut Node<'g>,
        split_index: IndexItem<'g>,
        guard: &'g Guard,
    ) -> Result<PageRef<'g>> {
        let left_index = (node.range.start, Index::new(node.id, node.page.ver()));
        let mut index_page = if let Some(right_start) = node.range.end {
            assert!(right_start > split_index.0);
            let delta_data = [left_index, split_index, (right_start, NULL_INDEX)];
            let mut delta_iter = SliceIter::from(&delta_data);
            IndexPageBuilder::default().build_from_iter(&self.cache, &mut delta_iter)?
        } else {
            let delta_data = [left_index, split_index];
            let mut delta_iter = SliceIter::from(&delta_data);
            IndexPageBuilder::default().build_from_iter(&self.cache, &mut delta_iter)?
        };

        // TODO: This is a bit of a hack.
        if parent.page.len() == u8::MAX {
            let _ = self.consolidate_index_node(&parent, guard);
            return Err(Error::Again);
        }
        index_page.set_ver(parent.page.ver());
        index_page.set_len(parent.page.len() + 1);
        index_page.set_next(parent.page.as_addr().into());
        let page = self.update_node(parent.id, index_page.next(), index_page, guard)?;

        if page.len() as usize >= self.opts.index_delta_length {
            parent.page = page.into();
            self.consolidate_index_node(parent, guard)
        } else {
            Ok(page)
        }
    }

    fn reconcile_split_root<'a: 'g, 'g>(
        &'a self,
        node: &Node<'g>,
        split_index: IndexItem<'g>,
        guard: &'g Guard,
    ) -> Result<PageRef<'g>> {
        assert_eq!(node.id, ROOT_INDEX.id);
        let root_addr = node.page.as_addr();

        // Builds a new root with the original root in the left and the split node in the right.
        let left_id = self.install_node(root_addr)?;
        let left_index = Index::new(left_id, node.page.ver());
        let root_data = [([].as_slice(), left_index), split_index];
        let mut root_iter = SliceIter::from(&root_data);
        let root_page = IndexPageBuilder::default().build_from_iter(&self.cache, &mut root_iter)?;

        self.update_node(node.id, root_addr, root_page, guard)
            .map_err(|err| {
                self.table.dealloc(left_id, guard);
                err
            })
    }

    fn dealloc_page_chain<'a: 'g, 'g>(
        &'a self,
        head: impl Into<u64>,
        until: impl Into<u64>,
        guard: &'g Guard,
    ) {
        let mut next = head.into();
        let until = until.into();
        let cache = self.cache.clone();
        guard.defer(move || unsafe {
            let mut old_addr = 0;
            let mut new_addr = 0;
            while next != until {
                if let PageAddr::Mem(addr) = next.into() {
                    if let Some(page) = PagePtr::new(addr as *mut u8) {
                        if let TypedPageRef::Switch(page) = page.into() {
                            old_addr = page.old_addr();
                            new_addr = page.new_addr();
                        }
                        next = page.next();
                        if next == old_addr {
                            next = new_addr;
                        }
                        cache.dealloc_page(page);
                        continue;
                    }
                }
                break;
            }
        });
    }

    fn consolidate_data_node<'a: 'g, 'g>(
        &'a self,
        node: &mut Node<'g>,
        guard: &'g Guard,
    ) -> Result<PageRef<'g>> {
        let bump = Bump::new();
        let (mut data_iter, base_page) = self.delta_data_iter(node, &bump, guard)?;
        let mut data_page =
            DataPageBuilder::default().build_from_iter(&self.cache, &mut data_iter)?;
        data_page.set_ver(node.page.ver());
        if let Some(base) = base_page {
            data_page.set_len(base.len() + 1);
            data_page.set_next(base.into());
        }
        let delta_len = node.page.len() - data_page.len();

        let old_addr = node.page.as_addr().into();
        if let Err(new_addr) = self.table.cas(node.id, old_addr, data_page.into()) {
            if node.page.is_locked() {
                if let Some(page) = self.page_view(new_addr.into(), guard) {
                    if page.ver() == node.page.ver() {
                        node.page = page;
                        return self
                            .switch_consolidated_page(node, old_addr, data_page, delta_len, guard);
                    }
                }
            }
            unsafe { self.cache.dealloc_page(data_page) };
            self.stats.smo_conflicted.num_data_consolidates.inc();
            return Err(Error::Again);
        }

        let new_page = PageRef::from(data_page);
        self.dealloc_page_chain(old_addr, new_page.next(), guard);
        self.stats.smo_succeeded.num_data_consolidates.inc();

        if new_page.next() == 0 && new_page.size() >= self.opts.data_node_size {
            self.split_data_node(node.id, new_page.into(), guard)
        } else {
            Ok(new_page)
        }
    }

    fn switch_consolidated_page<'a: 'g, 'g>(
        &'a self,
        node: &mut Node<'g>,
        old_addr: u64,
        new_page: PagePtr,
        delta_len: u8,
        guard: &'g Guard,
    ) -> Result<PageRef<'g>> {
        assert!(node.page.is_locked());
        let mut switch_page =
            SwitchPageBuilder::default().build(&self.cache, old_addr, new_page.into())?;
        switch_page.set_ver(node.page.ver());
        switch_page.set_leaf(node.page.is_leaf());

        loop {
            switch_page.set_len(node.page.len() - delta_len + 1);
            switch_page.set_next(node.page.as_addr().into());
            match self
                .table
                .cas(node.id, switch_page.next(), switch_page.into())
            {
                Ok(_) => {
                    self.dealloc_page_chain(old_addr, new_page.next(), guard);
                    self.stats.smo_succeeded.num_switch_pages.inc();
                    return Ok(switch_page.into());
                }
                Err(addr) => {
                    if let Some(page) = self.page_view(addr.into(), guard) {
                        if page.ver() == node.page.ver() {
                            node.page = page;
                            continue;
                        }
                    }
                    unsafe { self.cache.dealloc_page(new_page) };
                    self.stats.smo_conflicted.num_switch_pages.inc();
                    return Err(Error::Again);
                }
            }
        }
    }

    fn consolidate_index_node<'a: 'g, 'g>(
        &'a self,
        node: &Node<'g>,
        guard: &'g Guard,
    ) -> Result<PageRef<'g>> {
        let bump = Bump::new();
        let mut iter = self.index_node_iter(node, &bump, guard)?;
        let mut page = IndexPageBuilder::default().build_from_iter(&self.cache, &mut iter)?;
        page.set_ver(node.page.ver());

        let addr = node.page.as_addr();
        let page = self
            .update_node(node.id, addr, page, guard)
            .map(|page| {
                self.dealloc_page_chain(addr, page.next(), guard);
                self.stats.smo_succeeded.num_index_consolidates.inc();
                page
            })
            .map_err(|err| {
                self.stats.smo_conflicted.num_index_consolidates.inc();
                err
            })?;

        if page.next() == 0 && page.size() >= self.opts.index_node_size {
            self.split_index_node(node.id, page, guard)
        } else {
            Ok(page)
        }
    }
}

pub struct Iter {
    bump: Bump,
    guard: Guard,
    inner: Arc<Inner>,
    cursor: Option<Vec<u8>>,
}

impl Iter {
    fn new(inner: Arc<Inner>) -> Self {
        Self {
            bump: Bump::new(),
            guard: pin(),
            inner,
            cursor: Some(Vec::new()),
        }
    }

    pub fn next_with<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut((&[u8], &[u8])),
    {
        while let Some(mut iter) = self.next_iter()? {
            iter.rewind();
            while let Some(item) = iter.current() {
                f(item);
                iter.next();
            }
        }
        Ok(())
    }

    fn next_iter(&mut self) -> Result<Option<NodeIter<'_>>> {
        if let Some(cursor) = self.cursor.take() {
            self.bump.reset();
            self.guard.repin();
            let node = loop {
                // TODO: refactor this
                match self.inner.find_leaf(&cursor, &self.guard) {
                    Ok((node, _)) => break node,
                    Err(Error::Again) => continue,
                    Err(err) => return Err(err),
                }
            };
            self.cursor = node.range.end.map(|end| end.to_vec());
            let iter = self.inner.data_node_iter(&node, &self.bump, &self.guard)?;
            Ok(Some(NodeIter::new(iter)))
        } else {
            Ok(None)
        }
    }
}

struct NodeIter<'a> {
    iter: DataNodeIter<'a, 'a>,
    last: &'a [u8],
    current: Option<(&'a [u8], &'a [u8])>,
}

impl<'a> NodeIter<'a> {
    fn new(iter: DataNodeIter<'a, 'a>) -> Self {
        Self {
            iter,
            last: [].as_slice(),
            current: None,
        }
    }

    fn current(&self) -> Option<(&[u8], &[u8])> {
        self.current
    }

    fn rewind(&mut self) {
        self.iter.rewind();
        self.find_next();
    }

    fn next(&mut self) {
        self.iter.next();
        self.find_next();
    }

    fn find_next(&mut self) {
        while let Some((k, v)) = self.iter.current() {
            if self.last != k.raw {
                self.last = k.raw;
                if let Value::Put(value) = v {
                    self.current = Some((k.raw, value));
                    return;
                }
            }
            self.iter.next();
        }
        self.current = None;
    }
}