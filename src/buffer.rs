use crate::disk::{DiskManager, PageId};

use std::cell::{Cell, RefCell};
use std::collections::{VecDeque, HashMap};
use std::ops::{Index, IndexMut};
use std::sync::Arc;

pub type Page = Vec<u8>; // length is PAGE_SIZE

pub struct InnerBufferFrame {
    page_id: PageId,
    page: RefCell<Page>,
    is_dirty: Cell<bool>,
}

pub struct BufferFrame {
    inner: Arc<InnerBufferFrame>,
}

impl BufferFrame {
    pub fn new(page_id: PageId, page: Page) -> Self {
        Self {
            inner: Arc::new(InnerBufferFrame {
                page_id,
                page: RefCell::new(page),
                is_dirty: Cell::new(false),
            }),
        }
    }
    pub fn page_id(&self) -> PageId {
        self.inner.page_id
    }
    pub fn get_page_ref(&self) -> std::cell::Ref<Page> {
        self.inner.page.borrow()
    }
    pub fn get_page_mut(&self) -> std::cell::RefMut<Page> {
        self.inner.is_dirty.set(true);
        self.inner.page.borrow_mut()
    }
    pub fn is_shared(&self) -> bool {
        Arc::strong_count(&self.inner) == 1 && Arc::weak_count(&self.inner) == 0
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct FrameId(usize);

trait PoolAlgorithm {
    type Hint;

    fn request_with_hint(&mut self, hint: Self::Hint, page_id: PageId) -> &BufferFrame;
    fn request(&mut self, page_id: PageId) -> &BufferFrame;
    fn evict(&mut self) -> Option<FrameId>;
}

impl<T: PoolAlgorithm> Index<FrameId> for T {
    type Output = BufferFrame;

    fn index(&self, id: FrameId) -> &Self::Output {
        self.request(id)
    }
}

pub struct LRUPool {
    frames: Vec<BufferFrame>,
    free_list: VecDeque<FrameId>,
    table: HashMap<PageId, FrameId>,
}

pub struct BufferPoolManager {
    disk_manager: DiskManager,
}
