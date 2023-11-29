use crate::disk::{DiskManager, PageId};

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::num::NonZeroUsize;
use std::ops::{Index, IndexMut};
use std::sync::Arc;

use lru::LruCache;
use thiserror::Error;

pub type Page = Vec<u8>; // length is PAGE_SIZE

pub struct InnerBufferFrame {
    page_id: PageId,
    page: RefCell<Page>,
    is_dirty: Cell<bool>,
}

#[derive(Clone)]
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

trait PoolAlgorithm {
    type Hint;
    type PushError;

    fn new(size_hint: Option<usize>) -> Self;
    fn request_with_hint(&mut self, hint: Self::Hint, page_id: PageId) -> Option<BufferFrame>;
    fn request(&mut self, page_id: PageId) -> Option<BufferFrame>;
    fn push(
        &mut self,
        page_id: PageId,
        frame: BufferFrame,
    ) -> Result<(PageId, BufferFrame), Self::PushError>;
}

impl PoolAlgorithm for LruCache<PageId, BufferFrame> {
    type Hint = ();
    type PushError = ();

    fn new(size_hint: Option<usize>) -> Self {
        if let Some(size) = size_hint {
            if size == 0 {
                panic!("size_hint must be greater than 0");
            }
            Self::new(NonZeroUsize::new(size).unwrap())
        } else {
            Self::unbounded()
        }
    }

    fn request_with_hint(&mut self, _: Self::Hint, page_id: PageId) -> Option<BufferFrame> {
        self.get(&page_id).cloned()
    }

    fn request(&mut self, page_id: PageId) -> Option<BufferFrame> {
        self.get(&page_id).cloned()
    }

    fn push(&mut self, page_id: PageId, frame: BufferFrame) -> Result<(PageId, BufferFrame), ()> {
        self.push(page_id, frame).map_err(|(_, _)| ())
    }
}

pub struct BufferPoolManager<Alg: PoolAlgorithm> {
    disk_manager: DiskManager,
    pool: Alg,
}

#[derive(Error)]
pub enum BufferPoolError<Alg: PoolAlgorithm> {
    #[error("pool algorithm error: {0}")]
    PoolAlgorithmError(Alg::PushError),
    #[error("disk manager error: {0}")]
    DiskManagerError(std::io::Error),
}

impl<Alg: PoolAlgorithm> BufferPoolManager<Alg> {
    pub fn new(disk_manager: DiskManager, pool_size: usize) -> Self {
        Self {
            disk_manager,
            pool: Alg::new(Some(pool_size)),
        }
    }

    pub fn fetch_page(&mut self, page_id: PageId) -> Result<BufferFrame, BufferPoolError<Alg>> {
        if let Some(frame) = self.pool.request(page_id) {
            return Ok(frame.clone());
        }

        let page_data = vec![0; self.disk_manager.get_page_size() as usize];
        self.disk_manager
            .read_page(page_id, &mut page_data)
            .map_err(BufferPoolError::DiskManagerError)?;

        let frame = BufferFrame::new(page_id, page_data);
        let (evicted_page_id, evicted_frame) = self
            .pool
            .push(page_id, frame.clone())
            .map_err(BufferPoolError::PoolAlgorithmError)?;

        if evicted_frame.inner.is_dirty.get() {
            let page_data = evicted_frame.get_page_ref();
            self.disk_manager
                .write_page(evicted_page_id, &page_data)
                .map_err(BufferPoolError::DiskManagerError)?;
        }

        // 困った
        // ここで、つまみ出された BufferFrame を flush したいが、もし他のところに clone
        // されていたやつがこの後に書き込むと、誰もその変更を回収できない。
        // そもそも、オリジナルの実装では他の参照者がいないことを確かめて、その上でキャッシュから消していた。
        // LruCache crate を使っている限り、そういうことはできない気がする。諦めて自分で
        // 確認機能付き Lru を実装するしかないのか？

        Ok(evicted_frame)
    }
}
