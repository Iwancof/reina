use crate::disk::{DiskManager, PageId};

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::sync::Arc;

use thiserror::Error;

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
    pub fn is_dirty(&self) -> bool {
        self.inner.is_dirty.get()
    }
    pub fn is_unique(&self) -> bool {
        Arc::strong_count(&self.inner) == 1 && Arc::weak_count(&self.inner) == 0
    }
}

impl Clone for BufferFrame {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

pub trait PoolAlgorithm {
    type Hint;
    type PushError: std::fmt::Debug;

    fn new(size_hint: Option<usize>) -> Self;
    fn request_with_hint(&mut self, hint: Self::Hint, page_id: PageId) -> Option<BufferFrame>;
    fn request(&mut self, page_id: PageId) -> Option<BufferFrame>;
    fn push(
        &mut self,
        page_id: PageId,
        frame: BufferFrame,
    ) -> Result<(PageId, BufferFrame), Self::PushError>;
}

pub struct ClockSweep {
    clock_hand: usize,
    frames: Vec<(u64, BufferFrame)>, // (counter, frame)
    map: HashMap<PageId, usize>,     // page_id -> index
}

#[derive(Error, Debug)]
pub enum ClockSweepError {
    #[error("success")]
    Success,

    #[error("pool is full")]
    PoolIsFull,
}

impl ClockSweep {
    fn next_clock_hand(&self) -> usize {
        (self.clock_hand + 1) % self.frames.len()
    }
}

impl PoolAlgorithm for ClockSweep {
    type Hint = ();
    type PushError = ClockSweepError;

    fn new(size_hint: Option<usize>) -> Self {
        let size = size_hint.unwrap_or(1024);
        Self {
            clock_hand: 0,
            frames: Vec::with_capacity(size),
            map: HashMap::with_capacity(size),
        }
    }

    fn request_with_hint(&mut self, _hint: Self::Hint, page_id: PageId) -> Option<BufferFrame> {
        self.request(page_id)
    }

    fn request(&mut self, page_id: PageId) -> Option<BufferFrame> {
        if let Some(&index) = self.map.get(&page_id) {
            let (counter, frame) = &mut self.frames[index];
            *counter += 1;
            Some(frame.clone())
        } else {
            None
        }
    }

    fn push(
        &mut self,
        page_id: PageId,
        frame: BufferFrame,
    ) -> Result<(PageId, BufferFrame), Self::PushError> {
        if self.frames.len() < self.frames.capacity() {
            let index = self.frames.len();
            self.frames.push((1, frame.clone()));
            self.map.insert(page_id, index);
            return Err(ClockSweepError::Success);
        }

        let mut consecutive_fail = 0;

        let (buf_idx, page_id) = loop {
            let (counter, frame) = &mut self.frames[self.clock_hand];
            if counter == &0 {
                break (self.clock_hand, frame.page_id());
            }

            if frame.is_unique() {
                consecutive_fail = 0;
                *counter -= 1;
            } else {
                consecutive_fail += 1;
                if consecutive_fail >= self.frames.len() {
                    return Err(ClockSweepError::PoolIsFull);
                }
            }

            self.clock_hand = self.next_clock_hand();
        };

        let old_idx = self.map.remove(&page_id).unwrap();
        let (_, old_frame) = core::mem::replace(&mut self.frames[old_idx], (0, frame));
        self.map.insert(page_id, buf_idx);

        return Ok((page_id, old_frame));
    }
}

pub struct BufferPoolManager<Alg: PoolAlgorithm> {
    disk_manager: DiskManager,
    pool: Alg,
}

impl<Alg: PoolAlgorithm> BufferPoolManager<Alg> {
    pub fn new(disk_manager: DiskManager, pool_size: usize) -> Self {
        Self {
            disk_manager,
            pool: Alg::new(Some(pool_size)),
        }
    }

    pub fn fetch_page(&mut self, page_id: PageId) -> Result<BufferFrame, std::io::Error> {
        if let Some(frame) = self.pool.request(page_id) {
            return Ok(frame.clone());
        }

        let mut page_data = vec![0; self.disk_manager.get_page_size() as usize];
        self.disk_manager.read_page(page_id, &mut page_data)?;

        let frame = BufferFrame::new(page_id, page_data);
        if let Ok((old_page_id, old_frame)) = self.pool.push(page_id, frame.clone()) {
            // Todo: Pool がいっぱいになった場合のハンドルをする。

            if old_frame.is_dirty() {
                self.disk_manager
                    .write_page(old_page_id, &old_frame.get_page_ref())?;
            }
        }

        Ok(frame)
    }
}
