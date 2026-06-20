use std::convert::TryInto;
use std::time::{Duration, Instant};
use std::{io, mem, sync::Arc};

use crate::buffer::{Metadata, Type};
use crate::device::{Device, Handle};
use crate::io::mmap::arena::Arena;
use crate::io::traits::{CaptureStream, OutputStream, Stream as StreamTrait};
use crate::memory::Memory;
use crate::v4l2;
use crate::v4l_sys::*;

/// Stream of mapped buffers
///
/// An arena instance is used internally for buffer handling.
pub struct Stream<'a> {
    handle: Arc<Handle>,
    arena: Arena<'a>,
    arena_index: usize,
    // True means the buffer has been successfully handed to the driver with
    // QBUF and should not be queued again before DQBUF returns it.
    buffer_queued: Vec<bool>,
    // Startup queueing can be interrupted after partial progress. This cursor
    // lets the next call resume without replaying successful QBUFs.
    capture_initial_queue_index: usize,
    buf_type: Type,
    buf_meta: Vec<Metadata>,
    timeout: Option<i32>,

    active: bool,
}

impl<'a> Stream<'a> {
    /// Returns a stream for frame capturing
    ///
    /// # Arguments
    ///
    /// * `dev` - Capture device ref to get its file descriptor
    /// * `buf_type` - Type of the buffers
    ///
    /// # Example
    ///
    /// ```
    /// use v4l::buffer::Type;
    /// use v4l::device::Device;
    /// use v4l::io::mmap::Stream;
    ///
    /// let dev = Device::new(0);
    /// if let Ok(dev) = dev {
    ///     let stream = Stream::new(&dev, Type::VideoCapture);
    /// }
    /// ```
    pub fn new(dev: &Device, buf_type: Type) -> io::Result<Self> {
        Stream::with_buffers(dev, buf_type, 4)
    }

    pub fn with_buffers(dev: &Device, buf_type: Type, buf_count: u32) -> io::Result<Self> {
        let mut arena = Arena::new(dev.handle(), buf_type);
        let count = arena.allocate(buf_count)?;
        let mut buf_meta = Vec::new();
        buf_meta.resize(count as usize, Metadata::default());
        let buffer_queued = vec![false; count as usize];

        Ok(Stream {
            handle: dev.handle(),
            arena,
            arena_index: 0,
            buffer_queued,
            capture_initial_queue_index: 0,
            buf_type,
            buf_meta,
            active: false,
            timeout: None,
        })
    }

    /// Returns the raw device handle
    pub fn handle(&self) -> Arc<Handle> {
        self.handle.clone()
    }

    /// Sets a timeout of the v4l file handle.
    pub fn set_timeout(&mut self, duration: Duration) {
        self.timeout = Some(duration.as_millis().try_into().unwrap());
    }

    /// Clears the timeout of the v4l file handle.
    pub fn clear_timeout(&mut self) {
        self.timeout = None;
    }

    fn buffer_desc(&self) -> v4l2_buffer {
        v4l2_buffer {
            type_: self.buf_type as u32,
            memory: Memory::Mmap as u32,
            ..unsafe { mem::zeroed() }
        }
    }

    fn timeout_deadline(&self) -> Deadline {
        Deadline::from_timeout(self.timeout)
    }

    fn mark_queued(&mut self, index: usize) {
        self.buffer_queued[index] = true;
        while self.capture_initial_queue_index < self.buffer_queued.len()
            && self.buffer_queued[self.capture_initial_queue_index]
        {
            self.capture_initial_queue_index += 1;
        }
    }

    fn mark_dequeued(&mut self, index: usize) {
        self.buffer_queued[index] = false;
        self.arena_index = index;
    }

    fn reset_buffer_state(&mut self) {
        self.buffer_queued.fill(false);
        self.capture_initial_queue_index = 0;
        self.arena_index = 0;
    }

    fn ioctl_until<T, F>(
        deadline: Deadline,
        timeout_context: &'static str,
        mut op: F,
    ) -> io::Result<T>
    where
        F: FnMut() -> io::Result<T>,
    {
        loop {
            match op() {
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {
                    if deadline.expired() {
                        return Err(io::Error::new(io::ErrorKind::TimedOut, timeout_context));
                    }
                }
                result => return result,
            }
        }
    }

    fn poll_until(
        &self,
        events: i16,
        deadline: Deadline,
        timeout_context: &'static str,
    ) -> io::Result<()> {
        loop {
            let timeout = deadline.poll_timeout();
            match self.handle.poll(events, timeout) {
                Ok(0) => return Err(io::Error::new(io::ErrorKind::TimedOut, timeout_context)),
                Ok(_) => return Ok(()),
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {
                    if deadline.expired() {
                        return Err(io::Error::new(io::ErrorKind::TimedOut, timeout_context));
                    }
                }
                Err(err) => return Err(err),
            }
        }
    }

    fn start_until(&mut self, deadline: Deadline) -> io::Result<()> {
        Self::ioctl_until(deadline, "VIDIOC_STREAMON", || unsafe {
            let mut typ = self.buf_type as u32;
            v4l2::ioctl(
                self.handle.fd(),
                v4l2::vidioc::VIDIOC_STREAMON,
                &mut typ as *mut _ as *mut std::os::raw::c_void,
            )
        })?;

        self.active = true;
        Ok(())
    }

    fn stop_until(&mut self, deadline: Deadline) -> io::Result<()> {
        Self::ioctl_until(deadline, "VIDIOC_STREAMOFF", || unsafe {
            let mut typ = self.buf_type as u32;
            v4l2::ioctl(
                self.handle.fd(),
                v4l2::vidioc::VIDIOC_STREAMOFF,
                &mut typ as *mut _ as *mut std::os::raw::c_void,
            )
        })?;

        self.active = false;
        self.reset_buffer_state();
        Ok(())
    }

    fn queue_capture_until(&mut self, index: usize, deadline: Deadline) -> io::Result<()> {
        let mut v4l2_buf = v4l2_buffer {
            index: index as u32,
            ..self.buffer_desc()
        };

        Self::ioctl_until(deadline, "VIDIOC_QBUF", || unsafe {
            v4l2::ioctl(
                self.handle.fd(),
                v4l2::vidioc::VIDIOC_QBUF,
                &mut v4l2_buf as *mut _ as *mut std::os::raw::c_void,
            )
        })?;

        self.mark_queued(index);
        Ok(())
    }

    fn queue_output_until(&mut self, index: usize, deadline: Deadline) -> io::Result<()> {
        let mut v4l2_buf = v4l2_buffer {
            index: index as u32,
            ..self.buffer_desc()
        };

        // output settings
        //
        // MetaData.bytesused is initialized to 0. For an output device, when bytesused is
        // set to 0 v4l2 will set it to the size of the plane:
        // https://www.kernel.org/doc/html/v4.15/media/uapi/v4l/buffer.html#struct-v4l2-plane
        v4l2_buf.bytesused = self.buf_meta[index].bytesused;
        v4l2_buf.field = self.buf_meta[index].field;

        self.poll_until(libc::POLLOUT, deadline, "VIDIOC_QBUF")?;
        Self::ioctl_until(deadline, "VIDIOC_QBUF", || unsafe {
            v4l2::ioctl(
                self.handle.fd(),
                v4l2::vidioc::VIDIOC_QBUF,
                &mut v4l2_buf as *mut _ as *mut std::os::raw::c_void,
            )
        })?;

        self.mark_queued(index);
        Ok(())
    }

    fn dequeue_until(&mut self, deadline: Deadline) -> io::Result<usize> {
        let mut v4l2_buf = self.buffer_desc();

        Self::ioctl_until(deadline, "VIDIOC_DQBUF", || unsafe {
            v4l2::ioctl(
                self.handle.fd(),
                v4l2::vidioc::VIDIOC_DQBUF,
                &mut v4l2_buf as *mut _ as *mut std::os::raw::c_void,
            )
        })?;

        let index = v4l2_buf.index as usize;
        self.mark_dequeued(index);
        self.buf_meta[index] = Metadata {
            bytesused: v4l2_buf.bytesused,
            flags: v4l2_buf.flags.into(),
            field: v4l2_buf.field,
            timestamp: v4l2_buf.timestamp.into(),
            sequence: v4l2_buf.sequence,
        };

        Ok(index)
    }

    fn dequeue_capture_until(&mut self, deadline: Deadline) -> io::Result<usize> {
        self.poll_until(libc::POLLIN, deadline, "VIDIOC_DQBUF")?;
        self.dequeue_until(deadline)
    }

    fn ensure_capture_started_until(&mut self, deadline: Deadline) -> io::Result<()> {
        while self.capture_initial_queue_index < self.arena.bufs.len() {
            let index = self.capture_initial_queue_index;
            if self.buffer_queued[index] {
                self.capture_initial_queue_index += 1;
            } else {
                self.queue_capture_until(index, deadline)?;
            }
        }

        self.start_until(deadline)
    }
}

#[derive(Clone, Copy)]
struct Deadline {
    expires_at: Option<Instant>,
}

impl Deadline {
    fn from_timeout(timeout: Option<i32>) -> Self {
        let expires_at =
            timeout.map(|timeout| Instant::now() + Duration::from_millis(timeout as u64));
        Self { expires_at }
    }

    fn poll_timeout(self) -> i32 {
        match self.expires_at {
            None => -1,
            Some(expires_at) => {
                let Some(remaining) = expires_at.checked_duration_since(Instant::now()) else {
                    return 0;
                };
                if remaining.is_zero() {
                    return 0;
                }

                remaining.as_millis().clamp(1, i32::MAX as u128) as i32
            }
        }
    }

    fn expired(self) -> bool {
        self.expires_at
            .is_some_and(|expires_at| Instant::now() >= expires_at)
    }
}

impl<'a> Drop for Stream<'a> {
    fn drop(&mut self) {
        if let Err(e) = self.stop() {
            if let Some(code) = e.raw_os_error() {
                // ENODEV means the file descriptor wrapped in the handle became invalid, most
                // likely because the device was unplugged or the connection (USB, PCI, ..)
                // broke down. Handle this case gracefully by ignoring it.
                if code == 19 {
                    /* ignore */
                    return;
                }
            }

            panic!("{:?}", e)
        }
    }
}

impl<'a> StreamTrait for Stream<'a> {
    type Item = [u8];

    fn start(&mut self) -> io::Result<()> {
        self.start_until(self.timeout_deadline())
    }

    fn stop(&mut self) -> io::Result<()> {
        self.stop_until(self.timeout_deadline())
    }
}

impl<'a, 'b> CaptureStream<'b> for Stream<'a> {
    fn queue(&mut self, index: usize) -> io::Result<()> {
        self.queue_capture_until(index, self.timeout_deadline())
    }

    fn dequeue(&mut self) -> io::Result<usize> {
        self.dequeue_capture_until(self.timeout_deadline())
    }

    fn next(&'b mut self) -> io::Result<(&'b Self::Item, &'b Metadata)> {
        let deadline = self.timeout_deadline();
        if !self.active {
            self.ensure_capture_started_until(deadline)?;
        } else if !self.buffer_queued[self.arena_index] {
            self.queue_capture_until(self.arena_index, deadline)?;
        }

        self.dequeue_capture_until(deadline)?;

        // The index used to access the buffer elements is given to us by v4l2, so we assume it
        // will always be valid.
        let bytes = &self.arena.bufs[self.arena_index];
        let meta = &self.buf_meta[self.arena_index];
        Ok((bytes, meta))
    }
}

impl<'a, 'b> OutputStream<'b> for Stream<'a> {
    fn queue(&mut self, index: usize) -> io::Result<()> {
        self.queue_output_until(index, self.timeout_deadline())
    }

    fn dequeue(&mut self) -> io::Result<usize> {
        self.dequeue_until(self.timeout_deadline())
    }

    fn next(&'b mut self) -> io::Result<(&'b mut Self::Item, &'b mut Metadata)> {
        let deadline = self.timeout_deadline();
        let init = !self.active;
        if !self.active {
            self.start_until(deadline)?;
        }

        // Only queue and dequeue once the buffer has been filled at the call site. The initial
        // call to this function from the call site will happen just after the buffers have been
        // allocated, meaning we need to return the empty buffer initially so it can be filled.
        if !init {
            if !self.buffer_queued[self.arena_index] {
                self.queue_output_until(self.arena_index, deadline)?;
            }
            self.dequeue_until(deadline)?;
        }

        // The index used to access the buffer elements is given to us by v4l2, so we assume it
        // will always be valid.
        let bytes = &mut self.arena.bufs[self.arena_index];
        let meta = &mut self.buf_meta[self.arena_index];
        Ok((bytes, meta))
    }
}
