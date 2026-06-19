//! Hardware-backed EINTR scaffold for MMAP capture streams.
//!
//! This test is ignored by default because it needs a V4L2 capture device
//! and sends a process signal. For repeatable local/CI runs, prefer Linux
//! `vivid` (the in-kernel Virtual Video Test Driver) over physical cameras.
//! Run explicitly with:
//! `cargo test --test interrupts -- --ignored --nocapture`.

use std::io;
use std::mem;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use v4l::buffer::Type;
use v4l::context;
use v4l::device::Device;
use v4l::io::mmap::Stream;
use v4l::io::traits::CaptureStream;

const INTERRUPT_SIGNAL: libc::c_int = libc::SIGUSR1;

extern "C" fn handle_interrupt_signal(_: libc::c_int) {}

struct SignalGuard {
    old_action: libc::sigaction,
    old_mask: libc::sigset_t,
}

impl SignalGuard {
    fn install() -> io::Result<Self> {
        unsafe {
            let mut blocked: libc::sigset_t = mem::zeroed();
            cvt(libc::sigemptyset(&mut blocked))?;
            cvt(libc::sigaddset(&mut blocked, INTERRUPT_SIGNAL))?;

            let mut old_mask: libc::sigset_t = mem::zeroed();
            cvt(libc::pthread_sigmask(
                libc::SIG_BLOCK,
                &blocked,
                &mut old_mask,
            ))?;

            let mut action: libc::sigaction = mem::zeroed();
            action.sa_sigaction = handle_interrupt_signal as *const () as usize;
            action.sa_flags = 0;
            cvt(libc::sigemptyset(&mut action.sa_mask))?;

            let mut old_action: libc::sigaction = mem::zeroed();
            if libc::sigaction(INTERRUPT_SIGNAL, &action, &mut old_action) == -1 {
                let error = io::Error::last_os_error();
                let _ = libc::pthread_sigmask(libc::SIG_SETMASK, &old_mask, std::ptr::null_mut());
                return Err(error);
            }

            Ok(Self {
                old_action,
                old_mask,
            })
        }
    }

    fn unblock_test_signal_on_current_thread(&self) -> io::Result<()> {
        unsafe {
            let mut unblocked: libc::sigset_t = mem::zeroed();
            cvt(libc::sigemptyset(&mut unblocked))?;
            cvt(libc::sigaddset(&mut unblocked, INTERRUPT_SIGNAL))?;
            cvt(libc::pthread_sigmask(
                libc::SIG_UNBLOCK,
                &unblocked,
                std::ptr::null_mut(),
            ))
        }
    }
}

impl Drop for SignalGuard {
    fn drop(&mut self) {
        unsafe {
            libc::sigaction(INTERRUPT_SIGNAL, &self.old_action, std::ptr::null_mut());
            libc::pthread_sigmask(libc::SIG_SETMASK, &self.old_mask, std::ptr::null_mut());
        }
    }
}

fn cvt(ret: libc::c_int) -> io::Result<()> {
    if ret == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(ret))
    }
}

fn preferred_capture_device() -> Option<std::path::PathBuf> {
    let devices = context::enum_devices();

    devices
        .iter()
        .find(|node| {
            node.name()
                .map(|name| {
                    let name = name.to_ascii_lowercase();
                    name.contains("vivid") || name.contains("virtual video test driver")
                })
                .unwrap_or(false)
        })
        .or_else(|| devices.first())
        .map(|node| node.path().to_owned())
}

#[test]
#[ignore = "requires a V4L2 capture device and deliberately sends SIGUSR1"]
fn mmap_stream_next_can_be_interrupted_by_a_targeted_signal() {
    let guard = Arc::new(SignalGuard::install().expect("install non-restarting signal handler"));
    let device_path = match preferred_capture_device() {
        Some(path) => path,
        None => {
            eprintln!("skipping EINTR test: no /dev/video* devices found");
            return;
        }
    };

    let (tid_tx, tid_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    let done = Arc::new(AtomicBool::new(false));

    let worker_guard = Arc::clone(&guard);
    let worker_done = Arc::clone(&done);
    let stream_thread = thread::spawn(move || {
        let result = run_stream_until_interrupted(worker_guard, device_path, tid_tx);
        worker_done.store(true, Ordering::SeqCst);
        result_tx.send(result).expect("send stream result");
    });

    let target = tid_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("stream thread should publish its pthread id");

    let interrupter_done = Arc::clone(&done);
    let interrupter = thread::spawn(move || {
        while !interrupter_done.load(Ordering::SeqCst) {
            let ret = unsafe { libc::pthread_kill(target, INTERRUPT_SIGNAL) };
            assert_eq!(ret, 0, "pthread_kill failed: {}", ret);
            thread::sleep(Duration::from_millis(1));
        }
    });

    let result = result_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("stream thread did not finish after targeted interrupts")
        .expect("stream thread setup failed");

    done.store(true, Ordering::SeqCst);
    interrupter.join().expect("join interrupter thread");
    stream_thread.join().expect("join stream thread");

    assert_eq!(result.kind(), io::ErrorKind::Interrupted, "{result:?}");
}

fn run_stream_until_interrupted(
    guard: Arc<SignalGuard>,
    device_path: std::path::PathBuf,
    tid_tx: mpsc::Sender<libc::pthread_t>,
) -> io::Result<io::Error> {
    guard.unblock_test_signal_on_current_thread()?;

    let dev = Device::with_path(&device_path)?;
    let mut stream = Stream::with_buffers(&dev, Type::VideoCapture, 4)?;
    stream.set_timeout(Duration::from_secs(1));

    tid_tx
        .send(unsafe { libc::pthread_self() })
        .expect("publish stream thread pthread id");

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match stream.next() {
            Ok((_buf, _meta)) => {
                if Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "stream was not interrupted before the test deadline",
                    ));
                }
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => return Ok(err),
            Err(err) if err.kind() == io::ErrorKind::TimedOut => continue,
            Err(err) => return Err(err),
        }
    }
}
