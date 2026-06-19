use std::io;
use std::mem;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use v4l::buffer::Type;
use v4l::capability::Flags;
use v4l::context;
use v4l::device::Device;
use v4l::io::mmap::Stream;
use v4l::io::traits::CaptureStream;

pub(crate) const INTERRUPT_SIGNAL: libc::c_int = libc::SIGUSR1;
pub(crate) static SIGNAL_TEST_LOCK: Mutex<()> = Mutex::new(());

extern "C" fn handle_interrupt_signal(_: libc::c_int) {}

pub(crate) struct SignalGuard {
    old_action: libc::sigaction,
    old_mask: libc::sigset_t,
}

pub(crate) enum StreamThreadStatus {
    Ready(libc::pthread_t),
    SetupFailed(io::Error),
}

#[derive(Clone, Copy)]
pub(crate) enum DeviceSource {
    Physical,
    Vivid,
}

impl DeviceSource {
    pub(crate) fn description(self) -> &'static str {
        match self {
            Self::Physical => "physical capture-capable",
            Self::Vivid => "vivid virtual capture-capable",
        }
    }

    fn accepts_driver(self, driver: &str) -> bool {
        match self {
            Self::Physical => driver != "vivid",
            Self::Vivid => driver == "vivid",
        }
    }
}

impl SignalGuard {
    pub(crate) fn install() -> io::Result<Self> {
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

pub(crate) fn run_stream_until_interrupted(
    guard: Arc<SignalGuard>,
    device_paths: Vec<std::path::PathBuf>,
    status_tx: mpsc::Sender<StreamThreadStatus>,
) -> io::Result<io::Error> {
    if let Err(err) = guard.unblock_test_signal_on_current_thread() {
        let setup_error = io::Error::new(err.kind(), err.to_string());
        status_tx
            .send(StreamThreadStatus::SetupFailed(setup_error))
            .expect("publish stream thread setup failure");
        return Err(err);
    }

    let (_device_path, mut stream) = match open_first_mmap_capture_stream(device_paths) {
        Ok(stream) => stream,
        Err(err) => {
            let setup_error = io::Error::new(err.kind(), err.to_string());
            status_tx
                .send(StreamThreadStatus::SetupFailed(setup_error))
                .expect("publish stream thread setup failure");
            return Err(err);
        }
    };
    stream.set_timeout(Duration::from_secs(1));

    match stream.next() {
        Ok((_buf, _meta)) => {}
        Err(err) => {
            let setup_error = io::Error::new(err.kind(), format!("warm up MMAP stream: {err}"));
            status_tx
                .send(StreamThreadStatus::SetupFailed(setup_error))
                .expect("publish stream thread setup failure");
            return Err(err);
        }
    };

    status_tx
        .send(StreamThreadStatus::Ready(unsafe { libc::pthread_self() }))
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

pub(crate) fn capture_streaming_devices(source: DeviceSource) -> Vec<std::path::PathBuf> {
    let mut nodes = context::enum_devices();
    nodes.sort_by_key(|node| node.index());

    nodes
        .into_iter()
        .filter_map(|node| {
            let dev = Device::with_path(node.path()).ok()?;
            let caps = dev.query_caps().ok()?;
            if caps.capabilities.contains(Flags::VIDEO_CAPTURE)
                && caps.capabilities.contains(Flags::STREAMING)
                && source.accepts_driver(&caps.driver)
            {
                Some(node.path().to_owned())
            } else {
                None
            }
        })
        .collect()
}

fn cvt(ret: libc::c_int) -> io::Result<()> {
    if ret == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(ret))
    }
}

fn open_first_mmap_capture_stream(
    device_paths: Vec<std::path::PathBuf>,
) -> io::Result<(std::path::PathBuf, Stream<'static>)> {
    let mut errors = Vec::new();

    for device_path in device_paths {
        let dev = match Device::with_path(&device_path) {
            Ok(dev) => dev,
            Err(err) => {
                errors.push(format!("open {}: {err}", device_path.display()));
                continue;
            }
        };

        match Stream::with_buffers(&dev, Type::VideoCapture, 4) {
            Ok(stream) => return Ok((device_path, stream)),
            Err(err) => errors.push(format!(
                "create MMAP capture stream for {}: {err}",
                device_path.display()
            )),
        }
    }

    Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!(
            "no capture-capable /dev/video* device could create an MMAP stream: {}",
            errors.join("; ")
        ),
    ))
}
