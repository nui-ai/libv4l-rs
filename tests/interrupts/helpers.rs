use std::io;
use std::mem;
use std::sync::{Mutex, Once};

use v4l::buffer::Type;
use v4l::capability::Flags;
use v4l::context;
use v4l::device::Device;
use v4l::io::mmap::Stream;

pub(crate) const SOURCE_SELECTION_ENV: &str = "V4L_INTERRUPT_TEST_SOURCE";
pub(crate) const INTERRUPT_SIGNAL: libc::c_int = libc::SIGUSR1;
pub(crate) static SIGNAL_TEST_LOCK: Mutex<()> = Mutex::new(());
static SOURCE_SELECTION_LOG: Once = Once::new();

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

#[derive(Clone, Copy)]
pub(crate) enum SourceSelection {
    Physical,
    Vivid,
    Both,
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

impl SourceSelection {
    fn from_env_value(value: &str) -> Self {
        match value {
            "physical" => Self::Physical,
            "vivid" => Self::Vivid,
            "both" => Self::Both,
            value => panic!(
                "unsupported {}={:?}; expected one of: physical, vivid, both",
                SOURCE_SELECTION_ENV, value
            ),
        }
    }

    fn allows(self, source: DeviceSource) -> bool {
        matches!(
            (self, source),
            (Self::Physical, DeviceSource::Physical)
                | (Self::Vivid, DeviceSource::Vivid)
                | (Self::Both, _)
        )
    }

    fn description(self) -> &'static str {
        match self {
            Self::Physical => "a physical camera device",
            Self::Vivid => "a vivid virtual camera device",
            Self::Both => "both a physical and a vivid camera device",
        }
    }
}

pub(crate) fn source_selection_allows(source: DeviceSource) -> bool {
    let (selection, configured_value) = match std::env::var(SOURCE_SELECTION_ENV) {
        Ok(value) => (SourceSelection::from_env_value(&value), Some(value)),
        Err(std::env::VarError::NotPresent) => (SourceSelection::Both, None),
        Err(std::env::VarError::NotUnicode(value)) => {
            panic!(
                "{} must be valid UTF-8, got {:?}",
                SOURCE_SELECTION_ENV, value
            )
        }
    };

    SOURCE_SELECTION_LOG.call_once(|| match configured_value.as_deref() {
        Some(value) => eprintln!(
            "Interrupt tests are using {SOURCE_SELECTION_ENV}={value}; they will try {}. Other available values are: physical, vivid, both.",
            selection.description()
        ),
        None => eprintln!(
            "Interrupt tests will try both physical and vivid camera devices. Set {SOURCE_SELECTION_ENV}=physical|vivid|both to limit the device source.",
        ),
    });

    selection.allows(source)
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

    pub(crate) fn unblock_test_signal_on_current_thread(&self) -> io::Result<()> {
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

pub(crate) fn capture_streaming_devices_or_skip(
    source: DeviceSource,
    test_name: &str,
) -> Option<Vec<std::path::PathBuf>> {
    let device_paths = capture_streaming_devices(source);
    if !device_paths.is_empty() {
        return Some(device_paths);
    }

    match source {
        DeviceSource::Physical => {
            eprintln!(
                "skipping {test_name}: no {} /dev/video* device found",
                source.description()
            );
            None
        }
        DeviceSource::Vivid => {
            panic!(
                "no {} /dev/video* device found for {test_name}. The vivid-backed interrupt tests expect the Linux vivid virtual camera to be loaded; see tests/interrupts/mod.rs, section \"Setting up a vivid virtual camera device\". On Ubuntu this is usually: sudo modprobe vivid n_devs=1 node_types=0x1, then verify with v4l2-ctl --list-devices. To avoid vivid in this run, set {SOURCE_SELECTION_ENV}=physical.",
                source.description()
            );
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

pub(crate) fn open_first_mmap_capture_stream(
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
