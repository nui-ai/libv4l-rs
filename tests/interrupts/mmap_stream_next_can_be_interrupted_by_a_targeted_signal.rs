use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::helpers::{
    capture_streaming_devices, run_stream_until_interrupted, DeviceSource, SignalGuard,
    StreamThreadStatus, INTERRUPT_SIGNAL, SIGNAL_TEST_LOCK,
};

#[test]
// #[ignore = "requires a real V4L2 capture device and deliberately sends SIGUSR1"]
fn physical_mmap_stream_next_can_be_interrupted_by_a_targeted_signal() {
    mmap_stream_next_can_be_interrupted_by_a_targeted_signal(DeviceSource::Physical);
}

#[test]
// #[ignore = "requires the vivid kernel module and deliberately sends SIGUSR1"]
fn vivid_mmap_stream_next_can_be_interrupted_by_a_targeted_signal() {
    mmap_stream_next_can_be_interrupted_by_a_targeted_signal(DeviceSource::Vivid);
}

/// Signal timing is coordinated with an explicit thread handshake. The test
/// process installs a `SIGUSR1` handler while the signal is blocked, then the
/// stream thread unblocks it only for itself. That thread opens a usable MMAP
/// capture stream and performs one warm-up `next()` before publishing its
/// `pthread_t` through `StreamThreadStatus::Ready`. Only after that point does
/// the interrupter thread start sending targeted `pthread_kill` signals, so the
/// signal is neither delivered during stream setup nor left for the kernel to
/// route to an arbitrary thread. If setup or warm-up fails, the stream thread
/// reports `SetupFailed` instead and the interrupter is never started.
fn mmap_stream_next_can_be_interrupted_by_a_targeted_signal(source: DeviceSource) {
    let _signal_test_lock = SIGNAL_TEST_LOCK.lock().expect("lock signal test mutex");
    let guard = Arc::new(SignalGuard::install().expect("install non-restarting signal handler"));
    let device_paths = capture_streaming_devices(source);
    if device_paths.is_empty() {
        eprintln!(
            "skipping EINTR test: no {} /dev/video* device found",
            source.description()
        );
        return;
    }

    let (status_tx, status_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    let done = Arc::new(AtomicBool::new(false));

    let worker_guard = Arc::clone(&guard);
    let worker_done = Arc::clone(&done);
    let stream_thread = thread::spawn(move || {
        let result = run_stream_until_interrupted(worker_guard, device_paths, status_tx);
        worker_done.store(true, Ordering::SeqCst);
        result_tx.send(result).expect("send stream result");
    });

    let target = match status_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("stream thread should report setup status")
    {
        StreamThreadStatus::Ready(tid) => tid,
        StreamThreadStatus::SetupFailed(err) => {
            done.store(true, Ordering::SeqCst);
            let _ = result_rx.recv_timeout(Duration::from_secs(5));
            stream_thread.join().expect("join stream thread");
            panic!("stream thread setup failed: {}", err);
        }
    };

    let interrupter_done = Arc::clone(&done);
    let interrupter = thread::spawn(move || {
        while !interrupter_done.load(Ordering::SeqCst) {
            let ret = unsafe { libc::pthread_kill(target, INTERRUPT_SIGNAL) };
            assert_eq!(ret, 0, "pthread_kill failed: {}", ret);
            thread::sleep(Duration::from_millis(1));
        }
    });

    let stream_result = match result_rx.recv_timeout(Duration::from_secs(10)) {
        Ok(result) => result,
        Err(err) => {
            done.store(true, Ordering::SeqCst);
            interrupter.join().expect("join interrupter thread");
            stream_thread.join().expect("join stream thread");
            panic!(
                "stream thread did not finish after targeted interrupts: {}",
                err
            );
        }
    };

    done.store(true, Ordering::SeqCst);
    interrupter.join().expect("join interrupter thread");
    stream_thread.join().expect("join stream thread");

    let result = stream_result.expect("stream thread failed");
    assert_eq!(result.kind(), io::ErrorKind::Interrupted, "{result:?}");
}
