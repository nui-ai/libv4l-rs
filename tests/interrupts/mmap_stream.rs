use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::helpers::{
    capture_streaming_devices_or_skip, open_first_mmap_capture_stream, source_selection_allows,
    DeviceSource, SignalGuard, StreamThreadStatus, INTERRUPT_SIGNAL, SIGNAL_TEST_LOCK,
};
use v4l::io::traits::CaptureStream;

pub(crate) const DEFAULT_POST_READY_FRAME_COUNT: usize = 10;
pub(crate) const MIN_POST_READY_FRAME_COUNT: usize = 2;

struct PostInterruptFrames {
    sequences: Vec<u32>,
    terminal_error: Option<io::Error>,
}

#[derive(Clone, Copy)]
pub(crate) enum InterruptInjection {
    Enabled,
    Disabled,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct FrameCollectionConfig {
    post_ready_frame_count: usize,
}

impl FrameCollectionConfig {
    pub(crate) fn try_new(post_ready_frame_count: usize) -> io::Result<Self> {
        if post_ready_frame_count < MIN_POST_READY_FRAME_COUNT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "post-ready frame count must be at least {MIN_POST_READY_FRAME_COUNT}, got {post_ready_frame_count}"
                ),
            ));
        }

        Ok(Self {
            post_ready_frame_count,
        })
    }
}

pub(crate) fn mmap_stream_next_handles_targeted_signals(source: DeviceSource) {
    mmap_stream_next_collects_ordered_frames(
        source,
        InterruptInjection::Enabled,
        FrameCollectionConfig::try_new(MIN_POST_READY_FRAME_COUNT)
            .expect("valid frame collection config"),
    );
}

/// Verifies the MMAP stream remains usable after targeted signals reach the
/// stream thread.
///
/// Signal timing is coordinated with an explicit thread handshake. The test
/// process installs a `SIGUSR1` handler while the signal is blocked, then the
/// stream thread unblocks it only for itself. That thread opens a usable MMAP
/// capture stream and performs one warm-up `next()` before publishing its
/// `pthread_t` through `StreamThreadStatus::Ready`. Only after that point does
/// the interrupter thread start sending targeted `pthread_kill` signals, so the
/// signal is neither delivered during stream setup nor left for the kernel to
/// route to an arbitrary thread. If setup or warm-up fails, the stream thread
/// reports `SetupFailed` instead and the interrupter is never started.
pub(crate) fn mmap_stream_next_collects_ordered_frames(
    source: DeviceSource,
    injection: InterruptInjection,
    config: FrameCollectionConfig,
) {
    if !source_selection_allows(source) {
        eprintln!(
            "skipping MMAP stream post-ready frame test: {} source not selected",
            source.description()
        );
        return;
    }

    // Signal tests are serialized by `SIGNAL_TEST_LOCK` because the signal
    // handler is process-global.
    let _signal_test_lock = SIGNAL_TEST_LOCK.lock().expect("lock signal test mutex");
    let guard = Arc::new(SignalGuard::install().expect("install non-restarting signal handler"));

    // Select the same device population for both the signal-injection test and
    // the no-injection control, so the control rules out harness-only failures.
    let Some(device_paths) =
        capture_streaming_devices_or_skip(source, "MMAP stream post-ready frame test")
    else {
        return;
    };

    let (status_tx, status_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    let done = Arc::new(AtomicBool::new(false));
    let signal_sent = Arc::new(AtomicBool::new(false));

    let worker_guard = Arc::clone(&guard);
    let worker_done = Arc::clone(&done);
    let worker_signal_sent = Arc::clone(&signal_sent);

    // The worker owns the stream. It warms up `next()`, reports its pthread id,
    // and then collects frames after the interrupter has sent a targeted signal
    // or immediately for the no-signal control case.
    let stream_thread = thread::spawn(move || {
        let result = collect_frames_after_ready(
            worker_guard,
            device_paths,
            status_tx,
            worker_signal_sent,
            injection,
            config,
        );
        worker_done.store(true, Ordering::SeqCst);
        result_tx.send(result).expect("send stream result");
    });

    // Do not inject a signal until the worker has completed stream setup and
    // one warm-up `next()`, which keeps setup failures distinct from signal
    // handling during steady-state frame collection.
    let target = match status_rx
        // This is a deadlock guard, not the synchronization mechanism. The
        // worker sends `Ready` only after the stream can produce frames.
        .recv_timeout(Duration::from_secs(5))
        .expect("stream thread should report setup status")
    {
        StreamThreadStatus::Ready(tid) => tid,
        StreamThreadStatus::SetupFailed(err) => {
            done.store(true, Ordering::SeqCst);
            // Setup failures should still be reported through the worker result
            // channel; this timeout prevents a secondary hang while panicking.
            let _ = result_rx.recv_timeout(Duration::from_secs(5));
            stream_thread.join().expect("join stream thread");
            panic!("stream thread setup failed: {}", err);
        }
    };

    // The control path deliberately skips this final act of injecting SIGUSR1;
    // it should still collect ordered frames through the same setup harness.
    let interrupter = match injection {
        InterruptInjection::Enabled => {
            let interrupter_done = Arc::clone(&done);
            let interrupter_signal_sent = Arc::clone(&signal_sent);
            Some(thread::spawn(move || {
                while !interrupter_done.load(Ordering::SeqCst) {
                    let ret = unsafe { libc::pthread_kill(target, INTERRUPT_SIGNAL) };
                    assert_eq!(ret, 0, "pthread_kill failed: {}", ret);
                    interrupter_signal_sent.store(true, Ordering::SeqCst);
                    thread::sleep(Duration::from_millis(1));
                }
            }))
        }
        InterruptInjection::Disabled => None,
    };

    // Ten seconds gives the worker enough margin to collect the configured
    // frame count on real or vivid devices under CI load, while still catching
    // a stuck queue/dequeue path promptly.
    let stream_result = match result_rx.recv_timeout(Duration::from_secs(10)) {
        Ok(result) => result,
        Err(err) => {
            done.store(true, Ordering::SeqCst);
            if let Some(interrupter) = interrupter {
                interrupter.join().expect("join interrupter thread");
            }
            stream_thread.join().expect("join stream thread");
            panic!(
                "stream thread did not finish after targeted interrupt: {}",
                err
            );
        }
    };

    done.store(true, Ordering::SeqCst);
    if let Some(interrupter) = interrupter {
        interrupter.join().expect("join interrupter thread");
    }
    stream_thread.join().expect("join stream thread");

    let frames = stream_result.expect("stream thread failed");

    assert!(
        frames.terminal_error.is_none(),
        "stream should not return an error while collecting frames: {:?}",
        frames.terminal_error
    );
    assert_eq!(
        frames.sequences.len(),
        config.post_ready_frame_count,
        "stream should collect {} frames: {:?}",
        config.post_ready_frame_count,
        frames.sequences
    );
    assert!(
        is_strictly_increasing(&frames.sequences),
        "stream should collect ordered frames: {:?}",
        frames.sequences
    );
}

fn collect_frames_after_ready(
    guard: Arc<SignalGuard>,
    device_paths: Vec<std::path::PathBuf>,
    status_tx: mpsc::Sender<StreamThreadStatus>,
    signal_sent: Arc<AtomicBool>,
    injection: InterruptInjection,
    config: FrameCollectionConfig,
) -> io::Result<PostInterruptFrames> {
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
    stream.set_timeout(Duration::from_secs(2));

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
    let mut collect = matches!(injection, InterruptInjection::Disabled);
    let mut sequences = Vec::with_capacity(config.post_ready_frame_count);

    while Instant::now() < deadline && sequences.len() < config.post_ready_frame_count {
        match stream.next() {
            Ok((_buf, meta)) => {
                if !collect && signal_sent.load(Ordering::SeqCst) {
                    collect = true;
                }
                if collect {
                    sequences.push(meta.sequence);
                }
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {
                return Ok(PostInterruptFrames {
                    sequences,
                    terminal_error: Some(err),
                });
            }
            Err(err) if err.kind() == io::ErrorKind::TimedOut => continue,
            Err(err) if collect => {
                return Ok(PostInterruptFrames {
                    sequences,
                    terminal_error: Some(err),
                });
            }
            Err(err) => return Err(err),
        }
    }

    if matches!(injection, InterruptInjection::Enabled) && !signal_sent.load(Ordering::SeqCst) {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "targeted signal was not sent before the test deadline",
        ));
    }

    Ok(PostInterruptFrames {
        sequences,
        terminal_error: None,
    })
}

fn is_strictly_increasing(sequences: &[u32]) -> bool {
    sequences.windows(2).all(|window| window[0] < window[1])
}
