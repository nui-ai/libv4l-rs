//! Hardware-backed EINTR scaffold for MMAP capture streams.
//!
//! # Physical v.s. virtual camera devices
//!
//! A physical camera is not inherently required if `vivid` is installed.
//!
//! Linux's in-kernel `vivid` Virtual Video Test Driver is a good fit for this kind
//! of testing because it exposes V4L2 capture devices and supports MMAP streaming I/O.
//! Loading `vivid` in CI or on a developer machine can provide a deterministic virtual
//! capture source without depending on attached USB camera hardware.
//!
//! The physical-camera and `vivid` tests both call the same runner, but select different device
//! sets: non-`vivid` capture devices for the physical path and `vivid` capture devices
//! for the virtual path.
//!
//! # Signal test serialization
//!
//! Signal tests are serialized by `SIGNAL_TEST_LOCK` because the signal handler
//! is process-global. This is intentional even though Rust may otherwise run
//! tests in parallel.
//!
//! # Setting up a vivid virtual camera device
//!
//! On Ubuntu, `vivid` is usually shipped as a kernel module.
//! To make a vivid virtual camera device available:
//!
//! 1. Make sure you have v4l-utils or the nui-ai fork of it installe.d
//!
//! 2. Load one vivid instance with only a video-capture node:
//!
//! ```shell
//! sudo modprobe vivid n_devs=1 node_types=0x1
//! ```
//!
//! `node_types=0x1` asks vivid for just a video capture node, which is enough
//! for this test and avoids creating unrelated radio, SDR, output, metadata, or
//! touch nodes. Omit that option if you want vivid's full default device set.
//!
//! 3. Verify that a vivid device is now available to v4l2:
//! it should now show a "vivid" entry with at least one `/dev/videoN`, similar to the below example output:
//!
//! ```shell
//! $ v4l2-ctl --list-devices
//! vivid (platform:vivid-000):
//! 	/dev/video4
//! 	/dev/media1
//! ```
//!
//! You can now run the test/s which rely on a vivid virtual device.
//!
//! # Removing the virtual device
//!
//! When done, you can unload the virtual driver. This should remove the vivid entry from the list of v4l2 known devices:
//! ```shell
//! sudo modprobe -r vivid
//! v4l2-ctl --list-devices
//! ```
//!
//! If the vivid device still appears after unloading, `modprobe -r` most likely
//! failed because something still has a vivid node open. Check the command's
//! exit status and use `lsmod | grep '^vivid'` and `fuser -v /dev/video*` to
//! find remaining users before trying to unload it again, or just try again.
//!
//! `modinfo vivid` is a diagnostic for checking that the vivid module is installed;
//! it is not required if `sudo modprobe vivid ...` succeeds.

mod helpers;
mod mmap_stream;

use std::io;

use crate::helpers::DeviceSource;
use crate::mmap_stream::{
    mmap_stream_next_collects_ordered_frames, mmap_stream_next_handles_targeted_signals,
    FrameCollectionConfig, InterruptInjection, DEFAULT_POST_READY_FRAME_COUNT,
    MIN_POST_READY_FRAME_COUNT,
};

#[test]
fn physical_mmap_stream_next_handles_targeted_signals() {
    mmap_stream_next_handles_targeted_signals(DeviceSource::Physical);
}

#[test]
fn vivid_mmap_stream_next_handles_targeted_signals() {
    mmap_stream_next_handles_targeted_signals(DeviceSource::Vivid);
}

#[test]
fn physical_mmap_stream_next_after_targeted_signals_collects_ordered_frames() {
    mmap_stream_next_collects_ordered_frames(
        DeviceSource::Physical,
        InterruptInjection::Enabled,
        FrameCollectionConfig::try_new(DEFAULT_POST_READY_FRAME_COUNT)
            .expect("valid frame collection config"),
    );
}

#[test]
fn vivid_mmap_stream_next_after_targeted_signals_collects_ordered_frames() {
    mmap_stream_next_collects_ordered_frames(
        DeviceSource::Vivid,
        InterruptInjection::Enabled,
        FrameCollectionConfig::try_new(DEFAULT_POST_READY_FRAME_COUNT)
            .expect("valid frame collection config"),
    );
}

#[test]
fn physical_mmap_stream_next_without_interrupt_collects_ordered_frames() {
    mmap_stream_next_collects_ordered_frames(
        DeviceSource::Physical,
        InterruptInjection::Disabled,
        FrameCollectionConfig::try_new(DEFAULT_POST_READY_FRAME_COUNT)
            .expect("valid frame collection config"),
    );
}

#[test]
fn vivid_mmap_stream_next_without_interrupt_collects_ordered_frames() {
    mmap_stream_next_collects_ordered_frames(
        DeviceSource::Vivid,
        InterruptInjection::Disabled,
        FrameCollectionConfig::try_new(DEFAULT_POST_READY_FRAME_COUNT)
            .expect("valid frame collection config"),
    );
}

#[test]
fn mmap_stream_post_ready_frame_config_rejects_count_too_small_to_check_ordering() {
    let err = FrameCollectionConfig::try_new(MIN_POST_READY_FRAME_COUNT - 1)
        .expect_err("too few frames cannot prove ordering");
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
}
