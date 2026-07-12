# libv4l-rs fork for safer MMAP buffer management

## Motivation: 

- [MMAP](https://man7.org/linux/man-pages/man2/mmap.2.html) is one of two buffering modes used by [the libv4l-rs crate](https://github.com/raymanfx/) for its key role of the transport of images into user space code in, as well as by our fork of it which you are looking at now.
- Our crates use the MMAP mode, when using this crate for camera stream acquisition. 
- The original crate does not robustly handle system interrupts in its implementation of MMAP buffering: https://github.com/raymanfx/libv4l-rs/pull/88.

Hence, to avoid its interrupt handling fail cases give or take their performance impacts, we use our own fork, which makes the implementation of that buffer robust in the face of system interrupts.

The original implementation is also lacking in other areas of rubstness:

- multi-planar gaps: https://github.com/raymanfx/libv4l-rs/issues/121
- ARM64 mmap metadata initialization crash report:
  https://github.com/raymanfx/libv4l-rs/issues/77
- feature-selection cleanup is still pending upstream:
  https://github.com/raymanfx/libv4l-rs/pull/110

For the original documentation of the original crate see the original crate at https://github.com/raymanfx/.