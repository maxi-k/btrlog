/// Compact disk location: | device (10b) | size (6b) | offset (48b) |
/// Supports 1024 devices, 256KB pages, 1EB capacity (4KB granularity)
/// Translated from Spilly's C++
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(transparent)]
pub struct DiskLocation(u64);

impl DiskLocation {
    const OFFSET_BITS: u32 = 48;
    const SIZE_BITS: u32 = 6;
    const DEVICE_BITS: u32 = 10;

    const OFFSET_MASK: u64 = (1 << Self::OFFSET_BITS) - 1;
    const SIZE_MASK: u64 = (1 << Self::SIZE_BITS) - 1;
    // const DEVICE_MASK: u64 = (1 << Self::DEVICE_BITS) - 1;

    pub const GRANULARITY: u64 = 4096; // 4KB
    pub const MAX_SIZE: u64 = Self::GRANULARITY << Self::SIZE_BITS;

    const _BITS_CHECK: () = assert!(Self::DEVICE_BITS + Self::SIZE_BITS + Self::OFFSET_BITS == 64);

    #[inline]
    pub const fn granularity_usize() -> usize {
        Self::GRANULARITY as usize
    }

    #[inline]
    pub const fn new(device: u16, size: u32, offset: u64) -> Self {
        assert!(device < (1 << Self::DEVICE_BITS), "device out of range");
        assert!(size % Self::GRANULARITY as u32 == 0, "size not aligned");
        assert!(size as u64 <= Self::MAX_SIZE, "size too large");
        assert!(offset % Self::GRANULARITY == 0, "offset not aligned");
        let size_units = size as u64 / Self::GRANULARITY;
        let offset_units = offset / Self::GRANULARITY;

        assert!(size_units < (1 << Self::SIZE_BITS), "size units overflow");
        assert!(offset_units < (1 << Self::OFFSET_BITS), "offset units overflow");

        Self((device as u64) << (Self::OFFSET_BITS + Self::SIZE_BITS) | size_units << Self::OFFSET_BITS | offset_units)
    }

    pub const fn invalid() -> Self {
        Self(u64::MAX)
    }

    #[inline]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[inline]
    pub const fn device(self) -> u16 {
        (self.0 >> (Self::OFFSET_BITS + Self::SIZE_BITS)) as u16
    }

    #[inline]
    pub const fn with_device(self, device: u16) -> Self {
        Self::new(device, self.size(), self.offset())
    }

    #[inline]
    pub const fn size(self) -> u32 {
        (((self.0 >> Self::OFFSET_BITS) & Self::SIZE_MASK) * Self::GRANULARITY) as u32
    }

    #[inline]
    pub const fn with_size(self, size: u32) -> Self {
        Self::new(self.device(), size, self.offset())
    }

    #[inline]
    pub const fn offset(self) -> u64 {
        (self.0 & Self::OFFSET_MASK) * Self::GRANULARITY
    }

    #[inline]
    pub const fn with_offset(self, offset: u64) -> Self {
        Self::new(self.device(), self.size(), offset)
    }

    #[inline]
    pub const fn next_block_wrapping(self, range: (u64, u64)) -> Self {
        let (device, size, offset) = self.parts();
        let new_start = offset + size as u64;
        if new_start + size as u64 > range.1 {
            Self::new(device, size, range.0)
        } else {
            Self::new(device, size, new_start)
        }
    }

    #[inline]
    pub const fn parts(self) -> (u16, u32, u64) {
        (self.device(), self.size(), self.offset())
    }

    #[inline]
    pub const fn into_raw(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for DiskLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "(DiskLocation :device {} :size {} :offset {})", self.device(), self.size(), self.offset())
    }
}
