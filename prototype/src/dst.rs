use rand::SeedableRng;
use rand::distr::Distribution;
use std::cell::RefCell;

pub use rand::RngCore;
pub use rand::rngs::SmallRng as DstRng;

type LocalRng = RefCell<DstRng>;
thread_local! {
    #[cfg(any(test, feature = "deterministic"))]
    static DST_RNG: LocalRng = LocalRng::new(DstRng::seed_from_u64(0));

    #[cfg(all(not(test), not(feature = "deterministic")))]
    static DST_RNG: LocalRng = LocalRng::new(DstRng::from_os_rng());
}

pub trait RandomDST: Sized {
    fn dst_map(rng: &mut DstRng) -> Self;
    fn dst_random() -> Self {
        DST_RNG.with_borrow_mut(Self::dst_map)
    }
}

impl RandomDST for u64 {
    fn dst_map(rng: &mut DstRng) -> Self {
        rng.next_u64()
    }
}

impl RandomDST for usize {
    fn dst_map(rng: &mut DstRng) -> Self {
        rng.next_u64() as usize
    }
}

pub fn seed_thread_local_dst_rng(seed: u64) {
    DST_RNG.with_borrow_mut(|rng| {
        *rng = DstRng::seed_from_u64(seed);
    })
}

#[inline]
pub fn with_dst_rng<T>(f: impl FnOnce(&mut DstRng) -> T) -> T {
    DST_RNG.with_borrow_mut(f)
}

#[inline]
pub fn dst_sample<T, D: Distribution<T>>(distr: &D) -> T {
    DST_RNG.with_borrow_mut(|rng| distr.sample(rng))
}

pub fn random_byte_vec(size: usize) -> Vec<u8> {
    let mut v = vec![0; size];
    DST_RNG.with_borrow_mut(|rng| rng.fill_bytes(v.as_mut_slice()));
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deterministic_rng() {
        let n1 = u64::dst_random();
        let n2 = u64::dst_random();
        let n3 = u64::dst_random();
        assert_eq!(n1, 5987356902031041503);
        assert_eq!(n2, 7051070477665621255);
        assert_eq!(n3, 6633766593972829180);
    }

    #[test]
    fn test_reseeding_local_rng() {
        assert_eq!(u64::dst_random(), 5987356902031041503);
        assert_eq!(u64::dst_random(), 7051070477665621255);
        // reset
        seed_thread_local_dst_rng(0);
        assert_eq!(u64::dst_random(), 5987356902031041503);
        // set to different seed
        seed_thread_local_dst_rng(1);
        assert_eq!(u64::dst_random(), 14971601782005023387);
    }
}
