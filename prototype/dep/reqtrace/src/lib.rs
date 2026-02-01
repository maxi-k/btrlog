pub mod macro_support;

mod cell;
mod macros; // uses macro_export
mod rdtsc;

pub use cell::*;
pub use rdtsc::*;

////////////////////////////////////////////////////////////////////////////////

pub fn calibrate_cheap_clock() {
    rdtsc::TSCCalibration::ensure_initialized();
}

pub fn cycles_to_micros(cycles: u64) -> <MicroScale as FrequencyScale>::Output {
    MicroScale::scale_cycles(cycles)
}

pub fn cycles_to_nanos(cycles: u64) -> <NanoScale as FrequencyScale>::Output {
    NanoScale::scale_cycles(cycles)
}

pub fn cycles_to_secs(cycles: u64) -> <SecScale as FrequencyScale>::Output {
    SecScale::scale_cycles(cycles)
}
