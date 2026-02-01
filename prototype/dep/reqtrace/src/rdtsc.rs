use std::sync::OnceLock;
use std::thread;
use std::time::Duration;

#[cfg(target_arch = "x86_64")]
use core::arch::x86_64::_rdtsc;

#[cfg(not(target_arch = "x86_64"))]
compile_error!("TSC calibration only supported on x86_64");

/// TSC (Time Stamp Counter) calibration singleton
pub(crate) struct TSCCalibration {
    tsc_freq_hz: u64,
    tsc_freq_mhz: f64,
    tsc_freq_ghz: f64,
}

impl TSCCalibration {
    fn new() -> Self {
        let tsc_start = unsafe { _rdtsc() };
        // Shorter waits give inaccurate results
        thread::sleep(Duration::from_millis(10));
        let tsc_end = unsafe { _rdtsc() };
        let freq = (tsc_end - tsc_start) * 100; // 1s/10ms = 100

        // The actual frequency is likely rounded to the nearest 100 MHz
        let rounding = 1000u64 * 10;
        let tsc_freq_hz = (freq / rounding) * rounding;
        let tsc_freq_mhz = tsc_freq_hz as f64 / 1e6;
        let tsc_freq_ghz = tsc_freq_hz as f64 / 1e9;

        Self {
            tsc_freq_hz,
            tsc_freq_mhz,
            tsc_freq_ghz,
        }
    }

    fn instance() -> &'static TSCCalibration {
        // XXX how expensive is this? can we get rid of it?
        static INSTANCE: OnceLock<TSCCalibration> = OnceLock::new();
        INSTANCE.get_or_init(TSCCalibration::new)
    }

    /// Initialize the TSC calibration (takes ~10ms)
    pub(crate) fn ensure_initialized() {
        Self::instance();
    }

    pub(crate) fn freq_hz() -> u64 {
        // XXX how expensive is the instance() call?
        Self::instance().tsc_freq_hz
    }

    pub(crate) fn freq_mhz() -> f64 {
        Self::instance().tsc_freq_mhz
    }

    pub(crate) fn freq_ghz() -> f64 {
        Self::instance().tsc_freq_ghz
    }
}

////////////////////////////////////////////////////////////////////////////////

/// Trait for frequency/scale conversion
pub trait FrequencyScale {
    type Output;
    const UNIT_NAME: &'static str;

    /// Scale down raw cycles by dividing by the frequency
    fn scale_cycles(cycles: u64) -> Self::Output;

    /// Scale up a measurement value back to raw cycles
    fn unscale_cycles(scaled: Self::Output) -> u64;
}

////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(feature = "bincode", derive(bincode::Decode, bincode::Encode))]
#[derive(PartialEq, Eq, Default, Debug, Clone)]
pub struct SecScale;
impl FrequencyScale for SecScale {
    type Output = u64;
    const UNIT_NAME: &'static str = "s";

    fn scale_cycles(cycles: u64) -> Self::Output {
        cycles / TSCCalibration::freq_hz()
    }

    fn unscale_cycles(scaled: Self::Output) -> u64 {
        scaled * TSCCalibration::freq_hz()
    }
}

#[cfg_attr(feature = "bincode", derive(bincode::Decode, bincode::Encode))]
#[derive(PartialEq, Eq, Default, Debug, Clone)]
pub struct MicroScale;
impl FrequencyScale for MicroScale {
    type Output = f64;
    const UNIT_NAME: &'static str = "us";

    fn scale_cycles(cycles: u64) -> Self::Output {
        cycles as f64 / TSCCalibration::freq_mhz()
    }

    fn unscale_cycles(scaled: Self::Output) -> u64 {
        (scaled * TSCCalibration::freq_mhz()) as u64
    }
}

#[cfg_attr(feature = "bincode", derive(bincode::Decode, bincode::Encode))]
#[derive(PartialEq, Eq, Default, Debug, Clone)]
pub struct NanoScale;
impl FrequencyScale for NanoScale {
    type Output = f64;
    const UNIT_NAME: &'static str = "ns";

    fn scale_cycles(cycles: u64) -> Self::Output {
        cycles as f64 / TSCCalibration::freq_ghz()
    }

    fn unscale_cycles(scaled: Self::Output) -> u64 {
        (scaled * TSCCalibration::freq_ghz()) as u64
    }
}

#[cfg_attr(feature = "bincode", derive(bincode::Decode, bincode::Encode))]
#[derive(PartialEq, Eq, Default, Debug, Clone)]
pub struct CycleScale;
impl FrequencyScale for CycleScale {
    type Output = u64;
    const UNIT_NAME: &'static str = "cyc";

    fn scale_cycles(cycles: u64) -> Self::Output {
        cycles
    }

    fn unscale_cycles(scaled: Self::Output) -> u64 {
        scaled
    }
}

#[cfg_attr(feature = "bincode", derive(bincode::Decode, bincode::Encode))]
#[derive(PartialEq, Eq, Default, Debug, Clone)]
pub struct DurationScale;
impl FrequencyScale for DurationScale {
    type Output = Duration;
    const UNIT_NAME: &'static str = "dur";

    fn scale_cycles(cycles: u64) -> Duration {
        Duration::from_nanos(NanoScale::scale_cycles(cycles) as u64)
    }

    fn unscale_cycles(scaled: Self::Output) -> u64 {
        scaled.as_nanos() as u64
    }
}

/// Constant scale factor
#[cfg_attr(feature = "bincode", derive(bincode::Decode, bincode::Encode))]
pub struct ConstScale<const N: u64>;
impl<const N: u64> FrequencyScale for ConstScale<N> {
    type Output = u64;
    const UNIT_NAME: &'static str = "";

    fn scale_cycles(cycles: u64) -> u64 {
        cycles / N
    }

    fn unscale_cycles(scaled: u64) -> u64 {
        scaled * N
    }
}

////////////////////////////////////////////////////////////////////////////////

/// TSC-based measurement with configurable scale factor
pub struct TscMeasurement<F, const CHECK_TSC_INIT: bool = false>
where
    F: FrequencyScale,
{
    start_value: u64,
    _phantom: std::marker::PhantomData<F>,
}

impl<F, const CHECK_TSC_INIT: bool> Clone for TscMeasurement<F, CHECK_TSC_INIT>
where
    F: FrequencyScale,
{
    fn clone(&self) -> Self {
        Self {
            start_value: self.start_value,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<F, const CHECK_TSC_INIT: bool> TscMeasurement<F, CHECK_TSC_INIT>
where
    F: FrequencyScale,
{
    pub fn new() -> Self {
        if CHECK_TSC_INIT {
            TSCCalibration::ensure_initialized();
        }
        Self {
            start_value: 0,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn new_started() -> Self {
        let mut res = Self::new();
        res.start_measurement();
        res
    }

    pub const fn zero() -> Self {
        Self {
            start_value: 0,
            _phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    pub fn start_measurement(&mut self) {
        self.start_value = unsafe { _rdtsc() };
    }

    #[inline]
    pub fn stop_measurement(&self) -> F::Output {
        F::scale_cycles(unsafe { _rdtsc() } - self.start_value)
    }

    #[inline]
    pub fn stop_measurement_raw(&self) -> u64 {
        return unsafe { _rdtsc() } - self.start_value;
    }

    #[inline]
    pub fn next_measurement(&mut self) -> F::Output {
        let old_start = self.start_value;
        self.start_value = unsafe { _rdtsc() };
        F::scale_cycles(self.start_value - old_start)
    }

    #[inline]
    pub fn now() -> F::Output {
        F::scale_cycles(unsafe { _rdtsc() })
    }

    #[inline]
    pub fn since(other: F::Output) -> F::Output {
        F::scale_cycles(unsafe { _rdtsc() } - F::unscale_cycles(other))
    }

    #[inline]
    pub fn scale_down(cycles: u64) -> F::Output {
        F::scale_cycles(cycles)
    }

    #[inline]
    pub fn scale_up(scaled: F::Output) -> u64 {
        F::unscale_cycles(scaled)
    }
}

////////////////////////////////////////////////////////////////////////////////

impl<F, const CHECK_TSC_INIT: bool> Default for TscMeasurement<F, CHECK_TSC_INIT>
where
    F: FrequencyScale,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<F, const CHECK_TSC_INIT: bool> std::fmt::Debug for TscMeasurement<F, CHECK_TSC_INIT>
where
    F: FrequencyScale,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TscMeasurement").field("start_value", &self.start_value).finish()
    }
}

////////////////////////////////////////////////////////////////////////////////

/// RAII wrapper that automatically measures TSC cycles in a scope
pub struct MeasureTscBlock<'a, F: FrequencyScale> {
    measure: TscMeasurement<F>,
    cycles: &'a mut F::Output,
}

impl<'a, F: FrequencyScale> MeasureTscBlock<'a, F> {
    pub fn new(mut measure: TscMeasurement<F>, cycles: &'a mut F::Output) -> Self {
        measure.start_measurement();
        Self { measure, cycles }
    }
}

impl<'a, F: FrequencyScale> Drop for MeasureTscBlock<'a, F> {
    fn drop(&mut self) {
        *self.cycles = self.measure.stop_measurement();
    }
}

////////////////////////////////////////////////////////////////////////////////

// Type aliases for common measurement types
pub type CycleMeasurement = TscMeasurement<CycleScale, false>;
pub type NanosMeasurement = TscMeasurement<NanoScale, true>;
pub type MicrosecondMeasurement = TscMeasurement<MicroScale, true>;
pub type SecondMeasurement = TscMeasurement<SecScale, true>;
pub type DurationMeasurement = TscMeasurement<DurationScale, true>;
pub type ScaledCycleMeasurement<const N: u64> = TscMeasurement<ConstScale<N>, false>;

// Type aliases for RAII measurement blocks
pub type MeasureCyclesBlock<'a> = MeasureTscBlock<'a, CycleScale>;
pub type MeasureNSBlock<'a> = MeasureTscBlock<'a, NanoScale>;
pub type MeasureUSBlock<'a> = MeasureTscBlock<'a, MicroScale>;
pub type MeasureSBlock<'a> = MeasureTscBlock<'a, SecScale>;
pub type MeasureDurBlock<'a> = MeasureTscBlock<'a, DurationScale>;
pub type ScaledMeasureCyclesBlock<'a, const N: u64> = MeasureTscBlock<'a, ConstScale<N>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tsc_calibration() {
        TSCCalibration::ensure_initialized();
        let freq_hz = TSCCalibration::freq_hz();
        let freq_ghz = TSCCalibration::freq_ghz();

        assert!(freq_hz > 0);
        assert!(freq_ghz > 0.0);
        println!("TSC Frequency: {} Hz ({} GHz)", freq_hz, freq_ghz);
    }

    #[test]
    fn test_cycle_measurement() {
        let mut measure = CycleMeasurement::new();
        measure.start_measurement();

        // Do some work
        let mut sum = 0u64;
        for i in 0..1000 {
            sum = sum.wrapping_add(i);
        }

        let cycles = measure.stop_measurement();
        assert!(cycles > 0);
        println!("Measured {} cycles, sum={}", cycles, sum);
    }

    #[test]
    fn test_raii_measurement() {
        TSCCalibration::ensure_initialized();
        let mut cycles = 0u64;

        {
            let _measure = MeasureCyclesBlock::new(CycleMeasurement::new(), &mut cycles);

            // Do some work
            let mut sum = 0u64;
            for i in 0..1000 {
                sum = sum.wrapping_add(i);
            }
            std::hint::black_box(sum);
        }

        assert!(cycles > 0);
        println!("RAII measured {} cycles", cycles);
    }

    #[test]
    fn test_duration_conversion() {
        TSCCalibration::ensure_initialized();

        // Test nanosecond measurement
        let mut dur_measure = DurationMeasurement::new();
        let mut ns_measure = NanosMeasurement::new();
        dur_measure.start_measurement();
        ns_measure.start_measurement();

        thread::sleep(Duration::from_millis(10));

        let ns = ns_measure.stop_measurement();
        let duration = dur_measure.stop_measurement();

        println!("Nanosecond measurement: {} ns = {:?}", ns, duration);
        assert!(duration.as_nanos() > 0);
        assert!((ns - duration.as_nanos() as f64) <= 5.0);
    }

    #[test]
    fn test_now_duration() {
        TSCCalibration::ensure_initialized();

        let start = NanosMeasurement::now();
        thread::sleep(Duration::from_millis(1));
        let end = NanosMeasurement::now();

        let elapsed = end - start;

        println!("elapsed: {}", elapsed);
    }

    #[test]
    fn test_tsc_primitives_overhead() {
        let start = CycleMeasurement::now();
        TSCCalibration::ensure_initialized();
        let end = CycleMeasurement::now();
        println!("calibration took {} cycles", end - start);

        let mut cycle_sum = 0;
        for _i in 0..1000 {
            let start = CycleMeasurement::now();
            TSCCalibration::ensure_initialized();
            let end = CycleMeasurement::now();
            cycle_sum += end - start;
        }
        println!("avg calibration time: {}", cycle_sum / 1000);

        let mut cycle_sum = 0;
        let mut freq_sum = 0.0;
        for _i in 0..1000 {
            let start = CycleMeasurement::now();
            std::hint::black_box(freq_sum += TSCCalibration::freq_ghz());
            let end = CycleMeasurement::now();
            cycle_sum += end - start;
        }
        println!("avg ghz get cycles: {} with freq {}", cycle_sum / 1000, freq_sum / 1000.0);
    }
}
