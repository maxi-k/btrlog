use std::{cell::Cell, time::Duration};

use crate::rdtsc::*;

////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(feature = "bincode", derive(bincode::Decode, bincode::Encode))]
#[derive(PartialEq, Eq, Debug, Clone)]
pub struct MeasurementCell<T: FrequencyScale> {
    inner: Cell<u64>,
    _type: std::marker::PhantomData<T>,
}

impl<T: FrequencyScale<Output: Default>> Default for MeasurementCell<T> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
            _type: std::marker::PhantomData {},
        }
    }
}



impl<T: FrequencyScale> MeasurementCell<T> {
    #[inline]
    pub const fn zero() -> Self {
        Self {
            inner: Cell::new(0),
            _type: std::marker::PhantomData {},
        }
    }

    #[inline]
    pub fn set_cycles(&self, o: u64) {
        self.inner.set(o);
    }

    #[inline]
    pub fn add(&self, o: u64) {
        self.inner.update(|x| x + o);
    }

    #[inline]
    pub fn add_other<O: FrequencyScale>(&self, o: MeasurementCell<O>) {
        self.inner.update(|x| x + o.get_raw());
    }

    #[inline]
    pub fn get_raw(&self) -> u64 {
        self.inner.get()
    }

    #[inline]
    pub fn get_scaled(&self) -> T::Output {
        T::scale_cycles(self.get_raw())
    }

    #[inline]
    pub fn cast<T2: FrequencyScale>(self) -> MeasurementCell<T2> {
        MeasurementCell::<T2> {
            inner: Cell::new(self.get_raw()),
            _type: std::marker::PhantomData {},
        }
    }

    pub fn get_duration(&self) -> Duration {
        Duration::from_nanos(NanoScale::scale_cycles(self.get_raw()) as u64)
    }
}

impl<F1, F2> std::ops::Add<MeasurementCell<F2>> for &MeasurementCell<F1>
where
    F1: FrequencyScale,
    F2: FrequencyScale,
{
    type Output = MeasurementCell<F2>;

    fn add(self, rhs: MeasurementCell<F2>) -> Self::Output {
        MeasurementCell::<F2> {
            inner: Cell::new(rhs.get_raw() + self.get_raw()),
            _type: std::marker::PhantomData {},
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

pub struct MeasureTscRAIICell<'a, F: FrequencyScale> {
    measure: TscMeasurement<CycleScale>,
    cell: &'a MeasurementCell<F>,
}

impl<'a, F: FrequencyScale> MeasureTscRAIICell<'a, F> {
    pub fn new(cell: &'a MeasurementCell<F>) -> Self {
        let mut measure = TscMeasurement::<CycleScale, false>::new();
        measure.start_measurement();
        Self { measure, cell }
    }
}

impl<'a, F: FrequencyScale> Drop for MeasureTscRAIICell<'a, F> {
    fn drop(&mut self) {
        self.cell.set_cycles(self.measure.stop_measurement());
    }
}

pub struct MeasureTscAddingRAIICell<'a, F: FrequencyScale> {
    measure: TscMeasurement<CycleScale>,
    cell: &'a MeasurementCell<F>,
}

impl<'a, F: FrequencyScale> MeasureTscAddingRAIICell<'a, F> {
    pub fn new(cell: &'a MeasurementCell<F>) -> Self {
        let mut measure = TscMeasurement::<CycleScale, false>::new();
        measure.start_measurement();
        Self { measure, cell }
    }
}

impl<'a, F: FrequencyScale> Drop for MeasureTscAddingRAIICell<'a, F> {
    fn drop(&mut self) {
        self.cell.add(self.measure.stop_measurement());
    }
}
