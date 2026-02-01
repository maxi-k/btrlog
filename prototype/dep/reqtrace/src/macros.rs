#[cfg(feature = "enable-tracing")]
#[macro_export]
macro_rules! tracer {
    ($(#[$struct_meta:meta])*
     $struct_vis:vis struct $name:ident {
         $($fields:tt)*
     }
    ) => {
        $crate::__tracer_impl! {
            @parse
            struct_attrs: [$(#[$struct_meta])*]
            struct_vis: [$struct_vis]
            struct_name: [$name]
            traced: []
            notrace: []
            remaining: [$($fields)*]
        }
    };
}

#[cfg(not(feature = "enable-tracing"))]
#[macro_export]
macro_rules! tracer {
    ($(#[$struct_meta:meta])*
     $struct_vis:vis struct $name:ident {
         $($fields:tt)*
     }
    ) => {
        $crate::__tracer_impl_notrace! {
            @parse
            struct_attrs: [$(#[$struct_meta])*]
            struct_vis: [$struct_vis]
            struct_name: [$name]
            traced: []
            notrace: []
            remaining: [$($fields)*]
        }
    };
}

#[macro_export]
#[cfg(feature = "enable-tracing")]
macro_rules! trace {
    ($name:ident.$field_name:ident, $($param:tt)*) => {
        $crate::macro_support::paste! {
            {
                let [<_measurement_cell_ $name _ $field_name >] = $crate::MeasureTscRAIICell::new(& $name.$field_name);
                $($param)*
            }
        }
    };
}

#[macro_export]
#[cfg(feature = "enable-tracing")]
macro_rules! trace_adding {
    ($name:ident.$field_name:ident, $($param:tt)*) => {
        $crate::macro_support::paste! {
            {
                let [<_measurement_cell_ $name _ $field_name  >] = $crate::MeasureTscAddingRAIICell::new(& $name.$field_name);
                $($param)*
            }
        }
    };
}

#[macro_export]
#[cfg(not(feature = "enable-tracing"))]
macro_rules! trace {
    ($name:ident.$field_name:ident, $($param:tt)*) => {
        $crate::macro_support::paste! {
            {
                $($param)*
            }
        }
    }
}

#[macro_export]
#[cfg(not(feature = "enable-tracing"))]
macro_rules! trace_adding {
    ($name:ident.$field_name:ident, $($param:tt)*) => {
        $crate::macro_support::paste! {
            {
                $($param)*
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::rdtsc::*;
    use std::time::Duration;

    tracer! {
        #[derive(Default)]
        pub struct ReqTracer {
            request_time: MicroScale,
            ht_lookup: CycleScale
        }
    }

    #[test]
    fn can_call_trace_fns() {
        crate::calibrate_cheap_clock();
        let t = ReqTracer::default();
        t.trace_request_time(|| {
            std::thread::sleep(Duration::from_millis(1));
        });
        #[cfg(feature = "enable-tracing")]
        assert!(t.request_time.get_scaled() >= 1000.0);

        #[cfg(not(feature = "enable-tracing"))]
        assert_eq!(std::mem::size_of::<ReqTracer>(), 0);
    }

    #[test]
    fn can_call_trace_macro() {
        crate::calibrate_cheap_clock();
        let t = ReqTracer::default();
        let res = trace!(t.ht_lookup, {
            std::thread::sleep(Duration::from_millis(1));
            5
        });
        assert_eq!(res, 5);

        #[cfg(feature = "enable-tracing")]
        {
            // After 1ms sleep, should have at least some cycles measured
            let cycles = t.ht_lookup.get_scaled();
            println!("Measured {} cycles for 1ms sleep", cycles);
            assert!(cycles > 0, "Expected at least some cycles, got {}", cycles);
        }

        #[cfg(not(feature = "enable-tracing"))]
        assert_eq!(std::mem::size_of::<ReqTracer>(), 0);
    }

    #[test]
    fn test_total_duration() {
        crate::calibrate_cheap_clock();

        tracer! {
            #[derive(Default)]
            pub struct MultiFieldTracer {
                operation_a: MicroScale,
                operation_b: CycleScale,
                operation_c: NanoScale,
            }
        }

        let tracer = MultiFieldTracer::default();

        // Trace multiple operations with known durations
        tracer.trace_operation_a(|| {
            std::thread::sleep(Duration::from_millis(5));
        });

        tracer.trace_operation_b(|| {
            std::thread::sleep(Duration::from_millis(3));
        });

        tracer.trace_operation_c(|| {
            std::thread::sleep(Duration::from_millis(2));
        });

        // Get the total duration
        let total = tracer.total_duration();

        #[cfg(feature = "enable-tracing")]
        {
            // Total should be at least 10ms (5 + 3 + 2)
            println!("Total duration: {:?}", total);
            println!("  operation_a: {} us", tracer.operation_a.get_scaled());
            println!("  operation_b: {} cycles", tracer.operation_b.get_scaled());
            println!("  operation_c: {} ns", tracer.operation_c.get_scaled());

            assert!(total >= Duration::from_millis(10), "Total duration {:?} should be at least 10ms", total);

            // But not too much more (allow 5ms overhead for scheduler jitter)
            assert!(total <= Duration::from_millis(15), "Total duration {:?} should be less than 15ms", total);
        }

        #[cfg(not(feature = "enable-tracing"))]
        {
            // Without tracing, total_duration should return zero
            assert_eq!(total, Duration::from_nanos(0));
        }
    }

    #[test]
    fn test_total_duration_empty() {
        crate::calibrate_cheap_clock();

        tracer! {
            #[derive(Default)]
            pub struct EmptyTracer {
                operation_x: CycleScale,
                operation_y: NanoScale,
            }
        }

        let tracer = EmptyTracer::default();

        // Don't trace anything, just get the total
        let total = tracer.total_duration();

        // Should be zero or very small
        assert!(
            total <= Duration::from_micros(1),
            "Total duration for empty tracer should be near zero, got {:?}",
            total
        );
    }

    #[test]
    fn test_total_duration_single_field() {
        crate::calibrate_cheap_clock();

        tracer! {
            #[derive(Default)]
            pub struct SingleFieldTracer {
                single_op: MicroScale,
            }
        }

        let tracer = SingleFieldTracer::default();

        tracer.trace_single_op(|| {
            std::thread::sleep(Duration::from_millis(7));
        });

        let total = tracer.total_duration();

        #[cfg(feature = "enable-tracing")]
        {
            println!("Single field total: {:?}", total);
            println!("  single_op: {} us", tracer.single_op.get_scaled());

            assert!(total >= Duration::from_millis(7), "Total duration {:?} should be at least 7ms", total);
        }

        #[cfg(not(feature = "enable-tracing"))]
        {
            assert_eq!(total, Duration::from_nanos(0));
        }
    }

    #[test]
    fn test_transposed_csv() {
        crate::calibrate_cheap_clock();

        tracer! {
            #[derive(Default)]
            pub struct CsvTracer {
                operation_a: MicroScale,
                operation_b: CycleScale,
                operation_c: NanoScale,
            }
        }

        let tracer = CsvTracer::default();

        tracer.trace_operation_a(|| {
            std::thread::sleep(Duration::from_millis(2));
        });

        tracer.trace_operation_b(|| {
            std::thread::sleep(Duration::from_millis(1));
        });

        tracer.trace_operation_c(|| {
            std::thread::sleep(Duration::from_millis(3));
        });

        let csv = tracer.transposed_csv("test_id_123");

        #[cfg(feature = "enable-tracing")]
        {
            println!("Transposed CSV output:\n{}", csv);

            // Check that the output contains expected parts
            assert!(csv.contains("id : ID : test_id_123"), "CSV should contain the ID");
            assert!(csv.contains("operation_a"), "CSV should contain operation_a field name");
            assert!(csv.contains("operation_b"), "CSV should contain operation_b field name");
            assert!(csv.contains("operation_c"), "CSV should contain operation_c field name");
            assert!(csv.contains("MicroScale"), "CSV should contain MicroScale type");
            assert!(csv.contains("CycleScale"), "CSV should contain CycleScale type");
            assert!(csv.contains("NanoScale"), "CSV should contain NanoScale type");

            // Check that values are present (non-zero since we did work)
            let lines: Vec<&str> = csv.lines().collect();
            assert_eq!(lines.len(), 4, "Should have exactly 4 lines (id + 3 fields)");

            // Verify format: each line should have the pattern "name : type : value"
            for (i, line) in lines.iter().enumerate() {
                let parts: Vec<&str> = line.split(" : ").collect();
                assert_eq!(parts.len(), 3, "Line {} should have 3 parts separated by ' : '", i);
            }
        }

        #[cfg(not(feature = "enable-tracing"))]
        {
            // Without tracing, the output should still be valid
            println!("Transposed CSV (no tracing): {}", csv);
        }
    }

    #[test]
    fn test_transposed_csv_single_field() {
        crate::calibrate_cheap_clock();

        tracer! {
            #[derive(Default)]
            pub struct SingleCsvTracer {
                single: CycleScale,
            }
        }

        let tracer = SingleCsvTracer::default();
        tracer.trace_single(|| {
            // Do some work
            for _ in 0..1000 {
                std::hint::black_box(42);
            }
        });

        let csv = tracer.transposed_csv("single_test");

        #[cfg(feature = "enable-tracing")]
        {
            println!("Single field CSV:\n{}", csv);

            let lines: Vec<&str> = csv.lines().collect();
            assert_eq!(lines.len(), 2, "Should have exactly 2 lines (id + 1 field)");
            assert!(lines[0].starts_with("id : ID : single_test"));
            assert!(lines[1].starts_with("single : CycleScale : "));
        }
    }

    #[test]
    fn test_notrace_fields() {
        crate::calibrate_cheap_clock();

        tracer! {
            #[derive(Default)]
            pub struct MixedTracer {
                traced_field: MicroScale,
                #[notrace]
                request_id: u64,
                another_traced: CycleScale,
                #[notrace]
                context: String,
            }
        }

        let mut tracer = MixedTracer::default();

        // Set non-traced fields directly
        tracer.request_id = 42;
        tracer.context = String::from("test_context");

        // Trace the traced fields
        tracer.trace_traced_field(|| {
            std::thread::sleep(Duration::from_millis(1));
        });

        tracer.trace_another_traced(|| {
            std::thread::sleep(Duration::from_millis(1));
        });

        // Verify non-traced fields are accessible and have correct values
        assert_eq!(tracer.request_id, 42);
        assert_eq!(tracer.context, "test_context");

        // CSV should only contain traced fields
        let csv = tracer.transposed_csv("mixed_test");

        #[cfg(feature = "enable-tracing")]
        {
            println!("Mixed tracer CSV:\n{}", csv);

            // CSV should contain traced fields
            assert!(csv.contains("traced_field"), "CSV should contain traced_field");
            assert!(csv.contains("another_traced"), "CSV should contain another_traced");

            // CSV should NOT contain notrace fields
            assert!(!csv.contains("request_id"), "CSV should not contain request_id");
            assert!(!csv.contains("context"), "CSV should not contain context");

            // Verify format: should have 3 lines (id + 2 traced fields)
            let lines: Vec<&str> = csv.lines().collect();
            assert_eq!(lines.len(), 3, "Should have exactly 3 lines (id + 2 traced fields)");
        }

        #[cfg(not(feature = "enable-tracing"))]
        {
            // Without tracing, the struct should only have notrace fields
            assert_eq!(tracer.request_id, 42);
            assert_eq!(tracer.context, "test_context");
        }
    }
}
