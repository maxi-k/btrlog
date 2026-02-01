pub use paste::paste;

#[doc(hidden)]
#[cfg(feature = "enable-tracing")]
#[macro_export]
macro_rules! __tracer_impl {
    // Match field with #[notrace] (with trailing comma)
    (@parse
        struct_attrs: [$($struct_attrs:tt)*]
        struct_vis: [$struct_vis:vis]
        struct_name: [$name:ident]
        traced: [$($traced:tt)*]
        notrace: [$($notrace:tt)*]
        remaining: [#[notrace] $(#[$meta:meta])* $vis:vis $fname:ident : $fty:ty, $($rest:tt)*]
    ) => {
        $crate::__tracer_impl! {
            @parse
            struct_attrs: [$($struct_attrs)*]
            struct_vis: [$struct_vis]
            struct_name: [$name]
            traced: [$($traced)*]
            notrace: [$($notrace)* {$(#[$meta])* $vis $fname : $fty}]
            remaining: [$($rest)*]
        }
    };

    // Match field with #[notrace] (no trailing comma)
    (@parse
        struct_attrs: [$($struct_attrs:tt)*]
        struct_vis: [$struct_vis:vis]
        struct_name: [$name:ident]
        traced: [$($traced:tt)*]
        notrace: [$($notrace:tt)*]
        remaining: [#[notrace] $(#[$meta:meta])* $vis:vis $fname:ident : $fty:ty]
    ) => {
        $crate::__tracer_impl! {
            @parse
            struct_attrs: [$($struct_attrs)*]
            struct_vis: [$struct_vis]
            struct_name: [$name]
            traced: [$($traced)*]
            notrace: [$($notrace)* {$(#[$meta])* $vis $fname : $fty}]
            remaining: []
        }
    };

    // Match normal field (with trailing comma)
    (@parse
        struct_attrs: [$($struct_attrs:tt)*]
        struct_vis: [$struct_vis:vis]
        struct_name: [$name:ident]
        traced: [$($traced:tt)*]
        notrace: [$($notrace:tt)*]
        remaining: [$(#[$meta:meta])* $vis:vis $fname:ident : $fty:ty, $($rest:tt)*]
    ) => {
        $crate::__tracer_impl! {
            @parse
            struct_attrs: [$($struct_attrs)*]
            struct_vis: [$struct_vis]
            struct_name: [$name]
            traced: [$($traced)* {$(#[$meta])* $vis $fname : $fty}]
            notrace: [$($notrace)*]
            remaining: [$($rest)*]
        }
    };

    // Match normal field (no trailing comma)
    (@parse
        struct_attrs: [$($struct_attrs:tt)*]
        struct_vis: [$struct_vis:vis]
        struct_name: [$name:ident]
        traced: [$($traced:tt)*]
        notrace: [$($notrace:tt)*]
        remaining: [$(#[$meta:meta])* $vis:vis $fname:ident : $fty:ty]
    ) => {
        $crate::__tracer_impl! {
            @parse
            struct_attrs: [$($struct_attrs)*]
            struct_vis: [$struct_vis]
            struct_name: [$name]
            traced: [$($traced)* {$(#[$meta])* $vis $fname : $fty}]
            notrace: [$($notrace)*]
            remaining: []
        }
    };

    // Base case: no more fields, generate code
    (@parse
        struct_attrs: [$(#[$struct_meta:meta])*]
        struct_vis: [$struct_vis:vis]
        struct_name: [$name:ident]
        traced: [$({$(#[$t_meta:meta])* $t_vis:vis $t_name:ident : $t_ty:ty})*]
        notrace: [$({$(#[$nt_meta:meta])* $nt_vis:vis $nt_name:ident : $nt_ty:ty})*]
        remaining: []
    ) => {
        $crate::macro_support::paste! {
            $(#[$struct_meta])*
            // #[cfg_attr(feature = "bincode", derive(bincode::Decode, bincode::Encode))]
            $struct_vis struct $name {
                $(
                    $(#[$t_meta])*
                    $t_vis $t_name: $crate::MeasurementCell<$t_ty>,
                )*
                $(
                    $(#[$nt_meta])*
                    $nt_vis $nt_name: $nt_ty,
                )*
            }
        }

        $crate::macro_support::paste! {
            impl $name {
                $(
                    #[allow(dead_code)]
                    #[inline]
                    pub fn [<trace_ $t_name >] <T, F: FnOnce() -> T> (&self, f: F) -> T {
                        type Clock = $crate::TscMeasurement<$crate::CycleScale, false>;
                        let mut m = Clock::new();
                        m.start_measurement();
                        let res = f();
                        self.$t_name.set_cycles(m.stop_measurement());
                        res
                    }
                )*

                #[allow(dead_code)]
                #[inline]
                pub fn csv_header(&self) -> &'static str {
                    concat!( "id,_1,", $( stringify!($t_name), "_", stringify!($t_ty),",", )* "_2")
                }

                #[allow(dead_code)]
                #[inline]
                pub fn csv_row(&self, id: &str) -> String {
                    format!("{}, {:?}", id, ( "", $(&self.$t_name.get_scaled(),)* "" ))
                }

                #[allow(dead_code)]
                #[inline]
                pub fn transposed_csv(&self, id: &str) -> String {
                    let mut result = format!("id : ID : {}\n", id);
                    $(
                        result.push_str(&format!("{} : {} : {}\n",
                            stringify!($t_name),
                            stringify!($t_ty),
                            self.$t_name.get_scaled()
                        ));
                    )*
                    result
                }

                #[allow(dead_code)]
                #[inline]
                pub fn total_duration(&self) -> Duration {
                    let total = $crate::MeasurementCell::<$crate::CycleScale>::zero();
                    $( total.add(self.$t_name.get_raw()); )*
                    total.get_duration()
                }
            }
        }
    };
}

#[doc(hidden)]
#[cfg(not(feature = "enable-tracing"))]
#[macro_export]
macro_rules! __tracer_impl_notrace {
    // Match field with #[notrace] (with trailing comma)
    (@parse
        struct_attrs: [$($struct_attrs:tt)*]
        struct_vis: [$struct_vis:vis]
        struct_name: [$name:ident]
        traced: [$($traced:tt)*]
        notrace: [$($notrace:tt)*]
        remaining: [#[notrace] $(#[$meta:meta])* $vis:vis $fname:ident : $fty:ty, $($rest:tt)*]
    ) => {
        $crate::__tracer_impl_notrace! {
            @parse
            struct_attrs: [$($struct_attrs)*]
            struct_vis: [$struct_vis]
            struct_name: [$name]
            traced: [$($traced)*]
            notrace: [$($notrace)* {$(#[$meta])* $vis $fname : $fty}]
            remaining: [$($rest)*]
        }
    };

    // Match field with #[notrace] (no trailing comma)
    (@parse
        struct_attrs: [$($struct_attrs:tt)*]
        struct_vis: [$struct_vis:vis]
        struct_name: [$name:ident]
        traced: [$($traced:tt)*]
        notrace: [$($notrace:tt)*]
        remaining: [#[notrace] $(#[$meta:meta])* $vis:vis $fname:ident : $fty:ty]
    ) => {
        $crate::__tracer_impl_notrace! {
            @parse
            struct_attrs: [$($struct_attrs)*]
            struct_vis: [$struct_vis]
            struct_name: [$name]
            traced: [$($traced)*]
            notrace: [$($notrace)* {$(#[$meta])* $vis $fname : $fty}]
            remaining: []
        }
    };

    // Match normal field (with trailing comma)
    (@parse
        struct_attrs: [$($struct_attrs:tt)*]
        struct_vis: [$struct_vis:vis]
        struct_name: [$name:ident]
        traced: [$($traced:tt)*]
        notrace: [$($notrace:tt)*]
        remaining: [$(#[$meta:meta])* $vis:vis $fname:ident : $fty:ty, $($rest:tt)*]
    ) => {
        $crate::__tracer_impl_notrace! {
            @parse
            struct_attrs: [$($struct_attrs)*]
            struct_vis: [$struct_vis]
            struct_name: [$name]
            traced: [$($traced)* {$(#[$meta])* $vis $fname : $fty}]
            notrace: [$($notrace)*]
            remaining: [$($rest)*]
        }
    };

    // Match normal field (no trailing comma)
    (@parse
        struct_attrs: [$($struct_attrs:tt)*]
        struct_vis: [$struct_vis:vis]
        struct_name: [$name:ident]
        traced: [$($traced:tt)*]
        notrace: [$($notrace:tt)*]
        remaining: [$(#[$meta:meta])* $vis:vis $fname:ident : $fty:ty]
    ) => {
        $crate::__tracer_impl_notrace! {
            @parse
            struct_attrs: [$($struct_attrs)*]
            struct_vis: [$struct_vis]
            struct_name: [$name]
            traced: [$($traced)* {$(#[$meta])* $vis $fname : $fty}]
            notrace: [$($notrace)*]
            remaining: []
        }
    };

    // Base case: no more fields, generate code
    (@parse
        struct_attrs: [$(#[$struct_meta:meta])*]
        struct_vis: [$struct_vis:vis]
        struct_name: [$name:ident]
        traced: [$({$(#[$t_meta:meta])* $t_vis:vis $t_name:ident : $t_ty:ty})*]
        notrace: [$({$(#[$nt_meta:meta])* $nt_vis:vis $nt_name:ident : $nt_ty:ty})*]
        remaining: []
    ) => {
        $crate::macro_support::paste! {
            $(#[$struct_meta])*
            // #[cfg_attr(feature = "bincode", derive(bincode::Decode, bincode::Encode))]
            $struct_vis struct $name {
                // $(
                //     $(#[$nt_meta])*
                //     $nt_vis $nt_name: $nt_ty,
                // )*
            }
        }

        $crate::macro_support::paste! {
            impl $name {
                $(
                    #[allow(dead_code)]
                    #[inline]
                    pub fn [<trace_ $t_name >] <T, F: FnOnce() -> T> (&self, f: F) -> T { f() }
                )*

                #[allow(dead_code)]
                #[inline]
                pub fn csv_header(&self) -> &'static str { "" }

                #[allow(dead_code)]
                #[inline]
                pub fn csv_row(&self, _id: &str) -> String { String::new() }

                #[allow(dead_code)]
                #[inline]
                pub fn transposed_csv(&self, _id: &str) -> String { String::new() }

                #[allow(dead_code)]
                #[inline]
                pub fn total_duration(&self) -> Duration { Duration::from_nanos(0) }
            }
        }
    };
}
