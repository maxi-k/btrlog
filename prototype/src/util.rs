macro_rules! ifcfg {
    // long form
    ([$meta:meta] $($item:item)*) => {
        $(
            #[cfg($meta)]
            #[cfg_attr(docsrs, doc(cfg($meta)))]
            $item
        )*
    };
}
pub(crate) use ifcfg;

#[allow(unused_macros)]
macro_rules! integrated_builder {
    ($new_name:ident, $update_name:ident) => {
        pub fn $new_name(f: impl FnOnce(&mut Self)) -> Self {
            let mut c = Self::default();
            f(&mut c);
            c
        }

        pub fn $update_name(mut self, f: impl FnOnce(&mut Self)) -> Self {
            f(&mut self);
            self
        }
    };
    () => {
        $crate::util::integrated_builder!(build_with, update);
    };
}
pub(crate) use integrated_builder;

////////////////////////////////////////////////////////////////////////////////
//  modified scoped-tls variant that also exposes a RAII-style interface
// for integration with C++ etc.
//
//
// Copyright 2014-2015 The Rust Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution and at
// http://rust-lang.org/COPYRIGHT.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::cell::Cell;
use std::marker;
use std::ops::Deref;
use std::pin::Pin;
use std::thread::LocalKey;

/// The macro. See the module level documentation for the description and examples.
macro_rules! scoped_tls {
    ($(#[$attrs:meta])* $vis:vis static $name:ident: $ty:ty) => (
        $(#[$attrs])*
        $vis static $name: $crate::util::ScopedKey<$ty> = $crate::util::ScopedKey {
            inner: {
                ::std::thread_local!(static FOO: ::std::cell::Cell<*const ()> = const {
                    ::std::cell::Cell::new(::std::ptr::null())
                });
                &FOO
            },
            _marker: ::std::marker::PhantomData,
        };
    )
}
pub(crate) use scoped_tls;

pub(crate) struct TLSResetRAII {
    key: &'static LocalKey<Cell<*const ()>>,
    val: *const (),
}
impl Drop for TLSResetRAII {
    fn drop(&mut self) {
        self.key.with(|c| c.set(self.val));
    }
}

pub struct ScopedKey<T> {
    pub inner: &'static LocalKey<Cell<*const ()>>,
    pub _marker: marker::PhantomData<T>,
}

unsafe impl<T> Sync for ScopedKey<T> {}

impl<T> ScopedKey<T> {
    pub fn set<F, R>(&'static self, t: &T, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let prev = self.inner.with(|c| {
            let prev = c.get();
            c.set(t as *const T as *const ());
            prev
        });
        let _reset = TLSResetRAII {
            key: self.inner,
            val: prev,
        };
        f()
    }

    pub fn with<F, R>(&'static self, f: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        let val = self.inner.with(|c| c.get());
        assert!(
            !val.is_null(),
            "cannot access a scoped thread local \
                                 variable without calling `set` first"
        );
        unsafe { f(&*(val as *const T)) }
    }

    pub fn is_set(&'static self) -> bool {
        self.inner.with(|c| !c.get().is_null())
    }

    pub unsafe fn bind_pin<Ptr: Deref<Target = T>>(&'static self, pin: Pin<Ptr>) -> TLSResetRAII {
        let ptr = unsafe { Pin::into_inner_unchecked(pin).deref() as *const T };
        let prev = self.inner.with(|c| {
            let prev = c.get();
            c.set(ptr as *const ());
            prev
        });
        TLSResetRAII {
            key: self.inner,
            val: prev,
        }
    }
}
