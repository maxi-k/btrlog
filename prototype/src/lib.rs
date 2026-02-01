#![feature(type_alias_impl_trait)]
#![feature(inherent_associated_types)]
#![feature(likely_unlikely)]

pub mod config;
pub mod dst;
pub mod io;
pub mod node;
pub mod runtime;
pub mod trace;
pub mod types;
pub mod util;

#[cfg(feature = "client")]
pub mod client;

#[cfg(feature = "server")]
pub mod server;

#[cfg(feature = "bench")]
pub mod bench;

#[macro_export]
#[cfg(feature = "server")]
macro_rules! with_backend {
    ($config:expr, ($blob:ident, $wal:ident) => $body:expr) => {
       $crate::with_blobstore!($config, $blob => {
           $crate::with_wal!($config, $wal => {
               $body
           })
       })
    }
}
