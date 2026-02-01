use super::id::{JournalId, WireJournalId};

////////////////////////////////////////////////////////////////////////////////
//  messages on the wire

pub use bincode::error::{DecodeError, EncodeError};
use bincode::{BorrowDecode, Decode, Encode};

type WireConfig = bincode::config::Configuration<bincode::config::LittleEndian, bincode::config::Fixint>;
const WIRE_CONFIG: WireConfig = bincode::config::standard().with_little_endian().with_fixed_int_encoding();

pub trait WireMessage<Context>: Decode<Context> + Encode {
    fn est_wire_size(&self) -> usize;

    // encode into a slice
    fn encode_into(self, dst: &mut [u8]) -> Result<usize, EncodeError> {
        debug_assert!(dst.len() >= self.est_wire_size());
        bincode::encode_into_slice(self, dst, WIRE_CONFIG)
    }

    // decode from a slice
    fn decode_from(src: &[u8]) -> Result<(Self, usize), DecodeError>
    where
        Self: Decode<()>,
    {
        bincode::decode_from_slice(&src, WIRE_CONFIG)
    }

    // decode with context
    fn decode_with_context(src: &mut [u8], context: Context) -> Result<(Self, usize), DecodeError> {
        bincode::decode_from_slice_with_context(&src, WIRE_CONFIG, context)
    }
}

pub trait BoundedSizeWireMessage<Context = ()>: Sized {
    const WIRE_SIZE_BOUND: usize = size_of::<Self>();
}

impl<Context, T: BoundedSizeWireMessage<Context> + Encode + Decode<Context>> WireMessage<Context> for T {
    fn est_wire_size(&self) -> usize {
        Self::WIRE_SIZE_BOUND
    }
}

////////////////////////////////////////////////////////////////////////////////
//  implementations for journalid
impl<Context> bincode::Decode<Context> for WireJournalId {
    fn decode<D: bincode::de::Decoder<Context = Context>>(decoder: &mut D) -> Result<Self, bincode::error::DecodeError> {
        let uuid: [u8; 16] = bincode::Decode::decode(decoder)?;
        Ok(JournalId::from_bytes(uuid).into())
    }
}

impl<'d, Context> BorrowDecode<'d, Context> for WireJournalId {
    fn borrow_decode<D: bincode::de::BorrowDecoder<'d, Context = Context>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let uuid: [u8; 16] = bincode::BorrowDecode::borrow_decode(decoder)?;
        Ok(JournalId::from_bytes(uuid).into())
    }
}

impl Encode for WireJournalId {
    fn encode<E: bincode::enc::Encoder>(&self, encoder: &mut E) -> Result<(), bincode::error::EncodeError> {
        bincode::Encode::encode(self.as_bytes(), encoder)
    }
}

pub mod auto {
    pub use super::{BoundedSizeWireMessage, WireMessage};
    pub use bincode::{Decode, Encode};
    pub use macro_rules_attr::apply;

    macro_rules! on_wire_trait_impl {
        ($name:ident, unbounded) => {};
        ($name:ident, max_size = $num:expr) => {
            impl $crate::types::wire::BoundedSizeWireMessage for $name {
                const WIRE_SIZE_BOUND: usize = $num;
            }
        };
        ($name:ident) => {
            impl $crate::types::wire::BoundedSizeWireMessage for $name {}
        };
    }

    macro_rules! on_wire {
        // for structs
        ($(#[$struct_meta:meta])*
         pub struct $name:ident {
             $($(#[$field_meta:meta])* $field_vis:vis $field_name:ident : $field_ty:ty),* $(,)?
         }
         $($param:tt)*
        ) => {
            $(#[$struct_meta])*
            #[derive(Debug, Clone, PartialEq, Eq)]
            pub struct $name {
                $( $(#[$field_meta])* $field_vis $field_name: $field_ty, )*
            }
            $crate::types::wire::auto::on_wire_trait_impl!($name $($param)*);
        };
        // for enums
        (
            $(#[$enum_meta:meta])*
            pub enum $name:ident {
                $( $(#[$field_meta:meta])* $field_name:ident $(( $($T:ty),+ $(,)? ))? ),+ $(,)?
            }
            $($param:tt)*
        ) => {
            // Repeat the enum definition
            $(#[$enum_meta])*
            #[derive(Debug, Clone, PartialEq, Eq)]
            pub enum $name {
                $( $(#[$field_meta])* $field_name $(( $($T ,)+ ))? , )+
            }
            $crate::types::wire::auto::on_wire_trait_impl!($name $($param)*);
        };
    }

    // macro_rules! msg_enum_from_impl {
    //     ($name:ident, $field_name:ident, ()) => {};
    //     ($name:ident, $field_name:ident, ($T:ty)) => {
    //         impl From<$T> for $name {
    //             fn from(val: $T) -> Self {
    //                 $name::$field_name(val)
    //             }
    //         }
    //     };
    //     // XXX generic variant difficult b/c of tuple identifier generation
    //     // ($name:ident, $field_name:ident, ($($T:ty),+ $(,)?)) => {
    //     //     impl From<($(_ ,)+)> for $name {
    //     //         fn from(($(@$T ,)+): ($($T ,)+)) -> Self {
    //     //             $name::$field_name($(,)+) // pair impl
    //     //         }
    //     //     }
    //     // };
    // }

    // macro_rules! msg_enum_from {
    //     ($(#[$enum_meta:meta])*
    //      pub enum $name:ident {
    //          $($(#[$field_meta:meta])* $field_name:ident $($spec:tt)?),+ $(,)?

    //          $( $(#[$field_meta:meta])* $field_name:ident $(( $($T:ty),+ $(,)? ))? ),+ $(,)?
    //      }) => {
    //         $(#[$enum_meta])*
    //         pub enum $name {
    //             $( $(#[$field_meta])* $field_name $($spec)? , )+
    //         }
    //         $($(msg_enum_from_impl!($name, $field_name, $spec);)?)+
    //     };
    // }
    macro_rules! msg_enum_from {
        ($(#[$enum_meta:meta])*
         pub enum $name:ident {
             $($(#[$field_meta:meta])* $field_name:ident $(($T:ty $(,)?))?),+ $(,)?
         }) => {
            $(#[$enum_meta])*
            pub enum $name {$( $(#[$field_meta])* $field_name $(($T))?),+ }
            $($(impl From<$T> for $name {
                fn from(val: $T) -> Self {
                    $name::$field_name(val)
                }
            })?)+
        };
    }

    pub(crate) use msg_enum_from;
    pub(crate) use on_wire;

    //pub(crate) use msg_enum_from_impl;
    pub(crate) use on_wire_trait_impl;
}
