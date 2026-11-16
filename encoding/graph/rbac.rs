/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{fmt, ops::Range};

use bytes::{Bytes, byte_array::ByteArray};
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::key_value::StorageKey;

use crate::{
    AsBytes, EncodingKeyspace, Keyable, Prefixed,
    graph::type_::vertex::TypeVertex,
    layout::{
        infix::{Infix, InfixID},
        prefix::{Prefix, PrefixID},
    },
};

#[derive(Copy, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct RBACRoleId(u16);

impl RBACRoleId {
    const LENGTH: usize = 2;
    pub(crate) fn as_u16(&self) -> u16 {
        self.0
    }
}

impl fmt::Debug for RBACRoleId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:04X}", self.0)
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct RBACRoleVertex {
    value: u32,
}

impl fmt::Debug for RBACRoleVertex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:06X}", self.value)
    }
}

impl RBACRoleVertex {
    pub(crate) const KEYSPACE: EncodingKeyspace = EncodingKeyspace::DefaultOptimisedPrefix11;
    pub const FIXED_WIDTH_ENCODING: bool = true;

    pub const PREFIX: Prefix = Prefix::RBACRole;

    pub const LENGTH: usize = PrefixID::LENGTH + RBACRoleId::LENGTH;
    pub(crate) const LENGTH_PREFIX: usize = PrefixID::LENGTH;

    pub const fn new(role_id: RBACRoleId) -> Self {
        let prefix = (Self::PREFIX as u32) << 16;
        let role_id = role_id.0 as u32;
        Self { value: prefix + role_id }
    }

    pub fn decode(bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> Self {
        debug_assert_eq!(bytes.length(), Self::LENGTH);
        let mut be_bytes = [0; 4];
        be_bytes[1..].copy_from_slice(&bytes);
        Self { value: u32::from_be_bytes(be_bytes) }
    }

    pub fn try_decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != Self::LENGTH {
            return None;
        }
        if bytes[Self::INDEX_PREFIX] != Self::PREFIX.prefix_id().byte {
            return None;
        }
        let mut be_bytes = [0; 4];
        be_bytes[1..].copy_from_slice(&bytes);
        Some(Self { value: u32::from_be_bytes(be_bytes) })
    }

    pub fn role_id(&self) -> RBACRoleId {
        RBACRoleId((self.value & 0xFFFF) as u16)
    }
}

impl AsBytes<BUFFER_KEY_INLINE> for RBACRoleVertex {
    fn to_bytes(self) -> Bytes<'static, BUFFER_KEY_INLINE> {
        Bytes::Array(ByteArray::copy(&self.value.to_be_bytes()[1..]))
    }
}

impl Keyable<BUFFER_KEY_INLINE> for RBACRoleVertex {
    fn keyspace(&self) -> EncodingKeyspace {
        Self::KEYSPACE
    }

    fn into_storage_key(self) -> StorageKey<'static, BUFFER_KEY_INLINE> {
        StorageKey::new(self.keyspace(), self.to_bytes())
    }
}

impl Prefixed<BUFFER_KEY_INLINE> for RBACRoleVertex {}

// Properties such as label
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct RBACRoleProperty {
    role: RBACRoleVertex,
    infix: Infix,
    suffix: ByteArray<0>,
}

impl RBACRoleProperty {
    const KEYSPACE: EncodingKeyspace = EncodingKeyspace::DefaultOptimisedPrefix11;
    const PREFIX: Prefix = Prefix::PropertyRBACRole;
    pub const FIXED_WIDTH_ENCODING: bool = Self::PREFIX.fixed_width_keys();

    const LENGTH_NO_SUFFIX: usize = PrefixID::LENGTH + RBACRoleVertex::LENGTH + InfixID::LENGTH;
    const LENGTH_PREFIX: usize = PrefixID::LENGTH;

    fn new_suffixed<const INLINE_BYTES: usize>(role: RBACRoleVertex, infix: Infix, suffix: Bytes<'_, INLINE_BYTES>) -> Self {
        Self { role, infix, suffix: ByteArray::copy(&suffix) }
    }

    pub fn decode(bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> Self {
        debug_assert!(bytes.length() >= Self::LENGTH_NO_SUFFIX);
        debug_assert_eq!(bytes[Self::INDEX_PREFIX], Self::PREFIX.prefix_id().byte);

        let role = RBACRoleVertex::decode(bytes.clone().into_range(Self::range_rbac_role()));
        let infix = Infix::from_infix_id(InfixID::new((&bytes[Self::range_infix()]).try_into().unwrap()));
        let suffix = ByteArray::copy(&bytes[Self::LENGTH_NO_SUFFIX..]);
        Self { role, infix, suffix }
    }

    pub fn try_decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < Self::LENGTH_NO_SUFFIX {
            return None;
        }
        if bytes[Self::INDEX_PREFIX] != Self::PREFIX.prefix_id().byte {
            return None;
        }

        let role = RBACRoleVertex::try_decode(bytes.get(Self::range_rbac_role())?)?;
        let infix = Infix::from_infix_id(InfixID::new((bytes.get(Self::range_infix())?).try_into().ok()?));
        let suffix = ByteArray::copy(bytes.get(Self::LENGTH_NO_SUFFIX..)?);
        Some(Self { role, infix, suffix })
    }

    pub fn build_prefix() -> StorageKey<'static, { RBACRoleProperty::LENGTH_PREFIX }> {
        // TODO: is it better to have a const fn that is a reference to owned memory, or
        //       to always induce a tiny copy have a non-const function?
        const PREFIX_BYTES: [u8; PrefixID::LENGTH] = RBACRoleProperty::PREFIX.prefix_id().to_bytes();
        StorageKey::new_ref(Self::KEYSPACE, &PREFIX_BYTES)
    }

    pub fn rbac_role(&self) -> RBACRoleVertex {
        self.role
    }

    pub fn infix(&self) -> Infix {
        self.infix
    }

    fn suffix_length(&self) -> usize {
        self.suffix.len()
    }

    pub fn suffix(&self) -> &[u8] {
        &self.suffix
    }

    const fn range_rbac_role() -> Range<usize> {
        Self::INDEX_PREFIX + 1..Self::INDEX_PREFIX + 1 + RBACRoleVertex::LENGTH
    }

    const fn range_infix() -> Range<usize> {
        Self::range_rbac_role().end..Self::range_rbac_role().end + InfixID::LENGTH
    }

    fn range_suffix(suffix_length: usize) -> Range<usize> {
        Self::range_infix().end..Self::range_infix().end + suffix_length
    }
}

impl AsBytes<BUFFER_KEY_INLINE> for RBACRoleProperty {
    fn to_bytes(self) -> Bytes<'static, BUFFER_KEY_INLINE> {
        let mut array = ByteArray::zeros(Self::LENGTH_NO_SUFFIX + self.suffix_length());
        array[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        array[Self::range_rbac_role()].copy_from_slice(&self.role.to_bytes());
        array[Self::range_infix()].copy_from_slice(&self.infix.infix_id().bytes());
        array[Self::range_suffix(self.suffix_length())].copy_from_slice(self.suffix());
        Bytes::Array(array)
    }
}

impl Keyable<BUFFER_KEY_INLINE> for RBACRoleProperty {
    fn keyspace(&self) -> EncodingKeyspace {
        Self::KEYSPACE
    }
}

impl Prefixed<BUFFER_KEY_INLINE> for RBACRoleProperty {}

// Accesses
#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd)]
pub enum RBACAccessKind {
    ReadObjectVertex,
    WriteObjectVertex,
    // More
}

impl RBACAccessKind {
    pub fn decode(bytes: [u8; InfixID::LENGTH]) -> Self {
        let infix = Infix::from_infix_id(InfixID::new(bytes));
        match infix {
            Infix::RBACReadObjectVertex => Self::ReadObjectVertex,
            Infix::RBACWriteObjectVertex => Self::WriteObjectVertex,
            _other => panic!("Unexpected infix for RBACAccessType: {infix:?}"),
        }
    }

    pub(crate) fn infix(&self) -> Infix {
        match self {
            Self::ReadObjectVertex => Infix::RBACReadObjectVertex,
            Self::WriteObjectVertex => Infix::RBACWriteObjectVertex,
        }
    }

    fn try_decode(bytes: [u8; InfixID::LENGTH]) -> Option<Self> {
        let infix = Infix::from_infix_id(InfixID::new(bytes));
        match infix {
            Infix::RBACReadObjectVertex => Some(Self::ReadObjectVertex),
            Infix::RBACWriteObjectVertex => Some(Self::WriteObjectVertex),
            _other => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct RBACRoleTypeAccess {
    role: RBACRoleVertex,
    access_type: RBACAccessKind,
    type_: TypeVertex,
}

impl RBACRoleTypeAccess {
    const KEYSPACE: EncodingKeyspace = EncodingKeyspace::DefaultOptimisedPrefix11;
    const PREFIX: Prefix = Prefix::RBACAccess;
    pub const FIXED_WIDTH_ENCODING: bool = Self::PREFIX.fixed_width_keys();

    const LENGTH: usize = PrefixID::LENGTH + RBACRoleVertex::LENGTH + InfixID::LENGTH + TypeVertex::LENGTH;
    const LENGTH_PREFIX: usize = PrefixID::LENGTH;
    const LENGTH_PREFIX_ROLE: usize = PrefixID::LENGTH + RBACRoleVertex::LENGTH;

    pub fn new(role: RBACRoleVertex, access_type: RBACAccessKind, type_: TypeVertex) -> Self {
        Self { role, access_type, type_ }
    }

    pub fn decode(bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> Self {
        debug_assert!(bytes.length() >= Self::LENGTH);
        debug_assert_eq!(bytes[Self::INDEX_PREFIX], Self::PREFIX.prefix_id().byte);

        let role = RBACRoleVertex::decode(bytes.clone().into_range(Self::range_rbac_role()));
        let access_type = RBACAccessKind::decode((&bytes[Self::range_access_type()]).try_into().unwrap());
        let type_ = TypeVertex::decode(bytes.clone().into_range(Self::range_type()));
        Self { role, access_type, type_ }
    }

    pub fn try_decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != Self::LENGTH {
            return None;
        }
        if bytes[Self::INDEX_PREFIX] != Self::PREFIX.prefix_id().byte {
            return None;
        }

        let role = RBACRoleVertex::try_decode(bytes.get(Self::range_rbac_role())?)?;
        let access_type = RBACAccessKind::try_decode((bytes.get(Self::range_access_type())?).try_into().ok()?)?;
        let type_ = TypeVertex::try_decode(bytes.get(Self::range_type())?)?;
        Some(Self { role, access_type, type_ })
    }

    pub fn build_prefix() -> StorageKey<'static, { RBACRoleTypeAccess::LENGTH_PREFIX }> {
        // TODO: is it better to have a const fn that is a reference to owned memory, or
        //       to always induce a tiny copy have a non-const function?
        const PREFIX_BYTES: [u8; PrefixID::LENGTH] = RBACRoleTypeAccess::PREFIX.prefix_id().to_bytes();
        StorageKey::new_ref(Self::KEYSPACE, &PREFIX_BYTES)
    }

    pub fn build_prefix_role(role: RBACRoleVertex) -> StorageKey<'static, { Self::LENGTH_PREFIX_ROLE }> {
        let mut bytes = ByteArray::zeros(Self::LENGTH_PREFIX_ROLE);
        bytes[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        bytes[Self::range_rbac_role()].copy_from_slice(&role.to_bytes());
        StorageKey::new_owned(Self::KEYSPACE, bytes)
    }

    pub fn rbac_role(&self) -> RBACRoleVertex {
        self.role
    }

    pub fn access_type(&self) -> RBACAccessKind {
        self.access_type
    }

    pub fn type_(&self) -> TypeVertex {
        self.type_
    }

    const fn range_rbac_role() -> Range<usize> {
        Self::INDEX_PREFIX + 1..Self::INDEX_PREFIX + 1 + RBACRoleVertex::LENGTH
    }

    const fn range_access_type() -> Range<usize> {
        Self::range_rbac_role().end..Self::range_rbac_role().end + InfixID::LENGTH
    }

    fn range_type() -> Range<usize> {
        Self::range_access_type().end..Self::range_access_type().end + TypeVertex::LENGTH
    }
}

impl AsBytes<BUFFER_KEY_INLINE> for RBACRoleTypeAccess {
    fn to_bytes(self) -> Bytes<'static, BUFFER_KEY_INLINE> {
        let mut array = ByteArray::zeros(Self::LENGTH);
        array[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        array[Self::range_rbac_role()].copy_from_slice(&self.role.to_bytes());
        array[Self::range_access_type()].copy_from_slice(&self.access_type.infix().infix_id().bytes());
        array[Self::range_type()].copy_from_slice(&self.type_.to_bytes());
        Bytes::Array(array)
    }
}

impl Keyable<BUFFER_KEY_INLINE> for RBACRoleTypeAccess {
    fn keyspace(&self) -> EncodingKeyspace {
        Self::KEYSPACE
    }
}

impl Prefixed<BUFFER_KEY_INLINE> for RBACRoleTypeAccess {}
