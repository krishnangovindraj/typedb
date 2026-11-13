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
}

#[derive(Copy, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct RBACRole {
    value: u32,
}

impl fmt::Debug for RBACRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:06X}", self.value)
    }
}

impl RBACRole {
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
}

impl AsBytes<BUFFER_KEY_INLINE> for RBACRole {
    fn to_bytes(self) -> Bytes<'static, BUFFER_KEY_INLINE> {
        Bytes::Array(ByteArray::copy(&self.value.to_be_bytes()[1..]))
    }
}

impl Keyable<BUFFER_KEY_INLINE> for RBACRole {
    fn keyspace(&self) -> EncodingKeyspace {
        Self::KEYSPACE
    }

    fn into_storage_key(self) -> StorageKey<'static, BUFFER_KEY_INLINE> {
        StorageKey::new(self.keyspace(), self.to_bytes())
    }
}

impl Prefixed<BUFFER_KEY_INLINE> for RBACRole {}

// Properties such as label
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct RBACRoleProperty {
    role: RBACRole,
    infix: Infix,
    suffix: ByteArray<0>,
}

impl RBACRoleProperty {
    const KEYSPACE: EncodingKeyspace = EncodingKeyspace::DefaultOptimisedPrefix11;
    const PREFIX: Prefix = Prefix::PropertyRBACRole;
    pub const FIXED_WIDTH_ENCODING: bool = Self::PREFIX.fixed_width_keys();

    const LENGTH_NO_SUFFIX: usize = PrefixID::LENGTH + RBACRole::LENGTH + InfixID::LENGTH;
    const LENGTH_PREFIX: usize = PrefixID::LENGTH;

    fn new_suffixed<const INLINE_BYTES: usize>(role: RBACRole, infix: Infix, suffix: Bytes<'_, INLINE_BYTES>) -> Self {
        Self { role, infix, suffix: ByteArray::copy(&suffix) }
    }

    pub fn decode(bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> Self {
        debug_assert!(bytes.length() >= Self::LENGTH_NO_SUFFIX);
        debug_assert_eq!(bytes[Self::INDEX_PREFIX], Self::PREFIX.prefix_id().byte);

        let role = RBACRole::decode(bytes.clone().into_range(Self::range_rbac_role()));
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

        let role = RBACRole::try_decode(bytes.get(Self::range_rbac_role())?)?;
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

    pub fn rbac_role(&self) -> RBACRole {
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
        Self::INDEX_PREFIX + 1..Self::INDEX_PREFIX + 1 + RBACRole::LENGTH
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
pub enum RBACAccessType {
    Read,
    Write,
}

impl RBACAccessType {
    pub fn decode(bytes: [u8; InfixID::LENGTH]) -> Self {
        let infix = Infix::from_infix_id(InfixID::new(bytes));
        match infix {
            Infix::RBACAccessReads => Self::Read,
            Infix::RBACAccessWrites => Self::Write,
            _other => panic!("Unexpected infix for RBACAccessType: {infix:?}"),
        }
    }

    pub(crate) fn infix(&self) -> Infix {
        match self {
            Self::Read => Infix::RBACAccessReads,
            Self::Write => Infix::RBACAccessWrites,
        }
    }

    fn try_decode(bytes: [u8; InfixID::LENGTH]) -> Option<Self> {
        let infix = Infix::from_infix_id(InfixID::new(bytes));
        match infix {
            Infix::RBACAccessReads => Some(Self::Read),
            Infix::RBACAccessWrites => Some(Self::Write),
            _other => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct RBACRoleAccess {
    role: RBACRole,
    access_type: RBACAccessType,
    type_: TypeVertex,
}

impl RBACRoleAccess {
    const KEYSPACE: EncodingKeyspace = EncodingKeyspace::DefaultOptimisedPrefix11;
    const PREFIX: Prefix = Prefix::RBACAccess;
    pub const FIXED_WIDTH_ENCODING: bool = Self::PREFIX.fixed_width_keys();

    const LENGTH: usize = PrefixID::LENGTH + RBACRole::LENGTH + InfixID::LENGTH + TypeVertex::LENGTH;
    const LENGTH_PREFIX: usize = PrefixID::LENGTH;

    fn new(role: RBACRole, access_type: RBACAccessType, type_: TypeVertex) -> Self {
        Self { role, access_type, type_ }
    }

    pub fn decode(bytes: Bytes<'_, BUFFER_KEY_INLINE>) -> Self {
        debug_assert!(bytes.length() >= Self::LENGTH);
        debug_assert_eq!(bytes[Self::INDEX_PREFIX], Self::PREFIX.prefix_id().byte);

        let role = RBACRole::decode(bytes.clone().into_range(Self::range_rbac_role()));
        let access_type = RBACAccessType::decode((&bytes[Self::range_access_type()]).try_into().unwrap());
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

        let role = RBACRole::try_decode(bytes.get(Self::range_rbac_role())?)?;
        let access_type = RBACAccessType::try_decode((bytes.get(Self::range_access_type())?).try_into().ok()?)?;
        let type_ = TypeVertex::try_decode(bytes.get(Self::range_type())?)?;
        Some(Self { role, access_type, type_ })
    }

    pub fn build_prefix() -> StorageKey<'static, { RBACRoleAccess::LENGTH_PREFIX }> {
        // TODO: is it better to have a const fn that is a reference to owned memory, or
        //       to always induce a tiny copy have a non-const function?
        const PREFIX_BYTES: [u8; PrefixID::LENGTH] = RBACRoleAccess::PREFIX.prefix_id().to_bytes();
        StorageKey::new_ref(Self::KEYSPACE, &PREFIX_BYTES)
    }

    pub fn rbac_role(&self) -> RBACRole {
        self.role
    }

    pub fn access_type(&self) -> RBACAccessType {
        self.access_type
    }

    pub fn type_(&self) -> TypeVertex {
        self.type_
    }

    const fn range_rbac_role() -> Range<usize> {
        Self::INDEX_PREFIX + 1..Self::INDEX_PREFIX + 1 + RBACRole::LENGTH
    }

    const fn range_access_type() -> Range<usize> {
        Self::range_rbac_role().end..Self::range_rbac_role().end + InfixID::LENGTH
    }

    fn range_type() -> Range<usize> {
        Self::range_access_type().end..Self::range_access_type().end + TypeVertex::LENGTH
    }
}

impl AsBytes<BUFFER_KEY_INLINE> for RBACRoleAccess {
    fn to_bytes(self) -> Bytes<'static, BUFFER_KEY_INLINE> {
        let mut array = ByteArray::zeros(Self::LENGTH);
        array[Self::INDEX_PREFIX] = Self::PREFIX.prefix_id().byte;
        array[Self::range_rbac_role()].copy_from_slice(&self.role.to_bytes());
        array[Self::range_access_type()].copy_from_slice(&self.access_type.infix().infix_id().bytes());
        array[Self::range_type()].copy_from_slice(&self.type_.to_bytes());
        Bytes::Array(array)
    }
}

impl Keyable<BUFFER_KEY_INLINE> for RBACRoleAccess {
    fn keyspace(&self) -> EncodingKeyspace {
        Self::KEYSPACE
    }
}

impl Prefixed<BUFFER_KEY_INLINE> for RBACRoleAccess {}
