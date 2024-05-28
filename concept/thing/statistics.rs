/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, HashMap},
    hash::Hash,
    ops::{Add, AddAssign, Bound},
    sync::atomic::Ordering,
};

use bytes::Bytes;
use durability::DurabilityRecordType;
use encoding::graph::{
    thing::{
        edge::{ThingEdgeHas, ThingEdgeRelationIndex, ThingEdgeRolePlayer},
        vertex_attribute::AttributeVertex,
        vertex_object::ObjectVertex,
    },
    type_::vertex::{TypeID,TypeIDUInt},
    Typed,
};
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use serde::{
    de::{Error, SeqAccess, Visitor},
    ser::SerializeStruct,
    Deserialize, Deserializer, Serialize, Serializer,
};
use encoding::graph::type_::vertex::{EncodableTypeVertex, PrefixedEncodableTypeVertex};
use storage::{
    durability_client::{DurabilityRecord, UnsequencedDurabilityRecord},
    iterator::MVCCReadError,
    key_value::{StorageKeyArray, StorageKeyReference},
    sequence_number::SequenceNumber,
    snapshot::{write::Write, ReadableSnapshot},
    MVCCStorage,
};

use crate::{
    thing::{attribute::Attribute, entity::Entity, object::Object, relation::Relation},
    type_::{
        attribute_type::AttributeType, entity_type::EntityType, object_type::ObjectType, relation_type::RelationType,
        role_type::RoleType, TypeAPI,
    },
};

type StatisticsVersion = u64;

/// Thing statistics, reflecting a snapshot of statistics accurate as of a particular sequence number
/// When types are undefined, we retain the last count of the instances of the type
/// Invariant: all undefined types are
#[derive(Debug, Clone)]
pub struct Statistics {
    statistics_version: StatisticsVersion,
    pub sequence_number: SequenceNumber,

    pub total_thing_count: u64,
    pub total_entity_count: u64,
    pub total_relation_count: u64,
    pub total_attribute_count: u64,
    pub total_role_count: u64,
    pub total_has_count: u64,

    pub entity_counts: HashMap<EntityType<'static>, u64>,
    pub relation_counts: HashMap<RelationType<'static>, u64>,
    pub attribute_counts: HashMap<AttributeType<'static>, u64>,
    pub role_counts: HashMap<RoleType<'static>, u64>,

    pub has_attribute_counts: HashMap<ObjectType<'static>, HashMap<AttributeType<'static>, u64>>,
    pub attribute_owner_counts: HashMap<AttributeType<'static>, HashMap<ObjectType<'static>, u64>>,
    pub role_player_counts: HashMap<ObjectType<'static>, HashMap<RoleType<'static>, u64>>,
    pub relation_role_counts: HashMap<RelationType<'static>, HashMap<RoleType<'static>, u64>>,

    // TODO: adding role types is possible, but won't help with filtering before reading storage since roles are not in the prefix
    pub player_index_counts: HashMap<ObjectType<'static>, HashMap<ObjectType<'static>, u64>>,
    // future: attribute value distributions, attribute value ownership distributions, etc.
}

impl Statistics {
    const STATISTICS_VERSION: StatisticsVersion = 0;

    pub fn new(sequence_number: SequenceNumber) -> Self {
        Statistics {
            statistics_version: Self::STATISTICS_VERSION,
            sequence_number: sequence_number,
            total_thing_count: 0,
            total_entity_count: 0,
            total_relation_count: 0,
            total_attribute_count: 0,
            total_role_count: 0,
            total_has_count: 0,
            entity_counts: HashMap::new(),
            relation_counts: HashMap::new(),
            attribute_counts: HashMap::new(),
            role_counts: HashMap::new(),
            has_attribute_counts: HashMap::new(),
            attribute_owner_counts: HashMap::new(),
            role_player_counts: HashMap::new(),
            relation_role_counts: HashMap::new(),
            player_index_counts: HashMap::new(),
        }
    }

    pub fn update_writes<D, Snapshot: ReadableSnapshot>(
        &mut self,
        commits: &BTreeMap<SequenceNumber, Snapshot>,
        storage: &MVCCStorage<D>,
    ) -> Result<(), MVCCReadError> {
        if commits.is_empty() {
            return Ok(());
        }
        for (sequence_number, snapshot) in commits.iter() {
            self.update_write(*sequence_number, snapshot, commits, storage)?
        }
        let last_sequence_number = *commits.last_key_value().unwrap().0;
        self.sequence_number = last_sequence_number;
        Ok(())
    }

    fn update_write<D, Snapshot: ReadableSnapshot>(
        &mut self,
        snapshot_commit_sequence_number: SequenceNumber,
        snapshot: &impl ReadableSnapshot,
        commits: &BTreeMap<SequenceNumber, Snapshot>,
        storage: &MVCCStorage<D>,
    ) -> Result<(), MVCCReadError> {
        let record_open_sequence_number = snapshot.open_sequence_number();
        for (key, write) in snapshot.iterate_buffered_writes() {
            let key_reference = StorageKeyReference::from(&key);
            let delta = Self::write_to_delta(
                &key,
                &write,
                record_open_sequence_number,
                snapshot_commit_sequence_number,
                commits,
                storage,
            )?;
            if ObjectVertex::is_entity_vertex(key_reference) {
                let type_ = Entity::new(ObjectVertex::new(Bytes::Reference(key_reference.byte_ref()))).type_();
                self.update_entities(type_, delta);
            } else if ObjectVertex::is_relation_vertex(key_reference) {
                let type_ = Relation::new(ObjectVertex::new(Bytes::Reference(key_reference.byte_ref()))).type_();
                self.update_relations(type_, delta);
            } else if AttributeVertex::is_attribute_vertex(key_reference) {
                let type_ = Attribute::new(AttributeVertex::new(Bytes::Reference(key_reference.byte_ref()))).type_();
                self.update_attributes(type_, delta);
            } else if ThingEdgeHas::is_has(key_reference) {
                let edge = ThingEdgeHas::new(Bytes::Reference(key_reference.byte_ref()));
                self.update_has(Object::new(edge.from()).type_(), Attribute::new(edge.to()).type_(), delta)
            } else if ThingEdgeRolePlayer::is_role_player(key_reference) {
                let edge = ThingEdgeRolePlayer::new(Bytes::Reference(key_reference.byte_ref()));
                let role_type = RoleType::from_type_id(edge.role_id());
                self.update_role_player(
                    Object::new(edge.from()).type_(),
                    role_type,
                    Relation::new(edge.to()).type_(),
                    delta,
                )
            } else if ThingEdgeRelationIndex::is_index(key_reference) {
                let edge = ThingEdgeRelationIndex::new(Bytes::Reference(key_reference.byte_ref()));
                self.update_indexed_player(Object::new(edge.from()).type_(), Object::new(edge.to()).type_(), delta)
            } else if EntityType::is_of_kind(key_reference) {
                let type_ =
                    EntityType::decode(Bytes::Reference(key_reference.byte_ref()).into_owned());
                if matches!(write, Write::Delete) {
                    self.entity_counts.remove(&type_);
                    self.clear_object_type(ObjectType::Entity(type_));
                }
            } else if RelationType::is_of_kind(key_reference) {
                let type_ = RelationType::decode(Bytes::Reference(key_reference.byte_ref()).into_owned());
                if matches!(write, Write::Delete) {
                    self.relation_counts.remove(&type_);
                    self.relation_role_counts.remove(&type_);
                    let as_object_type = ObjectType::Relation(type_);
                    self.clear_object_type(as_object_type.clone());
                }
            } else if AttributeType::is_of_kind(key_reference) {
                let type_ = AttributeType::decode(
                    Bytes::Reference(key_reference.byte_ref()).into_owned()
                );
                if matches!(write, Write::Delete) {
                    self.attribute_counts.remove(&type_);
                    self.attribute_owner_counts.remove(&type_);
                    self.has_attribute_counts.iter_mut().for_each(|(_, map)| {
                        let _ = map.remove(&type_);
                    });
                    self.has_attribute_counts.retain(|_, map| !map.is_empty());
                }
            } else if RoleType::is_of_kind(key_reference) {
                let type_ =
                    RoleType::decode(Bytes::Reference(key_reference.byte_ref()).into_owned());
                if matches!(write, Write::Delete) {
                    self.role_counts.remove(&type_);
                    self.role_player_counts.iter_mut().for_each(|(_, map)| {
                        let _ = map.remove(&type_);
                    });
                    self.role_player_counts.retain(|_, map| !map.is_empty());
                    self.relation_role_counts.iter_mut().for_each(|(_, map)| {
                        let _ = map.remove(&type_);
                    });
                    self.relation_role_counts.retain(|_, map| !map.is_empty());
                }
            }
        }
        Ok(())
    }

    fn clear_object_type(&mut self, object_type: ObjectType<'static>) {
        self.has_attribute_counts.remove(&object_type);
        self.attribute_owner_counts.iter_mut().for_each(|(_, map)| {
            let _ = map.remove(&object_type);
        });
        self.attribute_owner_counts.retain(|_, map| !map.is_empty());

        self.role_player_counts.remove(&object_type);

        self.player_index_counts.remove(&object_type);
        self.player_index_counts.iter_mut().for_each(|(_, map)| {
            let _ = map.remove(&object_type);
        });
        self.player_index_counts.retain(|_, map| !map.is_empty());
    }

    fn write_to_delta<D, Snapshot: ReadableSnapshot>(
        write_key: &StorageKeyArray<{ BUFFER_KEY_INLINE }>,
        write: &Write,
        write_open_sequence_number: SequenceNumber,
        write_commit_sequence_number: SequenceNumber,
        commits: &BTreeMap<SequenceNumber, Snapshot>,
        storage: &MVCCStorage<D>,
    ) -> Result<i64, MVCCReadError> {
        match write {
            Write::Insert { .. } => Ok(1),
            Write::Put { reinsert, .. } => {
                // PUT operation which we may have a concurrent commit and may or may not be inserted in the end
                // The easiest way to check whether it was ultimately committed or not is to open the storage at
                // CommitSequenceNumber - 1, and check if it exists. If it exists, we don't count. If it does, we do.
                // However, this induces a read for every PUT, even though 99% of time there is no concurrent put.

                // So, we only read from storage, if :
                // 1. we can't tell from the current set of commits whether a predecessor could have written the same key (open < commits start)
                // 2. any commit in the set of commits modifies the same key at all

                let check_storage = write_open_sequence_number < *commits.first_key_value().unwrap().0
                    || (commits
                        .range::<SequenceNumber, _>((
                            Bound::Excluded(write_open_sequence_number),
                            Bound::Excluded(write_commit_sequence_number),
                        ))
                        .any(|(seq, snapshot)| {
                            snapshot
                                .get_buffered_write_mapped(StorageKeyReference::from(write_key), |v| true)
                                .unwrap_or(false)
                        }));

                if check_storage {
                    if storage
                        .get_mapped(
                            StorageKeyReference::from(write_key),
                            write_commit_sequence_number.previous(),
                            |_| true,
                        )?
                        .unwrap_or(false)
                    {
                        // exists in storage before PUT is committed
                        return Ok(0);
                    } else {
                        // does not exist in storage before PUT is committed
                        return Ok(1);
                    }
                } else {
                    // no concurrent commit could have occurred - fall back to the reinsert flag
                    if reinsert.load(Ordering::Relaxed) {
                        return Ok(1);
                    } else {
                        return Ok(0);
                    }
                }
            }
            Write::Delete => Ok(-1),
        }
    }

    fn update_entities(&mut self, entity_type: EntityType<'static>, delta: i64) {
        self.entity_counts.entry(entity_type).or_insert(0).checked_add_signed(delta).unwrap();
        self.total_entity_count.checked_add_signed(delta).unwrap();
        self.total_thing_count.checked_add_signed(delta).unwrap();
    }

    fn update_relations(&mut self, relation_type: RelationType<'static>, delta: i64) {
        self.relation_counts.entry(relation_type).or_insert(0).checked_add_signed(delta).unwrap();
        self.total_relation_count.checked_add_signed(delta).unwrap();
        self.total_thing_count.checked_add_signed(delta).unwrap();
    }

    fn update_attributes(&mut self, attribute_type: AttributeType<'static>, delta: i64) {
        self.attribute_counts.entry(attribute_type).or_insert(0).checked_add_signed(delta).unwrap();
        self.total_attribute_count.checked_add_signed(delta).unwrap();
        self.total_thing_count.checked_add_signed(delta).unwrap();
    }

    fn update_has(&mut self, owner_type: ObjectType<'static>, attribute_type: AttributeType<'static>, delta: i64) {
        self.has_attribute_counts
            .entry(owner_type.clone())
            .or_insert_with(|| HashMap::new())
            .entry(attribute_type.clone())
            .or_insert(0)
            .checked_add_signed(delta)
            .unwrap();
        self.attribute_owner_counts
            .entry(attribute_type)
            .or_insert_with(|| HashMap::new())
            .entry(owner_type)
            .or_insert(0)
            .checked_add_signed(delta)
            .unwrap();
        self.total_has_count.checked_add_signed(delta).unwrap();
    }

    fn update_role_player(
        &mut self,
        player_type: ObjectType<'static>,
        role_type: RoleType<'static>,
        relation_type: RelationType<'static>,
        delta: i64,
    ) {
        self.role_counts.entry(role_type.clone()).or_insert(0).checked_add_signed(delta).unwrap();
        self.total_role_count.checked_add_signed(delta).unwrap();
        self.role_player_counts
            .entry(player_type)
            .or_insert_with(|| HashMap::new())
            .entry(role_type.clone())
            .or_insert(0)
            .checked_add_signed(delta)
            .unwrap();
        self.relation_role_counts
            .entry(relation_type)
            .or_insert_with(|| HashMap::new())
            .entry(role_type)
            .or_insert(0)
            .checked_add_signed(delta)
            .unwrap();
    }

    fn update_indexed_player(
        &mut self,
        player_1_type: ObjectType<'static>,
        player_2_type: ObjectType<'static>,
        delta: i64,
    ) {
        self.player_index_counts
            .entry(player_1_type.clone())
            .or_insert_with(|| HashMap::new())
            .entry(player_2_type.clone())
            .or_insert(0)
            .checked_add_signed(delta)
            .unwrap();
        if player_1_type != player_2_type {
            self.player_index_counts
                .entry(player_2_type)
                .or_insert_with(|| HashMap::new())
                .entry(player_1_type)
                .or_insert(0)
                .checked_add_signed(delta)
                .unwrap();
        }
    }
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Hash)]
enum SerialisableType {
    Entity(TypeIDUInt),
    Relation(TypeIDUInt),
    Attribute(TypeIDUInt),
    Role(TypeIDUInt),
}

impl SerialisableType {
    pub(crate) fn id(&self) -> TypeIDUInt {
        match self {
            SerialisableType::Entity(id) => *id,
            SerialisableType::Relation(id) => *id,
            SerialisableType::Attribute(id) => *id,
            SerialisableType::Role(id) => *id,
        }
    }

    pub(crate) fn into_entity_type(self) -> EntityType<'static> {
        match self {
            Self::Entity(id) => EntityType::from_type_id(TypeID::build(id)),
            _ => panic!("Incompatible conversion."),
        }
    }

    pub(crate) fn into_relation_type(self) -> RelationType<'static> {
        match self {
            Self::Relation(id) => RelationType::from_type_id(TypeID::build(id)),
            _ => panic!("Incompatible conversion."),
        }
    }

    pub(crate) fn into_object_type(self) -> ObjectType<'static> {
        match self {
            Self::Entity(id) => ObjectType::Entity(EntityType::from_type_id(TypeID::build(id))),
            Self::Relation(id) => {
                ObjectType::Relation(RelationType::from_type_id(TypeID::build(id)))
            }
            _ => panic!("Incompatible conversion."),
        }
    }

    pub(crate) fn into_attribute_type(self) -> AttributeType<'static> {
        match self {
            Self::Attribute(id) => AttributeType::from_type_id(TypeID::build(id)),
            _ => panic!("Incompatible conversion."),
        }
    }

    pub(crate) fn into_role_type(self) -> RoleType<'static> {
        match self {
            Self::Role(id) => RoleType::from_type_id(TypeID::build(id)),
            _ => panic!("Incompatible conversion."),
        }
    }
}

impl From<ObjectType<'static>> for SerialisableType {
    fn from(object: ObjectType<'static>) -> Self {
        match object {
            ObjectType::Entity(entity) => Self::from(entity),
            ObjectType::Relation(relation) => Self::from(relation),
        }
    }
}

impl From<EntityType<'static>> for SerialisableType {
    fn from(entity: EntityType<'static>) -> Self {
        Self::Entity(entity.vertex().type_id_().as_u16())
    }
}

impl From<RelationType<'static>> for SerialisableType {
    fn from(relation: RelationType<'static>) -> Self {
        Self::Relation(relation.vertex().type_id_().as_u16())
    }
}

impl From<AttributeType<'static>> for SerialisableType {
    fn from(attribute: AttributeType<'static>) -> Self {
        Self::Attribute(attribute.vertex().type_id_().as_u16())
    }
}

impl From<RoleType<'static>> for SerialisableType {
    fn from(role_type: RoleType<'static>) -> Self {
        Self::Role(role_type.vertex().type_id_().as_u16())
    }
}

impl DurabilityRecord for Statistics {
    const RECORD_TYPE: DurabilityRecordType = 10;
    const RECORD_NAME: &'static str = "thing_statistics";

    fn serialise_into(&self, writer: &mut impl std::io::Write) -> bincode::Result<()> {
        bincode::serialize_into(writer, self)
    }

    fn deserialise_from(reader: &mut impl std::io::Read) -> bincode::Result<Self> {
        bincode::deserialize_from(reader)
    }
}

impl UnsequencedDurabilityRecord for Statistics {}

mod serialise {
    use std::{collections::HashMap, fmt};

    use serde::{
        de,
        de::{MapAccess, SeqAccess, Visitor},
        ser::SerializeStruct,
        Deserialize, Deserializer, Serialize, Serializer,
    };

    use crate::{
        thing::statistics::{SerialisableType, Statistics},
        type_::{
            attribute_type::AttributeType, entity_type::EntityType, object_type::ObjectType,
            relation_type::RelationType, role_type::RoleType,
        },
    };

    enum Field {
        StatisticsVersion,
        OpenSequenceNumber,
        TotalThingCount,
        TotalEntityCount,
        TotalRelationCount,
        TotalAttributeCount,
        TotalRoleCount,
        TotalHasCount,
        EntityCounts,
        RelationCounts,
        AttributeCounts,
        RoleCounts,
        HasAttributeCounts,
        AttributeOwnerCounts,
        RolePlayerCounts,
        RelationRoleCounts,
        PlayerIndexCounts,
    }

    impl Field {
        const NAMES: [&'static str; 17] = [
            Self::StatisticsVersion.name(),
            Self::OpenSequenceNumber.name(),
            Self::TotalThingCount.name(),
            Self::TotalEntityCount.name(),
            Self::TotalRelationCount.name(),
            Self::TotalAttributeCount.name(),
            Self::TotalRoleCount.name(),
            Self::TotalHasCount.name(),
            Self::EntityCounts.name(),
            Self::RelationCounts.name(),
            Self::AttributeCounts.name(),
            Self::RoleCounts.name(),
            Self::HasAttributeCounts.name(),
            Self::AttributeOwnerCounts.name(),
            Self::RolePlayerCounts.name(),
            Self::RelationRoleCounts.name(),
            Self::PlayerIndexCounts.name(),
        ];

        const fn name(&self) -> &str {
            match self {
                Field::StatisticsVersion => "StatisticsVersion",
                Field::OpenSequenceNumber => "OpenSequenceNumber",
                Field::TotalThingCount => "TotalThingCount",
                Field::TotalEntityCount => "TotalEntityCount",
                Field::TotalRelationCount => "TotalRelationCount",
                Field::TotalAttributeCount => "TotalAttributeCount",
                Field::TotalRoleCount => "TotalRoleCount",
                Field::TotalHasCount => "TotalHasCount",
                Field::EntityCounts => "EntityCounts",
                Field::RelationCounts => "RelationCounts",
                Field::AttributeCounts => "AttributeCounts",
                Field::RoleCounts => "RoleCounts",
                Field::HasAttributeCounts => "HasAttributeCounts",
                Field::AttributeOwnerCounts => "AttributeOwnerCounts",
                Field::RolePlayerCounts => "RolePlayerCounts",
                Field::RelationRoleCounts => "RelationRoleCounts",
                Field::PlayerIndexCounts => "PlayerIndexCounts",
            }
        }

        fn from(string: &str) -> Option<Self> {
            match string {
                "StatisticsVersion" => Some(Field::StatisticsVersion),
                "OpenSequenceNumber" => Some(Field::OpenSequenceNumber),
                "TotalThingCount" => Some(Field::TotalThingCount),
                "TotalEntityCount" => Some(Field::TotalEntityCount),
                "TotalRelationCount" => Some(Field::TotalRelationCount),
                "TotalAttributeCount" => Some(Field::TotalAttributeCount),
                "TotalRoleCount" => Some(Field::TotalRoleCount),
                "TotalHasCount" => Some(Field::TotalHasCount),
                "EntityCounts" => Some(Field::EntityCounts),
                "RelationCounts" => Some(Field::RelationCounts),
                "AttributeCounts" => Some(Field::AttributeCounts),
                "RoleCounts" => Some(Field::RoleCounts),
                "HasAttributeCounts" => Some(Field::HasAttributeCounts),
                "AttributeOwnerCounts" => Some(Field::AttributeOwnerCounts),
                "RolePlayerCounts" => Some(Field::RolePlayerCounts),
                "RelationRoleCounts" => Some(Field::RelationRoleCounts),
                "PlayerIndexCounts" => Some(Field::PlayerIndexCounts),
                _ => None,
            }
        }
    }

    impl Serialize for Statistics {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut state = serializer.serialize_struct("Statistics", Field::NAMES.len())?;

            state.serialize_field(Field::OpenSequenceNumber.name(), &self.sequence_number)?;

            state.serialize_field(Field::TotalThingCount.name(), &self.total_thing_count)?;
            state.serialize_field(Field::TotalEntityCount.name(), &self.total_entity_count)?;
            state.serialize_field(Field::TotalRelationCount.name(), &self.total_relation_count)?;
            state.serialize_field(Field::TotalAttributeCount.name(), &self.total_attribute_count)?;
            state.serialize_field(Field::TotalRoleCount.name(), &self.total_role_count)?;
            state.serialize_field(Field::TotalHasCount.name(), &self.total_has_count)?;

            state.serialize_field(Field::EntityCounts.name(), &to_serialisable_map(&self.entity_counts))?;
            state.serialize_field(Field::RelationCounts.name(), &to_serialisable_map(&self.relation_counts))?;
            state.serialize_field(Field::AttributeCounts.name(), &to_serialisable_map(&self.attribute_counts))?;
            state.serialize_field(Field::RoleCounts.name(), &to_serialisable_map(&self.role_counts))?;

            let has_attribute_counts: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                to_serialisable_map_map(&self.has_attribute_counts);
            state.serialize_field(Field::HasAttributeCounts.name(), &has_attribute_counts)?;

            let attribute_owner_counts: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                to_serialisable_map_map(&self.attribute_owner_counts);
            state.serialize_field(Field::AttributeOwnerCounts.name(), &attribute_owner_counts)?;

            let role_player_counts: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                to_serialisable_map_map(&self.role_player_counts);
            state.serialize_field(Field::RolePlayerCounts.name(), &role_player_counts)?;

            let relation_role_counts: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                to_serialisable_map_map(&self.relation_role_counts);
            state.serialize_field(Field::RelationRoleCounts.name(), &relation_role_counts)?;

            let player_index_counts: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                to_serialisable_map_map(&self.player_index_counts);
            state.serialize_field(Field::PlayerIndexCounts.name(), &player_index_counts)?;

            state.end()
        }
    }

    fn to_serialisable_map_map<Type1: Into<SerialisableType> + Clone, Type2: Into<SerialisableType> + Clone>(
        map: &HashMap<Type1, HashMap<Type2, u64>>,
    ) -> HashMap<SerialisableType, HashMap<SerialisableType, u64>> {
        map.iter().map(|(type_, value)| (type_.clone().into(), to_serialisable_map(value))).collect()
    }

    fn to_serialisable_map<Type_: Into<SerialisableType> + Clone>(
        map: &HashMap<Type_, u64>,
    ) -> HashMap<SerialisableType, u64> {
        map.iter().map(|(type_, value)| (type_.clone().into(), *value)).collect()
    }

    fn into_entity_map(map: HashMap<SerialisableType, u64>) -> HashMap<EntityType<'static>, u64> {
        map.into_iter().map(|(type_, value)| (type_.into_entity_type(), value)).collect()
    }

    fn into_relation_map(map: HashMap<SerialisableType, u64>) -> HashMap<RelationType<'static>, u64> {
        map.into_iter().map(|(type_, value)| (type_.into_relation_type(), value)).collect()
    }

    fn into_attribute_map(map: HashMap<SerialisableType, u64>) -> HashMap<AttributeType<'static>, u64> {
        map.into_iter().map(|(type_, value)| (type_.into_attribute_type(), value)).collect()
    }

    fn into_role_map(map: HashMap<SerialisableType, u64>) -> HashMap<RoleType<'static>, u64> {
        map.into_iter().map(|(type_, value)| (type_.into_role_type(), value)).collect()
    }

    fn into_object_map(map: HashMap<SerialisableType, u64>) -> HashMap<ObjectType<'static>, u64> {
        map.into_iter().map(|(type_, value)| (type_.into_object_type(), value)).collect()
    }

    impl<'de> Deserialize<'de> for Statistics {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            impl<'de> Deserialize<'de> for Field {
                fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
                where
                    D: Deserializer<'de>,
                {
                    struct FieldVisitor;

                    impl<'de> Visitor<'de> for FieldVisitor {
                        type Value = Field;

                        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                            formatter.write_str("Unrecognised field")
                        }

                        fn visit_str<E>(self, value: &str) -> Result<Field, E>
                        where
                            E: de::Error,
                        {
                            Field::from(value).ok_or_else(|| de::Error::unknown_field(value, &Field::NAMES))
                        }
                    }

                    deserializer.deserialize_identifier(FieldVisitor)
                }
            }

            struct StatisticsVisitor;

            impl<'de> Visitor<'de> for StatisticsVisitor {
                type Value = Statistics;

                fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str("struct StatisticsVisitor")
                }

                fn visit_seq<V>(self, mut seq: V) -> Result<Statistics, V::Error>
                where
                    V: SeqAccess<'de>,
                {
                    let statistics_version = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(0, &self))?;
                    let open_sequence_number =
                        seq.next_element()?.ok_or_else(|| de::Error::invalid_length(1, &self))?;
                    let total_thing_count = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(2, &self))?;
                    let total_entity_count = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(3, &self))?;
                    let total_relation_count =
                        seq.next_element()?.ok_or_else(|| de::Error::invalid_length(4, &self))?;
                    let total_attribute_count =
                        seq.next_element()?.ok_or_else(|| de::Error::invalid_length(5, &self))?;
                    let total_role_count = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(6, &self))?;
                    let total_has_count = seq.next_element()?.ok_or_else(|| de::Error::invalid_length(7, &self))?;
                    let encoded_entity_counts =
                        seq.next_element()?.ok_or_else(|| de::Error::invalid_length(8, &self))?;
                    let entity_counts = into_entity_map(encoded_entity_counts);
                    let encoded_relation_counts =
                        seq.next_element()?.ok_or_else(|| de::Error::invalid_length(9, &self))?;
                    let relation_counts = into_relation_map(encoded_relation_counts);
                    let encoded_attribute_counts =
                        seq.next_element()?.ok_or_else(|| de::Error::invalid_length(10, &self))?;
                    let attribute_counts = into_attribute_map(encoded_attribute_counts);
                    let encoded_role_counts =
                        seq.next_element()?.ok_or_else(|| de::Error::invalid_length(11, &self))?;
                    let role_counts = into_role_map(encoded_role_counts);
                    let encoded_has_attribute_counts: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                        seq.next_element()?.ok_or_else(|| de::Error::invalid_length(12, &self))?;
                    let has_attribute_counts = encoded_has_attribute_counts
                        .into_iter()
                        .map(|(type_1, map)| (type_1.into_object_type(), into_attribute_map(map)))
                        .collect();
                    let encoded_attribute_owner_counts: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                        seq.next_element()?.ok_or_else(|| de::Error::invalid_length(13, &self))?;
                    let attribute_owner_counts = encoded_attribute_owner_counts
                        .into_iter()
                        .map(|(type_1, map)| (type_1.into_attribute_type(), into_object_map(map)))
                        .collect();
                    let encoded_role_player_counts: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                        seq.next_element()?.ok_or_else(|| de::Error::invalid_length(14, &self))?;
                    let role_player_counts = encoded_role_player_counts
                        .into_iter()
                        .map(|(type_1, map)| (type_1.into_object_type(), into_role_map(map)))
                        .collect();
                    let encoded_relation_role_counts: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                        seq.next_element()?.ok_or_else(|| de::Error::invalid_length(15, &self))?;
                    let relation_role_counts = encoded_relation_role_counts
                        .into_iter()
                        .map(|(type_1, map)| (type_1.into_relation_type(), into_role_map(map)))
                        .collect();
                    let encoded_player_index_counts: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                        seq.next_element()?.ok_or_else(|| de::Error::invalid_length(16, &self))?;
                    let player_index_counts = encoded_player_index_counts
                        .into_iter()
                        .map(|(type_1, map)| (type_1.into_object_type(), into_object_map(map)))
                        .collect();
                    Ok(Statistics {
                        statistics_version: statistics_version,
                        sequence_number: open_sequence_number,
                        total_thing_count,
                        total_entity_count,
                        total_relation_count,
                        total_attribute_count,
                        total_role_count,
                        total_has_count,
                        entity_counts,
                        relation_counts,
                        attribute_counts,
                        role_counts,
                        has_attribute_counts,
                        attribute_owner_counts,
                        role_player_counts,
                        relation_role_counts,
                        player_index_counts,
                    })
                }

                fn visit_map<V>(self, mut map: V) -> Result<Statistics, V::Error>
                where
                    V: MapAccess<'de>,
                {
                    let mut statistics_version = None;
                    let mut open_sequence_number = None;
                    let mut total_thing_count = None;
                    let mut total_entity_count = None;
                    let mut total_relation_count = None;
                    let mut total_attribute_count = None;
                    let mut total_role_count = None;
                    let mut total_has_count = None;
                    let mut entity_counts = None;
                    let mut relation_counts = None;
                    let mut attribute_counts = None;
                    let mut role_counts = None;
                    let mut has_attribute_counts = None;
                    let mut attribute_owner_counts = None;
                    let mut role_player_counts = None;
                    let mut relation_role_counts = None;
                    let mut player_index_counts = None;
                    while let Some(key) = map.next_key()? {
                        match key {
                            Field::StatisticsVersion => {
                                if statistics_version.is_some() {
                                    return Err(de::Error::duplicate_field(Field::StatisticsVersion.name()));
                                }
                                statistics_version = Some(map.next_value()?);
                            }
                            Field::OpenSequenceNumber => {
                                if open_sequence_number.is_some() {
                                    return Err(de::Error::duplicate_field(Field::OpenSequenceNumber.name()));
                                }
                                open_sequence_number = Some(map.next_value()?);
                            }
                            Field::TotalThingCount => {
                                if total_thing_count.is_some() {
                                    return Err(de::Error::duplicate_field(Field::TotalThingCount.name()));
                                }
                                total_thing_count = Some(map.next_value()?);
                            }
                            Field::TotalEntityCount => {
                                if total_entity_count.is_some() {
                                    return Err(de::Error::duplicate_field(Field::TotalEntityCount.name()));
                                }
                                total_entity_count = Some(map.next_value()?);
                            }
                            Field::TotalRelationCount => {
                                if total_relation_count.is_some() {
                                    return Err(de::Error::duplicate_field(Field::TotalRelationCount.name()));
                                }
                                total_relation_count = Some(map.next_value()?);
                            }
                            Field::TotalAttributeCount => {
                                if total_attribute_count.is_some() {
                                    return Err(de::Error::duplicate_field(Field::TotalAttributeCount.name()));
                                }
                                total_attribute_count = Some(map.next_value()?);
                            }
                            Field::TotalRoleCount => {
                                if total_role_count.is_some() {
                                    return Err(de::Error::duplicate_field(Field::TotalRoleCount.name()));
                                }
                                total_role_count = Some(map.next_value()?);
                            }
                            Field::TotalHasCount => {
                                if total_has_count.is_some() {
                                    return Err(de::Error::duplicate_field(Field::TotalRoleCount.name()));
                                }
                                total_has_count = Some(map.next_value()?);
                            }
                            Field::EntityCounts => {
                                if entity_counts.is_some() {
                                    return Err(de::Error::duplicate_field(Field::TotalRoleCount.name()));
                                }
                                entity_counts = Some(into_entity_map(map.next_value()?));
                            }
                            Field::RelationCounts => {
                                if relation_counts.is_some() {
                                    return Err(de::Error::duplicate_field(Field::RelationCounts.name()));
                                }
                                relation_counts = Some(into_relation_map(map.next_value()?));
                            }
                            Field::AttributeCounts => {
                                if attribute_counts.is_some() {
                                    return Err(de::Error::duplicate_field(Field::AttributeCounts.name()));
                                }
                                attribute_counts = Some(into_attribute_map(map.next_value()?));
                            }
                            Field::RoleCounts => {
                                if role_counts.is_some() {
                                    return Err(de::Error::duplicate_field(Field::RoleCounts.name()));
                                }
                                role_counts = Some(into_role_map(map.next_value()?));
                            }
                            Field::HasAttributeCounts => {
                                if has_attribute_counts.is_some() {
                                    return Err(de::Error::duplicate_field(Field::HasAttributeCounts.name()));
                                }
                                let encoded: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                                    map.next_value()?;
                                has_attribute_counts = Some(
                                    encoded
                                        .into_iter()
                                        .map(|(type_1, map)| (type_1.into_object_type(), into_attribute_map(map)))
                                        .collect(),
                                );
                            }
                            Field::AttributeOwnerCounts => {
                                if attribute_owner_counts.is_some() {
                                    return Err(de::Error::duplicate_field(Field::AttributeOwnerCounts.name()));
                                }
                                let encoded: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                                    map.next_value()?;
                                attribute_owner_counts = Some(
                                    encoded
                                        .into_iter()
                                        .map(|(type_1, map)| (type_1.into_attribute_type(), into_object_map(map)))
                                        .collect(),
                                );
                            }
                            Field::RolePlayerCounts => {
                                if role_player_counts.is_some() {
                                    return Err(de::Error::duplicate_field(Field::RolePlayerCounts.name()));
                                }
                                let encoded: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                                    map.next_value()?;
                                role_player_counts = Some(
                                    encoded
                                        .into_iter()
                                        .map(|(type_1, map)| (type_1.into_object_type(), into_role_map(map)))
                                        .collect(),
                                );
                            }
                            Field::RelationRoleCounts => {
                                if relation_role_counts.is_some() {
                                    return Err(de::Error::duplicate_field(Field::RelationRoleCounts.name()));
                                }
                                let encoded: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                                    map.next_value()?;
                                relation_role_counts = Some(
                                    encoded
                                        .into_iter()
                                        .map(|(type_1, map)| (type_1.into_relation_type(), into_role_map(map)))
                                        .collect(),
                                );
                            }
                            Field::PlayerIndexCounts => {
                                if player_index_counts.is_some() {
                                    return Err(de::Error::duplicate_field(Field::PlayerIndexCounts.name()));
                                }
                                let encoded: HashMap<SerialisableType, HashMap<SerialisableType, u64>> =
                                    map.next_value()?;
                                player_index_counts = Some(
                                    encoded
                                        .into_iter()
                                        .map(|(type_1, map)| (type_1.into_object_type(), into_object_map(map)))
                                        .collect(),
                                );
                            }
                        }
                    }

                    Ok(Statistics {
                        statistics_version: statistics_version
                            .ok_or_else(|| de::Error::missing_field(Field::StatisticsVersion.name()))?,
                        sequence_number: open_sequence_number
                            .ok_or_else(|| de::Error::missing_field(Field::OpenSequenceNumber.name()))?,
                        total_thing_count: total_thing_count
                            .ok_or_else(|| de::Error::missing_field(Field::TotalThingCount.name()))?,
                        total_entity_count: total_entity_count
                            .ok_or_else(|| de::Error::missing_field(Field::TotalEntityCount.name()))?,
                        total_relation_count: total_relation_count
                            .ok_or_else(|| de::Error::missing_field(Field::TotalRelationCount.name()))?,
                        total_attribute_count: total_attribute_count
                            .ok_or_else(|| de::Error::missing_field(Field::TotalAttributeCount.name()))?,
                        total_role_count: total_role_count
                            .ok_or_else(|| de::Error::missing_field(Field::TotalRoleCount.name()))?,
                        total_has_count: total_has_count
                            .ok_or_else(|| de::Error::missing_field(Field::TotalHasCount.name()))?,
                        entity_counts: entity_counts
                            .ok_or_else(|| de::Error::missing_field(Field::EntityCounts.name()))?,
                        relation_counts: relation_counts
                            .ok_or_else(|| de::Error::missing_field(Field::RelationCounts.name()))?,
                        attribute_counts: attribute_counts
                            .ok_or_else(|| de::Error::missing_field(Field::AttributeCounts.name()))?,
                        role_counts: role_counts.ok_or_else(|| de::Error::missing_field(Field::RoleCounts.name()))?,
                        has_attribute_counts: has_attribute_counts
                            .ok_or_else(|| de::Error::missing_field(Field::HasAttributeCounts.name()))?,
                        attribute_owner_counts: attribute_owner_counts
                            .ok_or_else(|| de::Error::missing_field(Field::AttributeOwnerCounts.name()))?,
                        role_player_counts: role_player_counts
                            .ok_or_else(|| de::Error::missing_field(Field::RolePlayerCounts.name()))?,
                        relation_role_counts: relation_role_counts
                            .ok_or_else(|| de::Error::missing_field(Field::RelationRoleCounts.name()))?,
                        player_index_counts: player_index_counts
                            .ok_or_else(|| de::Error::missing_field(Field::PlayerIndexCounts.name()))?,
                    })
                }
            }

            deserializer.deserialize_struct("Statistics", &Field::NAMES, StatisticsVisitor)
        }
    }
}