/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package hypergraph.common.exception;

import java.util.HashMap;
import java.util.Map;

public abstract class Error<TYPE extends Error<TYPE>> {

    private static Map<String, Map<Integer, Error>> errors = new HashMap<>();
    private static int maxCodeNumber = 0;
    private static int maxCodeDigits = 0;

    private final String codePrefix;
    private final int codeNumber;
    private String code = null;
    private String description;

    Error(String codePrefix, int codeNumber, String descriptionPrefix, String descriptionBody) {
        this.codePrefix = codePrefix;
        this.codeNumber = codeNumber;
        this.description = descriptionPrefix + ": " + descriptionBody;

        assert errors.get(codePrefix) == null || errors.get(codePrefix).get(codeNumber) == null;
        errors.computeIfAbsent(codePrefix, s -> new HashMap<>()).put(codeNumber, this);
        maxCodeNumber = Math.max(codeNumber, maxCodeNumber);
        maxCodeDigits = (int) Math.ceil(Math.log10(maxCodeNumber));
    }

    protected abstract TYPE getThis();

    public String code() {
        if (code != null) return code;

        StringBuilder zeros = new StringBuilder();
        for (int digits = (int) Math.ceil(Math.log10(codeNumber)); digits < maxCodeDigits; digits++) {
            zeros.append("0");
        }

        code = codePrefix + zeros.toString() + codeNumber;
        return code;
    }

    public String description() {
        return description;
    }

    public TYPE format(Object... parameters) {
        description = String.format(description, parameters);
        return getThis();
    }

    @Override
    public String toString() {
        return String.format("[%s] %s", code(), description);
    }

    public static class Internal extends Error<Internal> {

        public static final Internal ILLEGAL_STATE =
                new Internal(0, "Illegal internal state!");
        public static final Internal UNRECOGNISED_VALUE =
                new Internal(1, "Unrecognised schema value!");

        private static final String codePrefix = "INT";
        private static final String descriptionPrefix = "Invalid Internal State";

        Internal(int number, String description) {
            super(codePrefix, number, descriptionPrefix, description);
        }

        protected Internal getThis() {
            return this;
        }

    }

    public static class Transaction extends Error<Error.Transaction> {

        public static final Transaction UNSUPPORTED_OPERATION =
                new Transaction(1, "Unsupported operation: calling '%s' for '%s' is not supported.");
        public static final Transaction ILLEGAL_OPERATION =
                new Transaction(2, "Attempted an illegal operation!");
        public static final Transaction CLOSED_TRANSACTION =
                new Transaction(3, "The transaction has been closed and cannot perform any operation.");
        public static final Transaction ILLEGAL_COMMIT =
                new Transaction(4, "Only write transactions can be committed.");
        public static final Transaction DIRTY_SCHEMA_WRITES =
                new Transaction(5, "Attempted schema writes when session type does not allow.");
        public static final Transaction DIRTY_DATA_WRITES =
                new Transaction(6, "Attempted data writes when session type does not allow.");

        private static final String codePrefix = "TXN";
        private static final String descriptionPrefix = "Invalid Transaction Operation";

        Transaction(int number, String description) {
            super(codePrefix, number, descriptionPrefix, description);
        }

        protected Error.Transaction getThis() {
            return this;
        }
    }

    public static class ConceptRead extends Error {

        public static final ConceptRead INVALID_CONCEPT_CASTING =
                new ConceptRead(1, "Invalid concept conversion to '%s'.");
        private static final String codePrefix = "CON";
        private static final String descriptionPrefix = "Invalid Concept Retrieval";

        ConceptRead(int number, String description) {
            super(codePrefix, number, descriptionPrefix, description);
        }

        protected ConceptRead getThis() {
            return this;
        }
    }

    public static class ThingRead extends Error {

        public static final ThingRead INVALID_IID_CASTING =
                new ThingRead(1, "'Invalid IID casting to '%s'.");
        public static final ThingRead INVALID_VERTEX_CASTING =
                new ThingRead(2, "Invalid ThingVertex casting to '%s'.");

        private static final String codePrefix = "THR";
        private static final String descriptionPrefix = "Invalid Thing Read";

        ThingRead(int number, String description) {
            super(codePrefix, number, descriptionPrefix, description);
        }

        protected ThingRead getThis() {
            return this;
        }
    }

    public static class ThingWrite extends Error {

        public static final ThingWrite ILLEGAL_ABSTRACT_WRITE =
                new ThingWrite(1, "Attempted an illegal write of a new '%s' of abstract type '%s'.");
        public static final ThingWrite ILLEGAL_STRING_SIZE =
                new ThingWrite(2, "Attempted to insert a string larger than the maximum size.");
        public static final ThingWrite CONCURRENT_ATTRIBUTE_WRITE_DELETE =
                new ThingWrite(3, "Attempted concurrent modification of attributes (writes and deletes).");
        public static final ThingWrite THING_ATTRIBUTE_UNDEFINED =
                new ThingWrite(4, "Attempted to assign an attribute to a(n) '%s' that is not defined to have that attribute type.");
        public static final ThingWrite THING_KEY_OVER =
                new ThingWrite(5, "Attempted to assign a key of type '%s' onto a(n) '%s' that already has one.");
        public static final ThingWrite THING_KEY_TAKEN =
                new ThingWrite(6, "Attempted to assign a key of type '%s' that had been taken by another '%s'.");
        public static final ThingWrite THING_KEY_MISSING =
                new ThingWrite(7, "Attempted to commit a(n) '%s' that is missing key(s) of type(s): %s"); // don't put quotes around the last %s
        public static final ThingWrite RELATION_UNRELATED_ROLE =
                new ThingWrite(8, "Relation type '%s' does not relate role type '%s'.");
        public static final ThingWrite RELATION_NO_PLAYER =
                new ThingWrite(9, "Relation instance of type '%s' does not have any role player");
        private static final String codePrefix = "THW";
        private static final String descriptionPrefix = "Invalid Thing Write";

        ThingWrite(int number, String description) {
            super(codePrefix, number, descriptionPrefix, description);
        }

        protected ThingWrite getThis() {
            return this;
        }
    }

    public static class TypeRead extends Error {

        public static final TypeRead TYPE_ROOT_MISMATCH =
                new TypeRead(1, "Attempted to retrieve '%s' as '%s', while it is actually a '%s'.");
        public static final TypeRead VALUE_TYPE_MISMATCH =
                new TypeRead(2, "Attempted to retrieve '%s' as AttributeType of ValueType '%s', while it actually has ValueType '%s'.");
        private static final String codePrefix = "TYR";
        private static final String descriptionPrefix = "Invalid Type Read";

        TypeRead(int number, String description) {
            super(codePrefix, number, descriptionPrefix, description);
        }

        protected TypeRead getThis() {
            return this;
        }
    }

    public static class TypeWrite extends Error {

        public static final TypeWrite ROOT_TYPE_MUTATION =
                new TypeWrite(1, "Root types are immutable.");
        public static final TypeWrite TYPE_HAS_SUBTYPES =
                new TypeWrite(2, "The type '%s' has subtypes, and cannot be deleted");
        public static final TypeWrite TYPE_HAS_INSTANCES =
                new TypeWrite(3, "The type '%s' has instances, and cannot be deleted");
        public static final TypeWrite SUPERTYPE_SELF =
                new TypeWrite(4, "The type '%s' cannot be a subtype of itself.");
        public static final TypeWrite SUPERTYPE_NON_ABSTRACT =
                new TypeWrite(5, "The type '%s' is abstract, but has a supertype '%s' which is not abstract.");
        public static final TypeWrite OVERRIDE_NOT_SUPERTYPE =
                new TypeWrite(6, "The type '%s' cannot override '%s' as it is not a supertype");
        public static final TypeWrite OVERRIDE_NOT_AVAILABLE =
                new TypeWrite(7, "The Type '%s' cannot override '%s' as it is either already declared, or not inherited");
        public static final TypeWrite ATTRIBUTE_SUPERTYPE_VALUE_TYPE =
                new TypeWrite(8, "The attribute type '%s' has value type '%s', and cannot have supertype '%s' with value type '%s'.");
        public static final TypeWrite HAS_KEY_VALUE_TYPE =
                new TypeWrite(9, "The attribute type '%s' has value type '%s', and cannot and cannot be used as a type key.");
        public static final TypeWrite HAS_KEY_NOT_AVAILABLE =
                new TypeWrite(10, "The attribute type '%s' has been inherited or overridden, and cannot be redeclared as a key");
        public static final TypeWrite HAS_KEY_PRECONDITION_OWNERSHIP =
                new TypeWrite(11, "The instances of type '%s' does not have exactly one attribute of type '%s' to convert to key");
        public static final TypeWrite HAS_ATT_NOT_AVAILABLE =
                new TypeWrite(12, "The attribute type '%s' has been inherited or overridden, and cannot be redeclared as an attribute");
        public static final TypeWrite HAS_KEY_PRECONDITION_UNIQUENESS =
                new TypeWrite(13, "The attributes of type '%s' are not uniquely owned by instances of type '%s' to convert to key");
        public static final TypeWrite PLAYS_ROLE_NOT_AVAILABLE =
                new TypeWrite(14, "The role type '%s' has been inherited or overridden, and cannot be redeclared");
        public static final TypeWrite RELATION_NO_ROLE =
                new TypeWrite(15, "RelationType '%s' does not relate any role type");

        private static final String codePrefix = "TYW";
        private static final String descriptionPrefix = "Invalid Type Definition";

        TypeWrite(int number, String description) {
            super(codePrefix, number, descriptionPrefix, description);
        }

        protected TypeWrite getThis() {
            return this;
        }
    }
}
