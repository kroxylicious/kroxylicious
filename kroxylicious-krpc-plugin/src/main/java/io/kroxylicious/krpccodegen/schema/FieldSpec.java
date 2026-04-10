/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.krpccodegen.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class FieldSpec {
    private static final Pattern VALID_FIELD_NAMES = Pattern.compile("[A-Za-z]([A-Za-z0-9]*)");

    private final String name;

    private final Versions versions;

    private final List<FieldSpec> fields;

    private final FieldType type;

    private final boolean mapKey;

    private final Versions nullableVersions;

    private final String fieldDefault;

    private final boolean ignorable;

    private final EntityType entityType;

    private final String about;

    private final Versions taggedVersions;

    private final Optional<Versions> flexibleVersions;

    private final Optional<Integer> tag;

    private final boolean zeroCopy;

    @JsonCreator
    public FieldSpec(@JsonProperty("name") String name,
                     @JsonProperty("versions") String versions,
                     @JsonProperty("fields") List<FieldSpec> fields,
                     @JsonProperty("type") String type,
                     @JsonProperty("mapKey") boolean mapKey,
                     @JsonProperty("nullableVersions") String nullableVersions,
                     @JsonProperty("default") String fieldDefault,
                     @JsonProperty("ignorable") boolean ignorable,
                     @JsonProperty("entityType") EntityType entityType,
                     @JsonProperty("about") String about,
                     @JsonProperty("taggedVersions") String taggedVersions,
                     @JsonProperty("flexibleVersions") String flexibleVersions,
                     @JsonProperty("tag") Integer tag,
                     @JsonProperty("zeroCopy") boolean zeroCopy) {
        this.name = requireValidName(name);
        this.taggedVersions = Versions.parse(taggedVersions, Versions.NONE);
        this.versions = requireVersions(versions);
        this.fields = parseFields(fields);
        this.type = FieldType.parse(Objects.requireNonNull(type));
        this.mapKey = mapKey;
        this.nullableVersions = Versions.parse(nullableVersions, Versions.NONE);
        this.fieldDefault = fieldDefault == null ? "" : fieldDefault;
        this.ignorable = ignorable;
        this.entityType = entityType == null ? EntityType.UNKNOWN : entityType;
        this.about = about == null ? "" : about;
        this.flexibleVersions = parseFlexibleVersions(flexibleVersions);
        this.tag = Optional.ofNullable(tag);
        this.zeroCopy = zeroCopy;

        validateNullableVersions();
        this.entityType.verifyTypeMatches(name, this.type);
        validateSubFields();
        validateTagNotMapKey();
        checkTagInvariants();
        validateZeroCopy();
    }

    private static String requireValidName(String name) {
        Objects.requireNonNull(name);
        if (!VALID_FIELD_NAMES.matcher(name).matches()) {
            throw new IllegalArgumentException("Invalid field name " + name);
        }
        return name;
    }

    // If versions is not set, but taggedVersions is, default to taggedVersions.
    private Versions requireVersions(String versions) {
        Versions v = Versions.parse(versions, this.taggedVersions.empty() ? null : this.taggedVersions);
        if (v == null) {
            throw new IllegalArgumentException("You must specify the version of the " + name + " structure.");
        }
        return v;
    }

    private static List<FieldSpec> parseFields(List<FieldSpec> fields) {
        return Collections.unmodifiableList(fields == null ? Collections.emptyList() : new ArrayList<>(fields));
    }

    private Optional<Versions> parseFlexibleVersions(String flexibleVersions) {
        if (flexibleVersions == null || flexibleVersions.isEmpty()) {
            return Optional.empty();
        }
        if (!(this.type.isString() || this.type.isBytes())) {
            // For now, only allow flexibleVersions overrides for the string and bytes
            // types. Overrides are only needed to keep compatibility with some old formats,
            // so there isn't any need to support them for all types.
            throw new IllegalArgumentException("Invalid flexibleVersions override for " + name +
                    ".  Only fields of type string or bytes can specify a flexibleVersions " +
                    "override.");
        }
        return Optional.of(Versions.parse(flexibleVersions, null));
    }

    private void validateNullableVersions() {
        if (!this.nullableVersions.empty() && !this.type.canBeNullable()) {
            throw new RuntimeException("Type " + this.type + " cannot be nullable.");
        }
    }

    private void validateSubFields() {
        if (!this.fields().isEmpty() && !this.type.isArray() && !this.type.isStruct()) {
            throw new IllegalArgumentException("Non-array or Struct field " + name + " cannot have fields");
        }
    }

    private void validateTagNotMapKey() {
        if (this.tag.isPresent() && mapKey) {
            throw new IllegalArgumentException("Tagged fields cannot be used as keys.");
        }
    }

    private void validateZeroCopy() {
        if (this.zeroCopy && !this.type.isBytes()) {
            throw new IllegalArgumentException("Invalid zeroCopy value for " + name +
                    ". Only fields of type bytes can use zeroCopy flag.");
        }
    }

    private void checkTagInvariants() {
        if (this.tag.isPresent()) {
            if (this.tag.get() < 0) {
                throw new IllegalArgumentException("Field " + name + " specifies a tag of " + this.tag.get() +
                        ".  Tags cannot be negative.");
            }
            if (this.taggedVersions.empty()) {
                throw new IllegalArgumentException("Field " + name + " specifies a tag of " + this.tag.get() +
                        ", but has no tagged versions.  If a tag is specified, taggedVersions must " +
                        "be specified as well.");
            }
            Versions nullableTaggedVersions = this.nullableVersions.intersect(this.taggedVersions);
            if (!(nullableTaggedVersions.empty() || nullableTaggedVersions.equals(this.taggedVersions))) {
                throw new IllegalArgumentException("Field " + name + " specifies nullableVersions " +
                        this.nullableVersions + " and taggedVersions " + this.taggedVersions + ".  " +
                        "Either all tagged versions must be nullable, or none must be.");
            }
            if (this.taggedVersions.highest() < Short.MAX_VALUE) {
                throw new IllegalArgumentException("Field " + name + " specifies taggedVersions " +
                        this.taggedVersions + ", which is not open-ended.  taggedVersions must " +
                        "be either none, or an open-ended range (that ends with a plus sign).");
            }
            if (!this.taggedVersions.intersect(this.versions).equals(this.taggedVersions)) {
                throw new IllegalArgumentException("Field " + name + " specifies taggedVersions " +
                        this.taggedVersions + ", and versions " + this.versions + ".  " +
                        "taggedVersions must be a subset of versions.");
            }
        }
        else if (!this.taggedVersions.empty()) {
            throw new IllegalArgumentException("Field " + name + " does not specify a tag, " +
                    "but specifies tagged versions of " + this.taggedVersions + ".  " +
                    "Please specify a tag, or remove the taggedVersions.");
        }
    }

    @JsonProperty("name")
    public String name() {
        return name;
    }

    public Versions versions() {
        return versions;
    }

    @JsonProperty("versions")
    public String versionsString() {
        return versions.toString();
    }

    @JsonProperty("fields")
    public List<FieldSpec> fields() {
        return fields;
    }

    @JsonProperty("type")
    public String typeString() {
        return type.toString();
    }

    public FieldType type() {
        return type;
    }

    @JsonProperty("mapKey")
    public boolean mapKey() {
        return mapKey;
    }

    public Versions nullableVersions() {
        return nullableVersions;
    }

    @JsonProperty("nullableVersions")
    public String nullableVersionsString() {
        return nullableVersions.toString();
    }

    @JsonProperty("default")
    public String defaultString() {
        return fieldDefault;
    }

    @JsonProperty("ignorable")
    public boolean ignorable() {
        return ignorable;
    }

    @JsonProperty("entityType")
    public EntityType entityType() {
        return entityType;
    }

    @JsonProperty("about")
    public String about() {
        return about;
    }

    @JsonProperty("taggedVersions")
    public String taggedVersionsString() {
        return taggedVersions.toString();
    }

    public Versions taggedVersions() {
        return taggedVersions;
    }

    @JsonProperty("flexibleVersions")
    public String flexibleVersionsString() {
        return flexibleVersions.isPresent() ? flexibleVersions.get().toString() : null;
    }

    public Optional<Versions> flexibleVersions() {
        return flexibleVersions;
    }

    @JsonProperty("tag")
    public Integer tagInteger() {
        return tag.orElse(null);
    }

    public Optional<Integer> tag() {
        return tag;
    }

    @JsonProperty("zeroCopy")
    public boolean zeroCopy() {
        return zeroCopy;
    }

    /**
     * Returns true if this field, or any subfields (recursive), is of one of the  given entity field types.
     *
     * @param entityFieldTypeNames entity field types
     * @return true if present, false otherwise
     */
    public boolean hasAtLeastOneEntityField(Set<EntityType> entityFieldTypeNames) {
        var found = entityFieldTypeNames.contains(entityType);
        if (found) {
            return true;
        }
        return fields().stream().anyMatch(f -> f.hasAtLeastOneEntityField(entityFieldTypeNames));
    }

    /**
     * Returns true if this field and its immediate children define a resource list.
     *
     * @return true if this is a resource list.
     */
    public boolean isResourceList() {
        return definesResourceListPredicate().test(this);
    }

    @Override
    public String toString() {
        return "FieldSpec{" +
                "name='" + name + '\'' +
                ", versions=" + versions +
                ", fields=" + fields +
                ", type=" + type +
                ", mapKey=" + mapKey +
                ", nullableVersions=" + nullableVersions +
                ", fieldDefault='" + fieldDefault + '\'' +
                ", ignorable=" + ignorable +
                ", entityType=" + entityType +
                ", about='" + about + '\'' +
                ", taggedVersions=" + taggedVersions +
                ", flexibleVersions=" + flexibleVersions +
                ", tag=" + tag +
                ", zeroCopy=" + zeroCopy +
                '}';
    }

    public static Predicate<FieldSpec> definesResourceListPredicate() {
        return f -> f.type().isStructArray() &&
                hasMatchingChild("ResourceType", FieldType.Int8FieldType.class, f) &&
                hasMatchingChild("ResourceName", FieldType.StringFieldType.class, f);
    }

    private static boolean hasMatchingChild(String fieldName, Class<? extends FieldType> fieldType, FieldSpec parent) {
        return parent.fields().stream().anyMatch(sf -> !sf.versions().intersect(parent.versions()).empty() &&
                sf.name().equals(fieldName) &&
                fieldType.isAssignableFrom(sf.type().getClass()));
    }
}
