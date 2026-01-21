/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.krpccodegen.schema;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class MessageSpec {
    private final StructSpec struct;

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private final Optional<Short> apiKey;

    private final MessageSpecType type;

    private final List<StructSpec> commonStructs;

    private final Versions flexibleVersions;

    private final List<RequestListenerType> listeners;

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private final Optional<Boolean> latestVersionUnstable;

    @JsonCreator
    public MessageSpec(@JsonProperty("name") String name,
                       @JsonProperty("validVersions") String validVersions,
                       @JsonProperty("deprecatedVersions") String deprecatedVersions,
                       @JsonProperty("fields") List<FieldSpec> fields,
                       @JsonProperty("apiKey") Short apiKey,
                       @JsonProperty("latestVersionUnstable") Boolean latestVersionUnstable,
                       @JsonProperty("type") MessageSpecType type,
                       @JsonProperty("commonStructs") List<StructSpec> commonStructs,
                       @JsonProperty("flexibleVersions") String flexibleVersions,
                       @JsonProperty("listeners") List<RequestListenerType> listeners) {
        this.struct = new StructSpec(name, validVersions, deprecatedVersions, fields);
        this.apiKey = apiKey == null ? Optional.empty() : Optional.of(apiKey);
        this.latestVersionUnstable = Optional.ofNullable(latestVersionUnstable);
        this.type = Objects.requireNonNull(type);
        this.commonStructs = commonStructs == null ? Collections.emptyList() : List.copyOf(commonStructs);

        // If the struct has no valid versions (the typical use case is to completely remove support for
        // an existing protocol api while ensuring the api key id is not reused), we configure the spec
        // to effectively be empty
        if (struct.versions().empty()) {
            this.flexibleVersions = Versions.NONE;
            this.listeners = Collections.emptyList();
        }
        else {
            if (flexibleVersions == null) {
                throw new RuntimeException("You must specify a value for flexibleVersions. " +
                        "Please use 0+ for all new messages.");
            }
            this.flexibleVersions = Versions.parse(flexibleVersions, Versions.NONE);
            if ((!this.flexibleVersions().empty()) &&
                    (this.flexibleVersions.highest() < Short.MAX_VALUE)) {
                throw new RuntimeException("Field " + name + " specifies flexibleVersions " +
                        this.flexibleVersions + ", which is not open-ended.  flexibleVersions must " +
                        "be either none, or an open-ended range (that ends with a plus sign).");
            }

            if (listeners != null && !listeners.isEmpty() && type != MessageSpecType.REQUEST) {
                throw new RuntimeException("The `requestScope` property is only valid for " +
                        "messages with type `request`");
            }
            this.listeners = listeners;

            if (Boolean.TRUE.equals(latestVersionUnstable) && type != MessageSpecType.REQUEST) {
                throw new RuntimeException("The `latestVersionUnstable` property is only valid for " +
                        "messages with type `request`");
            }
        }
    }

    public StructSpec struct() {
        return struct;
    }

    @JsonProperty("name")
    public String name() {
        return struct.name();
    }

    public Versions validVersions() {
        return struct.versions();
    }

    @JsonProperty("validVersions")
    public String validVersionsString() {
        return struct.versionsString();
    }

    @JsonProperty("fields")
    public List<FieldSpec> fields() {
        return struct.fields();
    }

    @JsonProperty("apiKey")
    public Optional<Short> apiKey() {
        return apiKey;
    }

    @JsonProperty("latestVersionUnstable")
    public Optional<Boolean> latestVersionUnstable() {
        return latestVersionUnstable;
    }

    @JsonProperty("type")
    public MessageSpecType type() {
        return type;
    }

    @JsonProperty("commonStructs")
    public List<StructSpec> commonStructs() {
        return commonStructs;
    }

    public Versions flexibleVersions() {
        return flexibleVersions;
    }

    @JsonProperty("flexibleVersions")
    public String flexibleVersionsString() {
        return flexibleVersions.toString();
    }

    @JsonProperty("listeners")
    public List<RequestListenerType> listeners() {
        return listeners;
    }

    public String dataClassName() {
        return switch (type) {
            case HEADER, REQUEST, RESPONSE -> struct.name() + "Data";
            default -> struct.name();
        };
    }

    /**
     * Returns true if this message spec has at least one field of one of the given entity field types.
     *
     * @param entityFieldTypeNames entity field types
     * @return true if present, false otherwise
     */
    public boolean hasAtLeastOneEntityField(Set<EntityType> entityFieldTypeNames) {
        return hasAtLeastOneEntityField(fields(), entityFieldTypeNames);
    }

    private boolean hasAtLeastOneEntityField(List<FieldSpec> fields, Set<EntityType> entityFieldTypeNames) {
        var found = fields.stream().anyMatch(f -> entityFieldTypeNames.contains(f.entityType()));
        if (found) {
            return true;
        }
        return fields.stream().anyMatch(f -> hasAtLeastOneEntityField(f.fields(), entityFieldTypeNames));
    }

    /**
     * Returns the intersected versions used by fields of this messages that are of the
     * given entity types.
     *
     * @param entityFieldTypeNames entity field types
     * @return intersected versions
     */
    public List<Short> intersectedVersionsForEntityFields(Set<EntityType> entityFieldTypeNames) {
        return intersectedVersionsForEntityFields(fields(), entityFieldTypeNames).stream().toList();
    }

    private Set<Short> intersectedVersionsForEntityFields(List<FieldSpec> fields, Set<EntityType> entityFields) {
        var versions = fields.stream()
                .filter(f -> entityFields.contains(f.entityType()))
                .map(f -> validVersions().intersect(f.versions()))
                .flatMapToInt(v -> IntStream.rangeClosed(v.lowest(), v.highest()))
                .distinct()
                .sorted()
                .boxed()
                .map(Integer::shortValue)
                .collect(Collectors.toCollection(TreeSet::new));

        versions.addAll(fields.stream()
                .flatMap(f -> intersectedVersionsForEntityFields(f.fields(), entityFields).stream())
                .collect(Collectors.toSet()));

        return versions;
    }

    @Override
    public String toString() {
        return "MessageSpec{" +
                "struct=" + struct +
                ", apiKey=" + apiKey +
                ", type=" + type +
                ", commonStructs=" + commonStructs +
                ", flexibleVersions=" + flexibleVersions +
                ", listeners=" + listeners +
                ", latestVersionUnstable=" + latestVersionUnstable +
                '}';
    }
}
