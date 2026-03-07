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
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.krpccodegen.schema.FieldType.Int8FieldType;
import io.kroxylicious.krpccodegen.schema.FieldType.StringFieldType;

public final class MessageSpec implements Named {
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
     * Returns the intersected versions for the fields matching the predicate.
     *
     * @param fieldSpecPredicate field spec predicate
     * @return intersected versions
     */
    public List<Short> intersectedVersions(Predicate<FieldSpec> fieldSpecPredicate) {
        return intersectedVersions(fields(), fieldSpecPredicate).stream().toList();
    }

    private Set<Short> intersectedVersions(List<FieldSpec> fields, Predicate<FieldSpec> fieldSpecPredicate) {
        var versions = fields.stream()
                .filter(fieldSpecPredicate)
                .map(f -> validVersions().intersect(f.versions()))
                .flatMapToInt(v -> IntStream.rangeClosed(v.lowest(), v.highest()))
                .distinct()
                .sorted()
                .boxed()
                .map(Integer::shortValue)
                .collect(Collectors.toCollection(TreeSet::new));

        versions.addAll(fields.stream()
                .flatMap(f -> intersectedVersions(f.fields(), fieldSpecPredicate).stream())
                .collect(Collectors.toSet()));

        return versions;
    }

    /**
     * Returns true if this message spec carries a resource list.
     * This is true if there is an array container containing the following children.
     * <ol>
     *     <li>ResourceType (type int8)</li>
     *     <li>ResourceName (type string)</li>
     * </ol>
     *
     * @return true if present, false otherwise
     */
    public boolean hasResourceList() {
        return fields().stream().anyMatch(isResourceList());
    }

    public static Predicate<FieldSpec> isResourceList() {
        return f -> f.type().isStructArray() &&
                hasMatchingChild("ResourceType", Int8FieldType.class, f) &&
                hasMatchingChild("ResourceName", StringFieldType.class, f) &&
                f.fields().stream().anyMatch(sf -> sf.name().equals("ResourceName") && sf.type() instanceof StringFieldType);
    }

    private static boolean hasMatchingChild(String fieldName, Class<? extends FieldType> fieldType, FieldSpec parent) {
        return parent.fields().stream().anyMatch(sf -> !sf.versions().intersect(parent.versions()).empty() &&
                sf.name().equals(fieldName) &&
                fieldType.isAssignableFrom(sf.type().getClass()));
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
