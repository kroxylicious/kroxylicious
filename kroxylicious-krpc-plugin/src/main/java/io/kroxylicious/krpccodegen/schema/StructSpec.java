/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.krpccodegen.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class StructSpec {
    private final String name;

    private final Versions versions;

    private final Versions deprecatedVersions;

    private final List<FieldSpec> fields;

    private final boolean hasKeys;

    @JsonCreator
    public StructSpec(@JsonProperty("name") String name,
                      @JsonProperty("versions") String versions,
                      @JsonProperty("deprecatedVersions") String deprecatedVersions,
                      @JsonProperty("fields") List<FieldSpec> fields) {
        this.name = Objects.requireNonNull(name);
        this.versions = parseVersions(versions, name);
        this.deprecatedVersions = Versions.parse(deprecatedVersions, Versions.NONE);
        this.fields = Collections.unmodifiableList(validateFields(fields, name));
        this.hasKeys = this.fields.stream().anyMatch(FieldSpec::mapKey);
    }

    private static Versions parseVersions(String versions, String name) {
        Versions parsed = Versions.parse(versions, null);
        if (parsed == null) {
            throw new RuntimeException("You must specify the version of the " +
                    name + " structure.");
        }
        return parsed;
    }

    private static List<FieldSpec> validateFields(List<FieldSpec> fields, String name) {
        if (fields == null) {
            return new ArrayList<>();
        }
        // Each field should have a unique tag ID (if the field has a tag ID).
        Set<Integer> tags = new HashSet<>();
        // Each field should have a unique name.
        Set<String> names = new HashSet<>();
        List<FieldSpec> newFields = new ArrayList<>();
        for (FieldSpec field : fields) {
            validateTagUniqueness(field, tags, name);
            validateNameUniqueness(field, names, name);
            newFields.add(field);
        }
        // Tag IDs should be contiguous and start at 0. This optimizes space on the wire,
        // since larger numbers take more space.
        validateTagContiguity(tags, name);
        return newFields;
    }

    private static void validateTagUniqueness(FieldSpec field, Set<Integer> tags, String name) {
        field.tag().ifPresent(tag -> {
            if (!tags.add(tag)) {
                throw new RuntimeException("In " + name + ", field " + field.name() +
                        " has a duplicate tag ID " + tag + ". All tags IDs " +
                        "must be unique.");
            }
        });
    }

    private static void validateNameUniqueness(FieldSpec field, Set<String> names, String name) {
        if (!names.add(field.name())) {
            throw new RuntimeException("In " + name + ", field " + field.name() +
                    " has a duplicate name " + field.name() + ". All field names " +
                    "must be unique.");
        }
    }

    private static void validateTagContiguity(Set<Integer> tags, String name) {
        for (int i = 0; i < tags.size(); i++) {
            if (!tags.contains(i)) {
                throw new RuntimeException("In " + name + ", the tag IDs are not " +
                        "contiguous.  Make use of tag " + i + " before using any " +
                        "higher tag IDs.");
            }
        }
    }

    @JsonProperty
    public String name() {
        return name;
    }

    public Versions versions() {
        return versions;
    }

    @JsonProperty
    public String versionsString() {
        return versions.toString();
    }

    public Versions deprecatedVersions() {
        return deprecatedVersions;
    }

    @JsonProperty
    public List<FieldSpec> fields() {
        return fields;
    }

    public boolean hasKeys() {
        return hasKeys;
    }

    @Override
    public String toString() {
        return "StructSpec{" +
                "name='" + name + '\'' +
                ", versions=" + versions +
                ", deprecatedVersions=" + deprecatedVersions +
                ", fields=" + fields +
                ", hasKeys=" + hasKeys +
                '}';
    }
}
