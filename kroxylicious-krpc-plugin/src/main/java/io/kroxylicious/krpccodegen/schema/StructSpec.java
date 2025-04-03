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
        this.versions = Versions.parse(versions, null);
        if (this.versions == null) {
            throw new RuntimeException("You must specify the version of the " +
                    name + " structure.");
        }
        this.deprecatedVersions = Versions.parse(deprecatedVersions, Versions.NONE);
        ArrayList<FieldSpec> newFields = new ArrayList<>();
        if (fields != null) {
            // Each field should have a unique tag ID (if the field has a tag ID).
            HashSet<Integer> tags = new HashSet<>();
            // Each field should have a unique name.
            HashSet<String> names = new HashSet<>();
            for (FieldSpec field : fields) {
                field.tag().ifPresent(tag -> {
                    if (!tags.add(tag)) {
                        throw new RuntimeException("In " + name + ", field " + field.name() +
                                " has a duplicate tag ID " + tag + ". All tags IDs " +
                                "must be unique.");
                    }
                });
                if (!names.add(field.name())) {
                    throw new RuntimeException("In " + name + ", field " + field.name() +
                            " has a duplicate name " + field.name() + ". All field names " +
                            "must be unique.");
                }
                newFields.add(field);
            }
            // Tag IDs should be contiguous and start at 0. This optimizes space on the wire,
            // since larger numbers take more space.
            for (int i = 0; i < tags.size(); i++) {
                if (!tags.contains(i)) {
                    throw new RuntimeException("In " + name + ", the tag IDs are not " +
                            "contiguous.  Make use of tag " + i + " before using any " +
                            "higher tag IDs.");
                }
            }
        }
        this.fields = Collections.unmodifiableList(newFields);
        this.hasKeys = this.fields.stream().anyMatch(f -> f.mapKey());
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
}
