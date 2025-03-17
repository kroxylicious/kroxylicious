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
            case HEADER, REQUEST, RESPONSE ->
                    // We append the Data suffix to request/response/header classes to avoid
                    // collisions with existing objects. This can go away once the protocols
                    // have all been converted and we begin using the generated types directly.
                    struct.name() + "Data";
            default -> struct.name();
        };
    }
}
