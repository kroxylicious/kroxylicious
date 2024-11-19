/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.tools.schema.model;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import edu.umd.cs.findbugs.annotations.DefaultAnnotationForParameters;
import edu.umd.cs.findbugs.annotations.Nullable;

import io.sundr.builder.annotations.Buildable;

@Buildable(editableEnabled = false, validationEnabled = false, generateBuilderPackage = false)
@DefaultAnnotationForParameters(Nullable.class)
public record SchemaObject(
                           @JsonProperty("$schema") String schema,
                           @JsonProperty("id") String id,
                           @JsonProperty("$ref") String ref,
                           @JsonProperty("description") String description,
                           @JsonProperty("type") @JsonDeserialize(using = ListOrSingleSerde.String.class) List<String> type,
                           @JsonProperty("properties") Map<String, SchemaObject> properties,
                           @JsonProperty("required") Set<String> required,
                           @JsonProperty("items") @JsonDeserialize(using = ListOrSingleSerde.SchemaObject.class) List<SchemaObject> items,
                           @JsonProperty("x-kubernetes-list-type") XKubeListType xKubernetesListType,
                           @JsonProperty("x-kubernetes-list-map-keys") List<String> xKubernetesListMapKeys,
                           @JsonProperty("x-kubernetes-map-type") XKubeMapType xKubernetesMapType,
                           @JsonProperty("$javaType") String javaType) {

    @Buildable(editableEnabled = false, validationEnabled = false, generateBuilderPackage = false)
    @JsonCreator
    public SchemaObject(
                        @JsonProperty("$schema") String schema,
                        @JsonProperty("id") String id,
                        @JsonProperty("$ref") String ref,
                        @JsonProperty("description") String description,
                        @JsonProperty("type") @JsonDeserialize(using = ListOrSingleSerde.String.class) List<String> type,
                        @JsonProperty("properties") Map<String, SchemaObject> properties,
                        @JsonProperty("required") Set<String> required,
                        @JsonProperty("items") @JsonDeserialize(using = ListOrSingleSerde.SchemaObject.class) List<SchemaObject> items,
                        @JsonProperty("x-kubernetes-list-type") XKubeListType xKubernetesListType,
                        @JsonProperty("x-kubernetes-list-map-keys") List<String> xKubernetesListMapKeys,
                        @JsonProperty("x-kubernetes-map-type") XKubeMapType xKubernetesMapType,
                        @JsonProperty("$javaType") String javaType) {
        this.schema = schema;
        this.id = id;
        this.ref = ref;
        this.description = description;
        this.type = type;
        this.properties = properties;
        this.required = required;
        this.items = items;
        this.xKubernetesListType = xKubernetesListType;
        this.xKubernetesMapType = xKubernetesMapType;
        this.xKubernetesListMapKeys = xKubernetesListMapKeys;
        this.javaType = javaType;
    }

    public SchemaObject(
                        List<String> type,
                        @Nullable Map<String, SchemaObject> properties,
                        @Nullable SchemaObject items) {
        this(null,
                null,
                null,
                null,
                type,
                properties,
                null,
                items == null ? null : List.of(items),
                null,
                null,
                null,
                null);
    }
}
