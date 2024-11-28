/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.tools.schema.model;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.sundr.builder.annotations.Buildable;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

@Buildable(editableEnabled = false, validationEnabled = false, generateBuilderPackage = false)
public final class SchemaObject {
    @Nullable
    @JsonProperty("$schema")
    private String schema;

    @Nullable
    @JsonProperty("id")
    private String id;

    @Nullable
    @JsonProperty("$ref")
    private String ref;

    @Nullable
    @JsonProperty("description")
    private String description;

    @Nullable
    @JsonProperty("definitions")
    private Map<String, SchemaObject> definitions;

    @Nullable
    @JsonProperty("type")
    @JsonDeserialize(using = ListOrSingleSerde.SchemaType.class)
    private List<SchemaType> type;

    @Nullable
    @JsonProperty("format")
    private String format;

    @Nullable
    @JsonProperty("properties")
    private Map<String, SchemaObject> properties;

    @Nullable
    @JsonProperty("required")
    private Set<String> required;

    @Nullable
    @JsonProperty("items")
    @JsonDeserialize(using = ListOrSingleSerde.SchemaObject.class)
    private List<SchemaObject> items;

    @Nullable
    @JsonProperty("oneOf")
    @JsonDeserialize(using = ListOrSingleSerde.SchemaObject.class)
    private List<SchemaObject> oneOf;

    @Nullable
    @JsonProperty("allOf")
    @JsonDeserialize(using = ListOrSingleSerde.SchemaObject.class)
    private List<SchemaObject> allOf;

    @Nullable
    @JsonProperty("anyOf")
    @JsonDeserialize(using = ListOrSingleSerde.SchemaObject.class)
    private List<SchemaObject> anyOf;

    @Nullable
    @JsonProperty("not")
    private SchemaObject not;

    @Nullable
    @JsonProperty("x-kubernetes-list-type")
    private XKubeListType xKubernetesListType;

    @Nullable
    @JsonProperty("x-kubernetes-list-map-keys")
    private List<String> xKubernetesListMapKeys;

    @Nullable
    @JsonProperty("x-kubernetes-map-type")
    private XKubeMapType xKubernetesMapType;

    @Nullable
    @JsonProperty("$javaType")
    private String javaType;

    @JsonCreator
    public SchemaObject() {
    }

    @Nullable
    @JsonProperty("$schema")
    public String getSchema() {
        return schema;
    }

    @Nullable
    @JsonProperty("id")
    public String getId() {
        return id;
    }

    @Nullable
    @JsonProperty("$ref")
    public String getRef() {
        return ref;
    }

    @Nullable
    @JsonProperty("description")
    public String getDescription() {
        return description;
    }

    @Nullable
    @JsonProperty("definitions")
    public Map<String, SchemaObject> getDefinitions() {
        return definitions;
    }

    @Nullable
    @JsonProperty("type")
    @JsonDeserialize(using = ListOrSingleSerde.SchemaType.class)
    public List<SchemaType> getType() {
        return type;
    }

    @Nullable
    public String getFormat() {
        return format;
    }

    @Nullable
    @JsonProperty("properties")
    public Map<String, SchemaObject> getProperties() {
        return properties;
    }

    @Nullable
    @JsonProperty("required")
    public Set<String> getRequired() {
        return required;
    }

    @Nullable
    @JsonProperty("items")
    @JsonDeserialize(using = ListOrSingleSerde.SchemaObject.class)
    public List<SchemaObject> getItems() {
        return items;
    }

    @Nullable
    public List<SchemaObject> getOneOf() {
        return oneOf;
    }

    @Nullable
    public List<SchemaObject> getAllOf() {
        return allOf;
    }

    @Nullable
    public List<SchemaObject> getAnyOf() {
        return anyOf;
    }

    @Nullable
    public SchemaObject getNot() {
        return not;
    }

    @Nullable
    @JsonProperty("x-kubernetes-list-type")
    public XKubeListType getXKubernetesListType() {
        return xKubernetesListType;
    }

    @Nullable
    @JsonProperty("x-kubernetes-list-map-keys")
    public List<String> getXKubernetesListMapKeys() {
        return xKubernetesListMapKeys;
    }

    @Nullable
    @JsonProperty("x-kubernetes-map-type")
    public XKubeMapType getXKubernetesMapType() {
        return xKubernetesMapType;
    }

    @Nullable
    @JsonProperty("$javaType")
    public String getJavaType() {
        return javaType;
    }

    public void setFormat(@Nullable String format) {
        this.format = format;
    }

    public void setDefinitions(@Nullable Map<String, SchemaObject> definitions) {
        this.definitions = definitions;
    }

    public void setDescription(@Nullable String description) {
        this.description = description;
    }

    public void setId(@Nullable String id) {
        this.id = id;
    }

    public void setItems(@Nullable List<SchemaObject> items) {
        this.items = items;
    }

    public void setOneOf(@Nullable List<SchemaObject> oneOf) {
        this.oneOf = oneOf;
    }

    public void setAllOf(@Nullable List<SchemaObject> allOf) {
        this.allOf = allOf;
    }

    public void setAnyOf(@Nullable List<SchemaObject> anyOf) {
        this.anyOf = anyOf;
    }

    public void setNot(@Nullable SchemaObject not) {
        this.not = not;
    }

    public void setJavaType(@Nullable String javaType) {
        this.javaType = javaType;
    }

    public void setProperties(@Nullable Map<String, SchemaObject> properties) {
        this.properties = properties;
    }

    public void setRef(@Nullable String ref) {
        this.ref = ref;
    }

    public void setRequired(@Nullable Set<String> required) {
        this.required = required;
    }

    public void setSchema(@Nullable String schema) {
        this.schema = schema;
    }

    public void setType(@Nullable List<SchemaType> type) {
        this.type = type;
    }

    public void setXKubernetesListMapKeys(@Nullable List<String> xKubernetesListMapKeys) {
        this.xKubernetesListMapKeys = xKubernetesListMapKeys;
    }

    public void setXKubernetesListType(@Nullable XKubeListType xKubernetesListType) {
        this.xKubernetesListType = xKubernetesListType;
    }

    public void setXKubernetesMapType(@Nullable XKubeMapType xKubernetesMapType) {
        this.xKubernetesMapType = xKubernetesMapType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SchemaObject that)) {
            return false;
        }
        return Objects.equals(schema, that.schema) && Objects.equals(id, that.id) && Objects.equals(ref, that.ref) && Objects.equals(description, that.description)
                && Objects.equals(definitions, that.definitions) && Objects.equals(type, that.type) && Objects.equals(format, that.format) && Objects.equals(properties,
                        that.properties)
                && Objects.equals(required, that.required) && Objects.equals(items, that.items) && Objects.equals(oneOf, that.oneOf) && Objects.equals(
                        allOf, that.allOf)
                && Objects.equals(anyOf, that.anyOf) && Objects.equals(not, that.not) && xKubernetesListType == that.xKubernetesListType
                && Objects.equals(xKubernetesListMapKeys, that.xKubernetesListMapKeys) && xKubernetesMapType == that.xKubernetesMapType && Objects.equals(javaType,
                        that.javaType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, id, ref, description, definitions, type, format, properties, required, items, oneOf, allOf, anyOf, not, xKubernetesListType,
                xKubernetesListMapKeys, xKubernetesMapType, javaType);
    }

    @Override
    public String toString() {
        return "SchemaObject{" +
                "schema='" + schema + '\'' +
                ", id='" + id + '\'' +
                ", ref='" + ref + '\'' +
                ", description='" + description + '\'' +
                ", definitions=" + definitions +
                ", type=" + type +
                ", format='" + format + '\'' +
                ", properties=" + properties +
                ", required=" + required +
                ", items=" + items +
                ", oneOf=" + oneOf +
                ", allOf=" + allOf +
                ", anyOf=" + anyOf +
                ", not=" + not +
                ", xKubernetesListType=" + xKubernetesListType +
                ", xKubernetesListMapKeys=" + xKubernetesListMapKeys +
                ", xKubernetesMapType=" + xKubernetesMapType +
                ", javaType='" + javaType + '\'' +
                '}';
    }

    public static class Visitor {
        protected static boolean isRootSchema(String path) {
            return path.isEmpty();
        }

        public void enterSchema(URI base, String path, String keyword, @NonNull SchemaObject schema) {
        }

        public void exitSchema(URI base, String path, String keyword, @NonNull SchemaObject schema) {
        }
    }

    public void visitSchemas(URI base, @NonNull Visitor visitor) throws VisitException {
        visitSchemas(base, "", "", this, visitor);
    }

    private static void visitSchemas(URI base,
                                     String path,
                                     String keyword,
                                     SchemaObject schemaObject,
                                     @NonNull Visitor visitor)
            throws VisitException {
        try {
            visitor.enterSchema(base, path, keyword, schemaObject);
        }
        catch (Exception e) {
            throw new VisitException(visitor.getClass().getName() + "#enterSchema() threw exception while visiting schema object at '" + path + "' from " + base, e);
        }
        visitSchemaMap(base, path, visitor, schemaObject.definitions, "definitions");
        visitSchemaMap(base, path, visitor, schemaObject.properties, "properties");
        visitSchemaArray(base, path, visitor, schemaObject.items, "items");
        visitSchemaArray(base, path, visitor, schemaObject.oneOf, "oneOf");
        visitSchemaArray(base, path, visitor, schemaObject.allOf, "allOf");
        visitSchemaArray(base, path, visitor, schemaObject.anyOf, "anyOf");
        if (schemaObject.not != null) {
            String path1 = path + "/not";
            visitSchemas(base, path1, "not", schemaObject.not, visitor);
        }
        try {
            visitor.exitSchema(base, path, keyword, schemaObject);
        }
        catch (Exception e) {
            throw new VisitException(visitor.getClass().getName() + "#exitSchema() threw exception while visiting schema object at '" + path + "' from " + base, e);
        }
    }

    private static void visitSchemaMap(URI base,
                                       String path,
                                       Visitor visitor,
                                       @Nullable Map<String, SchemaObject> map,
                                       String keyword) {
        if (map != null) {
            for (Map.Entry<String, SchemaObject> entry : map.entrySet()) {
                String definitionName = entry.getKey();
                SchemaObject definitionSchema = entry.getValue();
                String path1 = path + "/" + keyword + "/" + definitionName;
                visitSchemas(base, path1, keyword, definitionSchema, visitor);
            }
        }
    }

    private static void visitSchemaArray(URI base,
                                         String path,
                                         Visitor visitor,
                                         @Nullable List<SchemaObject> array,
                                         String keyword) {
        if (array != null) {
            for (int i = 0; i < array.size(); i++) {
                SchemaObject itemSchema = array.get(i);
                String path1 = path + "/" + keyword + "/" + i;
                visitSchemas(base, path1, keyword, itemSchema, visitor);
            }
        }
    }
}
