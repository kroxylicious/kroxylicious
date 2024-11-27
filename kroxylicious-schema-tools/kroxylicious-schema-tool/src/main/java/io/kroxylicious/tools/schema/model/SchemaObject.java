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

    // @Buildable(editableEnabled = false, validationEnabled = false, generateBuilderPackage = false)
    // @JsonCreator
    // public SchemaObject(
    // @JsonProperty("$schema") String schema,
    // @JsonProperty("id") String id,
    // @JsonProperty("$ref") String ref,
    // @JsonProperty("description") String description,
    // @JsonProperty("definitions") Map<String, SchemaObject> definitions,
    // @JsonProperty("type") @JsonDeserialize(using = ListOrSingleSerde.String.class) List<String> type,
    // @JsonProperty("properties") Map<String, SchemaObject> properties,
    // @JsonProperty("required") Set<String> required,
    // @JsonProperty("items") @JsonDeserialize(using = ListOrSingleSerde.SchemaObject.class) List<SchemaObject> items,
    // @JsonProperty("x-kubernetes-list-type") XKubeListType xKubernetesListType,
    // @JsonProperty("x-kubernetes-list-map-keys") List<String> xKubernetesListMapKeys,
    // @JsonProperty("x-kubernetes-map-type") XKubeMapType xKubernetesMapType,
    // @JsonProperty("$javaType") String javaType) {
    // this.schema = schema;
    // this.id = id;
    // this.ref = ref;
    // this.description = description;
    // this.definitions = definitions;
    // this.type = type;
    // this.properties = properties;
    // this.required = required;
    // this.items = items;
    // this.xKubernetesListType = xKubernetesListType;
    // this.xKubernetesMapType = xKubernetesMapType;
    // this.xKubernetesListMapKeys = xKubernetesListMapKeys;
    // this.javaType = javaType;
    // }

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
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (SchemaObject) obj;
        return Objects.equals(this.schema, that.schema) &&
                Objects.equals(this.id, that.id) &&
                Objects.equals(this.ref, that.ref) &&
                Objects.equals(this.description, that.description) &&
                Objects.equals(this.definitions, that.definitions) &&
                Objects.equals(this.type, that.type) &&
                Objects.equals(this.properties, that.properties) &&
                Objects.equals(this.required, that.required) &&
                Objects.equals(this.items, that.items) &&
                Objects.equals(this.xKubernetesListType, that.xKubernetesListType) &&
                Objects.equals(this.xKubernetesListMapKeys, that.xKubernetesListMapKeys) &&
                Objects.equals(this.xKubernetesMapType, that.xKubernetesMapType) &&
                Objects.equals(this.javaType, that.javaType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, id, ref, description, definitions, type, properties, required, items, xKubernetesListType, xKubernetesListMapKeys, xKubernetesMapType,
                javaType);
    }

    @Override
    public String toString() {
        return "SchemaObject[" +
                "schema=" + schema + ", " +
                "id=" + id + ", " +
                "ref=" + ref + ", " +
                "description=" + description + ", " +
                "definitions=" + definitions + ", " +
                "type=" + type + ", " +
                "properties=" + properties + ", " +
                "required=" + required + ", " +
                "items=" + items + ", " +
                "xKubernetesListType=" + xKubernetesListType + ", " +
                "xKubernetesListMapKeys=" + xKubernetesListMapKeys + ", " +
                "xKubernetesMapType=" + xKubernetesMapType + ", " +
                "javaType=" + javaType + ']';
    }

    public static class Visitor {
        protected static boolean isRootSchema(String path) {
            return path.isEmpty();
        }

        public void enterSchema(URI base, String path, @NonNull SchemaObject schema) {
        }

        public void exitSchema(URI base, String path, @NonNull SchemaObject schema) {
        }
    }

    public void visitSchemas(URI base, @NonNull Visitor visitor) {
        visitSchemas(base, "", this, visitor);
    }

    private static void visitSchemas(URI base, String path, SchemaObject schemaObject, @NonNull Visitor visitor) {
        try {
            visitor.enterSchema(base, path, schemaObject);
        }
        catch (Exception e) {
            throw new VisitException(visitor.getClass().getName() + "#enterSchema() threw exception while visiting schema object at '" + path + "' from " + base, e);
        }
        if (schemaObject.definitions != null) {
            for (Map.Entry<String, SchemaObject> entry : schemaObject.definitions.entrySet()) {
                String definitionName = entry.getKey();
                SchemaObject definitionSchema = entry.getValue();
                String path1 = path + "/definitions/" + definitionName;
                // visitor.enterSchema(base, path1, definitionSchema);
                visitSchemas(base, path1, definitionSchema, visitor);
                // visitor.exitSchema(base, path1, definitionSchema);
            }
        }
        if (schemaObject.properties != null) {
            for (var entry : schemaObject.properties.entrySet()) {
                var propertyName = entry.getKey();
                var propertySchema = entry.getValue();
                String path1 = path + "/properties/" + propertyName;
                // visitor.enterSchema(base, path1, propertySchema);
                visitSchemas(base, path1, propertySchema, visitor);
                // visitor.exitSchema(base, path1, propertySchema);
            }
        }
        if (schemaObject.items != null) {
            List<SchemaObject> schemaObjects = schemaObject.items;
            for (int i = 0; i < schemaObjects.size(); i++) {
                SchemaObject itemSchema = schemaObjects.get(i);
                String path1 = path + "/items/" + i;
                // visitor.enterSchema(base, path1, itemSchema);
                visitSchemas(base, path1, itemSchema, visitor);
                // visitor.exitSchema(base, path1, itemSchema);
            }
        }
        try {
            visitor.exitSchema(base, path, schemaObject);
        }
        catch (Exception e) {
            throw new VisitException(visitor.getClass().getName() + "#exitSchema() threw exception while visiting schema object at '" + path + "' from " + base, e);
        }
    }
}
