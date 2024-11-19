/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.tools.schema;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import io.kroxylicious.tools.schema.model.SchemaObject;
import io.kroxylicious.tools.schema.model.XKubeListType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CodeGenTest {

    static <K, V> Map<K, V> map(Map.Entry<K, V>... entries) {
        return Arrays.stream(entries).collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (V m1, V m2) -> {
                    throw new IllegalStateException();
                },
                LinkedHashMap::new));
    }

    CodeGen codeGen = new CodeGen();
    SchemaObject emptyTypes = new SchemaObject(List.of(), Map.of(), null);
    SchemaObject nullSchema = new SchemaObject(List.of("null"), Map.of(), null);
    SchemaObject booleanSchema = new SchemaObject(List.of("boolean"), Map.of(), null);
    SchemaObject stringSchema = new SchemaObjectBuilder().withDescription("A string").withType("string").build();
    SchemaObject integerSchema = new SchemaObject(List.of("integer"), Map.of(), null);
    SchemaObject numberSchema = new SchemaObject(List.of("number"), Map.of(), null);

    SchemaObject emptyObjectSchema = new SchemaObjectBuilder().withType("object").withJavaType("EmptyObject").build();

    SchemaObject objectWithScalarProperties = new SchemaObjectBuilder()
            .withDescription("An object with scalar-typed properties")
            .withType("object")
            .addToProperties("null", nullSchema)
            .addToProperties("boolean", booleanSchema)
            .addToProperties("string", stringSchema)
            .addToProperties("integer", integerSchema)
            .addToProperties("number", numberSchema)
            .withJavaType("ObjectWithScalarProperties")
            .build();

    SchemaObject stringArrayListSchema = new SchemaObject(List.of("array"), Map.of(), stringSchema);
    SchemaObject integerArrayListSchema = new SchemaObject(List.of("array"), Map.of(), integerSchema);
    SchemaObject stringArraySetSchema = new SchemaObjectBuilder(stringArrayListSchema).withXKubernetesListType(XKubeListType.SET).build();
    SchemaObject objectArrayMapStringKeySchema = new SchemaObjectBuilder()
            .withType("array")
            .withItems(objectWithScalarProperties)
            .withXKubernetesListType(XKubeListType.MAP)
            .withXKubernetesListMapKeys("string").build();
    SchemaObject objectArrayMapIntegerKeySchema = new SchemaObjectBuilder()
            .withType("array")
            .withItems(objectWithScalarProperties)
            .withXKubernetesListType(XKubeListType.MAP)
            .withXKubernetesListMapKeys("integer").build();
    SchemaObject objectArrayMapStringIntegerKeySchema = new SchemaObjectBuilder()
            .withType("array")
            .withItems(objectWithScalarProperties)
            .withXKubernetesListType(XKubeListType.MAP)
            .withXKubernetesListMapKeys("string", "integer").build();

    SchemaObject objectWithObjectProperties = new SchemaObjectBuilder()
            .withDescription("An object with Object-typed properties")
            .withType("object")
            .addToProperties("emptyObject", emptyObjectSchema)
            .withJavaType("WithObjectProperties")
            .build();

    SchemaObject objectWithCollectionProperties = new SchemaObjectBuilder()
            .withDescription("An object with List-, Set- and Map-typed properties")
            .withType("object")
            .addToProperties("stringArray", stringArrayListSchema)
            .addToProperties("integerArray", integerArrayListSchema)
            .addToProperties("stringSet", stringArraySetSchema)
            .addToProperties("objectArrayMapStringKeySchema", objectArrayMapStringKeySchema)
            .addToProperties("objectArrayMapIntegerKeySchema", objectArrayMapIntegerKeySchema)
            //.addToProperties("objectArrayMapStringIntegerKeySchema", objectArrayMapStringIntegerKeySchema)
            .withJavaType("ObjectWithCollectionProperties")
            .build();

    @Test
    void genTypeName() {
        String pkg = "foo";
        assertThatThrownBy(() -> codeGen.genTypeName(pkg, new SchemaObject(List.of("bob"), null, null))).isInstanceOf(InvalidSchemaException.class);
        assertThat(codeGen.genTypeName(pkg, emptyTypes)).hasToString("java.lang.Object");
        assertThat(codeGen.genTypeName(pkg, nullSchema)).hasToString("whatever.runtime.Null");
        assertThat(codeGen.genTypeName(pkg, booleanSchema)).hasToString("java.lang.Boolean");
        assertThat(codeGen.genTypeName(pkg, stringSchema)).hasToString("java.lang.String");
        assertThat(codeGen.genTypeName(pkg, integerSchema)).hasToString("java.lang.Long");
        assertThat(codeGen.genTypeName(pkg, numberSchema)).hasToString("java.lang.Double");
        assertThat(codeGen.genTypeName(pkg, stringArrayListSchema)).hasToString("java.util.List<java.lang.String>");
        assertThat(codeGen.genTypeName(pkg, integerArrayListSchema)).hasToString("java.util.List<java.lang.Long>");
        assertThat(codeGen.genTypeName(pkg, stringArraySetSchema)).hasToString("java.util.Set<java.lang.String>");
        assertThat(codeGen.genTypeName(pkg, emptyObjectSchema)).hasToString("foo.EmptyObject");
    }

    @Test
    void genDeclThrows() {
        CodeGen codeGen = new CodeGen();
        String pkg = "pkg";

        List<String> type = List.of("bob");
        assertThatThrownBy(() -> codeGen.genDecl(pkg, new SchemaObject(type, null, null)))
                .isInstanceOf(InvalidSchemaException.class)
                .hasMessage("'type' property in schema object has unsupported value 'bob' supported values are: "
                        + "'null', 'boolean', 'integer', 'number', 'string', 'array', 'object'.");
    }

    @Test
    void genEmptyObject() {
        CodeGen codeGen = new CodeGen();
        String pkg = "pkg";

        assertThat(codeGen.genDecl(pkg, emptyObjectSchema)).hasToString("""
package pkg;

@javax.annotation.processing.Generated("io.kroxylicious.tools.schema.CodeGen")
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
public class EmptyObject {

    @java.lang.Override()
    public java.lang.String toString() {
        return "EmptyObject[" + "]";
    }

    @java.lang.Override()
    public int hashCode() {
        return java.util.Objects.hash();
    }

    @java.lang.Override()
    public boolean equals(java.lang.Object other) {
        if (this == other)
            return true;
        else if (other instanceof EmptyObject otherEmptyObject)
            return true;
        else
            return false;
    }
}
""");
    }

    @Test
    void genObjectWithScalarProperties() {
        CodeGen codeGen = new CodeGen();
        String pkg = "pkg";

        assertThat(codeGen.genDecl(pkg, objectWithScalarProperties)).hasToString("""
package pkg;

/**
 * An object with scalar-typed properties
 */
@javax.annotation.processing.Generated("io.kroxylicious.tools.schema.CodeGen")
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "null", "boolean", "string", "integer", "number" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
public class ObjectWithScalarProperties {

    @com.fasterxml.jackson.annotation.JsonProperty("null")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private whatever.runtime.Null null_;

    @com.fasterxml.jackson.annotation.JsonProperty("boolean")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.Boolean boolean_;

    @com.fasterxml.jackson.annotation.JsonProperty("string")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.String string;

    @com.fasterxml.jackson.annotation.JsonProperty("integer")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.Long integer;

    @com.fasterxml.jackson.annotation.JsonProperty("number")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.Double number;

    public whatever.runtime.Null getNull() {
        return this.null_;
    }

    public void setNull(whatever.runtime.Null null_) {
        this.null_ = null_;
    }

    public java.lang.Boolean getBoolean() {
        return this.boolean_;
    }

    public void setBoolean(java.lang.Boolean boolean_) {
        this.boolean_ = boolean_;
    }

    /**
     * A string
     */
    public java.lang.String getString() {
        return this.string;
    }

    /**
     * A string
     */
    public void setString(java.lang.String string) {
        this.string = string;
    }

    public java.lang.Long getInteger() {
        return this.integer;
    }

    public void setInteger(java.lang.Long integer) {
        this.integer = integer;
    }

    public java.lang.Double getNumber() {
        return this.number;
    }

    public void setNumber(java.lang.Double number) {
        this.number = number;
    }

    @java.lang.Override()
    public java.lang.String toString() {
        return "ObjectWithScalarProperties[" + "null: " + this.null_ + ", boolean: " + this.boolean_ + ", string: " + this.string + ", integer: " + this.integer + ", number: " + this.number + "]";
    }

    @java.lang.Override()
    public int hashCode() {
        return java.util.Objects.hash(this.null_, this.boolean_, this.string, this.integer, this.number);
    }

    @java.lang.Override()
    public boolean equals(java.lang.Object other) {
        if (this == other)
            return true;
        else if (other instanceof ObjectWithScalarProperties otherObjectWithScalarProperties)
            return java.util.Objects.equals(this.null_, otherObjectWithScalarProperties.null_) && java.util.Objects.equals(this.boolean_, otherObjectWithScalarProperties.boolean_) && java.util.Objects.equals(this.string, otherObjectWithScalarProperties.string) && java.util.Objects.equals(this.integer, otherObjectWithScalarProperties.integer) && java.util.Objects.equals(this.number, otherObjectWithScalarProperties.number);
        else
            return false;
    }
}
""");
    }



    @Test
    void genObjectWithCollectionProperties() {
        CodeGen codeGen = new CodeGen();
        String pkg = "pkg";
        assertThat(codeGen.genDecl(pkg, objectWithCollectionProperties)).hasToString(
                """
package pkg;

/**
 * An object with List-, Set- and Map-typed properties
 */
@javax.annotation.processing.Generated("io.kroxylicious.tools.schema.CodeGen")
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "stringArray", "integerArray", "stringSet", "objectArrayMapStringKeySchema", "objectArrayMapIntegerKeySchema" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
public class ObjectWithCollectionProperties {

    @com.fasterxml.jackson.annotation.JsonProperty("stringArray")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.util.List<java.lang.String> stringArray;

    @com.fasterxml.jackson.annotation.JsonProperty("integerArray")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.util.List<java.lang.Long> integerArray;

    @com.fasterxml.jackson.annotation.JsonProperty("stringSet")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.util.Set<java.lang.String> stringSet;

    @com.fasterxml.jackson.annotation.JsonProperty("objectArrayMapStringKeySchema")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.util.Map<java.lang.String, pkg.ObjectWithScalarProperties> objectArrayMapStringKeySchema;

    @com.fasterxml.jackson.annotation.JsonProperty("objectArrayMapIntegerKeySchema")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.util.Map<java.lang.Long, pkg.ObjectWithScalarProperties> objectArrayMapIntegerKeySchema;

    public java.util.List<java.lang.String> getStringArray() {
        return this.stringArray;
    }

    public void setStringArray(java.util.List<java.lang.String> stringArray) {
        this.stringArray = stringArray;
    }

    public java.util.List<java.lang.Long> getIntegerArray() {
        return this.integerArray;
    }

    public void setIntegerArray(java.util.List<java.lang.Long> integerArray) {
        this.integerArray = integerArray;
    }

    public java.util.Set<java.lang.String> getStringSet() {
        return this.stringSet;
    }

    public void setStringSet(java.util.Set<java.lang.String> stringSet) {
        this.stringSet = stringSet;
    }

    public java.util.Map<java.lang.String, pkg.ObjectWithScalarProperties> getObjectArrayMapStringKeySchema() {
        return this.objectArrayMapStringKeySchema;
    }

    public void setObjectArrayMapStringKeySchema(java.util.Map<java.lang.String, pkg.ObjectWithScalarProperties> objectArrayMapStringKeySchema) {
        this.objectArrayMapStringKeySchema = objectArrayMapStringKeySchema;
    }

    public java.util.Map<java.lang.Long, pkg.ObjectWithScalarProperties> getObjectArrayMapIntegerKeySchema() {
        return this.objectArrayMapIntegerKeySchema;
    }

    public void setObjectArrayMapIntegerKeySchema(java.util.Map<java.lang.Long, pkg.ObjectWithScalarProperties> objectArrayMapIntegerKeySchema) {
        this.objectArrayMapIntegerKeySchema = objectArrayMapIntegerKeySchema;
    }

    @java.lang.Override()
    public java.lang.String toString() {
        return "ObjectWithCollectionProperties[" + "stringArray: " + this.stringArray + ", integerArray: " + this.integerArray + ", stringSet: " + this.stringSet + ", objectArrayMapStringKeySchema: " + this.objectArrayMapStringKeySchema + ", objectArrayMapIntegerKeySchema: " + this.objectArrayMapIntegerKeySchema + "]";
    }

    @java.lang.Override()
    public int hashCode() {
        return java.util.Objects.hash(this.stringArray, this.integerArray, this.stringSet, this.objectArrayMapStringKeySchema, this.objectArrayMapIntegerKeySchema);
    }

    @java.lang.Override()
    public boolean equals(java.lang.Object other) {
        if (this == other)
            return true;
        else if (other instanceof ObjectWithCollectionProperties otherObjectWithCollectionProperties)
            return java.util.Objects.equals(this.stringArray, otherObjectWithCollectionProperties.stringArray) && java.util.Objects.equals(this.integerArray, otherObjectWithCollectionProperties.integerArray) && java.util.Objects.equals(this.stringSet, otherObjectWithCollectionProperties.stringSet) && java.util.Objects.equals(this.objectArrayMapStringKeySchema, otherObjectWithCollectionProperties.objectArrayMapStringKeySchema) && java.util.Objects.equals(this.objectArrayMapIntegerKeySchema, otherObjectWithCollectionProperties.objectArrayMapIntegerKeySchema);
        else
            return false;
    }
}
""");

    }
}
