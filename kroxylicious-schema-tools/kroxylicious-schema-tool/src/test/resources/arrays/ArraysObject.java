/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package arrays;

/**
 * Auto-generated class representing the schema at /properties/objects/items/0.
 */
@javax.annotation.processing.Generated("io.kroxylicious.tools.schema.compiler.CodeGen")
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "name" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
public class ArraysObject {

    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "name")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.String name;

    /**
     * Required properties constructor.
     */
    public ArraysObject() {
    }

    /**
     * All properties constructor.
     * @param name The value of the {@code name} property. This is an optional property.
     */
    public ArraysObject(@edu.umd.cs.findbugs.annotations.Nullable() java.lang.String name) {
        this.name = name;
    }

    /**
     * Return the name.
     *
     * @return The value of this object's name.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    public java.lang.String getName() {
        return this.name;
    }

    /**
     * Set the name.
     *
     *  @param name The new value for this object's name.
     */
    public void setName(@edu.umd.cs.findbugs.annotations.Nullable() java.lang.String name) {
        this.name = name;
    }

    @java.lang.Override()
    public java.lang.String toString() {
        return "ArraysObject[" + "name: " + this.name + "]";
    }

    @java.lang.Override()
    public int hashCode() {
        return java.util.Objects.hash(this.name);
    }

    @java.lang.Override()
    public boolean equals(java.lang.Object other) {
        if (this == other)
            return true;
        else if (other instanceof arrays.ArraysObject otherArraysObject)
            return java.util.Objects.equals(this.name, otherArraysObject.name);
        else
            return false;
    }
}
