/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package arrays;

/**
 * An class with properties mapped from the array type.
 */
@javax.annotation.processing.Generated("io.kroxylicious.tools.schema.compiler.CodeGen")
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "strings", "stringSet", "integers", "objects", "fooBars" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
public class Arrays {

    @edu.umd.cs.findbugs.annotations.Nullable()
    private java.util.List<java.lang.String> strings;

    @edu.umd.cs.findbugs.annotations.Nullable()
    private java.util.Set<java.lang.String> stringSet;

    @edu.umd.cs.findbugs.annotations.Nullable()
    private java.util.List<java.lang.Long> integers;

    @edu.umd.cs.findbugs.annotations.Nullable()
    private java.util.List<arrays.ArraysObject> objects;

    @edu.umd.cs.findbugs.annotations.Nullable()
    private java.util.List<arrays.FooBar> fooBars;

    /**
     * Required properties constructor.
     */
    public Arrays() {
    }

    /**
     * All properties constructor.
     * @param strings The value of the {@code strings} property. This is an optional property.
     * @param stringSet The value of the {@code stringSet} property. This is an optional property.
     * @param integers The value of the {@code integers} property. This is an optional property.
     * @param objects The value of the {@code objects} property. This is an optional property.
     * @param fooBars The value of the {@code fooBars} property. This is an optional property.
     */
    public Arrays(@edu.umd.cs.findbugs.annotations.Nullable() java.util.List<java.lang.String> strings, @edu.umd.cs.findbugs.annotations.Nullable() java.util.Set<java.lang.String> stringSet, @edu.umd.cs.findbugs.annotations.Nullable() java.util.List<java.lang.Long> integers, @edu.umd.cs.findbugs.annotations.Nullable() java.util.List<arrays.ArraysObject> objects, @edu.umd.cs.findbugs.annotations.Nullable() java.util.List<arrays.FooBar> fooBars) {
        this.strings = strings;
        this.stringSet = stringSet;
        this.integers = integers;
        this.objects = objects;
        this.fooBars = fooBars;
    }

    /**
     * An array of strings
     * @return The value of this object's strings.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "strings")
    public java.util.List<java.lang.String> strings() {
        return this.strings;
    }

    /**
     * An array of strings
     *  @param strings The new value for this object's strings.
     */
    @com.fasterxml.jackson.annotation.JsonProperty(value = "strings")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    public void strings(@edu.umd.cs.findbugs.annotations.Nullable() java.util.List<java.lang.String> strings) {
        this.strings = strings;
    }

    /**
     * An array of strings
     * @return The value of this object's stringSet.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "stringSet")
    public java.util.Set<java.lang.String> stringSet() {
        return this.stringSet;
    }

    /**
     * An array of strings
     *  @param stringSet The new value for this object's stringSet.
     */
    @com.fasterxml.jackson.annotation.JsonProperty(value = "stringSet")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    public void stringSet(@edu.umd.cs.findbugs.annotations.Nullable() java.util.Set<java.lang.String> stringSet) {
        this.stringSet = stringSet;
    }

    /**
     * An array of integers
     * @return The value of this object's integers.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "integers")
    public java.util.List<java.lang.Long> integers() {
        return this.integers;
    }

    /**
     * An array of integers
     *  @param integers The new value for this object's integers.
     */
    @com.fasterxml.jackson.annotation.JsonProperty(value = "integers")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    public void integers(@edu.umd.cs.findbugs.annotations.Nullable() java.util.List<java.lang.Long> integers) {
        this.integers = integers;
    }

    /**
     * An array of objects (not via $ref)
     * @return The value of this object's objects.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "objects")
    public java.util.List<arrays.ArraysObject> objects() {
        return this.objects;
    }

    /**
     * An array of objects (not via $ref)
     *  @param objects The new value for this object's objects.
     */
    @com.fasterxml.jackson.annotation.JsonProperty(value = "objects")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    public void objects(@edu.umd.cs.findbugs.annotations.Nullable() java.util.List<arrays.ArraysObject> objects) {
        this.objects = objects;
    }

    /**
     * An array of FooBars
     * @return The value of this object's fooBars.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "fooBars")
    public java.util.List<arrays.FooBar> fooBars() {
        return this.fooBars;
    }

    /**
     * An array of FooBars
     *  @param fooBars The new value for this object's fooBars.
     */
    @com.fasterxml.jackson.annotation.JsonProperty(value = "fooBars")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    public void fooBars(@edu.umd.cs.findbugs.annotations.Nullable() java.util.List<arrays.FooBar> fooBars) {
        this.fooBars = fooBars;
    }

    @java.lang.Override()
    public java.lang.String toString() {
        return "Arrays[" + "strings: " + this.strings + ", stringSet: " + this.stringSet + ", integers: " + this.integers + ", objects: " + this.objects + ", fooBars: " + this.fooBars + "]";
    }

    @java.lang.Override()
    public int hashCode() {
        return java.util.Objects.hash(this.strings, this.stringSet, this.integers, this.objects, this.fooBars);
    }

    @java.lang.Override()
    public boolean equals(java.lang.Object other) {
        if (this == other)
            return true;
        else if (other instanceof arrays.Arrays otherArrays)
            return java.util.Objects.equals(this.strings, otherArrays.strings) && java.util.Objects.equals(this.stringSet, otherArrays.stringSet) && java.util.Objects.equals(this.integers, otherArrays.integers) && java.util.Objects.equals(this.objects, otherArrays.objects) && java.util.Objects.equals(this.fooBars, otherArrays.fooBars);
        else
            return false;
    }
}
