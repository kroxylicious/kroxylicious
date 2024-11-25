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
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "strings", "stringSet", "integers", "fooBars" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
public class Arrays {

    @com.fasterxml.jackson.annotation.JsonProperty("strings")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.util.List<java.lang.String> strings;

    @com.fasterxml.jackson.annotation.JsonProperty("stringSet")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.util.Set<java.lang.String> stringSet;

    @com.fasterxml.jackson.annotation.JsonProperty("integers")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.util.List<java.lang.Long> integers;

    @com.fasterxml.jackson.annotation.JsonProperty("fooBars")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.util.List<arrays.FooBar> fooBars;

    /**
     * An array of strings
     */
    public java.util.List<java.lang.String> getStrings() {
        return this.strings;
    }

    /**
     * An array of strings
     */
    public void setStrings(java.util.List<java.lang.String> strings) {
        this.strings = strings;
    }

    /**
     * An array of strings
     */
    public java.util.Set<java.lang.String> getStringSet() {
        return this.stringSet;
    }

    /**
     * An array of strings
     */
    public void setStringSet(java.util.Set<java.lang.String> stringSet) {
        this.stringSet = stringSet;
    }

    /**
     * An array of integers
     */
    public java.util.List<java.lang.Long> getIntegers() {
        return this.integers;
    }

    /**
     * An array of integers
     */
    public void setIntegers(java.util.List<java.lang.Long> integers) {
        this.integers = integers;
    }

    /**
     * An array of FooBars
     */
    public java.util.List<arrays.FooBar> getFooBars() {
        return this.fooBars;
    }

    /**
     * An array of FooBars
     */
    public void setFooBars(java.util.List<arrays.FooBar> fooBars) {
        this.fooBars = fooBars;
    }

    @java.lang.Override()
    public java.lang.String toString() {
        return "Arrays[" + "strings: " + this.strings + ", stringSet: " + this.stringSet + ", integers: " + this.integers + ", fooBars: " + this.fooBars + "]";
    }

    @java.lang.Override()
    public int hashCode() {
        return java.util.Objects.hash(this.strings, this.stringSet, this.integers, this.fooBars);
    }

    @java.lang.Override()
    public boolean equals(java.lang.Object other) {
        if (this == other)
            return true;
        else if (other instanceof arrays.Arrays otherArrays)
            return java.util.Objects.equals(this.strings, otherArrays.strings) && java.util.Objects.equals(this.stringSet, otherArrays.stringSet) && java.util.Objects.equals(this.integers, otherArrays.integers) && java.util.Objects.equals(this.fooBars, otherArrays.fooBars);
        else
            return false;
    }
}