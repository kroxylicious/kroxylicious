/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package maps;

/**
 * An class with properties mapped from the array type.
 */
@javax.annotation.processing.Generated("io.kroxylicious.tools.schema.CodeGen")
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "keyedOnFoo" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
public class Maps {

    @com.fasterxml.jackson.annotation.JsonProperty("keyedOnFoo")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.util.Map<java.lang.String, maps.FooBarBaz> keyedOnFoo;

    /**
     * An array of FooBars
     */
    public java.util.Map<java.lang.String, maps.FooBarBaz> getKeyedOnFoo() {
        return this.keyedOnFoo;
    }

    /**
     * An array of FooBars
     */
    public void setKeyedOnFoo(java.util.Map<java.lang.String, maps.FooBarBaz> keyedOnFoo) {
        this.keyedOnFoo = keyedOnFoo;
    }

    @java.lang.Override()
    public java.lang.String toString() {
        return "Maps[" + "keyedOnFoo: " + this.keyedOnFoo + "]";
    }

    @java.lang.Override()
    public int hashCode() {
        return java.util.Objects.hash(this.keyedOnFoo);
    }

    @java.lang.Override()
    public boolean equals(java.lang.Object other) {
        if (this == other)
            return true;
        else if (other instanceof maps.Maps otherMaps)
            return java.util.Objects.equals(this.keyedOnFoo, otherMaps.keyedOnFoo);
        else
            return false;
    }
}