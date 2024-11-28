/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package junctor;

/**
 * A class with scalar properties
 */
@javax.annotation.processing.Generated("io.kroxylicious.tools.schema.compiler.CodeGen")
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "oneOf", "anyOf", "allOf", "not" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
public class Junctor {

    @com.fasterxml.jackson.annotation.JsonProperty(value = "oneOf")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private junctor.JunctorOneOf oneOf;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "anyOf")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private junctor.JunctorAnyOf anyOf;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "allOf")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private junctor.JunctorAllOf allOf;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "not")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private junctor.JunctorNot not;

    /**
     * Return the oneOf.
     *
     * @return The value of this object's oneOf.
     */
    public junctor.JunctorOneOf getOneOf() {
        return this.oneOf;
    }

    /**
     * Set the oneOf.
     *
     *  @param oneOf The new value for this object's oneOf.
     */
    public void setOneOf(junctor.JunctorOneOf oneOf) {
        this.oneOf = oneOf;
    }

    /**
     * Return the anyOf.
     *
     * @return The value of this object's anyOf.
     */
    public junctor.JunctorAnyOf getAnyOf() {
        return this.anyOf;
    }

    /**
     * Set the anyOf.
     *
     *  @param anyOf The new value for this object's anyOf.
     */
    public void setAnyOf(junctor.JunctorAnyOf anyOf) {
        this.anyOf = anyOf;
    }

    /**
     * Return the allOf.
     *
     * @return The value of this object's allOf.
     */
    public junctor.JunctorAllOf getAllOf() {
        return this.allOf;
    }

    /**
     * Set the allOf.
     *
     *  @param allOf The new value for this object's allOf.
     */
    public void setAllOf(junctor.JunctorAllOf allOf) {
        this.allOf = allOf;
    }

    /**
     * Return the not.
     *
     * @return The value of this object's not.
     */
    public junctor.JunctorNot getNot() {
        return this.not;
    }

    /**
     * Set the not.
     *
     *  @param not The new value for this object's not.
     */
    public void setNot(junctor.JunctorNot not) {
        this.not = not;
    }

    @java.lang.Override()
    public java.lang.String toString() {
        return "Junctor[" + "oneOf: " + this.oneOf + ", anyOf: " + this.anyOf + ", allOf: " + this.allOf + ", not: " + this.not + "]";
    }

    @java.lang.Override()
    public int hashCode() {
        return java.util.Objects.hash(this.oneOf, this.anyOf, this.allOf, this.not);
    }

    @java.lang.Override()
    public boolean equals(java.lang.Object other) {
        if (this == other)
            return true;
        else if (other instanceof junctor.Junctor otherJunctor)
            return java.util.Objects.equals(this.oneOf, otherJunctor.oneOf) && java.util.Objects.equals(this.anyOf, otherJunctor.anyOf) && java.util.Objects.equals(this.allOf, otherJunctor.allOf) && java.util.Objects.equals(this.not, otherJunctor.not);
        else
            return false;
    }
}