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

    @edu.umd.cs.findbugs.annotations.Nullable
    private junctor.JunctorOneOf oneOf;

    @edu.umd.cs.findbugs.annotations.Nullable
    private junctor.JunctorAnyOf anyOf;

    @edu.umd.cs.findbugs.annotations.Nullable
    private junctor.JunctorAllOf allOf;

    @edu.umd.cs.findbugs.annotations.Nullable
    private junctor.JunctorNot not;

    /**
     * All properties constructor.
     * @param oneOf The value of the {@code oneOf} property. This is an optional property.
     * @param anyOf The value of the {@code anyOf} property. This is an optional property.
     * @param allOf The value of the {@code allOf} property. This is an optional property.
     * @param not The value of the {@code not} property. This is an optional property.
     */
    @com.fasterxml.jackson.annotation.JsonCreator
    public Junctor(@com.fasterxml.jackson.annotation.JsonProperty(value = "oneOf") @edu.umd.cs.findbugs.annotations.Nullable junctor.JunctorOneOf oneOf, @com.fasterxml.jackson.annotation.JsonProperty(value = "anyOf") @edu.umd.cs.findbugs.annotations.Nullable junctor.JunctorAnyOf anyOf, @com.fasterxml.jackson.annotation.JsonProperty(value = "allOf") @edu.umd.cs.findbugs.annotations.Nullable junctor.JunctorAllOf allOf, @com.fasterxml.jackson.annotation.JsonProperty(value = "not") @edu.umd.cs.findbugs.annotations.Nullable junctor.JunctorNot not) {
        this.oneOf = oneOf;
        this.anyOf = anyOf;
        this.allOf = allOf;
        this.not = not;
    }

    /**
     * Return the oneOf.
     *
     * @return The value of this object's oneOf.
     */
    @edu.umd.cs.findbugs.annotations.Nullable
    @com.fasterxml.jackson.annotation.JsonProperty(value = "oneOf")
    public junctor.JunctorOneOf oneOf() {
        return this.oneOf;
    }

    /**
     * Set the oneOf.
     *
     *  @param oneOf The new value for this object's oneOf.
     */
    public void oneOf(@edu.umd.cs.findbugs.annotations.Nullable junctor.JunctorOneOf oneOf) {
        this.oneOf = oneOf;
    }

    /**
     * Return the anyOf.
     *
     * @return The value of this object's anyOf.
     */
    @edu.umd.cs.findbugs.annotations.Nullable
    @com.fasterxml.jackson.annotation.JsonProperty(value = "anyOf")
    public junctor.JunctorAnyOf anyOf() {
        return this.anyOf;
    }

    /**
     * Set the anyOf.
     *
     *  @param anyOf The new value for this object's anyOf.
     */
    public void anyOf(@edu.umd.cs.findbugs.annotations.Nullable junctor.JunctorAnyOf anyOf) {
        this.anyOf = anyOf;
    }

    /**
     * Return the allOf.
     *
     * @return The value of this object's allOf.
     */
    @edu.umd.cs.findbugs.annotations.Nullable
    @com.fasterxml.jackson.annotation.JsonProperty(value = "allOf")
    public junctor.JunctorAllOf allOf() {
        return this.allOf;
    }

    /**
     * Set the allOf.
     *
     *  @param allOf The new value for this object's allOf.
     */
    public void allOf(@edu.umd.cs.findbugs.annotations.Nullable junctor.JunctorAllOf allOf) {
        this.allOf = allOf;
    }

    /**
     * Return the not.
     *
     * @return The value of this object's not.
     */
    @edu.umd.cs.findbugs.annotations.Nullable
    @com.fasterxml.jackson.annotation.JsonProperty(value = "not")
    public junctor.JunctorNot not() {
        return this.not;
    }

    /**
     * Set the not.
     *
     *  @param not The new value for this object's not.
     */
    public void not(@edu.umd.cs.findbugs.annotations.Nullable junctor.JunctorNot not) {
        this.not = not;
    }

    @java.lang.Override
    public java.lang.String toString() {
        return "Junctor[" + "oneOf: " + this.oneOf + ", anyOf: " + this.anyOf + ", allOf: " + this.allOf + ", not: " + this.not + "]";
    }

    @java.lang.Override
    public int hashCode() {
        return java.util.Objects.hash(this.oneOf, this.anyOf, this.allOf, this.not);
    }

    @java.lang.Override
    public boolean equals(java.lang.Object other) {
        if (this == other)
            return true;
        else if (other instanceof junctor.Junctor otherJunctor)
            return java.util.Objects.equals(this.oneOf, otherJunctor.oneOf) && java.util.Objects.equals(this.anyOf, otherJunctor.anyOf) && java.util.Objects.equals(this.allOf, otherJunctor.allOf) && java.util.Objects.equals(this.not, otherJunctor.not);
        else
            return false;
    }
}
