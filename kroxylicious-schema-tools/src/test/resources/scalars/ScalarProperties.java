/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package scalars;

/**
 * A class with scalar properties
 */
@javax.annotation.processing.Generated("io.kroxylicious.tools.schema.CodeGen")
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "null", "boolean", "integer", "number", "string" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
public class ScalarProperties {

    @com.fasterxml.jackson.annotation.JsonProperty("null")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.Object null_;

    @com.fasterxml.jackson.annotation.JsonProperty("boolean")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.Boolean boolean_;

    @com.fasterxml.jackson.annotation.JsonProperty("integer")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.Long integer;

    @com.fasterxml.jackson.annotation.JsonProperty("number")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.Double number;

    @com.fasterxml.jackson.annotation.JsonProperty("string")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.String string;

    public java.lang.Object getNull() {
        return this.null_;
    }

    public void setNull(java.lang.Object null_) {
        this.null_ = null_;
    }

    public java.lang.Boolean getBoolean() {
        return this.boolean_;
    }

    public void setBoolean(java.lang.Boolean boolean_) {
        this.boolean_ = boolean_;
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

    public java.lang.String getString() {
        return this.string;
    }

    public void setString(java.lang.String string) {
        this.string = string;
    }

    @java.lang.Override()
    public java.lang.String toString() {
        return "ScalarProperties[" + "null: " + this.null_ + ", boolean: " + this.boolean_ + ", integer: " + this.integer + ", number: " + this.number + ", string: " + this.string + "]";
    }

    @java.lang.Override()
    public int hashCode() {
        return java.util.Objects.hash(this.null_, this.boolean_, this.integer, this.number, this.string);
    }

    @java.lang.Override()
    public boolean equals(java.lang.Object other) {
        if (this == other)
            return true;
        else if (other instanceof scalars.ScalarProperties otherScalarProperties)
            return java.util.Objects.equals(this.null_, otherScalarProperties.null_) && java.util.Objects.equals(this.boolean_, otherScalarProperties.boolean_) && java.util.Objects.equals(this.integer, otherScalarProperties.integer) && java.util.Objects.equals(this.number, otherScalarProperties.number) && java.util.Objects.equals(this.string, otherScalarProperties.string);
        else
            return false;
    }
}
