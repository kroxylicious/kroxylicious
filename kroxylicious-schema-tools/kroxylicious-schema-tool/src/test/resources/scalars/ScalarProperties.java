/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package scalars;

/**
 * A class with scalar properties
 */
@javax.annotation.processing.Generated("io.kroxylicious.tools.schema.compiler.CodeGen")
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "null", "boolean", "integer", "number", "string", "requiredNull", "requiredBoolean", "requiredInteger", "requiredNumber", "requiredString" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
public class ScalarProperties {

    @com.fasterxml.jackson.annotation.JsonProperty(value = "null")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.Object null_;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "boolean")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.Boolean boolean_;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "integer")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.Long integer;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "number")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.Double number;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "string")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.String string;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "requiredNull", required = true)
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.Object requiredNull;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "requiredBoolean", required = true)
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.Boolean requiredBoolean;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "requiredInteger", required = true)
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.Long requiredInteger;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "requiredNumber", required = true)
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.Double requiredNumber;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "requiredString", required = true)
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.String requiredString;

    /**
     * Return the null.
     *
     * @return The value of this object's null.
     */
    public java.lang.Object getNull() {
        return this.null_;
    }

    /**
     * Set the null.
     *
     *  @param null_ The new value for this object's null.
     */
    public void setNull(java.lang.Object null_) {
        this.null_ = null_;
    }

    /**
     * Return the boolean.
     *
     * @return The value of this object's boolean.
     */
    public java.lang.Boolean getBoolean() {
        return this.boolean_;
    }

    /**
     * Set the boolean.
     *
     *  @param boolean_ The new value for this object's boolean.
     */
    public void setBoolean(java.lang.Boolean boolean_) {
        this.boolean_ = boolean_;
    }

    /**
     * Return the integer.
     *
     * @return The value of this object's integer.
     */
    public java.lang.Long getInteger() {
        return this.integer;
    }

    /**
     * Set the integer.
     *
     *  @param integer The new value for this object's integer.
     */
    public void setInteger(java.lang.Long integer) {
        this.integer = integer;
    }

    /**
     * Return the number.
     *
     * @return The value of this object's number.
     */
    public java.lang.Double getNumber() {
        return this.number;
    }

    /**
     * Set the number.
     *
     *  @param number The new value for this object's number.
     */
    public void setNumber(java.lang.Double number) {
        this.number = number;
    }

    /**
     * Return the string.
     *
     * @return The value of this object's string.
     */
    public java.lang.String getString() {
        return this.string;
    }

    /**
     * Set the string.
     *
     *  @param string The new value for this object's string.
     */
    public void setString(java.lang.String string) {
        this.string = string;
    }

    /**
     * Return the requiredNull.
     *
     * @return The value of this object's requiredNull.
     */
    public java.lang.Object getRequiredNull() {
        return this.requiredNull;
    }

    /**
     * Set the requiredNull.
     *
     *  @param requiredNull The new value for this object's requiredNull.
     */
    public void setRequiredNull(java.lang.Object requiredNull) {
        this.requiredNull = requiredNull;
    }

    /**
     * Return the requiredBoolean.
     *
     * @return The value of this object's requiredBoolean.
     */
    public java.lang.Boolean getRequiredBoolean() {
        return this.requiredBoolean;
    }

    /**
     * Set the requiredBoolean.
     *
     *  @param requiredBoolean The new value for this object's requiredBoolean.
     */
    public void setRequiredBoolean(java.lang.Boolean requiredBoolean) {
        this.requiredBoolean = requiredBoolean;
    }

    /**
     * Return the requiredInteger.
     *
     * @return The value of this object's requiredInteger.
     */
    public java.lang.Long getRequiredInteger() {
        return this.requiredInteger;
    }

    /**
     * Set the requiredInteger.
     *
     *  @param requiredInteger The new value for this object's requiredInteger.
     */
    public void setRequiredInteger(java.lang.Long requiredInteger) {
        this.requiredInteger = requiredInteger;
    }

    /**
     * Return the requiredNumber.
     *
     * @return The value of this object's requiredNumber.
     */
    public java.lang.Double getRequiredNumber() {
        return this.requiredNumber;
    }

    /**
     * Set the requiredNumber.
     *
     *  @param requiredNumber The new value for this object's requiredNumber.
     */
    public void setRequiredNumber(java.lang.Double requiredNumber) {
        this.requiredNumber = requiredNumber;
    }

    /**
     * Return the requiredString.
     *
     * @return The value of this object's requiredString.
     */
    public java.lang.String getRequiredString() {
        return this.requiredString;
    }

    /**
     * Set the requiredString.
     *
     *  @param requiredString The new value for this object's requiredString.
     */
    public void setRequiredString(java.lang.String requiredString) {
        this.requiredString = requiredString;
    }

    @java.lang.Override()
    public java.lang.String toString() {
        return "ScalarProperties[" + "null: " + this.null_ + ", boolean: " + this.boolean_ + ", integer: " + this.integer + ", number: " + this.number + ", string: " + this.string + ", requiredNull: " + this.requiredNull + ", requiredBoolean: " + this.requiredBoolean + ", requiredInteger: " + this.requiredInteger + ", requiredNumber: " + this.requiredNumber + ", requiredString: " + this.requiredString + "]";
    }

    @java.lang.Override()
    public int hashCode() {
        return java.util.Objects.hash(this.null_, this.boolean_, this.integer, this.number, this.string, this.requiredNull, this.requiredBoolean, this.requiredInteger, this.requiredNumber, this.requiredString);
    }

    @java.lang.Override()
    public boolean equals(java.lang.Object other) {
        if (this == other)
            return true;
        else if (other instanceof scalars.ScalarProperties otherScalarProperties)
            return java.util.Objects.equals(this.null_, otherScalarProperties.null_) && java.util.Objects.equals(this.boolean_, otherScalarProperties.boolean_) && java.util.Objects.equals(this.integer, otherScalarProperties.integer) && java.util.Objects.equals(this.number, otherScalarProperties.number) && java.util.Objects.equals(this.string, otherScalarProperties.string) && java.util.Objects.equals(this.requiredNull, otherScalarProperties.requiredNull) && java.util.Objects.equals(this.requiredBoolean, otherScalarProperties.requiredBoolean) && java.util.Objects.equals(this.requiredInteger, otherScalarProperties.requiredInteger) && java.util.Objects.equals(this.requiredNumber, otherScalarProperties.requiredNumber) && java.util.Objects.equals(this.requiredString, otherScalarProperties.requiredString);
        else
            return false;
    }
}