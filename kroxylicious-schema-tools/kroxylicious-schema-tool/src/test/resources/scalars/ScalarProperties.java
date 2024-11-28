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

    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "null")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.Object null_;

    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "boolean")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.Boolean boolean_;

    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "integer")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.Long integer;

    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "number")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.Double number;

    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "string")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.String string;

    @edu.umd.cs.findbugs.annotations.NonNull()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "requiredNull", required = true)
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.Object requiredNull;

    @edu.umd.cs.findbugs.annotations.NonNull()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "requiredBoolean", required = true)
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.Boolean requiredBoolean;

    @edu.umd.cs.findbugs.annotations.NonNull()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "requiredInteger", required = true)
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.Long requiredInteger;

    @edu.umd.cs.findbugs.annotations.NonNull()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "requiredNumber", required = true)
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.Double requiredNumber;

    @edu.umd.cs.findbugs.annotations.NonNull()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "requiredString", required = true)
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.String requiredString;

    /**
     * Nullary constructor (used for deserialization).
     */
    public ScalarProperties() {
    }

    /**
     * Required properties constructor.
     * @param requiredNull The value of the {@code requiredNull} property. This is a required property.
     * @param requiredBoolean The value of the {@code requiredBoolean} property. This is a required property.
     * @param requiredInteger The value of the {@code requiredInteger} property. This is a required property.
     * @param requiredNumber The value of the {@code requiredNumber} property. This is a required property.
     * @param requiredString The value of the {@code requiredString} property. This is a required property.
     */
    public ScalarProperties(@edu.umd.cs.findbugs.annotations.NonNull() java.lang.Object requiredNull, @edu.umd.cs.findbugs.annotations.NonNull() java.lang.Boolean requiredBoolean, @edu.umd.cs.findbugs.annotations.NonNull() java.lang.Long requiredInteger, @edu.umd.cs.findbugs.annotations.NonNull() java.lang.Double requiredNumber, @edu.umd.cs.findbugs.annotations.NonNull() java.lang.String requiredString) {
        this.requiredNull = requiredNull;
        this.requiredBoolean = requiredBoolean;
        this.requiredInteger = requiredInteger;
        this.requiredNumber = requiredNumber;
        this.requiredString = requiredString;
    }

    /**
     * All properties constructor.
     * @param requiredNull The value of the {@code requiredNull} property. This is a required property.
     * @param requiredBoolean The value of the {@code requiredBoolean} property. This is a required property.
     * @param requiredInteger The value of the {@code requiredInteger} property. This is a required property.
     * @param requiredNumber The value of the {@code requiredNumber} property. This is a required property.
     * @param requiredString The value of the {@code requiredString} property. This is a required property.
     * @param null_ The value of the {@code null} property. This is an optional property.
     * @param boolean_ The value of the {@code boolean} property. This is an optional property.
     * @param integer The value of the {@code integer} property. This is an optional property.
     * @param number The value of the {@code number} property. This is an optional property.
     * @param string The value of the {@code string} property. This is an optional property.
     */
    public ScalarProperties(@edu.umd.cs.findbugs.annotations.NonNull() java.lang.Object requiredNull, @edu.umd.cs.findbugs.annotations.NonNull() java.lang.Boolean requiredBoolean, @edu.umd.cs.findbugs.annotations.NonNull() java.lang.Long requiredInteger, @edu.umd.cs.findbugs.annotations.NonNull() java.lang.Double requiredNumber, @edu.umd.cs.findbugs.annotations.NonNull() java.lang.String requiredString, @edu.umd.cs.findbugs.annotations.Nullable() java.lang.Object null_, @edu.umd.cs.findbugs.annotations.Nullable() java.lang.Boolean boolean_, @edu.umd.cs.findbugs.annotations.Nullable() java.lang.Long integer, @edu.umd.cs.findbugs.annotations.Nullable() java.lang.Double number, @edu.umd.cs.findbugs.annotations.Nullable() java.lang.String string) {
        this.requiredNull = requiredNull;
        this.requiredBoolean = requiredBoolean;
        this.requiredInteger = requiredInteger;
        this.requiredNumber = requiredNumber;
        this.requiredString = requiredString;
        this.null_ = null_;
        this.boolean_ = boolean_;
        this.integer = integer;
        this.number = number;
        this.string = string;
    }

    /**
     * Return the null.
     *
     * @return The value of this object's null.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    public java.lang.Object getNull() {
        return this.null_;
    }

    /**
     * Set the null.
     *
     *  @param null_ The new value for this object's null.
     */
    public void setNull(@edu.umd.cs.findbugs.annotations.Nullable() java.lang.Object null_) {
        this.null_ = null_;
    }

    /**
     * Return the boolean.
     *
     * @return The value of this object's boolean.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    public java.lang.Boolean getBoolean() {
        return this.boolean_;
    }

    /**
     * Set the boolean.
     *
     *  @param boolean_ The new value for this object's boolean.
     */
    public void setBoolean(@edu.umd.cs.findbugs.annotations.Nullable() java.lang.Boolean boolean_) {
        this.boolean_ = boolean_;
    }

    /**
     * Return the integer.
     *
     * @return The value of this object's integer.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    public java.lang.Long getInteger() {
        return this.integer;
    }

    /**
     * Set the integer.
     *
     *  @param integer The new value for this object's integer.
     */
    public void setInteger(@edu.umd.cs.findbugs.annotations.Nullable() java.lang.Long integer) {
        this.integer = integer;
    }

    /**
     * Return the number.
     *
     * @return The value of this object's number.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    public java.lang.Double getNumber() {
        return this.number;
    }

    /**
     * Set the number.
     *
     *  @param number The new value for this object's number.
     */
    public void setNumber(@edu.umd.cs.findbugs.annotations.Nullable() java.lang.Double number) {
        this.number = number;
    }

    /**
     * Return the string.
     *
     * @return The value of this object's string.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    public java.lang.String getString() {
        return this.string;
    }

    /**
     * Set the string.
     *
     *  @param string The new value for this object's string.
     */
    public void setString(@edu.umd.cs.findbugs.annotations.Nullable() java.lang.String string) {
        this.string = string;
    }

    /**
     * Return the requiredNull.
     *
     * @return The value of this object's requiredNull.
     */
    @edu.umd.cs.findbugs.annotations.NonNull()
    public java.lang.Object getRequiredNull() {
        return this.requiredNull;
    }

    /**
     * Set the requiredNull.
     *
     *  @param requiredNull The new value for this object's requiredNull.
     */
    public void setRequiredNull(@edu.umd.cs.findbugs.annotations.NonNull() java.lang.Object requiredNull) {
        this.requiredNull = requiredNull;
    }

    /**
     * Return the requiredBoolean.
     *
     * @return The value of this object's requiredBoolean.
     */
    @edu.umd.cs.findbugs.annotations.NonNull()
    public java.lang.Boolean getRequiredBoolean() {
        return this.requiredBoolean;
    }

    /**
     * Set the requiredBoolean.
     *
     *  @param requiredBoolean The new value for this object's requiredBoolean.
     */
    public void setRequiredBoolean(@edu.umd.cs.findbugs.annotations.NonNull() java.lang.Boolean requiredBoolean) {
        this.requiredBoolean = requiredBoolean;
    }

    /**
     * Return the requiredInteger.
     *
     * @return The value of this object's requiredInteger.
     */
    @edu.umd.cs.findbugs.annotations.NonNull()
    public java.lang.Long getRequiredInteger() {
        return this.requiredInteger;
    }

    /**
     * Set the requiredInteger.
     *
     *  @param requiredInteger The new value for this object's requiredInteger.
     */
    public void setRequiredInteger(@edu.umd.cs.findbugs.annotations.NonNull() java.lang.Long requiredInteger) {
        this.requiredInteger = requiredInteger;
    }

    /**
     * Return the requiredNumber.
     *
     * @return The value of this object's requiredNumber.
     */
    @edu.umd.cs.findbugs.annotations.NonNull()
    public java.lang.Double getRequiredNumber() {
        return this.requiredNumber;
    }

    /**
     * Set the requiredNumber.
     *
     *  @param requiredNumber The new value for this object's requiredNumber.
     */
    public void setRequiredNumber(@edu.umd.cs.findbugs.annotations.NonNull() java.lang.Double requiredNumber) {
        this.requiredNumber = requiredNumber;
    }

    /**
     * Return the requiredString.
     *
     * @return The value of this object's requiredString.
     */
    @edu.umd.cs.findbugs.annotations.NonNull()
    public java.lang.String getRequiredString() {
        return this.requiredString;
    }

    /**
     * Set the requiredString.
     *
     *  @param requiredString The new value for this object's requiredString.
     */
    public void setRequiredString(@edu.umd.cs.findbugs.annotations.NonNull() java.lang.String requiredString) {
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
