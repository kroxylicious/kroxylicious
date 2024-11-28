/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package xref;

/**
 * A class with scalar properties
 */
@javax.annotation.processing.Generated("io.kroxylicious.tools.schema.compiler.CodeGen")
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "myBoolean", "myList", "myObject" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
public class Xref {

    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "myBoolean")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.Boolean myBoolean;

    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "myList")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.util.List<java.lang.Long> myList;

    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "myObject")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private xref.MyObject myObject;

    /**
     * Required properties constructor.
     */
    public Xref() {
    }

    /**
     * All properties constructor.
     * @param myBoolean The value of the {@code myBoolean} property. This is an optional property.
     * @param myList The value of the {@code myList} property. This is an optional property.
     * @param myObject The value of the {@code myObject} property. This is an optional property.
     */
    public Xref(@edu.umd.cs.findbugs.annotations.Nullable() java.lang.Boolean myBoolean, @edu.umd.cs.findbugs.annotations.Nullable() java.util.List<java.lang.Long> myList, @edu.umd.cs.findbugs.annotations.Nullable() xref.MyObject myObject) {
        this.myBoolean = myBoolean;
        this.myList = myList;
        this.myObject = myObject;
    }

    /**
     * A class with scalar properties
     * @return The value of this object's myBoolean.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    public java.lang.Boolean getMyBoolean() {
        return this.myBoolean;
    }

    /**
     * A class with scalar properties
     *  @param myBoolean The new value for this object's myBoolean.
     */
    public void setMyBoolean(@edu.umd.cs.findbugs.annotations.Nullable() java.lang.Boolean myBoolean) {
        this.myBoolean = myBoolean;
    }

    /**
     * A class with scalar properties
     * @return The value of this object's myList.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    public java.util.List<java.lang.Long> getMyList() {
        return this.myList;
    }

    /**
     * A class with scalar properties
     *  @param myList The new value for this object's myList.
     */
    public void setMyList(@edu.umd.cs.findbugs.annotations.Nullable() java.util.List<java.lang.Long> myList) {
        this.myList = myList;
    }

    /**
     * A class with scalar properties
     * @return The value of this object's myObject.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    public xref.MyObject getMyObject() {
        return this.myObject;
    }

    /**
     * A class with scalar properties
     *  @param myObject The new value for this object's myObject.
     */
    public void setMyObject(@edu.umd.cs.findbugs.annotations.Nullable() xref.MyObject myObject) {
        this.myObject = myObject;
    }

    @java.lang.Override()
    public java.lang.String toString() {
        return "Xref[" + "myBoolean: " + this.myBoolean + ", myList: " + this.myList + ", myObject: " + this.myObject + "]";
    }

    @java.lang.Override()
    public int hashCode() {
        return java.util.Objects.hash(this.myBoolean, this.myList, this.myObject);
    }

    @java.lang.Override()
    public boolean equals(java.lang.Object other) {
        if (this == other)
            return true;
        else if (other instanceof xref.Xref otherXref)
            return java.util.Objects.equals(this.myBoolean, otherXref.myBoolean) && java.util.Objects.equals(this.myList, otherXref.myList) && java.util.Objects.equals(this.myObject, otherXref.myObject);
        else
            return false;
    }
}
