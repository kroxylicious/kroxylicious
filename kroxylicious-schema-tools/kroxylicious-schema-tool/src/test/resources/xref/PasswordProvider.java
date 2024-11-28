/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package xref;

/**
 * Specification of a password
 */
@javax.annotation.processing.Generated("io.kroxylicious.tools.schema.compiler.CodeGen")
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "password", "filePath" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
public class PasswordProvider {

    @com.fasterxml.jackson.annotation.JsonProperty(value = "password")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.String password;

    @com.fasterxml.jackson.annotation.JsonProperty(value = "filePath")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.String filePath;

    /**
     * DEPRECATED. The password.
     * @return The value of this object's password.
     */
    public java.lang.String getPassword() {
        return this.password;
    }

    /**
     * DEPRECATED. The password.
     *  @param password The new value for this object's password.
     */
    public void setPassword(java.lang.String password) {
        this.password = password;
    }

    /**
     * A file path pointing to a UTF-8 encoded file containing the password
     * @return The value of this object's filePath.
     */
    public java.lang.String getFilePath() {
        return this.filePath;
    }

    /**
     * A file path pointing to a UTF-8 encoded file containing the password
     *  @param filePath The new value for this object's filePath.
     */
    public void setFilePath(java.lang.String filePath) {
        this.filePath = filePath;
    }

    @java.lang.Override()
    public java.lang.String toString() {
        return "PasswordProvider[" + "password: " + this.password + ", filePath: " + this.filePath + "]";
    }

    @java.lang.Override()
    public int hashCode() {
        return java.util.Objects.hash(this.password, this.filePath);
    }

    @java.lang.Override()
    public boolean equals(java.lang.Object other) {
        if (this == other)
            return true;
        else if (other instanceof xref.PasswordProvider otherPasswordProvider)
            return java.util.Objects.equals(this.password, otherPasswordProvider.password) && java.util.Objects.equals(this.filePath, otherPasswordProvider.filePath);
        else
            return false;
    }
}