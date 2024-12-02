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

    @edu.umd.cs.findbugs.annotations.Nullable
    private java.lang.String password;

    @edu.umd.cs.findbugs.annotations.Nullable
    private java.lang.String filePath;

    /**
     * All properties constructor.
     * @param password The value of the {@code password} property. This is an optional property.
     * @param filePath The value of the {@code filePath} property. This is an optional property.
     */
    @com.fasterxml.jackson.annotation.JsonCreator
    public PasswordProvider(@com.fasterxml.jackson.annotation.JsonProperty(value = "password") @edu.umd.cs.findbugs.annotations.Nullable java.lang.String password, @com.fasterxml.jackson.annotation.JsonProperty(value = "filePath") @edu.umd.cs.findbugs.annotations.Nullable java.lang.String filePath) {
        this.password = password;
        this.filePath = filePath;
    }

    /**
     * DEPRECATED. The password.
     * @return The value of this object's password.
     */
    @edu.umd.cs.findbugs.annotations.Nullable
    @com.fasterxml.jackson.annotation.JsonProperty(value = "password")
    public java.lang.String password() {
        return this.password;
    }

    /**
     * DEPRECATED. The password.
     *  @param password The new value for this object's password.
     */
    public void password(@edu.umd.cs.findbugs.annotations.Nullable java.lang.String password) {
        this.password = password;
    }

    /**
     * A file path pointing to a UTF-8 encoded file containing the password
     * @return The value of this object's filePath.
     */
    @edu.umd.cs.findbugs.annotations.Nullable
    @com.fasterxml.jackson.annotation.JsonProperty(value = "filePath")
    public java.lang.String filePath() {
        return this.filePath;
    }

    /**
     * A file path pointing to a UTF-8 encoded file containing the password
     *  @param filePath The new value for this object's filePath.
     */
    public void filePath(@edu.umd.cs.findbugs.annotations.Nullable java.lang.String filePath) {
        this.filePath = filePath;
    }

    @java.lang.Override
    public java.lang.String toString() {
        return "PasswordProvider[" + "password: " + this.password + ", filePath: " + this.filePath + "]";
    }

    @java.lang.Override
    public int hashCode() {
        return java.util.Objects.hash(this.password, this.filePath);
    }

    @java.lang.Override
    public boolean equals(java.lang.Object other) {
        if (this == other)
            return true;
        else if (other instanceof xref.PasswordProvider otherPasswordProvider)
            return java.util.Objects.equals(this.password, otherPasswordProvider.password) && java.util.Objects.equals(this.filePath, otherPasswordProvider.filePath);
        else
            return false;
    }
}
