/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package xref;

/**
 * Auto-generated class representing the schema at /definitions/Tls/properties/key.
 */
@javax.annotation.processing.Generated("io.kroxylicious.tools.schema.compiler.CodeGen")
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "privateKeyFile", "certificateFile", "storeFile", "storePassword", "keyPassword", "storeType" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
public class ConfigKey {

    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "privateKeyFile")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.String privateKeyFile;

    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "certificateFile")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.String certificateFile;

    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "storeFile")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.String storeFile;

    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "storePassword")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private xref.PasswordProvider storePassword;

    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "keyPassword")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private xref.PasswordProvider keyPassword;

    @edu.umd.cs.findbugs.annotations.Nullable()
    @com.fasterxml.jackson.annotation.JsonProperty(value = "storeType")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private java.lang.String storeType;

    /**
     * Required properties constructor.
     */
    public ConfigKey() {
    }

    /**
     * All properties constructor.
     * @param privateKeyFile The value of the {@code privateKeyFile} property. This is an optional property.
     * @param certificateFile The value of the {@code certificateFile} property. This is an optional property.
     * @param storeFile The value of the {@code storeFile} property. This is an optional property.
     * @param storePassword The value of the {@code storePassword} property. This is an optional property.
     * @param keyPassword The value of the {@code keyPassword} property. This is an optional property.
     * @param storeType The value of the {@code storeType} property. This is an optional property.
     */
    public ConfigKey(@edu.umd.cs.findbugs.annotations.Nullable() java.lang.String privateKeyFile, @edu.umd.cs.findbugs.annotations.Nullable() java.lang.String certificateFile, @edu.umd.cs.findbugs.annotations.Nullable() java.lang.String storeFile, @edu.umd.cs.findbugs.annotations.Nullable() xref.PasswordProvider storePassword, @edu.umd.cs.findbugs.annotations.Nullable() xref.PasswordProvider keyPassword, @edu.umd.cs.findbugs.annotations.Nullable() java.lang.String storeType) {
        this.privateKeyFile = privateKeyFile;
        this.certificateFile = certificateFile;
        this.storeFile = storeFile;
        this.storePassword = storePassword;
        this.keyPassword = keyPassword;
        this.storeType = storeType;
    }

    /**
     * location of a file containing the private key.
     * @return The value of this object's privateKeyFile.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    public java.lang.String getPrivateKeyFile() {
        return this.privateKeyFile;
    }

    /**
     * location of a file containing the private key.
     *  @param privateKeyFile The new value for this object's privateKeyFile.
     */
    public void setPrivateKeyFile(@edu.umd.cs.findbugs.annotations.Nullable() java.lang.String privateKeyFile) {
        this.privateKeyFile = privateKeyFile;
    }

    /**
     * location of a file containing the certificate and intermediates.
     * @return The value of this object's certificateFile.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    public java.lang.String getCertificateFile() {
        return this.certificateFile;
    }

    /**
     * location of a file containing the certificate and intermediates.
     *  @param certificateFile The new value for this object's certificateFile.
     */
    public void setCertificateFile(@edu.umd.cs.findbugs.annotations.Nullable() java.lang.String certificateFile) {
        this.certificateFile = certificateFile;
    }

    /**
     * location of a key store, or reference to a PEM file containing both private-key and certificate/intermediates.
     * @return The value of this object's storeFile.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    public java.lang.String getStoreFile() {
        return this.storeFile;
    }

    /**
     * location of a key store, or reference to a PEM file containing both private-key and certificate/intermediates.
     *  @param storeFile The new value for this object's storeFile.
     */
    public void setStoreFile(@edu.umd.cs.findbugs.annotations.Nullable() java.lang.String storeFile) {
        this.storeFile = storeFile;
    }

    /**
     * Specification of a password
     * @return The value of this object's storePassword.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    public xref.PasswordProvider getStorePassword() {
        return this.storePassword;
    }

    /**
     * Specification of a password
     *  @param storePassword The new value for this object's storePassword.
     */
    public void setStorePassword(@edu.umd.cs.findbugs.annotations.Nullable() xref.PasswordProvider storePassword) {
        this.storePassword = storePassword;
    }

    /**
     * Specification of a password
     * @return The value of this object's keyPassword.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    public xref.PasswordProvider getKeyPassword() {
        return this.keyPassword;
    }

    /**
     * Specification of a password
     *  @param keyPassword The new value for this object's keyPassword.
     */
    public void setKeyPassword(@edu.umd.cs.findbugs.annotations.Nullable() xref.PasswordProvider keyPassword) {
        this.keyPassword = keyPassword;
    }

    /**
     * specifies the server key type.
     * Legal values are those types supported by the platform {@link java.security.KeyStore},
     * and PEM (for X-509 certificates express in PEM format).
     *
     * @return The value of this object's storeType.
     */
    @edu.umd.cs.findbugs.annotations.Nullable()
    public java.lang.String getStoreType() {
        return this.storeType;
    }

    /**
     * specifies the server key type.
     * Legal values are those types supported by the platform {@link java.security.KeyStore},
     * and PEM (for X-509 certificates express in PEM format).
     *
     *  @param storeType The new value for this object's storeType.
     */
    public void setStoreType(@edu.umd.cs.findbugs.annotations.Nullable() java.lang.String storeType) {
        this.storeType = storeType;
    }

    @java.lang.Override()
    public java.lang.String toString() {
        return "ConfigKey[" + "privateKeyFile: " + this.privateKeyFile + ", certificateFile: " + this.certificateFile + ", storeFile: " + this.storeFile + ", storePassword: " + this.storePassword + ", keyPassword: " + this.keyPassword + ", storeType: " + this.storeType + "]";
    }

    @java.lang.Override()
    public int hashCode() {
        return java.util.Objects.hash(this.privateKeyFile, this.certificateFile, this.storeFile, this.storePassword, this.keyPassword, this.storeType);
    }

    @java.lang.Override()
    public boolean equals(java.lang.Object other) {
        if (this == other)
            return true;
        else if (other instanceof xref.ConfigKey otherConfigKey)
            return java.util.Objects.equals(this.privateKeyFile, otherConfigKey.privateKeyFile) && java.util.Objects.equals(this.certificateFile, otherConfigKey.certificateFile) && java.util.Objects.equals(this.storeFile, otherConfigKey.storeFile) && java.util.Objects.equals(this.storePassword, otherConfigKey.storePassword) && java.util.Objects.equals(this.keyPassword, otherConfigKey.keyPassword) && java.util.Objects.equals(this.storeType, otherConfigKey.storeType);
        else
            return false;
    }
}
