/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package trickynaming;

/**
 * The schema of this version
 */
@javax.annotation.processing.Generated("io.kroxylicious.tools.schema.compiler.CodeGen")
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.annotation.JsonPropertyOrder({ "openAPIV3Schema" })
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
public class CrdSchema {

    @com.fasterxml.jackson.annotation.JsonProperty(value = "openAPIV3Schema")
    @com.fasterxml.jackson.annotation.JsonSetter(nulls = com.fasterxml.jackson.annotation.Nulls.SKIP)
    private trickynaming.TrickySpecVersionSchemaOpenAPIV3Schema openAPIV3Schema;

    /**
     * Return the openAPIV3Schema.
     *
     * @return The value of this object's openAPIV3Schema.
     */
    public trickynaming.TrickySpecVersionSchemaOpenAPIV3Schema getOpenAPIV3Schema() {
        return this.openAPIV3Schema;
    }

    /**
     * Set the openAPIV3Schema.
     *
     *  @param openAPIV3Schema The new value for this object's openAPIV3Schema.
     */
    public void setOpenAPIV3Schema(trickynaming.TrickySpecVersionSchemaOpenAPIV3Schema openAPIV3Schema) {
        this.openAPIV3Schema = openAPIV3Schema;
    }

    @java.lang.Override()
    public java.lang.String toString() {
        return "CrdSchema[" + "openAPIV3Schema: " + this.openAPIV3Schema + "]";
    }

    @java.lang.Override()
    public int hashCode() {
        return java.util.Objects.hash(this.openAPIV3Schema);
    }

    @java.lang.Override()
    public boolean equals(java.lang.Object other) {
        if (this == other)
            return true;
        else if (other instanceof trickynaming.CrdSchema otherCrdSchema)
            return java.util.Objects.equals(this.openAPIV3Schema, otherCrdSchema.openAPIV3Schema);
        else
            return false;
    }
}