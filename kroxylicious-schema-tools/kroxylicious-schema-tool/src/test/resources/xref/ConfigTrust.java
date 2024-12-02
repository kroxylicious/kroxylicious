/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package xref;

/**
 * Auto-generated class representing the schema at /definitions/Tls/properties/trust.
 */
@javax.annotation.processing.Generated("io.kroxylicious.tools.schema.compiler.CodeGen")
@com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = com.fasterxml.jackson.databind.JsonDeserializer.None.class)
public class ConfigTrust {

    /**
     * All properties constructor.
     */
    @com.fasterxml.jackson.annotation.JsonCreator
    public ConfigTrust() {
    }

    @java.lang.Override
    public java.lang.String toString() {
        return "ConfigTrust[" + "]";
    }

    @java.lang.Override
    public int hashCode() {
        return java.util.Objects.hash();
    }

    @java.lang.Override
    public boolean equals(java.lang.Object other) {
        if (this == other)
            return true;
        else if (other instanceof xref.ConfigTrust otherConfigTrust)
            return true;
        else
            return false;
    }
}
