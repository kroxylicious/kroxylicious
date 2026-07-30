/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Base type for mechanism-specific configuration.
 * <p>
 * Jackson uses name-based polymorphism with the {@code mechanism} field as the
 * type discriminator. The mechanism name is the IANA-registered SASL mechanism
 * name (e.g. {@code SCRAM-SHA-256}, {@code OAUTHBEARER}).
 * </p>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "mechanism")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ScramSha256MechanismConfig.class, name = "SCRAM-SHA-256"),
        @JsonSubTypes.Type(value = ScramSha512MechanismConfig.class, name = "SCRAM-SHA-512"),
        @JsonSubTypes.Type(value = OauthBearerMechanismConfig.class, name = "OAUTHBEARER")
})
public sealed interface MechanismConfig
        permits ScramMechanismConfig, OauthBearerMechanismConfig {

    /**
     * Returns the IANA-registered mechanism name.
     *
     * @return the IANA-registered mechanism name
     */
    String mechanismName();
}
