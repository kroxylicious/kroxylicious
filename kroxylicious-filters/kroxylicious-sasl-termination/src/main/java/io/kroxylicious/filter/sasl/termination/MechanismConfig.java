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
 * name (e.g. {@code OAUTHBEARER}).
 * </p>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "mechanism")
@JsonSubTypes({
        @JsonSubTypes.Type(value = OauthBearerMechanismConfig.class, name = OauthBearerMechanismConfig.MECHANISM_NAME)
})
public sealed interface MechanismConfig
        permits OauthBearerMechanismConfig {

    /**
     * Returns the IANA-registered mechanism name.
     *
     * @return the IANA-registered mechanism name
     */
    String mechanismName();
}
