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
 * Jackson uses deduction-based polymorphism to determine the concrete type
 * from the fields present in the configuration. Each mechanism type has
 * distinct required fields that serve as discriminators.
 * </p>
 *
 * @see ScramMechanismConfig
 * @see OauthBearerMechanismConfig
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.DEDUCTION)
@JsonSubTypes({
        @JsonSubTypes.Type(ScramMechanismConfig.class),
        @JsonSubTypes.Type(OauthBearerMechanismConfig.class)
})
public sealed interface MechanismConfig permits ScramMechanismConfig, OauthBearerMechanismConfig {
}
