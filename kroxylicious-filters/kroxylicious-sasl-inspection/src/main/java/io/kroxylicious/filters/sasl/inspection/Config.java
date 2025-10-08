/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters.sasl.inspection;

import java.util.Set;

/**
 * Config for the Sasl Initiation Filter.
 *
 * @param enabledMechanisms set of SASL mechanisms to enable. Refer to the mechanism by its
 * IANA registered name.  If enabledMechanisms is null, the system will automatically enable
 * the mechanism from all {@link SaslObserverFactory}s with
 * {@link SaslObserverFactory#transmitsCredentialInCleartext()} values of false.
 */
public record Config(Set<String> enabledMechanisms) {}
