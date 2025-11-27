/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters.sasl.inspection;

import java.util.Set;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for the Sasl Initiation Filter.
 *
 * @param enabledMechanisms set of SASL mechanisms to enable. Refer to the mechanism by its
 * IANA registered name.  If enabledMechanisms is null, the system will automatically enable
 * the mechanism from all {@link SaslObserverFactory}s with
 * {@link SaslObserverFactory#transmitsCredentialInCleartext()} values of false.
 * @param subjectBuilder The name of a plugin class implementing {@link io.kroxylicious.proxy.authentication.SaslSubjectBuilderService}
 * @param subjectBuilderConfig The configuration for the SaslSubjectBuilderService.
 * @param requireAuthentication If true then successful authentication will be required before the filter
 * will forward any requests other than those strictly required to perform SASL authentication.
 * If false then the filter will forward all requests regardless of whether
 * SASL authentication has been attempted or was successful. Defaults to true.
 */
public record Config(@Nullable Set<String> enabledMechanisms,
                     @Nullable String subjectBuilder,
                     @Nullable Object subjectBuilderConfig,
                     @Nullable Boolean requireAuthentication) {}
