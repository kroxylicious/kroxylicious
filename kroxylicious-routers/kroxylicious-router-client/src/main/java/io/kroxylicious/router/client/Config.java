/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.router.client;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration for {@link ClientRouter}.
 *
 * @param routingRules ordered rules, evaluated in declaration order against the connection's
 *                     pinned {@code client.id}; the router uses the route of the first matching
 *                     rule.
 */
public record Config(@JsonProperty(required = true) List<Rule> routingRules) {}
