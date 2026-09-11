/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config.admin;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Enables the Prometheus scrape endpoint on the management HTTP server. Its presence in the
 * configuration is what enables the endpoint; there are currently no further options.
 */
@JsonSerialize
public record PrometheusMetricsConfig() {}
