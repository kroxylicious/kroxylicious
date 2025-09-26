/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters.sasl.inspection;

/**
 * Config for the Sasl Initiation Filter.
 *
 * @param enableInsecureMechanisms if true, mechanisms considered insecure will be enabled.
 */
public record Config(boolean enableInsecureMechanisms) {}
