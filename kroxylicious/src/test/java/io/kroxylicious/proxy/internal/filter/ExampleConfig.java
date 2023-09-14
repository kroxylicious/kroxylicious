/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import io.kroxylicious.proxy.config.BaseConfig;

@JsonSerialize
public class ExampleConfig extends BaseConfig {
}
