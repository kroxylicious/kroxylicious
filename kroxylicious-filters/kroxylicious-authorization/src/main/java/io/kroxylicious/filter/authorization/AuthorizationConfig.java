/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.authorizer.service.AuthorizerService;
import io.kroxylicious.proxy.plugin.PluginImplConfig;
import io.kroxylicious.proxy.plugin.PluginImplName;

public record AuthorizationConfig(
                                  @JsonProperty(required = true) @PluginImplName(AuthorizerService.class) String authorizer,
                                  @PluginImplConfig(implNameProperty = "authorizer") Object authorizerConfig) {

}
