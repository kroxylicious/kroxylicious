/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.testplugins;

import java.util.Map;

public record SaslPlainTerminationConfig(Map<String, String> userNameToPassword) {}
