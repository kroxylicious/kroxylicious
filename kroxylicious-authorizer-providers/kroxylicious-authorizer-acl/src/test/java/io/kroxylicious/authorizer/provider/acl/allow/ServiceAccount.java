/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl.allow;

import io.kroxylicious.proxy.authentication.Principal;

public record ServiceAccount(String name) implements Principal {}
