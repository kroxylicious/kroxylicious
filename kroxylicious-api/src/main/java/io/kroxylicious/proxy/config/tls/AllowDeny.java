/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.util.List;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Class to keep track of allow and deny lists meant for use when building context for SSLEngine.
 *
 * @param allowed specifies a list of allowed objects.
 * @param denied specifies a list of denied objects.
 */
public record AllowDeny<T>(@Nullable List<T> allowed, @Nullable List<T> denied) {}
