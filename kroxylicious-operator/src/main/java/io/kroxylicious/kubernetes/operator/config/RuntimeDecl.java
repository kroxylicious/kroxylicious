/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.config;

import java.util.List;

/**
 * Information about a proxy runtime (presumably in a container image)
 * which the operator to knows by configuration.
 * @param filterApis The filter kinds which this operator instance knows about.
 */
public record RuntimeDecl(List<FilterApiDecl> filterApis) {}
