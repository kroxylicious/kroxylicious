/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.opa;

import edu.umd.cs.findbugs.annotations.Nullable;

public record OpaAuthorizerConfig(
                                  String opaFile,
                                  @Nullable String dataFile) {}
