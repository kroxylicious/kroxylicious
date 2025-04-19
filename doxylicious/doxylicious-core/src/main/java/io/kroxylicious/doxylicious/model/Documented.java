/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doxylicious.model;

import edu.umd.cs.findbugs.annotations.Nullable;

public interface Documented {
    @Nullable
    String adoc();
}
