/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.audit;

import edu.umd.cs.findbugs.annotations.CheckReturnValue;

public interface Contextual<S extends Contextual<S> & Loggable> {
    @CheckReturnValue(explanation = "log() must be called on the result of this method")
    S addToContext(String key, String value);
}
