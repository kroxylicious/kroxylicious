/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter.audit;


import edu.umd.cs.findbugs.annotations.NonNull;

import java.util.List;

public interface EventSink {
    void acceptEvents(@NonNull List<Event> events);
}