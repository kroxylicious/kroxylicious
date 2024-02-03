/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.audit;

import edu.umd.cs.findbugs.annotations.NonNull;

import org.apache.kafka.common.protocol.ApiKeys;

import java.util.List;
import java.util.UUID;

public record Event(@NonNull UUID eventId, @NonNull ApiKeys apiKey, @NonNull List<Entity> entities, short result) {
}
