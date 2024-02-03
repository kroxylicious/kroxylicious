/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.audit;

// TODO maybe an (optional) entityUuid
public record Entity(EntityType entityType, String entityName)  {

    public enum EntityType {
        TOPIC_NAME
    }
}
