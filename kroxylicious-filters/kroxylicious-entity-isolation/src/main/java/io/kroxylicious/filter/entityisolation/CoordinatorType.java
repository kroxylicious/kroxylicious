/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.entityisolation;

/**
 * CoordinatorType enum corresponding to {@code FindCoordinatorRequest#CoordinatorType} in kafka-clients
 */
public enum CoordinatorType {
    GROUP((byte) 0),
    TRANSACTION((byte) 1),
    SHARE((byte) 2);

    final byte id;

    CoordinatorType(byte id) {
        this.id = id;
    }

    public static CoordinatorType forId(byte id) {
        return switch (id) {
            case 0 -> GROUP;
            case 1 -> TRANSACTION;
            case 2 -> SHARE;
            default -> throw new IllegalArgumentException("Unknown coordinator type received: " + id);
        };
    }

    public byte id() {
        return id;
    }

}