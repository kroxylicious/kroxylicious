/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.networking;

public class NetworkPlanningException extends RuntimeException {

    public NetworkPlanningException(String message) {
        super(message);
    }

}
