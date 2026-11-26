/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.networking;

/**
 * A shared SNI container port: the port number and the Kubernetes port name used together
 * when exposing the port on the proxy pod.
 *
 * @param containerPort the port number to expose on the proxy container
 * @param portName the Kubernetes port name for the container port
 */
public record SharedSniContainerPort(int containerPort, String portName) {}
