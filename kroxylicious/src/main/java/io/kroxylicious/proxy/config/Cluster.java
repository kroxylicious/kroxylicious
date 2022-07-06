/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

public class Cluster {

    private final String address;

    public Cluster(String address) {
        this.address = address;
    }

    public String address() {
        return address;
    }

    @Override
    public String toString() {
        return "Cluster [address=" + address + "]";
    }
}
