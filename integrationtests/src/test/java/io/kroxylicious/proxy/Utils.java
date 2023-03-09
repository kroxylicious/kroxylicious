/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.config.Configuration;

public class Utils {
    public static KafkaProxy startProxy(String config) throws InterruptedException {
        Configuration proxyConfig = new ConfigParser().parseConfiguration(config);

        KafkaProxy kafkaProxy = new KafkaProxy(proxyConfig);
        kafkaProxy.startup();

        return kafkaProxy;
    }
}
