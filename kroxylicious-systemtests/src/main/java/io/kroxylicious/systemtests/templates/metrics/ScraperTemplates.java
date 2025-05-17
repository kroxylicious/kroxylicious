/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.metrics;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousKafkaClusterRefTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousKafkaProxyIngressTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousKafkaProxyTemplates;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousVirtualKafkaClusterTemplates;

public class ScraperTemplates {
    private static final ResourceManager resourceManager = ResourceManager.getInstance();

    private ScraperTemplates() {
    }

    /**
     * Deploy port identifies node with no filters.
     *
     * @param namespace the namespace
     * @param clusterName the cluster name
     */
    public static void deployPortIdentifiesNodeWithNoFilters(String namespace, String clusterName) {
        resourceManager.createResourceFromBuilder(
                KroxyliciousKafkaProxyTemplates.defaultKafkaProxyCR(namespace, clusterName),
                KroxyliciousKafkaProxyIngressTemplates.defaultKafkaProxyIngressCR(namespace, Constants.SCRAPER_LABEL_VALUE,
                        clusterName),
                KroxyliciousKafkaClusterRefTemplates.defaultKafkaClusterRefCR(namespace, clusterName),
                KroxyliciousVirtualKafkaClusterTemplates.defaultVirtualKafkaClusterCR(namespace, clusterName, clusterName,
                        clusterName, Constants.SCRAPER_LABEL_VALUE));
    }
}
