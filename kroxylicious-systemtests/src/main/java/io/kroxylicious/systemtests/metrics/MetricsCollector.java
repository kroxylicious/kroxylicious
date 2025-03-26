/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.metrics;

import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.Message;

import io.fabric8.kubernetes.api.model.LabelSelector;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.enums.ComponentType;
import io.kroxylicious.systemtests.executor.Exec;
import io.kroxylicious.systemtests.executor.ExecResult;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.cmdKubeClient;
import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.awaitility.Awaitility.await;

/**
 * The type Metrics collector.
 */
public class MetricsCollector {

    private static final Logger LOGGER = LogManager.getLogger(MetricsCollector.class);

    private final String namespaceName;
    private final String scraperPodName;
    private final ComponentType componentType;
    private final String componentName;
    private final int metricsPort;
    private final String metricsPath;
    private Map<String, String> collectedData;
    private final LabelSelector componentLabelSelector;

    /**
     * The type Builder.
     */
    public static class Builder {
        private String namespaceName;
        private String scraperPodName;
        private ComponentType componentType;
        private String componentName;
        private int metricsPort;
        private String metricsPath;

        /**
         * With namespace name builder.
         *
         * @param namespaceName the namespace name
         * @return the builder
         */
        public Builder withNamespaceName(String namespaceName) {
            this.namespaceName = namespaceName;
            return this;
        }

        /**
         * With scraper pod name builder.
         *
         * @param scraperPodName the scraper pod name
         * @return the builder
         */
        public Builder withScraperPodName(String scraperPodName) {
            this.scraperPodName = scraperPodName;
            return this;
        }

        /**
         * With component type builder.
         *
         * @param componentType the component type
         * @return the builder
         */
        public Builder withComponentType(ComponentType componentType) {
            this.componentType = componentType;
            return this;
        }

        /**
         * With component name builder.
         *
         * @param componentName the component name
         * @return the builder
         */
        public Builder withComponentName(String componentName) {
            this.componentName = componentName;
            return this;
        }

        /**
         * With metrics port builder.
         *
         * @param metricsPort the metrics port
         * @return the builder
         */
        public Builder withMetricsPort(int metricsPort) {
            this.metricsPort = metricsPort;
            return this;
        }

        /**
         * With metrics path builder.
         *
         * @param metricsPath the metrics path
         * @return the builder
         */
        public Builder withMetricsPath(String metricsPath) {
            this.metricsPath = metricsPath;
            return this;
        }

        /**
         * Build metrics collector.
         *
         * @return the metrics collector
         */
        public MetricsCollector build() {
            return new MetricsCollector(this);
        }
    }

    /**
     * Gets namespace name.
     *
     * @return the namespace name
     */
    public String getNamespaceName() {
        return namespaceName;
    }

    /**
     * Gets collected data.
     *
     * @return the collected data
     */
    public Map<String, String> getCollectedData() {
        return collectedData;
    }

    /**
     * Instantiates a new Metrics collector.
     *
     * @param builder the builder
     */
    protected MetricsCollector(Builder builder) {
        Objects.requireNonNull(builder.componentType, "Component type not set");

        if (Optional.ofNullable(builder.scraperPodName).isEmpty()) {
            throw new InvalidParameterException("Scraper Pod name is not set");
        }

        scraperPodName = builder.scraperPodName;
        namespaceName = Optional.ofNullable(builder.namespaceName).orElse(kubeClient().getNamespace());
        metricsPort = (builder.metricsPort <= 0) ? 9190 : builder.metricsPort;
        metricsPath = Optional.ofNullable(builder.metricsPath).orElse("/metrics");
        componentType = builder.componentType;
        componentName = builder.componentName;
        componentLabelSelector = getLabelSelectorForResource();
    }

    private LabelSelector getLabelSelectorForResource() {
        if (this.componentType == ComponentType.KROXYLICIOUS) {
            return kubeClient().getPodSelectorFromDeployment(namespaceName, componentName);
        }
        return new LabelSelector();
    }

    /**
     * Parse out specific metric from whole metrics file
     * @param pattern regex pattern for specific metric
     * @return list of parsed values
     */
    public List<Double> collectSpecificMetric(Pattern pattern) {
        List<Double> values = new ArrayList<>();

        if (collectedData != null && !collectedData.isEmpty()) {
            for (Map.Entry<String, String> entry : collectedData.entrySet()) {
                Matcher t = pattern.matcher(entry.getValue());
                if (t.find()) {
                    values.add(Double.parseDouble(t.group(1)));
                }
            }
        }

        return values;
    }

    /**
     * Method checks already collected metrics data for Pattern containing desired metric
     * @param pattern Pattern of metric which is desired
     * @return ArrayList of values collected from the metrics
     */
    public synchronized List<Double> waitForSpecificMetricAndCollect(Pattern pattern) {
        List<Double> values = collectSpecificMetric(pattern);

        if (values.isEmpty()) {
            await().atMost(Constants.GLOBAL_STATUS_TIMEOUT).pollInterval(Constants.GLOBAL_POLL_INTERVAL_MEDIUM)
                    .until(() -> {
                        this.collectMetricsFromPods();
                        LOGGER.debug("matching {} against Collected data: \n{}", pattern, collectedData);
                        List<Double> vals = this.collectSpecificMetric(pattern);

                        if (!vals.isEmpty()) {
                            values.addAll(vals);
                            return true;
                        }

                        return false;
                    });
        }

        return values;
    }

    /**
     * Collect metrics from specific pod
     * @return collected metrics
     */
    private String collectMetrics(String metricsPodIp, String podName) {
        List<String> executableCommand = Arrays.asList(cmdKubeClient(namespaceName).toString(), "exec", scraperPodName,
                "-n", namespaceName,
                "--", "curl", metricsPodIp + ":" + metricsPort + metricsPath);

        LOGGER.debug("Executing command:{} for scrape the metrics", executableCommand);

        ExecResult result = Exec.exec(null, executableCommand, Duration.ofSeconds(20), true, false, null);

        Message message = LOGGER.getMessageFactory().newMessage("Metrics collection for Pod: {}/{}({}) from Pod: {}/{} finished with return code: {}", namespaceName,
                podName, metricsPodIp, namespaceName, scraperPodName, result.returnCode());

        if (!result.isSuccess()) {
            LOGGER.warn(message);
        }
        else {
            LOGGER.info(message);
        }

        return result.out();
    }

    /**
     * Collect metrics from all Pods with specific selector with wait
     */
    public void collectMetricsFromPods() {
        collectedData = await().atMost(Constants.GLOBAL_TIMEOUT).pollInterval(Constants.GLOBAL_POLL_INTERVAL)
                .until(this::collectMetricsFromPodsWithoutWait,
                        collected -> !(collected.isEmpty() || collected.entrySet().stream().anyMatch(e -> e.getValue().isEmpty())));
    }

    /**
     * Collect metrics from pods without wait.
     *
     * @return the map
     */
    public Map<String, String> collectMetricsFromPodsWithoutWait() {
        Map<String, String> map = new HashMap<>();
        kubeClient(namespaceName).listPods(namespaceName, componentLabelSelector).forEach(p -> {
            final String podName = p.getMetadata().getName();
            String podIP = p.getStatus().getPodIP();
            map.put(podName, collectMetrics(podIP, podName));
        });
        return map;
    }
}
