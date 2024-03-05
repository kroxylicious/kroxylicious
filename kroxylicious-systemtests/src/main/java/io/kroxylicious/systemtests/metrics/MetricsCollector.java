/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.metrics;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.fabric8.kubernetes.api.model.LabelSelector;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.executor.Exec;
import io.kroxylicious.systemtests.resources.ComponentType;
import io.kroxylicious.systemtests.utils.TestUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.cmdKubeClient;
import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

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
    private final LabelSelector componentLabelSelector;
    private Map<String, String> collectedData;

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
     * Gets scraper pod name.
     *
     * @return the scraper pod name
     */
    public String getScraperPodName() {
        return scraperPodName;
    }

    /**
     * Gets component type.
     *
     * @return the component type
     */
    public ComponentType getComponentType() {
        return componentType;
    }

    /**
     * Gets component name.
     *
     * @return the component name
     */
    public String getComponentName() {
        return componentName;
    }

    /**
     * Gets metrics path.
     *
     * @return the metrics path
     */
    public String getMetricsPath() {
        return metricsPath;
    }

    /**
     * Gets metrics port.
     *
     * @return the metrics port
     */
    public int getMetricsPort() {
        return metricsPort;
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
     * New builder metrics collector . builder.
     *
     * @return the metrics collector . builder
     */
    protected MetricsCollector.Builder newBuilder() {
        return new MetricsCollector.Builder();
    }

    /**
     * Update builder metrics collector . builder.
     *
     * @param builder the builder
     * @return the metrics collector . builder
     */
    protected MetricsCollector.Builder updateBuilder(MetricsCollector.Builder builder) {
        return builder
                .withNamespaceName(getNamespaceName())
                .withComponentName(getComponentName())
                .withComponentType(getComponentType())
                .withScraperPodName(getScraperPodName());
    }

    /**
     * To builder metrics collector.
     *
     * @return the metrics collector
     */
    public MetricsCollector.Builder toBuilder() {
        return updateBuilder(newBuilder());
    }

    /**
     * Instantiates a new Metrics collector.
     *
     * @param builder the builder
     */
    protected MetricsCollector(Builder builder) {
        if (builder.namespaceName == null || builder.namespaceName.isEmpty()) {
            builder.namespaceName = kubeClient().getNamespace();
        }
        if (builder.scraperPodName == null || builder.scraperPodName.isEmpty()) {
            throw new InvalidParameterException("Scraper Pod name is not set");
        }
        if (builder.componentType == null) {
            throw new InvalidParameterException("Component type is not set");
        }

        componentType = builder.componentType;

        if (builder.metricsPort <= 0) {
            builder.metricsPort = getDefaultMetricsPort();
        }
        if (builder.metricsPath == null || builder.metricsPath.isEmpty()) {
            builder.metricsPath = getDefaultMetricsPath();
        }

        namespaceName = builder.namespaceName;
        scraperPodName = builder.scraperPodName;
        metricsPort = builder.metricsPort;
        metricsPath = builder.metricsPath;
        componentName = builder.componentName;
        componentLabelSelector = getLabelSelectorForResource();
    }

    private LabelSelector getLabelSelectorForResource() {
        if (Objects.requireNonNull(this.componentType) == ComponentType.Kroxylicious) {
            return kubeClient().getDeploymentSelectors(namespaceName, componentName);
        }
        return new LabelSelector();
    }

    private String getDefaultMetricsPath() {
        return "/metrics";
    }

    private int getDefaultMetricsPort() {
        return 9190;
    }

    /**
     * Parse out specific metric from whole metrics file
     * @param pattern regex pattern for specific metric
     * @return list of parsed values
     */
    public ArrayList<Double> collectSpecificMetric(Pattern pattern) {
        ArrayList<Double> values = new ArrayList<>();

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
    public synchronized ArrayList<Double> waitForSpecificMetricAndCollect(Pattern pattern) {
        ArrayList<Double> values = collectSpecificMetric(pattern);

        if (values.isEmpty()) {
            TestUtils.waitFor(String.format("metrics contain pattern: %s", pattern.toString()), Constants.GLOBAL_POLL_INTERVAL_MEDIUM, Constants.GLOBAL_STATUS_TIMEOUT,
                    () -> {
                        this.collectMetricsFromPods();
                        LOGGER.debug("Collected data: {}", collectedData);
                        ArrayList<Double> vals = this.collectSpecificMetric(pattern);

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
    private String collectMetrics(String metricsPodIp, String podName) throws InterruptedException, ExecutionException, IOException {
        List<String> executableCommand = Arrays.asList(cmdKubeClient(namespaceName).toString(), "exec", scraperPodName,
                "-n", namespaceName,
                "--", "curl", metricsPodIp + ":" + metricsPort + metricsPath);

        LOGGER.debug("Executing command:{} for scrape the metrics", executableCommand);

        Exec exec = new Exec();
        // 20 seconds should be enough for collect data from the pod
        int ret = exec.execute(null, executableCommand, 20_000, null);

        LOGGER.info("Metrics collection for Pod: {}/{}({}) from Pod: {}/{} finished with return code: {}", namespaceName, podName, metricsPodIp, namespaceName,
                scraperPodName, ret);
        return exec.out();
    }

    /**
     * Collect metrics from all Pods with specific selector with wait
     */
    @SuppressWarnings("unchecked")
    public void collectMetricsFromPods() {
        Map<String, String>[] metricsData = (Map<String, String>[]) new HashMap[1];
        TestUtils.waitFor("metrics to contain data", Constants.GLOBAL_POLL_INTERVAL, Constants.GLOBAL_TIMEOUT,
                () -> {
                    metricsData[0] = collectMetricsFromPodsWithoutWait();

                    // KafkaExporter metrics should be non-empty
                    if (metricsData[0].isEmpty()) {
                        return false;
                    }

                    for (Map.Entry<String, String> item : metricsData[0].entrySet()) {
                        if (item.getValue().isEmpty()) {
                            return false;
                        }
                    }
                    return true;
                });

        collectedData = metricsData[0];
    }

    /**
     * Collect metrics from pods without wait.
     *
     * @return the map
     */
    public Map<String, String> collectMetricsFromPodsWithoutWait() {
        Map<String, String> map = new HashMap<>();
        kubeClient(namespaceName).listPods(namespaceName, componentLabelSelector).forEach(p -> {
            try {
                final String podName = p.getMetadata().getName();
                String podIP = p.getStatus().getPodIP();
                map.put(podName, collectMetrics(podIP, podName));
            }
            catch (InterruptedException | ExecutionException | IOException e) {
                throw new RuntimeException(e);
            }
        });
        return map;
    }
}
