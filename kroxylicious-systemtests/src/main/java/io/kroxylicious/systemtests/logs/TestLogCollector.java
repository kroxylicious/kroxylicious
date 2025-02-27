/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.logs;

import java.io.File;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import io.skodjob.testframe.LogCollector;
import io.skodjob.testframe.LogCollectorBuilder;
import io.skodjob.testframe.clients.KubeClient;
import io.skodjob.testframe.clients.cmdClient.Kubectl;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.utils.NamespaceUtils;

/**
 * Class for encapsulating Test-Frame's {@link LogCollector}.
 * It provides methods for collecting logs for the test-cases -> collects Namespaces that are created for
 * particular test-class and test-case and builds the whole path to the logs.
 * The structure of the logs then looks like this:
 *
 * kroxylicious-systemtests/target/logs/2025-02-09-18-45-39
 * └── io.kroxylicious.systemtests.RecordEncryptionST
 *     ├── produceAndConsumeMessageWithRotatedKEK
 *     │          └── 1
 *     │              ├── kafka
 *     │              │          ├── configmap
 *     │              │          │          ├── kube-root-ca.crt.yaml
 *     │              │          │          ├── my-cluster-kafka-0.yaml
 *     │              │          │          ├── my-cluster-kafka-1.yaml
 *     │              │          │          ├── my-cluster-kafka-2.yaml
 *     │              │          │          └── strimzi-cluster-operator.yaml
 *     │              │          ├── deployment
 *     │              │          │          └── strimzi-cluster-operator.yaml
 *     │              │          ├── kafka
 *     │              │          │          └── my-cluster.yaml
 *     │              │          ├── kafkanodepool
 *     │              │          │          └── kafka.yaml
 *     │              │          ├── events.log
 *     │              │          ├── pod
 *     │              │          │          ├── describe-pod-my-cluster-kafka-0.log
 *     │              │          │          ├── describe-pod-my-cluster-kafka-1.log
 *     │              │          │          ├── describe-pod-my-cluster-kafka-2.log
 *     │              │          │          └── logs-pod-strimzi-cluster-operator-57d455495c-98t8f-container-strimzi-cluster-operator.log
 *     │              │          └── secret
 *     │              │              ├── builder-dockercfg-j99st.yaml
 *     │              │              ├── default-dockercfg-pb9z6.yaml
 *     │              │              ├── deployer-dockercfg-ltvsj.yaml
 *     │              │              └── strimzi-cluster-operator-dockercfg-64p64.yaml
 *     │              └── kafka-XXXXXX
 *     │                  ├── configmap
 *     │                  │          ├── cluster-2acba704-b-6eac3ce0-0.yaml
 *     │                  │          ├── cluster-2acba704-bridge-config.yaml
 *     │                  │          ├── cluster-2acba704-c-6eac3ce0-1.yaml
 *     │                  │          ├── cluster-2acba704-entity-topic-operator-config.yaml
 *     │                  │          ├── cluster-2acba704-entity-user-operator-config.yaml
 *     │                  │          ├── regcred.yaml
 *     │                  │          └── openshift-service-ca.crt.yaml
 *     │                  ├── deployment
 *     │                  │          ├── cluster-2acba704-bridge.yaml
 *     │                  │          └── cluster-2acba704-entity-operator.yaml
 *     │                  ├── events.log
 *     │                  ├── job
 *     │                  │          ├── admin-client-cli-create.yaml
 *     │                  │          ├── admin-client-cli-delete.yaml
 *     │                  │          ├── ...
 *     │                  │          └── kafka-producer-client-zzzzzzz.yaml
 *     │                  ├── pod
 *     │                  │          ├── describe-pod-kroxylicious-proxy-7db664df48-bxxrx.log
 *     │                  │          ├── ...
 *     │                  │          └── logs-pod-kroxylicious-proxy-7db664df48-bxxrx-container-kroxylicious.log
 *     │                  └── secret
 *     │                      └── regcred.yaml
 * ...
 */
public class TestLogCollector {
    private static final String CURRENT_DATE;
    private final LogCollector logCollector;

    static {
        // Get current date to create a unique folder
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss");
        dateTimeFormatter = dateTimeFormatter.withZone(ZoneId.of("GMT"));
        CURRENT_DATE = dateTimeFormatter.format(LocalDateTime.now());
    }

    /**
     * TestLogCollector's constructor
     */
    public TestLogCollector() {
        this.logCollector = defaultLogCollector();
    }

    /**
     * Method for creating default configuration of the {@link LogCollector}.
     * It provides default list of resources into the builder and configures the required {@link KubeClient} and
     * {@link Kubectl}
     *
     * @return  {@link LogCollector} configured with default configuration for the tests
     */
    private LogCollector defaultLogCollector() {
        List<String> resources = new ArrayList<>(List.of(
                Constants.SECRET.toLowerCase(Locale.ROOT),
                Constants.DEPLOYMENT.toLowerCase(Locale.ROOT),
                Constants.CONFIG_MAP.toLowerCase(Locale.ROOT),
                Constants.SERVICE.toLowerCase(Locale.ROOT),
                Constants.JOB.toLowerCase(Locale.ROOT),
                Kafka.RESOURCE_SINGULAR,
                KafkaNodePool.RESOURCE_SINGULAR));

        return new LogCollectorBuilder()
                .withKubeClient(new KubeClient())
                .withKubeCmdClient(new Kubectl())
                .withRootFolderPath(Environment.CLUSTER_DUMP_DIR)
                .withNamespacedResources(resources.toArray(new String[0]))
                .build();
    }

    /**
     * Method that checks existence of the folder on specified path.
     * From there, if there are no sub-dirs created - for each of the test-case run/re-run - the method returns the
     * full path containing the specified path and index (1).
     * Otherwise, it lists all the directories, filtering all the folders that are indexes, takes the last one, and returns
     * the full path containing specified path and index increased by one.
     *
     * @param rootPathToLogsForTestCase     complete path for test-class/test-class and test-case logs
     *
     * @return  full path to logs directory built from specified root path and index
     */
    private Path checkPathAndReturnFullRootPathWithIndexFolder(Path rootPathToLogsForTestCase) {
        File logsForTestCase = rootPathToLogsForTestCase.toFile();
        int index = 1;

        if (logsForTestCase.exists()) {
            String[] filesInLogsDir = logsForTestCase.list();

            if (filesInLogsDir != null && filesInLogsDir.length > 0) {
                index = Integer.parseInt(
                        Arrays
                                .stream(filesInLogsDir)
                                .filter(file -> {
                                    try {
                                        Integer.parseInt(file);
                                        return true;
                                    }
                                    catch (NumberFormatException e) {
                                        return false;
                                    }
                                })
                                .sorted()
                                .toList()
                                .get(filesInLogsDir.length - 1))
                        + 1;
            }
        }

        return rootPathToLogsForTestCase.resolve(String.valueOf(index));
    }

    /**
     * Method for building the full path to logs for specified test-class and test-case.
     *
     * @param testClass     name of the test-class
     * @param testCase      name of the test-case
     *
     * @return full path to the logs for test-class and test-case, together with index
     */
    private Path buildFullPathToLogs(String testClass, String testCase) {
        Path rootPathToLogsForTestCase = Path.of(Environment.CLUSTER_DUMP_DIR, CURRENT_DATE, testClass);

        if (testCase != null) {
            rootPathToLogsForTestCase = rootPathToLogsForTestCase.resolve(testCase);
        }

        return checkPathAndReturnFullRootPathWithIndexFolder(rootPathToLogsForTestCase);
    }

    /**
     * Method that encapsulates {@link #collectLogs(String, String)}, taking the test-class and test-case names from
     * {@link ResourceManager#getTestContext()}
     */
    public void collectLogs() {
        collectLogs(
                ResourceManager.getTestContext().getRequiredTestClass().getName(),
                ResourceManager.getTestContext().getRequiredTestMethod().getName());
    }

    /**
     * Method that encapsulates {@link #collectLogs(String, String)}, where test-class is passed as a parameter and the
     * test-case name is `null` -> that's used when the test fail in `@BeforeAll` or `@AfterAll` phases.
     *
     * @param testClass     name of the test-class, for which the logs should be collected
     */
    public void collectLogs(String testClass) {
        collectLogs(testClass, null);
    }

    /**
     * Method that uses {@link LogCollector#collectFromNamespaces(String...)} method for collecting logs from Namespaces
     * for the particular combination of test-class and test-case.
     *
     * @param testClass     name of the test-class, for which the logs should be collected
     * @param testCase      name of the test-case, for which the logs should be collected
     */
    public void collectLogs(String testClass, String testCase) {
        Path rootPathToLogsForTestCase = buildFullPathToLogs(testClass, testCase);

        final LogCollector testCaseCollector = new LogCollectorBuilder(logCollector)
                .withRootFolderPath(rootPathToLogsForTestCase.toString())
                .build();

        List<String> namespaces = NamespaceUtils.getListOfNamespacesForTestClassAndTestCase(testClass, testCase);

        testCaseCollector.collectFromNamespaces(namespaces.toArray(new String[0]));
        NamespaceUtils.deleteNamespacesFromSet(namespaces, testClass, testCase);
    }
}
