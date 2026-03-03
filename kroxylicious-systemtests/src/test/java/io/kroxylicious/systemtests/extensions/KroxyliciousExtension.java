/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.extensions;

import java.lang.reflect.Parameter;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.skodjob.testframe.resources.KubeResourceManager;

import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.logs.TestLogCollector;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.utils.NamespaceUtils;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * The type Kroxylicious extension.
 */
public class KroxyliciousExtension implements ParameterResolver, BeforeAllCallback, BeforeEachCallback, AfterEachCallback, AfterAllCallback {
    private static final Logger LOGGER = LoggerFactory.getLogger(KroxyliciousExtension.class);
    private static final String K8S_NAMESPACE_KEY = "namespace";
    private static final String EXTENSION_STORE_NAME = "io.kroxylicious.systemtests";
    private static final int MAX_NAMESPACE_PREFIX_LENGTH = 12;
    private final ExtensionContext.Namespace junitNamespace;
    private final TestLogCollector logCollector = TestLogCollector.getInstance();

    /**
     * Instantiates a new Kroxylicious extension.
     */
    public KroxyliciousExtension() {
        junitNamespace = ExtensionContext.Namespace.create(EXTENSION_STORE_NAME);
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return (parameterContext.getParameter().getType().isAssignableFrom(String.class)
                && parameterContext.getParameter().getName().toLowerCase().contains("namespace"))
                || parameterContext.getParameter().getType().isAssignableFrom(ExtensionContext.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        Parameter parameter = parameterContext.getParameter();
        Class<?> type = parameter.getType();
        LOGGER.trace("test {}: Resolving parameter ({} {})", extensionContext.getUniqueId(), type.getSimpleName(), parameter.getName());
        if (parameter.getName().toLowerCase().contains("namespace")) {
            return extractK8sNamespace(extensionContext);
        }

        return extensionContext;
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) {
        String testClassName = extensionContext.getRequiredTestClass().getName();
        ResourceManager.setTestContext(extensionContext);
        NamespaceUtils.addNamespaceToSet(Environment.STRIMZI_NAMESPACE, testClassName);
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) {
        if (!Environment.SKIP_TEARDOWN) {
            ResourceManager.setTestContext(extensionContext);
            NamespaceUtils.deleteAllNamespacesFromSet(!Environment.SYNC_RESOURCES_DELETION);
            KubeResourceManager.get().deleteResources(!Environment.SYNC_RESOURCES_DELETION);
        }
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) {
        ResourceManager.setTestContext(extensionContext);
        String namespace = extractK8sNamespace(extensionContext);
        String testClassName = extensionContext.getRequiredTestClass().getName();
        String testMethodName = extensionContext.getRequiredTestMethod().getName();
        try {
            Optional<Throwable> exception = extensionContext.getExecutionException();
            exception.filter(t -> !t.getClass().getSimpleName().equals("AssumptionViolatedException"))
                    .ifPresent(e -> logCollector.collectLogs(testClassName, testMethodName));
        }
        finally {
            if (Environment.SYNC_RESOURCES_DELETION) {
                NamespaceUtils.deleteNamespaceWithWaitAndRemoveFromSet(namespace, testClassName);
            }
            else {
                NamespaceUtils.deleteNamespaceWithoutWaitAndRemoveFromSet(namespace, testClassName);
            }
        }
    }

    @Override
    public void beforeEach(ExtensionContext extensionContext) {
        ResourceManager.setTestContext(extensionContext);
        final String k8sNamespace = generateNamespaceName(extensionContext);
        extensionContext.getStore(junitNamespace).put(K8S_NAMESPACE_KEY, k8sNamespace);
        NamespaceUtils.createNamespaceAndPrepare(k8sNamespace);
    }

    @NonNull
    private static String generateNamespaceName(ExtensionContext extensionContext) {
        String namespacePrefix = getPrefix(extensionContext);
        return namespacePrefix + "-" + UUID.randomUUID().toString().replace("-", "").substring(0, 6);
    }

    @NonNull
    private static String getPrefix(ExtensionContext extensionContext) {
        String simpleName = extensionContext.getRequiredTestClass().getSimpleName().replace("ST", "");
        String limitedTestName = simpleName.substring(0, Math.min(simpleName.length(), MAX_NAMESPACE_PREFIX_LENGTH));
        return String.join("-", limitedTestName.split("(?=\\p{Upper})")).toLowerCase(Locale.ROOT) + "-st";
    }

    private String extractK8sNamespace(ExtensionContext extensionContext) {
        return extensionContext.getStore(junitNamespace).get(K8S_NAMESPACE_KEY, String.class);
    }
}
