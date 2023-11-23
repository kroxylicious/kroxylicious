/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.extensions;

import java.lang.reflect.Parameter;
import java.util.UUID;

import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.utils.NamespaceUtils;

/**
 * The type Kroxylicious extension.
 */
public class KroxyliciousExtension implements ParameterResolver, BeforeEachCallback, AfterEachCallback {
    private static final Logger LOGGER = LoggerFactory.getLogger(KroxyliciousExtension.class);
    private static final String K8S_NAMESPACE_KEY = "namespace";
    private static final String EXTENSION_STORE_NAME = "io.kroxylicious.systemtests";
    private final ExtensionContext.Namespace junitNamespace;

    /**
     * Instantiates a new Kroxylicious extension.
     */
    public KroxyliciousExtension() {
        junitNamespace = ExtensionContext.Namespace.create(EXTENSION_STORE_NAME);
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return !parameterContext.getParameter().getType().isAssignableFrom(TestInfo.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        Parameter parameter = parameterContext.getParameter();
        Class<?> type = parameter.getType();
        LOGGER.trace("test {}: Resolving parameter ({} {})", extensionContext.getUniqueId(), type.getSimpleName(), parameter.getName());
        if (String.class.getTypeName().equals(type.getName())) {
            if (parameter.getName().toLowerCase().contains("namespace")) {
                return extractK8sNamespace(extensionContext);
            }
        }
        return extensionContext;
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) {
        NamespaceUtils.deleteNamespaceWithWait(extractK8sNamespace(extensionContext));
    }

    @Override
    public void beforeEach(ExtensionContext extensionContext) {
        final String k8sNamespace = Constants.KROXY_DEFAULT_NAMESPACE + "-" + UUID.randomUUID().toString().replace("-", "").substring(0, 6);
        extensionContext.getStore(junitNamespace).put(K8S_NAMESPACE_KEY, k8sNamespace);
        NamespaceUtils.createNamespaceWithWait(k8sNamespace);
    }

    private String extractK8sNamespace(ExtensionContext extensionContext) {
        return extensionContext.getStore(junitNamespace).get(K8S_NAMESPACE_KEY, String.class);
    }
}
