/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.service;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.junit.jupiter.api.extension.support.TypeBasedParameterResolver;

/**
 * Junit Context Provider providing available {@link TestKmsFacade}s to integration tests.
 * <br/>
 * The variable {@code KROYXLICIOUS_KMS_FACADE_CLASS_NAME_FILTER} is a regular expression that limits the facades available
 * to the test.  It matches against the facade class name.
 */
public class TestKmsFacadeInvocationContextProvider implements TestTemplateInvocationContextProvider, ParameterResolver {

    private static final ExtensionContext.Namespace STORE_NAMESPACE = ExtensionContext.Namespace.create("TEST_KMS");
    private static final String FACADE_FACTORIES = "KMS_FACADE_FACTORIES";

    private static final Pattern FACADE_CLASS_NAME_FILTER = Optional.ofNullable(System.getenv("KROYXLICIOUS_KMS_FACADE_CLASS_NAME_FILTER")).map(Pattern::compile)
            .orElse(Pattern.compile(".*"));

    @Override
    public boolean supportsTestTemplate(ExtensionContext context) {
        return true;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
        return getTestKmsFacadeStream(context)
                .values()
                .stream()
                .filter(f -> FACADE_CLASS_NAME_FILTER.matcher(f.getClass().getName()).matches())
                .map(TemplateInvocationContext::new);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        final TestKmsFacade<?, ?, ?> testKmsFacade = getTestKmsFacadeStream(extensionContext)
                .get(parameterContext.getParameter().getType());
        if (testKmsFacade == null) {
            throw new ParameterResolutionException("Unable to resolve " + parameterContext.getParameter().getType().getSimpleName());
        }
        else {
            testKmsFacade.start();
            return testKmsFacade;
        }
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        final Map<Class<TestKmsFacade<?, ?, ?>>, TestKmsFacade<?, ?, ?>> facadeFactories = getTestKmsFacadeStream(extensionContext);
        return facadeFactories.containsKey(parameterContext.getParameter().getType());
    }

    @SuppressWarnings("unchecked")
    private static Map<Class<TestKmsFacade<?, ?, ?>>, TestKmsFacade<?, ?, ?>> getTestKmsFacadeStream(ExtensionContext extensionContext) {
        final ExtensionContext.Store store = extensionContext.getStore(STORE_NAMESPACE);
        return store.getOrComputeIfAbsent(FACADE_FACTORIES, key -> TestKmsFacadeFactory.getTestKmsFacadeFactories()
                .map(TestKmsFacadeFactory::build)
                .collect(
                        Collectors.toMap(
                                Object::getClass,
                                Function.identity()

                        )), Map.class);
    }

    private record TemplateInvocationContext(TestKmsFacade<?, ?, ?> kmsFacade) implements TestTemplateInvocationContext {

        @Override
        public String getDisplayName(int invocationIndex) {
            return kmsFacade.getClass().getSimpleName();
        }

        @Override
        public List<Extension> getAdditionalExtensions() {
            if (!kmsFacade.isAvailable()) {
                return List.of(
                        (ExecutionCondition) extensionContext -> kmsFacade.isAvailable() ? ConditionEvaluationResult.enabled(null)
                                : ConditionEvaluationResult.disabled(null));
            }

            return List.of(
                    (BeforeEachCallback) extensionContext -> kmsFacade.start(),
                    new TypeBasedParameterResolver<TestKmsFacade<?, ?, ?>>() {
                        @Override
                        public TestKmsFacade<?, ?, ?> resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
                            return kmsFacade;
                        }
                    },
                    (AfterEachCallback) extensionContext -> kmsFacade.stop());
        }
    }
}
