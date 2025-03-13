/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.extensions;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.junit.jupiter.api.extension.support.TypeBasedParameterResolver;

import io.kroxylicious.kms.service.TestKmsFacade;
import io.kroxylicious.kms.service.TestKmsFacadeFactory;
import io.kroxylicious.systemtests.logs.TestLogCollector;

public class TestKubeKmsFacadeInvocationContextProvider implements TestTemplateInvocationContextProvider {
    private static final TestLogCollector logCollector = TestLogCollector.getInstance();

    @Override
    public boolean supportsTestTemplate(ExtensionContext context) {
        return true;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
        return TestKmsFacadeFactory.getTestKmsFacadeFactories()
                .filter(x -> x.getClass().getName().contains("systemtests"))
                .map(TestKmsFacadeFactory::build)
                .map(TestKubeKmsFacadeInvocationContextProvider.TemplateInvocationContext::new);
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
                    (AfterEachCallback) extensionContext -> {
                        try {
                            Optional<Throwable> exception = extensionContext.getExecutionException();
                            exception.filter(t -> !t.getClass().getSimpleName().equals("AssumptionViolatedException")).ifPresent(e -> {
                                logCollector.collectLogs(extensionContext.getRequiredTestClass().getName(), extensionContext.getRequiredTestMethod().getName());
                            });
                        }
                        finally {
                            kmsFacade.stop();
                        }
                    });
        }
    }
}
