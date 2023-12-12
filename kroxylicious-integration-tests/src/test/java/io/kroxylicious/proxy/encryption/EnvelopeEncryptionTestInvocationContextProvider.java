/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.encryption;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.junit.jupiter.api.extension.support.TypeBasedParameterResolver;

class EnvelopeEncryptionTestInvocationContextProvider implements TestTemplateInvocationContextProvider {

    @Override
    public boolean supportsTestTemplate(ExtensionContext context) {
        return true;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
        return TestKmsFacadeFactory.getTestKmsFacadeFactories()
                .map(TestKmsFacadeFactory::build)
                .map(TemplateInvocationContext::new);
    }

    private record TemplateInvocationContext(TestKmsFacade kmsFacade) implements TestTemplateInvocationContext {

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
                    (BeforeTestExecutionCallback) extensionContext -> kmsFacade.start(),
                    new TypeBasedParameterResolver<TestKmsFacade>() {
                        @Override
                        public TestKmsFacade resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
                            return kmsFacade;
                        }
                    },
                    (AfterTestExecutionCallback) extensionContext -> {
                        if (kmsFacade != null) {
                            kmsFacade.stop();
                        }
                    });
        }
    }
}
