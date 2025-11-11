/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.extensions;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.kafka.common.record.CompressionType;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.junit.jupiter.api.extension.support.TypeBasedParameterResolver;

import io.kroxylicious.kms.service.TestKmsFacade;
import io.kroxylicious.kms.service.TestKmsFacadeFactory;

public class CompressionTypeInvocationContextProvider implements TestTemplateInvocationContextProvider {

    @Override
    public boolean supportsTestTemplate(ExtensionContext context) {
        return true;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
        var kmsFacadeList = TestKmsFacadeFactory.getTestKmsFacadeFactories()
                .filter(x -> x.getClass().getName().contains("systemtests"))
                .map(TestKmsFacadeFactory::build).toList();
        var compressionTypeList = Stream.of(CompressionType.values()).toList();

        var newList = cartesianProduct(List.of(kmsFacadeList, compressionTypeList));

        List<TestTemplateInvocationContext> testTemplateInvocationContextList = new ArrayList<>();
        newList.forEach(element -> {
            testTemplateInvocationContextList.add(new TemplateInvocationContext((TestKmsFacade<?, ?, ?>) element.get(0), (CompressionType) element.get(1)));
        });
        return testTemplateInvocationContextList.stream();
    }

    private static List<? extends List<?>> cartesianProduct(List<List<?>> domains) {
        if (domains.isEmpty()) {
            throw new IllegalArgumentException();
        }
        return _cartesianProduct(0, domains);
    }

    private static List<? extends List<?>> _cartesianProduct(int index, List<List<?>> domains) {
        List<List<Object>> ret = new ArrayList<>();
        if (index == domains.size()) {
            ret.add(new ArrayList<>(domains.size()));
        }
        else {
            for (Object obj : domains.get(index)) {
                for (List tuple : _cartesianProduct(index + 1, domains)) {
                    tuple.add(0, obj);
                    ret.add(tuple);
                }
            }
        }
        return ret;
    }

    private record TemplateInvocationContext(TestKmsFacade<?, ?, ?> kmsFacade, CompressionType compressionType) implements TestTemplateInvocationContext {

        @Override
        public String getDisplayName(int invocationIndex) {
            return kmsFacade.getClass().getSimpleName() + "_" + compressionType.name;
        }

        @Override
        public List<Extension> getAdditionalExtensions() {
            List<Extension> extensions = new ArrayList<>(new TestKubeKmsFacadeInvocationContextProvider.TemplateInvocationContext(kmsFacade).getAdditionalExtensions());
            extensions.add(
                    new TypeBasedParameterResolver<CompressionType>() {
                        @Override
                        public CompressionType resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
                            return compressionType;
                        }
                    });
            return extensions;
        }
    }
}
