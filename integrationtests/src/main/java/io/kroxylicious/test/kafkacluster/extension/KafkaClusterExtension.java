/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.test.kafkacluster.extension;

import java.io.File;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.io.TempDir;
import org.junit.platform.commons.util.ExceptionUtils;
import org.junit.platform.commons.util.ReflectionUtils;

import io.kroxylicious.test.kafkacluster.ContainerBasedKafkaCluster;
import io.kroxylicious.test.kafkacluster.KafkaCluster;
import io.kroxylicious.test.kafkacluster.KafkaClusterConfig;

import static org.junit.platform.commons.util.AnnotationUtils.findAnnotatedFields;
import static org.junit.platform.commons.util.ReflectionUtils.makeAccessible;

public class KafkaClusterExtension implements
        ParameterResolver, BeforeEachCallback,
        BeforeAllCallback {

    private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace.create(KafkaClusterExtension.class);

    static class CloseableKafkaCluster implements ExtensionContext.Store.CloseableResource {

        private final KafkaCluster cluster;
        private final AnnotatedElement sourceElement;

        public CloseableKafkaCluster(AnnotatedElement sourceElement, KafkaCluster cluster) {
            this.sourceElement = sourceElement;
            this.cluster = cluster;
        }

        public KafkaCluster get() {
            return cluster;
        }

        @Override
        public void close() throws Throwable {
            System.err.println("Stopping cluster for " + sourceElement);
            cluster.close();
        }
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return parameterContext.isAnnotated(BrokerCluster.class) &&
                KafkaCluster.class.isAssignableFrom(parameterContext.getParameter().getType());
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        assertSupportedType("parameter", parameterContext.getParameter().getType());
        return getCluster(parameterContext.getParameter(), null, extensionContext);
    }

    /**
     * Perform field injection for non-private, static fields
     * of type {@link KafkaCluster} or {@link KafkaCluster} that are annotated
     * with {@link BrokerCluster @BrokerCluster}.
     */
    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        injectStaticFields(context, context.getRequiredTestClass());
    }

    /**
     * Perform field injection for non-private, non-static fields (i.e.,
     * instance fields) of type {@link Path} or {@link File} that are annotated
     * with {@link TempDir @TempDir}.
     */
    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        context.getRequiredTestInstances().getAllInstances().forEach(instance -> injectInstanceFields(context, instance));
    }

    private void injectInstanceFields(ExtensionContext context, Object instance) {
        injectFields(context, instance, instance.getClass(), ReflectionUtils::isNotStatic);
    }

    private void injectStaticFields(ExtensionContext context, Class<?> testClass) {
        injectFields(context, null, testClass, ReflectionUtils::isStatic);
    }

    private void injectFields(ExtensionContext context, Object testInstance, Class<?> testClass, Predicate<Field> predicate) {
        findAnnotatedFields(testClass, BrokerCluster.class, predicate).forEach(field -> {
            assertSupportedType("field", field.getType());
            try {
                makeAccessible(field).set(testInstance, getCluster(field, field.getType(), context));
            }
            catch (Throwable t) {
                ExceptionUtils.throwAsUncheckedException(t);
            }
        });
    }

    private KafkaCluster getCluster(AnnotatedElement sourceElement, Class<?> type, ExtensionContext extensionContext) {

        ExtensionContext.Namespace namespace = NAMESPACE.append(sourceElement);
        Object key = sourceElement;
        KafkaCluster cluster = extensionContext.getStore(namespace)
                .getOrComputeIfAbsent(key, __ -> createCluster(sourceElement), CloseableKafkaCluster.class)
                .get();
        System.err.println("Starting cluster for key " + key);
        cluster.start();
        return cluster;
    }

    private CloseableKafkaCluster createCluster(AnnotatedElement sourceElement) {
        var broker = sourceElement.getAnnotation(BrokerCluster.class);
        var builder = KafkaClusterConfig.builder()
                .brokersNum(broker.numBrokers());
        if (sourceElement.isAnnotationPresent(KRaftCluster.class)
                && sourceElement.isAnnotationPresent(ZooKeeperCluster.class)) {
            throw new ExtensionConfigurationException(
                    "Either @" + KRaftCluster.class.getSimpleName() + " or " + ZooKeeperCluster.class.getSimpleName() + " can be used, not both");
        }
        else if (sourceElement.isAnnotationPresent(KRaftCluster.class)) {
            var kraft = sourceElement.getAnnotation(KRaftCluster.class);
            builder.kraftMode(true).kraftControllers(kraft.numControllers());
        }
        else if (sourceElement.isAnnotationPresent(ZooKeeperCluster.class)) {
            builder.kraftMode(false);
        }
        else {
            builder.kraftMode(true).kraftControllers(1);
        }

        if (sourceElement.isAnnotationPresent(SaslPlainAuth.class)) {
            var authn = sourceElement.getAnnotation(SaslPlainAuth.class);
            builder.saslMechanism("PLAIN");
            builder.users(Arrays.stream(authn.value())
                    .collect(Collectors.toMap(
                            SaslPlainAuth.UserPassword::user,
                            SaslPlainAuth.UserPassword::password)));
        }

        ContainerBasedKafkaCluster c = new ContainerBasedKafkaCluster(builder.build());
        return new CloseableKafkaCluster(sourceElement, c);
    }

    private void assertSupportedType(String target, Class<?> type) {
        if (type != KafkaCluster.class) {
            throw new ExtensionConfigurationException("Can only resolve @" + BrokerCluster.class.getSimpleName() + " " + target + " of type "
                    + KafkaCluster.class + " but was: " + type.getName());
        }
    }
}
