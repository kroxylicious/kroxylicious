/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.javaoperatorsdk.operator.api.config.informer.InformerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceInitializer;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.RecordEncryption;

@ControllerConfiguration
public class FilterReconciler implements EventSourceInitializer<RecordEncryption>,
        Reconciler<RecordEncryption> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilterReconciler.class);

    @Override
    public UpdateControl<RecordEncryption> reconcile(
            RecordEncryption resource,
            Context<RecordEncryption> context
    ) throws Exception {
        List<KafkaProxy> proxies = context.getSecondaryResourcesAsStream(KafkaProxy.class).toList();
        LOGGER.info("Reconciled the filter {}, found associated proxies {}", resource, proxies);
        return UpdateControl.noUpdate();
    }

    @Override
    public Map<String, EventSource> prepareEventSources(EventSourceContext<RecordEncryption> context) {
        SecondaryToPrimaryMapper<KafkaProxy> proxyToFilters = (KafkaProxy proxy) -> {

            var stream = proxy.getSpec().getClusters().stream().toList();
            LOGGER.info("Event source SecondaryToPrimaryMapper got {}", stream);
            Set<ResourceID> collect = stream.stream()
                    .flatMap(cluster -> cluster.getFilters().stream())
                    .map(filter -> {
                        ResourceID resourceID = new ResourceID(filter.getName(), proxy.getMetadata().getNamespace());
                        context.getPrimaryCache().get(resourceID);
                        return resourceID;
                    })
                    .collect(Collectors.toSet());
            LOGGER.info("Event source SecondaryToPrimaryMapper returning {}", collect);
            return collect;
        };

        PrimaryToSecondaryMapper<RecordEncryption> filterToProxy = (RecordEncryption filter) -> {
            var list = context.getClient().resources(KafkaProxy.class).inNamespace(filter.getMetadata().getNamespace()).list().getItems();
//            List<RecordEncryption> list = context.getPrimaryCache().list(filter.getMetadata().getNamespace()).toList();
            LOGGER.info("Event source PrimaryToSecondaryMapper got {}", list);
            return list.stream()
                    .map(proxy -> new ResourceID(proxy.getMetadata().getName()))
                    .collect(Collectors.toSet());
        };

        var configuration =
                InformerConfiguration.from(KafkaProxy.class, context)
                        .withSecondaryToPrimaryMapper(proxyToFilters)
                        .withPrimaryToSecondaryMapper(filterToProxy)
                        .build();
        return EventSourceInitializer.nameEventSources(new InformerEventSource<>(configuration, context));
    }
}
