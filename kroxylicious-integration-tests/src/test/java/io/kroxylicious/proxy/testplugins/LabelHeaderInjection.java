/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.testplugins;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.Record;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.labels.Label;
import io.kroxylicious.proxy.labels.LabelledResource;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import edu.umd.cs.findbugs.annotations.NonNull;

@Plugin(configType = LabelHeaderInjection.Config.class)
public class LabelHeaderInjection implements FilterFactory<LabelHeaderInjection.Config, LabelHeaderInjection.Config> {

    @Override
    public Config initialize(FilterFactoryContext context, Config config) throws PluginConfigurationException {
        return config;
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, Config initializationData) {
        return new AbstractProduceHeaderInjectionFilter() {

            @NonNull
            @Override
            protected List<RecordHeader> headersToAdd(Record record, String topic, int partition, FilterContext context) {
                TopicLabels labelsToLookup = initializationData.topicNameToLabels.getOrDefault(topic, new TopicLabels(List.of()));
                Set<Label> labels = context.labels().labels(LabelledResource.TOPIC, topic);
                Stream<Label> labelStream = labelsToLookup.labelKeys.stream().flatMap(key -> labels.stream().filter(label -> label.key().equals(key)));
                Stream<RecordHeader> recordHeaderStream = labelStream.map(
                        label -> new RecordHeader(AbstractProduceHeaderInjectionFilter.headerName(LabelHeaderInjection.class, label.key()), label.value().getBytes()));
                return recordHeaderStream.toList();
            }
        };
    }

    public record TopicLabels(List<String> labelKeys) {

    }

    public record Config(Map<String, TopicLabels> topicNameToLabels) {

    }
}
