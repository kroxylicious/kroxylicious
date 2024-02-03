/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.audit.sink.remotekafkacluster;

import io.kroxylicious.proxy.filter.audit.EventSink;
import io.kroxylicious.proxy.filter.audit.EventSinkService;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import java.util.Map;

@Plugin(configType = KafkaProducerEventSinkService.Config.class)
public class KafkaProducerEventSinkService implements EventSinkService<KafkaProducerEventSinkService.Config> {
    @Override
    public void validateConfiguration(KafkaProducerEventSinkService.Config config) throws PluginConfigurationException {

    }

    @Override
    public EventSink createAuditSink(KafkaProducerEventSinkService.Config configuration) {
        return new KafkaProducerEventSink(configuration);
    }

    // TODO shroud any passwords in the producerConfig
    public record Config(String topic,
                         Map<String, Object> producerConfig) {
    }
}
