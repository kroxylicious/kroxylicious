/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.audit.emitter.metrics;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.ImmutableTag;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.search.MeterNotFoundException;

import io.kroxylicious.proxy.audit.AuditEmitter;
import io.kroxylicious.proxy.audit.AuditableAction;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.micrometer.core.instrument.Metrics.globalRegistry;

public class MetricsEmitter implements AuditEmitter {

    private final Map<String, String> tagMapping;
    private Map<String, Object> meterNames = new ConcurrentHashMap<>();

    public MetricsEmitter(Map<String, String> tagMapping) {
        this.tagMapping = tagMapping;
    }

    private static Meter.MeterProvider<Counter> buildCounterMeterProvider(String meterName,
                                                                          String description) {
        return Counter
                .builder(meterName)
                .description(description)
                .withRegistry(globalRegistry);
    }

    private static String toSnakeCase(String input) {
        // TODO this could be unneccesary if we recommend that actions are in snake case
        return input;
    }

    private Iterable<? extends Tag> tags(Map<String, String> objectRef) {
        List<ImmutableTag> list = tagMapping.entrySet().stream()
                .filter(e -> objectRef.containsKey(e.getKey()))
                .map(e -> new ImmutableTag(e.getValue(), toSnakeCase(objectRef.get(e.getKey()))))
                .toList();
        return list;
    }

    @Override
    public boolean isInterested(String action, @Nullable String status) {
        return true;
    }

    @Override
    public void emitAction(AuditableAction action, Context context) {
        String meterName;
        String meterDescription;
        if (action.status() == null) {
            meterName = "kroxylicious_audit_" + toSnakeCase(action.action()) + "_success";
            meterDescription = "The number of successful " + action + " events";
        }
        else {
            meterName = "kroxylicious_audit_" + toSnakeCase(action.action()) + "_failure";
            meterDescription = "The number of failed " + action + " events";
        }
        meterNames.put(meterName, this);
        Counter counter = buildCounterMeterProvider(meterName, meterDescription)
                .withTags(tags(action.objectRef()));
        counter.increment();
    }

    @Override
    public void close() {
        meterNames.keySet().forEach(meterName -> {
            try {
                globalRegistry.get(meterName).meters().forEach(globalRegistry::remove);
            }
            catch (MeterNotFoundException e) {
                // If it didn't exist, I guess we don't have to remove it
            }
        });
    }
}
