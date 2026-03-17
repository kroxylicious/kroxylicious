/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.audit.emitter.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

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

    public static final String METER_NAME = "kroxylicious_audit";
    private final Map<String, String> tagMapping;
    private final Meter.MeterProvider<Counter> meterProvider;

    public MetricsEmitter(Map<String, String> tagMapping) {
        this.tagMapping = tagMapping;
        this.meterProvider = buildCounterMeterProvider(METER_NAME, "The number of audit actions");
    }

    private static Meter.MeterProvider<Counter> buildCounterMeterProvider(String meterName,
                                                                          String description) {
        return Counter
                .builder(meterName)
                .description(description)
                .withRegistry(globalRegistry);
    }

    private Iterable<? extends Tag> tags(String action, @Nullable String status, Map<String, String> objectRef) {
        return Stream.concat(
                Stream.of(
                        new ImmutableTag("action", action),
                        new ImmutableTag("outcome", status == null ? "success" : "failure")),
                tagMapping.entrySet().stream()
                        .filter(e -> objectRef.containsKey(e.getKey()))
                        .map(e -> new ImmutableTag(e.getValue(), objectRef.get(e.getKey()))))
                        .toList();
    }

    @Override
    public boolean isInterested(String action, @Nullable String status) {
        return true;
    }

    @Override
    public void emitAction(AuditableAction action, Context context) {

        var counter = meterProvider
                .withTags(tags(action.action(), action.status(), action.objectRef()));
        counter.increment();
    }

    @Override
    public void close() {
        try {
            globalRegistry.get(METER_NAME).meters().forEach(globalRegistry::remove);
        }
        catch (MeterNotFoundException e) {
            // If it didn't exist, I guess we don't have to remove it
        }
    }
}
