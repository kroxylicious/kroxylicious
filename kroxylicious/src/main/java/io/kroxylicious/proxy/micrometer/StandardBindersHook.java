/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.micrometer;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmCompilationMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmHeapPressureMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmInfoMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;

import io.kroxylicious.proxy.config.BaseConfig;

public class StandardBindersHook implements MicrometerConfigurationHook {
    private static final Logger log = LoggerFactory.getLogger(StandardBindersHook.class);
    private final StandardBindersHookConfig config;
    private final List<AutoCloseable> closeableBinders = new CopyOnWriteArrayList<>();

    public static class StandardBindersHookConfig extends BaseConfig {
        private final List<String> binderNames;

        @JsonCreator
        public StandardBindersHookConfig(List<String> binderNames) {
            this.binderNames = binderNames == null ? List.of() : binderNames;
        }
    }

    public StandardBindersHook(StandardBindersHookConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("config should be non-null");
        }
        this.config = config;
    }

    @Override
    public void configure(MeterRegistry targetRegistry) {
        for (String binderName : this.config.binderNames) {
            MeterBinder binder = getBinder(binderName);
            binder.bindTo(targetRegistry);
            if (binder instanceof AutoCloseable closeable) {
                this.closeableBinders.add(closeable);
            }
            log.info("bound {} to micrometer registry", binderName);
        }

    }

    @Override
    public void close() {
        closeableBinders.forEach(closeable -> {
            try {
                closeable.close();
            }
            catch (Exception ignore) {
            }
        });
    }

    private MeterBinder getBinder(String binderName) {
        switch (binderName) {
            case "UptimeMetrics":
                return new UptimeMetrics();
            case "ProcessorMetrics":
                return new ProcessorMetrics();
            case "FileDescriptorMetrics":
                return new FileDescriptorMetrics();
            case "ClassLoaderMetrics":
                return new ClassLoaderMetrics();
            case "JvmCompilationMetrics":
                return new JvmCompilationMetrics();
            case "JvmGcMetrics":
                return new JvmGcMetrics();
            case "JvmHeapPressureMetrics":
                return new JvmHeapPressureMetrics();
            case "JvmInfoMetrics":
                return new JvmInfoMetrics();
            case "JvmMemoryMetrics":
                return new JvmMemoryMetrics();
            case "JvmThreadMetrics":
                return new JvmThreadMetrics();
            default:
                throw new IllegalArgumentException("no binder available for " + binderName);
        }
    }
}
