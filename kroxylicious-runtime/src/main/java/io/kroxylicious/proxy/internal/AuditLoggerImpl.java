/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.audit.Actor;
import io.kroxylicious.proxy.audit.AuditEmitter;
import io.kroxylicious.proxy.audit.AuditLogger;
import io.kroxylicious.proxy.audit.AuditableAction;
import io.kroxylicious.proxy.audit.AuditableActionBuilder;
import io.kroxylicious.proxy.audit.Correlation;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.CheckReturnValue;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

public class AuditLoggerImpl implements AuditLogger, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuditLoggerImpl.class);

    private final Supplier<Instant> instantSupplier;
    private final Supplier<Actor> actorSupplier;
    private final List<AuditEmitter> emitters;
    private final NoopAuditableActionBuilder noopBuilder;

    public AuditLoggerImpl(List<AuditEmitter> auditEmitters) {
        this(auditEmitters, () -> new ProxyActorImpl(""), Instant::now);
    }

    @VisibleForTesting
    AuditLoggerImpl(
                    List<AuditEmitter> auditEmitters,
                    Supplier<Actor> actorSupplier,
                    Supplier<Instant> instantSupplier) {
        // TODO need config about whether ip addresses should be resolved to hostnames?
        this.emitters = Objects.requireNonNull(auditEmitters);
        this.actorSupplier = actorSupplier;
        this.instantSupplier = instantSupplier;
        this.noopBuilder = NoopAuditableActionBuilder.INSTANCE;
    }

    public AuditLoggerImpl derive(Supplier<Actor> actorSupplier) {
        // TODO also one with correlation supplier ?and context?
        return new AuditLoggerImpl(emitters, actorSupplier, instantSupplier);
    }

    @Override
    @CheckReturnValue(explanation = "log() must be called on the result of this method")
    public AuditableActionBuilder action(String action) {
        return action(Objects.requireNonNull(action), null, null);
    }

    @Override
    @CheckReturnValue(explanation = "log() must be called on the result of this method")
    public AuditableActionBuilder actionWithOutcome(String action, String status, @Nullable String reason) {
        return action(Objects.requireNonNull(action), Objects.requireNonNull(status), reason);
    }

    private AuditableActionBuilder action(String action, @Nullable String status, @Nullable String reason) {
        if (anyEmitterHasInterest(action, status)) {
            return new AuditableActionBuilderImpl(action, status, reason);
        }
        return noopBuilder;
    }

    private boolean anyEmitterHasInterest(String action, @Nullable String status) {
        if (!emitters.isEmpty()) {
            for (AuditEmitter emitter : emitters) {
                try {
                    if (emitter.isInterested(action, status)) {
                        return true;
                    }
                }
                catch (Exception e) {
                    LOGGER.error("Emitter threw exception ", e);
                }
            }
        }
        return false;
    }

    @Override
    public void close() throws Exception {
        for (var emitter : emitters) {
            try {
                emitter.close();
            }
            catch (Exception e) {
                LOGGER.error("Ignoring exception thrown from {}.close()", emitter.getClass().getName(), e);
            }
        }
    }

    class AuditableActionBuilderImpl implements AuditableActionBuilder {

        sealed interface Value<T> extends io.kroxylicious.proxy.audit.Value<T>
                permits BooleanValue, BooleansValue, DoubleValue, DoublesValue, LongValue, LongsValue, StringValue, StringsValue {
        }

        record StringValue(String value) implements Value<String> {}

        record LongValue(long longValue) implements Value<Long> {
            @Override
            public Long value() {
                return longValue;
            }

            @Override
            public String toString() {
                return "LongValue[value=" + longValue + "]";
            }
        }

        record BooleanValue(boolean booleanValue) implements Value<Boolean> {
            @Override
            public Boolean value() {
                return booleanValue;
            }

            @Override
            public String toString() {
                return "BooleanValue[value=" + booleanValue + "]";
            }
        }

        record DoubleValue(double doubleValue) implements Value<Double> {
            @Override
            public Double value() {
                return doubleValue;
            }

            @Override
            public String toString() {
                return "DoubleValue[value=" + doubleValue + "]";
            }
        }

        record StringsValue(String[] value) implements Value<String[]> {
            @Override
            public boolean equals(Object o) {
                if (!(o instanceof StringsValue that)) {
                    return false;
                }
                return Objects.deepEquals(value, that.value);
            }

            @Override
            public int hashCode() {
                return Arrays.hashCode(value);
            }

            @Override
            public String toString() {
                return "StringsValue[value=" +
                        Arrays.toString(value) +
                        ']';
            }
        }

        record LongsValue(long[] value) implements Value<long[]> {
            @Override
            public boolean equals(Object o) {
                if (!(o instanceof LongsValue that)) {
                    return false;
                }
                return Objects.deepEquals(value, that.value);
            }

            @Override
            public int hashCode() {
                return Arrays.hashCode(value);
            }

            @Override
            public String toString() {
                return "LongsValue[value=" +
                        Arrays.toString(value) +
                        ']';
            }
        }

        record BooleansValue(boolean[] value) implements Value<boolean[]> {
            @Override
            public boolean equals(Object o) {
                if (!(o instanceof BooleansValue that)) {
                    return false;
                }
                return Objects.deepEquals(value, that.value);
            }

            @Override
            public int hashCode() {
                return Arrays.hashCode(value);
            }

            @Override
            public String toString() {
                return "BooleansValue[value=" +
                        Arrays.toString(value) +
                        ']';
            }
        }

        record DoublesValue(double[] value) implements Value<double[]> {
            @Override
            public boolean equals(Object o) {
                if (!(o instanceof DoublesValue that)) {
                    return false;
                }
                return Objects.deepEquals(value, that.value);
            }

            @Override
            public int hashCode() {
                return Arrays.hashCode(value);
            }

            @Override
            public String toString() {
                return "DoublesValue[value=" +
                        Arrays.toString(value) +
                        ']';
            }
        }

        private final String action;
        private final @Nullable String status;
        private final @Nullable String reason;
        private @Nullable SortedMap<String, String> objectRef = null;
        private @Nullable SortedMap<String, Value<?>> context = null;

        AuditableActionBuilderImpl(String action, @Nullable String status, @Nullable String reason) {
            this.action = action;
            this.status = status;
            this.reason = reason;
        }

        @Override
        public AuditableActionBuilder withObjectRef(Map<String, String> objectRef) {
            this.objectRef = new TreeMap<>(Objects.requireNonNull(objectRef));
            return this;
        }

        @NonNull
        private AuditableActionBuilderImpl addToContext(String key, Value<?> value1) {
            if (this.context == null) {
                this.context = new TreeMap<>();
            }
            this.context.put(Objects.requireNonNull(key), value1);
            return this;
        }

        @Override
        public AuditableActionBuilder addToContext(String key, String value) {
            return addToContext(key, new StringValue(Objects.requireNonNull(value)));
        }

        @Override
        public AuditableActionBuilder addToContext(String key, boolean value) {
            return addToContext(key, new BooleanValue(value));
        }

        @Override
        public AuditableActionBuilder addToContext(String key, long value) {
            return addToContext(key, new LongValue(value));
        }

        @Override
        public AuditableActionBuilder addToContext(String key, double value) {
            return addToContext(key, new DoubleValue(value));
        }

        @Override
        public AuditableActionBuilder addToContext(String key, boolean[] value) {
            return addToContext(key, new BooleansValue(value));
        }

        @Override
        public AuditableActionBuilder addToContext(String key, long[] value) {
            return addToContext(key, new LongsValue(value));
        }

        @Override
        public AuditableActionBuilder addToContext(String key, double[] value) {
            return addToContext(key, new DoublesValue(value));
        }

        @Override
        public AuditableActionBuilder addToContext(String key, String[] value) {
            return addToContext(key, new StringsValue(value));
        }

        @Override
        public void log() {
            Objects.requireNonNull(objectRef, "objectRef is null");
            doLog(new AuditableActionImpl(instantSupplier.get(),
                    action,
                    status,
                    reason,
                    actorSupplier.get(), // TODO can throw
                    objectRef,
                    new Correlation(null, null), // TODO get this from somewhere
                    context));
        }
    }

    private void doLog(AuditableAction action) {
        for (var emitter : emitters) {
            try {
                if (emitter.isInterested(action.action(), action.status())) {
                    emitter.emitAction(action, new AuditEmitterContextImpl());
                }
            }
            catch (Exception e) {
                LOGGER.atError()
                        .setMessage("Emitter threw exception")
                        .addKeyValue("emitter", emitter)
                        .setCause(e)
                        .log();
            }
        }
    }
}
