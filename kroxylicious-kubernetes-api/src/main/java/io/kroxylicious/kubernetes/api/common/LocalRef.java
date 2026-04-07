/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.api.common;

import java.util.Comparator;
import java.util.Objects;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Base class for all references within the {@code LocalRef} hierarchy.
 * A {@code LocalRef} represents a reference from one Kubernetes resource to another
 * resource <em>in the same namespace</em>, identified by {@code group}, {@code kind} and {@code name}.
 *
 * <h2>Why this hierarchy exists</h2>
 * <p>
 * The CRD-to-Java code generator ({@code io.fabric8.java.generator.CRGeneratorRunner}) produces
 * a distinct Java class for each reference field defined in a CRD schema (e.g. {@link ProxyRef},
 * {@link FilterRef}, {@link KafkaServiceRef}). These generated classes are structurally
 * identical but are unrelated in the Java type system, which makes it impossible to
 * compare or collect them uniformly without a common supertype.
 * </p>
 * <p>
 * {@code LocalRef} solves this by acting as that common supertype. Its {@link #equals},
 * {@link #hashCode} and {@link #compareTo} implementations are based solely on
 * {@code group + kind + name}, <strong>regardless of the concrete subclass</strong>.
 * This means a typed reference and a generic reference pointing to the same resource
 * are considered equal:
 * </p>
 * <pre>{@code
 * ProxyRef typed = new ProxyRef();
 * typed.setName("my-proxy");
 *
 * AnyLocalRef generic = new AnyLocalRef();
 * generic.setGroup("kroxylicious.io");
 * generic.setKind("KafkaProxy");
 * generic.setName("my-proxy");
 *
 * typed.equals(generic); // true
 * }</pre>
 *
 * <h2>Two subtype patterns</h2>
 * <ul>
 *   <li><strong>Statically-typed refs</strong> (e.g. {@link ProxyRef}, {@link FilterRef},
 *       {@link IngressRef}, {@link KafkaServiceRef}) extend {@code LocalRef<T>} directly,
 *       where {@code T} is the specific Kubernetes resource type they reference.
 *       Their {@code group} and {@code kind} are hardcoded constants and are <em>not</em>
 *       serialized to JSON; only {@code name} appears in the CRD schema.</li>
 *   <li><strong>Dynamically-typed refs</strong> (e.g. {@link AnyLocalRef}, {@link CertificateRef})
 *       extend {@link AbstractLocalRef}. Their {@code group} and {@code kind} are
 *       user-supplied fields that are serialized to JSON, because the target resource
 *       type is not known at compile time.</li>
 * </ul>
 *
 * @param <T> The Java type of the referenced Kubernetes resource
 */
public abstract class LocalRef<T> implements Comparable<LocalRef<T>> {

    private static final Comparator<LocalRef<?>> COMPARATOR = Comparator.<LocalRef<?>, String> comparing(LocalRef::getKind, Comparator.nullsLast(String::compareTo))
            .thenComparing(LocalRef::getGroup, Comparator.nullsLast(String::compareTo))
            .thenComparing(LocalRef::getName, Comparator.nullsLast(String::compareTo));

    @Nullable
    public abstract String getGroup();

    @Nullable
    public abstract String getKind();

    @Nullable
    public abstract String getName();

    @Override
    public final int hashCode() {
        return Objects.hash(getGroup(), getKind(), getName());
    }

    @Override
    @SuppressWarnings("unchecked")
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof LocalRef)) {
            return false;
        }
        LocalRef<T> other = (LocalRef<T>) obj;
        return Objects.equals(getGroup(), other.getGroup())
                && Objects.equals(getKind(), other.getKind())
                && Objects.equals(getName(), other.getName());
    }

    @Override
    public int compareTo(LocalRef<T> o) {
        return COMPARATOR.compare(this, o);
    }
}
