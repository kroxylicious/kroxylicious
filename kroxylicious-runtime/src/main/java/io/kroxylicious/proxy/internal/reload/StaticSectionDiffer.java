/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import io.kroxylicious.proxy.config.Configuration;

/**
 * Helper that detects whether two {@link Configuration} instances differ in any
 * <em>static</em> configuration section &mdash; i.e. a section whose values are fixed at
 * proxy startup and cannot be reconciled at runtime.
 * <p>
 * Used by {@link ConfigurationReloadOrchestrator} as a pre-flight check before any
 * virtual-cluster change is attempted. If this differ reports a non-empty set, the
 * orchestrator rejects the reconfigure with
 * {@code StaticConfigurationChangedException}.
 *
 * <p><b>Compile-time safety against new {@link Configuration} components.</b> The per-section
 * checks below would silently miss a newly-added {@code Configuration} field unless the
 * developer remembers to update them. To prevent that, this class also declares
 * {@link #compileTimeFieldCoverageCheck(Configuration)}, a private method whose body uses a
 * positional invocation of {@code Configuration}'s constructor. Any new record component on
 * {@code Configuration} breaks that constructor call at compile time, forcing the developer
 * to explicitly classify the new component as static (add to {@link #diff} <em>and</em> pass
 * through the constructor) or reconcilable (just pass through). See the method's Javadoc for
 * the full invariant.
 */
public class StaticSectionDiffer {

    /**
     * Returns the names of static configuration sections that differ between {@code oldConfig}
     * and {@code newConfig}. An empty set indicates the two configurations agree on every
     * static section; the orchestrator may proceed to reconcile the reconcilable sections.
     * The returned set is immutable and its iteration order is unspecified.
     *
     * @param oldConfig the currently-running configuration
     * @param newConfig the submitted configuration
     * @return the names of sections that differ; never null, may be empty
     */
    public Set<String> diff(Configuration oldConfig, Configuration newConfig) {
        Objects.requireNonNull(oldConfig, "oldConfig");
        Objects.requireNonNull(newConfig, "newConfig");

        Set<String> diffs = new HashSet<>();
        if (!Objects.equals(oldConfig.management(), newConfig.management())) {
            diffs.add("management");
        }
        if (!Objects.equals(oldConfig.micrometer(), newConfig.micrometer())) {
            diffs.add("micrometer");
        }
        if (oldConfig.useIoUring() != newConfig.useIoUring()) {
            diffs.add("useIoUring");
        }
        if (!Objects.equals(oldConfig.development(), newConfig.development())) {
            diffs.add("development");
        }
        if (!Objects.equals(oldConfig.network(), newConfig.network())) {
            diffs.add("network");
        }
        if (!Objects.equals(oldConfig.proxyProtocol(), newConfig.proxyProtocol())) {
            diffs.add("proxyProtocol");
        }
        return Set.copyOf(diffs);
    }

    /**
     * Compile-time invariant: the positional invocation of {@link Configuration}'s constructor
     * below ensures every component of {@code Configuration} is explicitly classified by this
     * differ. Adding a new component to {@code Configuration} breaks this method's compilation,
     * forcing the developer to update the classification:
     * <ul>
     *   <li>A <b>static</b> field must be added to the per-section checks in
     *       {@link #diff(Configuration, Configuration)} above <em>and</em> passed through this
     *       constructor call below.</li>
     *   <li>A <b>reconcilable</b> field needs no per-section check (change detection in the
     *       orchestrator handles it) but still must be passed through this constructor call.</li>
     * </ul>
     * Either way, the developer must explicitly classify the new component, and the build
     * fails until they do so.
     *
     * <p>This method is intentionally never invoked at runtime; its sole purpose is to act as
     * a compile-time invariant. The {@code @SuppressWarnings} silences both the "unused
     * parameter" warning and the "unused private method" SonarQube rule
     * (<a href="https://rules.sonarsource.com/java/RSPEC-1144">java:S1144</a>).
     */
    @SuppressWarnings({ "unused", "java:S1144" })
    private static void compileTimeFieldCoverageCheck(Configuration c) {
        new Configuration(
                c.management(), // static — covered by diff()'s per-section check
                c.filterDefinitions(), // reconcilable — change detection handles
                c.defaultFilters(), // reconcilable — change detection handles
                c.virtualClusters(), // reconcilable — change detection handles
                c.micrometer(), // static — covered by diff()'s per-section check
                c.useIoUring(), // static — covered by diff()'s per-section check
                c.development(), // static — covered by diff()'s per-section check
                c.network(), // static — covered by diff()'s per-section check
                c.proxyProtocol() // static — covered by diff()'s per-section check
        );
    }
}
