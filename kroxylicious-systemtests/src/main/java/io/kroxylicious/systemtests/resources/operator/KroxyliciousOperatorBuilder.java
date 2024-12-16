/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.operator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * The type Kroxylicious operator builder.
 */
public class KroxyliciousOperatorBuilder {

    private ExtensionContext extensionContext;
    private String kroxyliciousOperatorName;
    private String namespaceInstallTo;
    private String namespaceToWatch;
    private List<String> bindingsNamespaces;
    private Duration operationTimeout;
    private Duration reconciliationInterval;
    private Map<String, String> extraLabels;
    private int replicas = 1;

    /**
     * With extension context kroxylicious operator builder.
     *
     * @param extensionContext the extension context
     * @return the kroxylicious operator builder
     */
    public KroxyliciousOperatorBuilder withExtensionContext(ExtensionContext extensionContext) {
        this.extensionContext = extensionContext;
        return self();
    }

    /**
     * With kroxylicious operator name kroxylicious operator builder.
     *
     * @param kroxyliciousOperatorName the kroxylicious operator name
     * @return the kroxylicious operator builder
     */
    public KroxyliciousOperatorBuilder withKroxyliciousOperatorName(String kroxyliciousOperatorName) {
        this.kroxyliciousOperatorName = kroxyliciousOperatorName;
        return self();
    }

    /**
     * With namespace kroxylicious operator builder.
     *
     * @param namespaceInstallTo the namespace install to
     * @return the kroxylicious operator builder
     */
    public KroxyliciousOperatorBuilder withNamespace(String namespaceInstallTo) {
        this.namespaceInstallTo = namespaceInstallTo;
        return self();
    }

    /**
     * With watching namespaces kroxylicious operator builder.
     *
     * @param namespaceToWatch the namespace to watch
     * @return the kroxylicious operator builder
     */
    public KroxyliciousOperatorBuilder withWatchingNamespaces(String namespaceToWatch) {
        this.namespaceToWatch = namespaceToWatch;
        return self();
    }

    /**
     * Add to the watching namespaces kroxylicious operator builder.
     *
     * @param namespaceToWatch the namespace to watch
     * @return the kroxylicious operator builder
     */
    public KroxyliciousOperatorBuilder addToTheWatchingNamespaces(String namespaceToWatch) {
        if (this.namespaceToWatch != null) {
            if (!this.namespaceToWatch.equals("*")) {
                this.namespaceToWatch += "," + namespaceToWatch;
            }
        }
        else {
            this.namespaceToWatch = namespaceToWatch;
        }
        return self();
    }

    /**
     * With bindings namespaces kroxylicious operator builder.
     *
     * @param bindingsNamespaces the bindings namespaces
     * @return the kroxylicious operator builder
     */
    public KroxyliciousOperatorBuilder withBindingsNamespaces(List<String> bindingsNamespaces) {
        this.bindingsNamespaces = bindingsNamespaces;
        return self();
    }

    /**
     * Add to the bindings namespaces kroxylicious operator builder.
     *
     * @param bindingsNamespace the bindings namespace
     * @return the kroxylicious operator builder
     */
    public KroxyliciousOperatorBuilder addToTheBindingsNamespaces(String bindingsNamespace) {
        this.bindingsNamespaces = new ArrayList<>(Objects.requireNonNullElseGet(this.bindingsNamespaces, () -> Collections.singletonList(bindingsNamespace)));
        return self();
    }

    /**
     * With operation timeout kroxylicious operator builder.
     *
     * @param operationTimeout the operation timeout
     * @return the kroxylicious operator builder
     */
    public KroxyliciousOperatorBuilder withOperationTimeout(Duration operationTimeout) {
        this.operationTimeout = operationTimeout;
        return self();
    }

    /**
     * With reconciliation interval kroxylicious operator builder.
     *
     * @param reconciliationInterval the reconciliation interval
     * @return the kroxylicious operator builder
     */
    public KroxyliciousOperatorBuilder withReconciliationInterval(Duration reconciliationInterval) {
        this.reconciliationInterval = reconciliationInterval;
        return self();
    }

    /**
     * With extra labels kroxylicious operator builder.
     *
     * @param extraLabels the extra labels
     * @return the kroxylicious operator builder
     */
    public KroxyliciousOperatorBuilder withExtraLabels(Map<String, String> extraLabels) {
        this.extraLabels = extraLabels;
        return self();
    }

    /**
     * With replicas kroxylicious operator builder.
     *
     * @param replicas the replicas
     * @return the kroxylicious operator builder
     */
    public KroxyliciousOperatorBuilder withReplicas(int replicas) {
        this.replicas = replicas;
        return self();
    }

    private KroxyliciousOperatorBuilder self() {
        return this;
    }

    /**
     * Create bundle installation kroxylicious operator bundle installer.
     *
     * @return the kroxylicious operator bundle installer
     */
    public KroxyliciousOperatorBundleInstaller createBundleInstallation() {
        return new KroxyliciousOperatorBundleInstaller(this);
    }

    /**
     * Gets extension context.
     *
     * @return the extension context
     */
    public ExtensionContext getExtensionContext() {
        return extensionContext;
    }

    /**
     * Gets kroxylicious operator name.
     *
     * @return the kroxylicious operator name
     */
    public String getKroxyliciousOperatorName() {
        return kroxyliciousOperatorName;
    }

    /**
     * Gets namespace install to.
     *
     * @return the namespace install to
     */
    public String getNamespaceInstallTo() {
        return namespaceInstallTo;
    }

    /**
     * Gets operation timeout.
     *
     * @return the operation timeout
     */
    public Duration getOperationTimeout() {
        return operationTimeout;
    }

    /**
     * Gets reconciliation interval.
     *
     * @return the reconciliation interval
     */
    public Duration getReconciliationInterval() {
        return reconciliationInterval;
    }

    /**
     * Gets extra labels.
     *
     * @return the extra labels
     */
    public Map<String, String> getExtraLabels() {
        return extraLabels;
    }

    /**
     * Gets replicas.
     *
     * @return the replicas
     */
    public int getReplicas() {
        return replicas;
    }

    /**
     * Gets namespace to watch.
     *
     * @return the namespace to watch
     */
    public String getNamespaceToWatch() {
        return namespaceToWatch;
    }

    /**
     * Gets binding namespaces.
     *
     * @return the binding namespaces
     */
    public List<String> getBindingNamespaces() {
        return bindingsNamespaces;
    }
}
