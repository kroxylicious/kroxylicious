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

import org.junit.jupiter.api.extension.ExtensionContext;

public class KroxyliciousOperatorBuilder {

    public ExtensionContext extensionContext;
    public String kroxyliciousOperatorName;
    public String namespaceInstallTo;
    public String namespaceToWatch;
    public List<String> bindingsNamespaces;
    public Duration operationTimeout;
    public Duration reconciliationInterval;
    public Map<String, String> extraLabels;
    public int replicas = 1;

    public KroxyliciousOperatorBuilder withExtensionContext(ExtensionContext extensionContext) {
        this.extensionContext = extensionContext;
        return self();
    }

    public KroxyliciousOperatorBuilder withKroxyliciousOperatorName(String kroxyliciousOperatorName) {
        this.kroxyliciousOperatorName = kroxyliciousOperatorName;
        return self();
    }

    public KroxyliciousOperatorBuilder withNamespace(String namespaceInstallTo) {
        this.namespaceInstallTo = namespaceInstallTo;
        return self();
    }

    public KroxyliciousOperatorBuilder withWatchingNamespaces(String namespaceToWatch) {
        this.namespaceToWatch = namespaceToWatch;
        return self();
    }

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

    public KroxyliciousOperatorBuilder withBindingsNamespaces(List<String> bindingsNamespaces) {
        this.bindingsNamespaces = bindingsNamespaces;
        return self();
    }

    public KroxyliciousOperatorBuilder addToTheBindingsNamespaces(String bindingsNamespace) {
        if (this.bindingsNamespaces != null) {
            this.bindingsNamespaces = new ArrayList<>(this.bindingsNamespaces);
        }
        else {
            this.bindingsNamespaces = new ArrayList<>(Collections.singletonList(bindingsNamespace));
        }
        return self();
    }

    public KroxyliciousOperatorBuilder withOperationTimeout(Duration operationTimeout) {
        this.operationTimeout = operationTimeout;
        return self();
    }

    public KroxyliciousOperatorBuilder withReconciliationInterval(Duration reconciliationInterval) {
        this.reconciliationInterval = reconciliationInterval;
        return self();
    }

    public KroxyliciousOperatorBuilder withExtraLabels(Map<String, String> extraLabels) {
        this.extraLabels = extraLabels;
        return self();
    }

    public KroxyliciousOperatorBuilder withReplicas(int replicas) {
        this.replicas = replicas;
        return self();
    }

    private KroxyliciousOperatorBuilder self() {
        return this;
    }

    public KroxyliciousOperatorBundleInstaller createBundleInstallation() {
        return new KroxyliciousOperatorBundleInstaller(this);
    }
}
