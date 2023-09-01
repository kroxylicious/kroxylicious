/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.kroxylicious.systemtest.utils.specific;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.fabric8.openshift.api.model.operatorhub.v1alpha1.InstallPlan;
import io.strimzi.test.TestUtils;

import io.kroxylicious.systemtest.Constants;
import io.kroxylicious.systemtest.resources.operator.SetupClusterOperator;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

public class OlmUtils {

    private static final Logger LOGGER = LogManager.getLogger(SetupClusterOperator.class);

    private OlmUtils() {
    }

    public static void waitUntilNonUsedInstallPlanIsPresent(String namespaceName) {
        TestUtils.waitFor("unused InstallPlan to be present", Constants.OLM_UPGRADE_INSTALL_PLAN_POLL, Constants.OLM_UPGRADE_INSTALL_PLAN_TIMEOUT,
                () -> kubeClient().getNonApprovedInstallPlan(namespaceName) != null);
    }

    public static void waitUntilNonUsedInstallPlanWithSpecificCsvIsPresentAndApprove(String namespaceName, String csvName) {
        TestUtils.waitFor("unused InstallPlan with CSV: " + namespaceName + "/" + csvName + " to be present", Constants.OLM_UPGRADE_INSTALL_PLAN_POLL,
                Constants.OLM_UPGRADE_INSTALL_PLAN_TIMEOUT,
                () -> {
                    if (kubeClient().getNonApprovedInstallPlan(namespaceName) != null) {
                        InstallPlan installPlan = kubeClient().getNonApprovedInstallPlan(namespaceName);
                        if (installPlan.getSpec().getClusterServiceVersionNames().get(0).contains(csvName)) {
                            kubeClient().approveInstallPlan(namespaceName, installPlan.getMetadata().getName());
                            return true;
                        }
                    }
                    return false;
                });
    }
}
