/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.testclients;

import java.util.List;
import java.util.Map;

import io.fabric8.kubernetes.api.model.CapabilitiesBuilder;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.SeccompProfileBuilder;
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;

/**
 * The type Test Clients job templates.
 */
public class TestClientsJobTemplates {

    private TestClientsJobTemplates() {
    }

    /**
     * Default admin client job builder.
     *
     * @param jobName the job name
     * @param args the args
     * @return the deployment builder
     */
    public static JobBuilder defaultAdminClientJob(String jobName, List<String> args) {
        Map<String, String> labelSelector = Map.of("app", jobName);
        return new JobBuilder()
                .withApiVersion("batch/v1")
                .withKind(Constants.JOB)
                .withNewMetadata()
                .withName(jobName)
                .addToLabels(labelSelector)
                .endMetadata()
                .withNewSpec()
                .withBackoffLimit(0)
                .withCompletions(1)
                .withParallelism(1)
                .withNewTemplate()
                .withNewMetadata()
                .withLabels(labelSelector)
                .withName(jobName)
                .endMetadata()
                .withNewSpec()
                .withContainers(new ContainerBuilder()
                        .withName("admin")
                        .withImage(Constants.TEST_CLIENTS_IMAGE)
                        .withImagePullPolicy("IfNotPresent")
                        .withCommand("admin-client")
                        .withArgs(args)
                        .withSecurityContext(new SecurityContextBuilder()
                                .withAllowPrivilegeEscalation(false)
                                .withSeccompProfile(new SeccompProfileBuilder()
                                        .withType("RuntimeDefault")
                                        .build())
                                .withCapabilities(new CapabilitiesBuilder()
                                        .withDrop("ALL")
                                        .build())
                                .build())
                        .build())
                .withRestartPolicy("Never")
                .endSpec()
                .endTemplate()
                .endSpec();
    }
}
