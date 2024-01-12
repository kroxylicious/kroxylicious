/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.strimzi;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidObjectException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.dsl.NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.utils.DeploymentUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type Strimzi.
 */
public class Strimzi {
    private static final Logger LOGGER = LoggerFactory.getLogger(Strimzi.class);
    private final String deploymentNamespace;
    private final NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable<HasMetadata> deployment;

    /**
     * Instantiates a new Strimzi.
     *
     * @param deploymentNamespace the deployment namespace
     */
    public Strimzi(String deploymentNamespace) throws IOException {
        this.deploymentNamespace = deploymentNamespace;
        InputStream strimziYaml = DeploymentUtils.getDeploymentFileFromURL(Environment.STRIMZI_URL);
        InputStream strimziYamlKRaft = configureKRaftModeForStrimzi(strimziYaml);
        deployment = kubeClient().getClient().load(strimziYamlKRaft);
    }

    private InputStream configureKRaftModeForStrimzi(InputStream strimziYaml) throws IOException {
        YAMLFactory yamlFactory = new YAMLFactory();
        ObjectMapper mapper = new YAMLMapper();

        YAMLParser yamlParser = yamlFactory.createParser(strimziYaml);
        List<JsonNode> docs = mapper
                .readValues(yamlParser, new TypeReference<JsonNode>() {
                })
                .readAll();
        boolean found = false;
        for (JsonNode doc : docs) {
            String kind = doc.at("/kind").asText("");
            if (kind.equals(Constants.DEPLOYMENT)) {
                ArrayNode arrayNode = (ArrayNode) doc.at("/spec/template/spec/containers");
                ArrayNode envNode = (ArrayNode) arrayNode.get(0).at("/env");
                for (JsonNode node : envNode) {
                    if (node.at("/name").asText().equals(Environment.STRIMZI_FEATURE_GATES_ENV)) {
                        found = true;
                        ((ObjectNode) node).put("value", String.join(",", Constants.USE_KRAFT_MODE, Constants.USE_KAFKA_NODE_POOLS));
                    }
                }
            }
        }

        if (!found) {
            throw new InvalidObjectException("STRIMZI_FEATURE_GATES env variable not found in yaml!");
        }

        return new ByteArrayInputStream(mapper.writeValueAsBytes(docs));
    }

    /**
     * Deploy strimzi.
     */
    public void deploy() {
        LOGGER.info("Deploy Strimzi in {} namespace", deploymentNamespace);
        if (kubeClient().getDeployment(deploymentNamespace, Constants.STRIMZI_DEPLOYMENT_NAME) != null) {
            LOGGER.warn("Skipping strimzi deployment. It is already deployed!");
            return;
        }
        deployment.inNamespace(deploymentNamespace).create();
        DeploymentUtils.waitForDeploymentReady(deploymentNamespace, Constants.STRIMZI_DEPLOYMENT_NAME);
    }

    /**
     * Delete strimzi.
     * @throws IOException the io exception
     */
    public void delete() throws IOException {
        LOGGER.info("Deleting Strimzi in {} namespace", deploymentNamespace);
        deployment.inNamespace(deploymentNamespace).delete();
        DeploymentUtils.waitForDeploymentDeletion(deploymentNamespace, Constants.STRIMZI_DEPLOYMENT_NAME);
    }
}
