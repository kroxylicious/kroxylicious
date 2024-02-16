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

import io.kroxylicious.systemtests.utils.NamespaceUtils;

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
import io.kroxylicious.systemtests.utils.TestUtils;

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
        InputStream strimziMultiDocumentYaml = DeploymentUtils.getDeploymentFileFromURL(Environment.STRIMZI_URL);
        InputStream strimziMultiDocumentYamlKRaft = configureKRaftModeForStrimzi(strimziMultiDocumentYaml);
        deployment = kubeClient().getClient().load(strimziMultiDocumentYamlKRaft);
    }

    private InputStream configureKRaftModeForStrimzi(InputStream strimziMultiDocumentYaml) throws IOException {
        YAMLFactory yamlFactory = new YAMLFactory();
        ObjectMapper mapper = new YAMLMapper();

        YAMLParser multiDocumentYamlParser = yamlFactory.createParser(strimziMultiDocumentYaml);
        List<JsonNode> docs = mapper
                .readValues(multiDocumentYamlParser, new TypeReference<JsonNode>() {
                })
                .readAll();
        boolean found = false;
        List<JsonNode> deploymentDocs = docs.stream().filter(p -> p.at("/kind").asText("").equals(Constants.DEPLOYMENT)).toList();
        for (JsonNode doc : deploymentDocs) {
            ArrayNode arrayNode = (ArrayNode) doc.at("/spec/template/spec/containers");
            ArrayNode envNode = (ArrayNode) arrayNode.get(0).at("/env");
            for (JsonNode node : envNode) {
                if (node.at("/name").asText().equals(Environment.STRIMZI_FEATURE_GATES_ENV)) {
                    found = true;
                    String value = node.at("/value").asText();
                    if (value.isEmpty() || value.isBlank()) {
                        value = String.join(",", Constants.USE_KRAFT_MODE, Constants.USE_KAFKA_NODE_POOLS);
                    }
                    else {
                        value = value.replace(Constants.DONT_USE_KRAFT_MODE, Constants.USE_KRAFT_MODE)
                                .replace(Constants.DONT_USE_KAFKA_NODE_POOLS, Constants.USE_KAFKA_NODE_POOLS);

                        value = TestUtils.concatStringIfValueDontExist(value, Constants.USE_KRAFT_MODE, ",");
                        value = TestUtils.concatStringIfValueDontExist(value, Constants.USE_KAFKA_NODE_POOLS, ",");
                    }
                    ((ObjectNode) node).put("value", value);
                    break;
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
        if (kubeClient().getDeployment(deploymentNamespace, Constants.STRIMZI_DEPLOYMENT_NAME) != null
            || Environment.STRIMZI_INSTALLED.equalsIgnoreCase("true")) {
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
        if (Environment.STRIMZI_INSTALLED.equalsIgnoreCase("true")) {
            LOGGER.warn("Skipping Strimzi deletion. STRIMZI_INSTALLED was set to true");
            return;
        }
        LOGGER.info("Deleting Strimzi in {} namespace", deploymentNamespace);
        deployment.inNamespace(deploymentNamespace).delete();
        DeploymentUtils.waitForDeploymentDeletion(deploymentNamespace, Constants.STRIMZI_DEPLOYMENT_NAME);
    }
}
