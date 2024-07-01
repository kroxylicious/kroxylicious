/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms.aws;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.core.type.TypeReference;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.provider.aws.kms.model.DescribeKeyResponse;
import io.kroxylicious.kms.service.TestKekManager;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.systemtests.executor.ExecResult;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;

public abstract class AbstractAwsKmsTestKekManager implements TestKekManager {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public void generateKek(String alias) {
        Objects.requireNonNull(alias);

        if (exists(alias)) {
            throw new AlreadyExistsException(alias);
        }
        else {
            create(alias);
        }
    }

    @Override
    public void rotateKek(String alias) {
        Objects.requireNonNull(alias);

        if (!exists(alias)) {
            throw new UnknownAliasException(alias);
        }
        else {
            rotate(alias);
        }
    }

    @Override
    public void deleteKek(String alias) {
        if (!exists(alias)) {
            throw new UnknownAliasException(alias);
        }
        else {
            delete(alias);
        }
    }

    @Override
    public boolean exists(String alias) {
        try {
            read(alias);
            return true;
        }
        catch (KubeClusterException nfe) {
            return false;
        }
    }

    abstract void create(String alias);

    abstract DescribeKeyResponse read(String alias);

    abstract void rotate(String alias);

    abstract void delete(String alias);

    abstract ExecResult runAwsKmsCommand(String... command);

    protected <T> T runAwsKmsCommand(TypeReference<T> valueTypeRef, String... command) {
        try {
            var execResult = runAwsKmsCommand(command);
            return OBJECT_MAPPER.readValue(execResult.out(), valueTypeRef);
        }
        catch (IOException e) {
            throw new KubeClusterException("Failed to run AWS Kms command: %s".formatted(List.of(command)), e);
        }
    }
}
