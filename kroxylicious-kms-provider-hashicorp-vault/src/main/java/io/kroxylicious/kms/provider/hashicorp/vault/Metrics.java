/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;

import static io.micrometer.core.instrument.Metrics.counter;

public class Metrics {

    private static final Logger LOG = LoggerFactory.getLogger(Metrics.class);

    public record OperationMetrics(Counter attempt, Counter success, Counter notFound, Counter exception) {

    }

    static final Tag OUTCOME_SUCCESS = Tag.of("outcome", "success");
    static final Tag OUTCOME_FAILURE = Tag.of("outcome", "failure");
    static final Tag EXCEPTION_FAILURE = Tag.of("failure", "exception");
    static final Tag NOT_FOUND_FAILURE = Tag.of("failure", "not_found");

    // the prometheus java SDK requires all the permutations of a meter to have the same set of tags
    static final Tag EMPTY_FAILURE = Tag.of("failure", "");
    static final Tag CREATE_DATAKEY_OPERATION = Tag.of("operation", "create_datakey");
    static final Tag DECRYPT_EDEK_OPERATION = Tag.of("operation", "decrypt_edek");
    static final Tag RESOLVE_ALIAS_OPERATION = Tag.of("operation", "resolve_alias");
    static final String OUTCOME_NAME = "kroxylicious_kms_vault_request_outcome_total";
    static final String ATTEMPT_NAME = "kroxylicious_kms_vault_request_attempt_total";
    private static OperationMetrics CREATE_DATA_KEY;
    private static OperationMetrics DECRYPT_EDEK;
    private static OperationMetrics RESOLVE_ALIAS;

    static {
        initialize();
    }

    public static void initialize() {
        LOG.trace("initializing metric class members");

        CREATE_DATA_KEY = new OperationMetrics(counter(ATTEMPT_NAME, List.of(CREATE_DATAKEY_OPERATION)),
                counter(OUTCOME_NAME, List.of(CREATE_DATAKEY_OPERATION, OUTCOME_SUCCESS, EMPTY_FAILURE)),
                counter(OUTCOME_NAME, List.of(CREATE_DATAKEY_OPERATION, OUTCOME_FAILURE, NOT_FOUND_FAILURE)),
                counter(OUTCOME_NAME, List.of(CREATE_DATAKEY_OPERATION, OUTCOME_FAILURE, EXCEPTION_FAILURE)));

        DECRYPT_EDEK = new OperationMetrics(counter(ATTEMPT_NAME, List.of(DECRYPT_EDEK_OPERATION)),
                counter(OUTCOME_NAME, List.of(DECRYPT_EDEK_OPERATION, OUTCOME_SUCCESS, EMPTY_FAILURE)),
                counter(OUTCOME_NAME, List.of(DECRYPT_EDEK_OPERATION, OUTCOME_FAILURE, NOT_FOUND_FAILURE)),
                counter(OUTCOME_NAME, List.of(DECRYPT_EDEK_OPERATION, OUTCOME_FAILURE, EXCEPTION_FAILURE)));

        RESOLVE_ALIAS = new OperationMetrics(counter(ATTEMPT_NAME, List.of(RESOLVE_ALIAS_OPERATION)),
                counter(OUTCOME_NAME, List.of(RESOLVE_ALIAS_OPERATION, OUTCOME_SUCCESS, EMPTY_FAILURE)),
                counter(OUTCOME_NAME, List.of(RESOLVE_ALIAS_OPERATION, OUTCOME_FAILURE, NOT_FOUND_FAILURE)),
                counter(OUTCOME_NAME, List.of(RESOLVE_ALIAS_OPERATION, OUTCOME_FAILURE, EXCEPTION_FAILURE)));
    }

    static OperationMetrics createDataKey() {
        return CREATE_DATA_KEY;
    }

    static OperationMetrics decryptEdek() {
        return DECRYPT_EDEK;
    }

    static OperationMetrics resolveAlias() {
        return RESOLVE_ALIAS;
    }
}
