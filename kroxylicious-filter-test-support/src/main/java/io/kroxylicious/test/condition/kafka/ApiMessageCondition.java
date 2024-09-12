/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.condition.kafka;

import java.util.function.Predicate;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.assertj.core.api.Condition;
import org.assertj.core.description.Description;
import org.assertj.core.description.TextDescription;

public class ApiMessageCondition<X extends ApiMessage> extends Condition<X> {

    private final Predicate<X> predicate;

    public ApiMessageCondition(Predicate<X> xPredicate, Description description) {
        super(description);
        this.predicate = xPredicate;
    }

    public static <X extends ApiMessage> ApiMessageCondition<X> forApiKey(short expectedApiKey) {
        return new ApiMessageCondition<>(
                apiMessage -> apiMessage.apiKey() == expectedApiKey,
                new TextDescription("an ApiMessage of type %s (%d)", ApiKeys.forId(expectedApiKey), expectedApiKey)
        );
    }

    public static <X extends ApiMessage> ApiMessageCondition<X> forApiKey(ApiKeys expectedApiKey) {
        return new ApiMessageCondition<>(
                apiMessage -> ApiKeys.forId(apiMessage.apiKey()) == expectedApiKey,
                new TextDescription("an ApiMessage of type %s (%d)", expectedApiKey, expectedApiKey.id)
        );
    }

    public ApiMessageCondition(Predicate<X> predicate) {
        this(predicate, new TextDescription("an Api Message matching a custom predicate"));
    }

    @Override
    public boolean matches(X apiMessage) {
        if (apiMessage != null) {
            return predicate.test(apiMessage);
        } else {
            return false;
        }
    }
}
