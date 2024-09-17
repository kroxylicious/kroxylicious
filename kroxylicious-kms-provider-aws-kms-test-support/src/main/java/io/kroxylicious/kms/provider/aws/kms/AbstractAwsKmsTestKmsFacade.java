/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.aws.kms;

import java.net.URI;

import io.kroxylicious.kms.provider.aws.kms.config.Config;
import io.kroxylicious.kms.service.TestKekManager;
import io.kroxylicious.kms.service.TestKmsFacade;
import io.kroxylicious.proxy.config.secret.InlinePassword;

import edu.umd.cs.findbugs.annotations.NonNull;

public abstract class AbstractAwsKmsTestKmsFacade implements TestKmsFacade<Config, String, AwsKmsEdek> {

    protected AbstractAwsKmsTestKmsFacade() {
    }

    protected abstract void startKms();

    protected abstract void stopKms();

    @Override
    public final void start() {
        startKms();
    }

    @NonNull
    protected abstract URI getAwsUrl();

    @Override
    public Config getKmsServiceConfig() {
        return new Config(getAwsUrl(), new InlinePassword(getAccessKey()), new InlinePassword(getSecretKey()), getRegion(), null);
    }

    protected abstract String getRegion();

    protected abstract String getSecretKey();

    protected abstract String getAccessKey();

    @Override
    public final Class<AwsKmsService> getKmsServiceClass() {
        return AwsKmsService.class;
    }

    @Override
    public final void stop() {
        stopKms();
    }

    @Override
    public final TestKekManager getTestKekManager() {
        return new AwsKmsTestKekManager(getAwsUrl(), getAccessKey(), getSecretKey(), getRegion());
    }
}
