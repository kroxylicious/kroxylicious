/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.dek;

import org.junit.jupiter.api.Test;

import io.kroxylicious.kms.provider.kroxylicious.inmemory.UnitTestingKmsService;

import static org.assertj.core.api.Assertions.assertThat;

class DekManagerTest {

    @Test
    void test1() {
        UnitTestingKmsService unitTestingKmsService = UnitTestingKmsService.newInstance();
        UnitTestingKmsService.Config options = new UnitTestingKmsService.Config(12, 96);
        var kms = unitTestingKmsService.buildKms(options);
        var kekId = kms.generateKey();
        kms.createAlias(kekId, "foo");

        var dm = new DekManager<>(unitTestingKmsService, options, 1);
        var resolvedKekId = dm.resolveAlias("foo").toCompletableFuture().join();
        assertThat(resolvedKekId).isEqualTo(kekId);
    }

}
