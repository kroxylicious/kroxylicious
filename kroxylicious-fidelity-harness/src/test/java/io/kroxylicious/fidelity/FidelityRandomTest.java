/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity;

import java.util.List;
import java.util.Random;

import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.protocol.types.BoundField;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.ClearSystemProperty;
import org.junitpioneer.jupiter.SetSystemProperty;

import io.kroxylicious.fidelity.populate.NullableFieldEnumerator;

import static org.assertj.core.api.Assertions.assertThat;

class FidelityRandomTest {

    @BeforeEach
    @AfterEach
    void resetBaseSeed() {
        FidelityRandom.resetBaseSeedForTesting();
    }

    @Test
    @ClearSystemProperty(key = FidelityRandom.SEED_PROPERTY)
    void defaultRunPicksNonNullSeed() {
        long seed = FidelityRandom.baseSeed();
        assertThat(FidelityRandom.baseSeed()).isEqualTo(seed);
    }

    @Test
    @SetSystemProperty(key = FidelityRandom.SEED_PROPERTY, value = "987654321")
    void systemPropertyOverridesBaseSeedDecimal() {
        assertThat(FidelityRandom.baseSeed()).isEqualTo(987654321L);
    }

    @Test
    @SetSystemProperty(key = FidelityRandom.SEED_PROPERTY, value = "0x1234abcd")
    void systemPropertyOverridesBaseSeedHex() {
        assertThat(FidelityRandom.baseSeed()).isEqualTo(0x1234abcdL);
    }

    @Test
    void deterministicCaseSeedDerivation() {
        long baseSeed = 42L;
        long seed1 = FidelityRandom.deriveSeed(baseSeed, "HeartbeatRequest - v0");
        long seed2 = FidelityRandom.deriveSeed(baseSeed, "HeartbeatRequest - v0");
        long seedDifferentCase = FidelityRandom.deriveSeed(baseSeed, "ProduceRequest - v0");
        long seedDifferentBase = FidelityRandom.deriveSeed(43L, "HeartbeatRequest - v0");

        assertThat(seed1)
                .isEqualTo(seed2)
                .isNotEqualTo(seedDifferentCase)
                .isNotEqualTo(seedDifferentBase);
    }

    @Test
    void executionOrderIndependence() {
        FidelityRandom.setBaseSeedForTesting(100L);

        Random randomA1 = FidelityRandom.randomFor("CaseA");
        Random randomB1 = FidelityRandom.randomFor("CaseB");

        int valA1 = randomA1.nextInt();
        int valB1 = randomB1.nextInt();

        // Access in reverse order
        Random randomB2 = FidelityRandom.randomFor("CaseB");
        Random randomA2 = FidelityRandom.randomFor("CaseA");

        assertThat(randomB2.nextInt()).isEqualTo(valB1);
        assertThat(randomA2.nextInt()).isEqualTo(valA1);
    }

    @Test
    void caseIdentifierFromMessageAndVersion() {
        HeartbeatRequestData request = new HeartbeatRequestData();
        HeartbeatResponseData response = new HeartbeatResponseData();

        assertThat(FidelityRandom.caseIdentifier(request, (short) 1)).isEqualTo("HeartbeatRequest - v1");
        assertThat(FidelityRandom.caseIdentifier(response, (short) 2)).isEqualTo("HeartbeatResponse - v2");
    }

    @Test
    void caseIdentifierFromMessageVersionAndNullableField() {
        JoinGroupRequestData request = new JoinGroupRequestData();
        List<BoundField> fields = NullableFieldEnumerator.nullableFields(request, (short) 5);
        assertThat(fields).isNotEmpty();
        BoundField field = fields.get(0);

        assertThat(FidelityRandom.caseIdentifier(request, (short) 5, field))
                .isEqualTo("JoinGroupRequest - v5 - null " + field.def.name);
    }
}
