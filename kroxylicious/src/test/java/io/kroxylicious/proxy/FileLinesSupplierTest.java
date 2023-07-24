/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.util.stream.Stream;

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;

import static java.util.function.Predicate.not;
import static org.assertj.core.api.Assertions.assertThat;

class FileLinesSupplierTest {

    @Test
    void shouldSupplyEmptyStreamForMissingResource() {
        // Given
        final BannerLogger.FileLinesSupplier bannerSupplier = new BannerLogger.FileLinesSupplier("random.txt");

        // When
        final Stream<String> actualStream = bannerSupplier.get();

        // Then
        assertThat(actualStream).isEmpty();
    }

    @Test
    void shouldSupplyStreamForDefaultResource() {
        // Given
        final BannerLogger.FileLinesSupplier bannerSupplier = new BannerLogger.FileLinesSupplier("banner.txt");

        // When
        final Stream<String> actualStream = bannerSupplier.get();

        // Then
        assertThat(actualStream).areAtLeastOne(new Condition<>(not(String::isBlank), "Not blank"));
    }
}