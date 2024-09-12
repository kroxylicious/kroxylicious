/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.app;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BannerSupplierTest {

    private List<String> licenseStream;

    @BeforeEach
    void setUp() {
        licenseStream = List.of(
                "====",
                "Copyright Kroxylicious Authors.",
                "Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0"
        );
    }

    @Test
    void shouldRemoveLicenseHeader() {
        // Given
        final BannerLogger.BannerSupplier bannerSupplier = new BannerLogger.BannerSupplier(licenseStream::stream, licenseStream::stream);

        // When
        final Stream<String> actualBannerStream = bannerSupplier.get();

        // Then
        assertThat(actualBannerStream).isEmpty();
    }

    @Test
    void shouldPassThroughBannerStream() {
        // Given
        final List<String> bannerLines = List.of("one", "two", "three");
        final BannerLogger.BannerSupplier bannerSupplier = new BannerLogger.BannerSupplier(bannerLines::stream, Stream::empty);

        // When
        final Stream<String> actualBannerStream = bannerSupplier.get();

        // Then
        assertThat(actualBannerStream).containsExactlyInAnyOrderElementsOf(bannerLines);
    }

    @Test
    void shouldPassThroughRemainingBannerStream() {
        // Given
        final List<String> expectedLines = List.of("one", "two", "three");
        final List<String> bannerLines = new ArrayList<>();
        bannerLines.addAll(licenseStream);
        bannerLines.addAll(expectedLines);

        final BannerLogger.BannerSupplier bannerSupplier = new BannerLogger.BannerSupplier(bannerLines::stream, licenseStream::stream);

        // When
        final Stream<String> actualBannerStream = bannerSupplier.get();

        // Then
        assertThat(actualBannerStream).containsExactlyInAnyOrderElementsOf(expectedLines);
    }
}
