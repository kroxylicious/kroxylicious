/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TypeNameMapTest {

    @Test
    void testPutWithEq_SearchWithStarts() {
        var map = new TypeNameMap<String, Integer>();
        map.addApply(new ResourceMatcherNameEquals<>(String.class, "foo"), val -> {
            return 2;
        });
        assertThat(map.matchingOperations(new ResourceMatcherNameStarts<>(String.class, "foo")))
                .isNull();
        assertThat(map.matchingOperations(new ResourceMatcherNameStarts<>(String.class, "fooo")))
                .isNull();
        assertThat(map.matchingOperations(new ResourceMatcherNameStarts<>(String.class, "fo")))
                .isNull();
    }

    @Test
    void testPutWithEq_SearchWithEq() {
        var map = new TypeNameMap<String, Integer>();
        map.addApply(new ResourceMatcherNameEquals<>(String.class, "foo"), val -> {
            return 2;
        });
        assertThat(map.matchingOperations(new ResourceMatcherNameEquals<>(String.class, "foo")))
                .isEqualTo(2);
        assertThat(map.matchingOperations(new ResourceMatcherNameEquals<>(String.class, "fooo")))
                .isNull();
        assertThat(map.matchingOperations(new ResourceMatcherNameEquals<>(String.class, "fo")))
                .isNull();
    }

    @Test
    void testPutWithStarts_SearchWithStarts() {
        var map = new TypeNameMap<String, Integer>();
        map.addApply(new ResourceMatcherNameStarts<>(String.class, "foo"), val -> {
            return 2;
        });
        assertThat(map.matchingOperations(new ResourceMatcherNameStarts<>(String.class, "foo")))
                .isEqualTo(2);
        assertThat(map.matchingOperations(new ResourceMatcherNameStarts<>(String.class, "fooo")))
                .isEqualTo(2);
        assertThat(map.matchingOperations(new ResourceMatcherNameStarts<>(String.class, "fo")))
                .isNull();
    }

    @Test
    void testPutWithStarts_SearchWithEq() {
        var map = new TypeNameMap<String, Integer>();
        map.addApply(new ResourceMatcherNameStarts<>(String.class, "foo"), val -> {
            return 2;
        });
        assertThat(map.matchingOperations(new ResourceMatcherNameEquals<>(String.class, "foo")))
                .isNull();
        assertThat(map.matchingOperations(new ResourceMatcherNameEquals<>(String.class, "fooo")))
                .isNull();
        assertThat(map.matchingOperations(new ResourceMatcherNameEquals<>(String.class, "fo")))
                .isNull();
    }

}