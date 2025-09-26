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
        map.compute(String.class, "foo", TypeNameMap.Predicate.TYPE_EQUAL_NAME_EQUAL, val -> {
            return 2;
        });
        assertThat(map.lookup(String.class, TypeNameMap.Predicate.TYPE_EQUAL_NAME_STARTS_WITH, "foo"))
                .isNull();
        assertThat(map.lookup(String.class, TypeNameMap.Predicate.TYPE_EQUAL_NAME_STARTS_WITH, "fooo"))
                .isNull();
        assertThat(map.lookup(String.class, TypeNameMap.Predicate.TYPE_EQUAL_NAME_STARTS_WITH, "fo"))
                .isNull();
    }

    @Test
    void testPutWithEq_SearchWithEq() {
        var map = new TypeNameMap<String, Integer>();
        map.compute(String.class, "foo", TypeNameMap.Predicate.TYPE_EQUAL_NAME_EQUAL, val -> {
            return 2;
        });
        assertThat(map.lookup(String.class, TypeNameMap.Predicate.TYPE_EQUAL_NAME_EQUAL, "foo"))
                .isEqualTo(2);
        assertThat(map.lookup(String.class, TypeNameMap.Predicate.TYPE_EQUAL_NAME_EQUAL, "fooo"))
                .isNull();
        assertThat(map.lookup(String.class, TypeNameMap.Predicate.TYPE_EQUAL_NAME_EQUAL, "fo"))
                .isNull();
    }

    @Test
    void testPutWithStarts_SearchWithStarts() {
        var map = new TypeNameMap<String, Integer>();
        map.compute(String.class, "foo", TypeNameMap.Predicate.TYPE_EQUAL_NAME_STARTS_WITH, val -> {
            return 2;
        });
        assertThat(map.lookup(String.class, TypeNameMap.Predicate.TYPE_EQUAL_NAME_STARTS_WITH, "foo"))
                .isEqualTo(2);
        assertThat(map.lookup(String.class, TypeNameMap.Predicate.TYPE_EQUAL_NAME_STARTS_WITH, "fooo"))
                .isEqualTo(2);
        assertThat(map.lookup(String.class, TypeNameMap.Predicate.TYPE_EQUAL_NAME_STARTS_WITH, "fo"))
                .isNull();
    }

    @Test
    void testPutWithStarts_SearchWithEq() {
        var map = new TypeNameMap<String, Integer>();
        map.compute(String.class, "foo", TypeNameMap.Predicate.TYPE_EQUAL_NAME_STARTS_WITH, val -> {
            return 2;
        });
        assertThat(map.lookup(String.class, TypeNameMap.Predicate.TYPE_EQUAL_NAME_EQUAL, "foo"))
                .isNull();
        assertThat(map.lookup(String.class, TypeNameMap.Predicate.TYPE_EQUAL_NAME_EQUAL, "fooo"))
                .isNull();
        assertThat(map.lookup(String.class, TypeNameMap.Predicate.TYPE_EQUAL_NAME_EQUAL, "fo"))
                .isNull();
    }

}