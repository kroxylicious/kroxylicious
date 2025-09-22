/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authorization.simple;

import java.util.Set;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.authorization.ClusterResource;
import io.kroxylicious.proxy.authorization.TopicResource;

import static org.assertj.core.api.Assertions.assertThat;

class TypePatternMatchTest {
    @Test
    void match() {
        var m = new TypePatternMatch();
        m.compute(TopicResource.class, Pattern.compile("baz*"), Set.of(TopicResource.CREATE));
        m.compute(TopicResource.class, Pattern.compile("baz*"), Set.of(TopicResource.ALTER));
        m.compute(ClusterResource.class, Pattern.compile("foo*"), Set.of(ClusterResource.CONNECT));

        assertThat(m.lookup(TopicResource.class, "absent")).isNull();
        assertThat(m.lookup(TopicResource.class, "ba")).isEqualTo(Set.of(TopicResource.CREATE, TopicResource.ALTER));
        assertThat(m.lookup(TopicResource.class, "baz")).isEqualTo(Set.of(TopicResource.CREATE, TopicResource.ALTER));
        assertThat(m.lookup(TopicResource.class, "bazz")).isEqualTo(Set.of(TopicResource.CREATE, TopicResource.ALTER));

        assertThat(m.lookup(ClusterResource.class, "absent")).isNull();
        assertThat(m.lookup(ClusterResource.class, "fo")).isEqualTo(Set.of(ClusterResource.CONNECT));
        assertThat(m.lookup(ClusterResource.class, "foo")).isEqualTo(Set.of(ClusterResource.CONNECT));
        assertThat(m.lookup(ClusterResource.class, "fooo")).isEqualTo(Set.of(ClusterResource.CONNECT));
    }

}