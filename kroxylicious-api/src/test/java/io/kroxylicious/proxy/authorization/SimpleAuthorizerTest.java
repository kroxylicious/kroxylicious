/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authorization;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SimpleAuthorizerTest {

    record RolePrincipal(String name) implements Principal {}

    @Test
    void t() {

        SimpleAuthorizer simple = new SimpleAuthorizer();
        UserPrincipal bob = new UserPrincipal("bob");
        UserPrincipal alice = new UserPrincipal("alice");
        RolePrincipal admins = new RolePrincipal("admins");
        simple.grant(Set.of(TopicResource.READ, TopicResource.WRITE), "my-topic", Set.of(bob));
        simple.grant(Set.of(TopicResource.WRITE), "your", Set.of(bob, alice));
        simple.grantToAllPrincipalsOfType(EnumSet.allOf(ClusterResource.class), "your", UserPrincipal.class);
        Authorizer a = simple;

        System.out.print(a);

        List<Action> query = Stream.concat(TopicResource.READ.of(List.of("my-topic")).stream(),
                TopicResource.WRITE.of(List.of("your")).stream()).toList();
        var bobresult = a.authorize(
                new Subject(Set.of(bob)),
                query);
        var bobdecision = bobresult.decision(TopicResource.READ, "my-topic");

        var aliceresult = a.authorize(
                new Subject(Set.of(alice)),
                query);
        // TODO what's most helpful?
        // If it's grouped by Decision then can easily shortcircuit if there's no ALLOW
        //        if (result.allDenied()) {
        //            // shortcircuit
        //            result.denied()...
        //        }

        //result.denied().stream().map();

        var alicedecision = aliceresult.decision(TopicResource.READ, "my-topic");

        System.out.println("bobdecision: " + bobresult);
        System.out.println("aliceresult: " + aliceresult);

        System.out.println("bob connect: " + a.authorize(
                new Subject(Set.of(bob)),
                List.of(new Action(ClusterResource.CONNECT, "your"))).decision(ClusterResource.CONNECT, "your"));
        System.out.println("alice connect: " + a.authorize(
                new Subject(Set.of(alice)),
                List.of(new Action(ClusterResource.CONNECT, "your"))).decision(ClusterResource.CONNECT, "your"));

    }

}