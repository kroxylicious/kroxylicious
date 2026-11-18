/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TypeNameMapTest {

    // Test types for type boundary testing
    static class Type1 {
    }

    static class Type2 {
    }

    static class Type3 {
    }

    @Test
    void putWithEq_SearchWithStartsNeverMatches() {
        var map = new TypeNameMap<String, Integer>();
        map.addApply(new ResourceMatcherNameEquals<>(String.class, "foo"), val -> 2);
        assertThat(map.matchingValues(new ResourceMatcherNameStarts<>(String.class, "foo")))
                .isEmpty();
        assertThat(map.matchingValues(new ResourceMatcherNameStarts<>(String.class, "fooo")))
                .isEmpty();
        assertThat(map.matchingValues(new ResourceMatcherNameStarts<>(String.class, "fo")))
                .isEmpty();
    }

    @Test
    void putWithEq_SearchWithEqMatchesExactName() {
        var map = new TypeNameMap<String, Integer>();
        map.addApply(new ResourceMatcherNameEquals<>(String.class, "foo"), val -> 2);
        assertThat(map.matchingValues(new ResourceMatcherNameEquals<>(String.class, "foo")))
                .containsExactly(2);
        assertThat(map.matchingValues(new ResourceMatcherNameEquals<>(String.class, "fooo")))
                .isEmpty();
        assertThat(map.matchingValues(new ResourceMatcherNameEquals<>(String.class, "fo")))
                .isEmpty();
    }

    @Test
    void putWithStarts_SearchWithStartsMatchesOnPrefix() {
        var map = new TypeNameMap<String, Integer>();
        map.addApply(new ResourceMatcherNameStarts<>(String.class, "foo"), val -> 2);
        assertThat(map.matchingValues(new ResourceMatcherNameStarts<>(String.class, "foo")))
                .containsExactly(2);
        assertThat(map.matchingValues(new ResourceMatcherNameStarts<>(String.class, "fooo")))
                .containsExactly(2);
        assertThat(map.matchingValues(new ResourceMatcherNameStarts<>(String.class, "fo")))
                .isEmpty();
    }

    @Test
    void putWithStarts_SearchWithEqNeverMatches() {
        var map = new TypeNameMap<String, Integer>();
        map.addApply(new ResourceMatcherNameStarts<>(String.class, "foo"), val -> 2);
        assertThat(map.matchingValues(new ResourceMatcherNameEquals<>(String.class, "foo")))
                .isEmpty();
        assertThat(map.matchingValues(new ResourceMatcherNameEquals<>(String.class, "fooo")))
                .isEmpty();
        assertThat(map.matchingValues(new ResourceMatcherNameEquals<>(String.class, "fo")))
                .isEmpty();
    }

    @Test
    void putWithAny_SearchWithAnyMatches() {
        var map = new TypeNameMap<String, Integer>();
        map.addApply(new ResourceMatcherAnyOfType<>(String.class), val -> 2);
        assertThat(map.matchingValues(new ResourceMatcherAnyOfType<>(String.class)))
                .containsExactly(2);
    }

    @Test
    void putWithAny_SearchWithEqNeverMatches() {
        var map = new TypeNameMap<String, Integer>();
        map.addApply(new ResourceMatcherAnyOfType<>(String.class), val -> 2);
        assertThat(map.matchingValues(new ResourceMatcherNameEquals<>(String.class, "foo")))
                .isEmpty();
    }

    @Test
    void putWithAny_SearchWithStartsNeverMatches() {
        var map = new TypeNameMap<String, Integer>();
        map.addApply(new ResourceMatcherAnyOfType<>(String.class), val -> 2);
        assertThat(map.matchingValues(new ResourceMatcherNameStarts<>(String.class, "foo")))
                .isEmpty();
    }

    @Test
    void putWithOverlappingStarts_SearchWithStartsMatchesShorterPrefixOnly() {
        // Given
        var map = new TypeNameMap<String, Integer>();
        // Add two overlapping prefix rules: "team-b-internal*" and "team-b*"
        map.addApply(new ResourceMatcherNameStarts<>(String.class, "team-b-internal"), val -> 100);
        map.addApply(new ResourceMatcherNameStarts<>(String.class, "team-b"), val -> 200);

        // When
        var result = map.matchingValues(new ResourceMatcherNameStarts<>(String.class, "team-b-public"));

        // Then
        assertThat(result)
                .as("team-b-public should match the team-b* prefix rule")
                .containsExactly(200);
    }

    @Test
    void putWithOverlappingStarts_SearchWithStartsMatchesAllLongestFirst() {
        // Given
        var map = new TypeNameMap<String, Integer>();
        map.addApply(new ResourceMatcherNameStarts<>(String.class, "a"), val -> 1);
        map.addApply(new ResourceMatcherNameStarts<>(String.class, "ab"), val -> 2);
        map.addApply(new ResourceMatcherNameStarts<>(String.class, "abc"), val -> 3);

        // When / Then
        // Every overlapping prefix matches: a shorter one can carry a rule the longer one does not
        assertThat(map.matchingValues(new ResourceMatcherNameStarts<>(String.class, "abc-def")))
                .containsExactly(3, 2, 1);
    }

    @Test
    void emptyPrefixMatchesEverything() {
        // Given
        var map = new TypeNameMap<String, Integer>();
        map.addApply(new ResourceMatcherNameStarts<>(String.class, ""), val -> 1);

        // When / Then
        assertThat(map.matchingValues(new ResourceMatcherNameStarts<>(String.class, "anything")))
                .containsExactly(1);
    }

    @Test
    void prefixOfAnotherTypeNeverMatches() {
        var map = new TypeNameMap<Object, Integer>();
        map.addApply(new ResourceMatcherNameStarts<>(Type2.class, "prefix"), val -> 999);

        // When / Then
        assertThat(map.matchingValues(new ResourceMatcherNameStarts<>(Type3.class, "prefix-x")))
                .isEmpty();
        assertThat(map.matchingValues(new ResourceMatcherNameStarts<>(Type1.class, "prefix-x")))
                .isEmpty();
        assertThat(map.matchingValues(new ResourceMatcherNameStarts<>(Type2.class, "prefix-x")))
                .containsExactly(999);
    }

    @Test
    void nonMatchingPrefixOfEarlierTypeMustNotStopScanOfLaterType() {
        // Given
        var map = new TypeNameMap<Object, Integer>();
        map.addApply(new ResourceMatcherNameStarts<>(Type1.class, "xyz"), val -> 111);
        map.addApply(new ResourceMatcherNameStarts<>(Type2.class, "pr"), val -> 222);

        // When
        var result = map.matchingValues(new ResourceMatcherNameStarts<>(Type2.class, "prefix-x"));

        // Then
        assertThat(result)
                .as("Type2's pr* rule must still match, even though Type1's xyz* rule sorts earlier and doesn't")
                .containsExactly(222);
    }

    @Test
    void siblingPrefixWithDifferentFirstCharacterMustNotStopScan() {
        // Given
        var map = new TypeNameMap<Object, Integer>();
        map.addApply(new ResourceMatcherNameStarts<>(Type1.class, "alpha"), val -> 111);
        map.addApply(new ResourceMatcherNameStarts<>(Type1.class, "zulu"), val -> 222);

        // When
        var result = map.matchingValues(new ResourceMatcherNameStarts<>(Type1.class, "zulu-1"));

        // Then
        assertThat(result)
                .as("zulu* rule must still match, even though alpha* sorts earlier and shares no common prefix")
                .containsExactly(222);
    }

    @Test
    void siblingPrefixesOfSearchedTypeAreNotHiddenByEntriesOfOtherTypes() {
        // Given
        var map = new TypeNameMap<Object, Integer>();
        map.addApply(new ResourceMatcherNameStarts<>(Type1.class, "zzz"), val -> 999);
        map.addApply(new ResourceMatcherNameStarts<>(Type2.class, "alpha"), val -> 111);
        map.addApply(new ResourceMatcherNameStarts<>(Type2.class, "zulu"), val -> 222);
        map.addApply(new ResourceMatcherNameStarts<>(Type3.class, "aaa"), val -> 888);

        // When
        var result = map.matchingValues(new ResourceMatcherNameStarts<>(Type2.class, "zulu-1"));

        // Then
        assertThat(result)
                .as("Type2's zulu* rule must match despite Type1/Type3 entries and a non-matching Type2 sibling sorting earlier")
                .containsExactly(222);
    }

    @Test
    void searchingWithEmptyOperandOnlyMatchesRegisteredEmptyPrefix() {
        // Given
        var map = new TypeNameMap<String, Integer>();
        map.addApply(new ResourceMatcherNameStarts<>(String.class, ""), val -> 1);
        map.addApply(new ResourceMatcherNameStarts<>(String.class, "foo"), val -> 2);

        // When
        var result = map.matchingValues(new ResourceMatcherNameStarts<>(String.class, ""));

        // Then
        assertThat(result)
                .as("only the registered empty prefix can match a search for the empty string")
                .containsExactly(1);
    }

    @Test
    void mixedKindsForSameType_StartsLookupIgnoresEqualsAndAnyEntries() {
        // Given
        var map = new TypeNameMap<Object, Integer>();
        map.addApply(new ResourceMatcherAnyOfType<>(Type1.class), val -> 1);
        map.addApply(new ResourceMatcherNameEquals<>(Type1.class, "foo"), val -> 2);
        map.addApply(new ResourceMatcherNameStarts<>(Type1.class, "foo"), val -> 3);

        // When
        var result = map.matchingValues(new ResourceMatcherNameStarts<>(Type1.class, "foo"));

        // Then
        assertThat(result)
                .as("a Starts lookup must only match the Starts entry, even though Any and Equals entries of the same type and operand coexist in the map")
                .containsExactly(3);
    }

    @Test
    void mixedKindsForSameType_EqualsLookupIgnoresAnyAndStartsEntries() {
        // Given
        var map = new TypeNameMap<Object, Integer>();
        map.addApply(new ResourceMatcherAnyOfType<>(Type1.class), val -> 1);
        map.addApply(new ResourceMatcherNameEquals<>(Type1.class, "foo"), val -> 2);
        map.addApply(new ResourceMatcherNameStarts<>(Type1.class, "foo"), val -> 3);

        // When
        var result = map.matchingValues(new ResourceMatcherNameEquals<>(Type1.class, "foo"));

        // Then
        assertThat(result)
                .as("an Equals lookup must only match the Equals entry, even though Any and Starts entries of the same type and operand coexist in the map")
                .containsExactly(2);
    }

    @Test
    void mixedKindsForSameType_AnyLookupIgnoresEqualsAndStartsEntries() {
        // Given
        var map = new TypeNameMap<Object, Integer>();
        map.addApply(new ResourceMatcherAnyOfType<>(Type1.class), val -> 1);
        map.addApply(new ResourceMatcherNameEquals<>(Type1.class, "foo"), val -> 2);
        map.addApply(new ResourceMatcherNameStarts<>(Type1.class, "foo"), val -> 3);

        // When
        var result = map.matchingValues(new ResourceMatcherAnyOfType<>(Type1.class));

        // Then
        assertThat(result)
                .as("an Any lookup must only match the Any entry, even though Equals and Starts entries of the same type and operand coexist in the map")
                .containsExactly(1);
    }

}
