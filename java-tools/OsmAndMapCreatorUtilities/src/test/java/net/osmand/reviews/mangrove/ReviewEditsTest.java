package net.osmand.reviews.mangrove;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class ReviewEditsTest {
    @Test
    public void doesNotTouchAReviewThatWasNotEdited() {
        Review review = testReview(1, "original", "geo:12.34,56.78?q=The%20Place&u=9", null);
        Map<String, Review> input = reviewBySignatureMap(review);
        Map<String, Review> result = ReviewEdits.applyEdits(input);
        assertEquals(input, result);
    }

    @Test
    public void reducesSingleEdit() {
        Review original = testReview(1, "original", "geo:12.34,56.78?q=The%20Place&u=9", null);
        Review edit = testReview(2, "edit1", "urn:maresi:original", "edit");
        Map<String, Review> input = reviewBySignatureMap(original, edit);

        Map<String, Review> result = ReviewEdits.applyEdits(input);

        assertEquals(ImmutableSet.of("edit1"), result.keySet());
        Review actual = result.get(edit.signature());
        Review expected = copyExpectedFields(original, edit);
        assertEquals(expected, actual);
    }

    @Test
    public void usesTheLastEditInAChain() {
        Review original = testReview(1, "original", "geo:12.34,56.78?q=The%20Place&u=9", null);
        Review edit1 = testReview(2, "edit1", "urn:maresi:original", "edit");
        Review edit2 = testReview(3, "edit2", "urn:maresi:edit1", "edit");
        Map<String, Review> input = reviewBySignatureMap(original, edit1, edit2);

        Map<String, Review> result = ReviewEdits.applyEdits(input);

        assertEquals(ImmutableSet.of("edit2"), result.keySet());
        Review actual = result.get("edit2");
        Review expected = copyExpectedFields(original, edit2);
        assertEquals(expected, actual);
    }

    @Test
    public void usesTheMostRecentParallelEdit() {
        Review original = testReview(1, "original", "geo:12.34,56.78?q=The%20Place&u=9", null);
        Review edit1 = testReview(2, "edit1", "urn:maresi:original", "edit");
        Review edit2 = testReview(3, "edit2", "urn:maresi:original", "edit");
        Map<String, Review> input = reviewBySignatureMap(original, edit1, edit2);

        Map<String, Review> result = ReviewEdits.applyEdits(input);

        assertEquals(ImmutableSet.of("edit2"), result.keySet());
        Review actual = result.get("edit2");
        Review expected = copyExpectedFields(original, edit2);
        assertEquals(expected, actual);
    }

    @Test
    public void usesTheMostRecentEditEvenIfItIsNotTheLastInAChain() {
        // edge case, should not happen in practice: the last edit in a chain of edits has earlier timestamp
        // than one of the edits earlier in the chain

        Review original = testReview(1, "original", "geo:12.34,56.78?q=The%20Place&u=9", null);
        // note: seqNum determines the ordering of reviews in time
        Review edit1 = testReview(3, "edit1", "urn:maresi:original", "edit");
        Review edit2 = testReview(2, "edit2", "urn:maresi:edit1", "edit");
        Map<String, Review> input = reviewBySignatureMap(original, edit1, edit2);

        Map<String, Review> result = ReviewEdits.applyEdits(input);

        assertEquals(ImmutableSet.of("edit1"), result.keySet());
        Review actual = result.get("edit1");
        Review expected = copyExpectedFields(original, edit1);
        assertEquals(expected, actual);
    }

    @Test
    public void complexCase() {
        /*
        original
        + edit1
          + edit3
          + edit5
        + edit2
          + edit4
        */
        Review original = testReview(1, "original", "geo:12.34,56.78?q=The%20Place&u=9", null);
        Review edit1 = testReview(2, "edit1", "urn:maresi:original", "edit");
        Review edit2 = testReview(3, "edit2", "urn:maresi:original", "edit");
        Review edit3 = testReview(4, "edit3", "urn:maresi:edit1", "edit");
        Review edit4 = testReview(5, "edit4", "urn:maresi:edit2", "edit");
        Review edit5 = testReview(6, "edit5", "urn:maresi:edit1", "edit");
        Map<String, Review> input = reviewBySignatureMap(original, edit1, edit2, edit3, edit4, edit5);

        Map<String, Review> result = ReviewEdits.applyEdits(input);

        assertEquals(ImmutableSet.of("edit5"), result.keySet());
        Review actual = result.get("edit5");
        Review expected = copyExpectedFields(original, edit5);
        assertEquals(expected, actual);
    }

    private static Review copyExpectedFields(Review original, Review edit) {
        return new Review(
                edit.signature(),
                edit.kid(),
                new Review.Payload(
                        edit.payload().iat(),
                        original.payload().sub(), // <-- sub is propagated
                        edit.payload().rating(),
                        edit.payload().opinion(),
                        edit.payload().action(),
                        edit.payload().images(),
                        new Review.Payload.Metadata(
                                original.payload().metadata().clientId(),  // <-- client id is propagated
                                edit.payload().metadata().familyName(),
                                edit.payload().metadata().givenName(),
                                edit.payload().metadata().isAffiliated(),
                                edit.payload().metadata().isGenerated(),
                                edit.payload().metadata().isPersonalExperience(),
                                edit.payload().metadata().nickname(),
                                edit.payload().metadata().preferredUsername()
                        )
                ),
                original.scheme(),  // <-- scheme is extracted from sub, so must be consistent
                original.geo()  // <-- geo is extracted from sub, so must be consistent
        );
    }

    private static Map<String, Review> reviewBySignatureMap(Review... reviews) {
        return ImmutableList.copyOf(reviews).stream().collect(Collectors.toMap(Review::signature, Function.identity()));
    }

    private static Review testReview(int seqNum, String signature, String sub, String action) {
        return new Review(
                signature,
                "irrelevant" + seqNum,
                new Review.Payload(
                        100000000L + seqNum,
                        sub,
                        seqNum,
                        "opinion" + seqNum,
                        action,
                        null,
                        new Review.Payload.Metadata(
                                "clientId" + seqNum,
                                "familyName" + seqNum,
                                "givenName" + seqNum,
                                null,
                                null,
                                null,
                                "nickname" + seqNum,
                                "preferredUsername" + seqNum
                        )
                ),
                "scheme" + seqNum,
                new Review.Geo(null, null)

        );
    }
}
