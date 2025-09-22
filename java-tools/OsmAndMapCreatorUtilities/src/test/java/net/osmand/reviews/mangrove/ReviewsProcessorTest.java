package net.osmand.reviews.mangrove;

import com.google.common.collect.ImmutableList;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class ReviewsProcessorTest {
    @Test
    public void doesNotTouchAReviewThatWasNotEdited() {
        Review review = testReview(1, "sig1", "geo:12.34,56.78?q=The%20Place&u=9", null);
        Map<String, Review> input = reviewBySignatureMap(review);
        Map<String, Review> result = ReviewsProcessor.applyEdits(input);
        assertEquals(input, result);
    }

    @Test
    public void reducesSingleEdit() {
        Review original = testReview(1, "sig1", "geo:12.34,56.78?q=The%20Place&u=9", null);
        Review edit = testReview(2, "sig2", "urn:maresi:sig1", "edit");
        // verify that the test data is set up in a way that will cause the test to fail if the code does not
        // behave as expected
        assertNotEquals(original.payload().metadata().clientId(), edit.payload().metadata().clientId());

        Map<String, Review> input = reviewBySignatureMap(original, edit);

        Map<String, Review> result = ReviewsProcessor.applyEdits(input);

        Review expected = new Review(
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
                original.scheme(),  // <-- scheme and geo are derived from sub, so are propagated
                original.geo()
        );
        assertEquals(1, result.size());
        Review actual = result.get(edit.signature());
        assertEquals(expected, actual);
    }

    private static @NotNull Map<String, Review> reviewBySignatureMap(Review... reviews) {
        return ImmutableList.copyOf(reviews).stream().collect(Collectors.toMap(Review::signature, Function.identity()));
    }

    // TODO: test cases
    // - edit chain
    // - parallel edits

    private static Review testReview(int seqNum, String signature, String sub, String action) {
        return new Review(
                signature,
                "irrelevant" + seqNum,
                new Review.Payload(
                        123456789L,
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
