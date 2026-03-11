package net.osmand.reviews.mangrove;

import org.junit.Test;

import java.net.URI;
import java.time.LocalDate;

import static org.junit.Assert.assertEquals;

public class ReviewTest {
    @Test
    public void convertsToOsmAndReview() {
        Review mangroveReview = testReview(
                "sig1",
                1758363120L,
                "the opinion",
                23,
                "the nickname"
        );
        net.osmand.reviews.Review osmandReview = new net.osmand.reviews.Review(
                "sig1",
                "the opinion",
                23,
                "the nickname",
                LocalDate.of(2025, 9, 20),
                URI.create("https://mangrove.reviews/list?signature=sig1")
        );
        assertEquals(osmandReview, mangroveReview.asOsmAndReview());
    }

    private static Review testReview(String signature, long iat, String opinion, Integer rating, String nickname) {
        return new Review(
                signature,
                "irrelevant kid",
                new Review.Payload(
                        iat,
                        "sub",
                        rating,
                        opinion,
                        null,
                        null,
                        new Review.Payload.Metadata(
                                null,
                                null,
                                null,
                                null,
                                null,
                                null,
                                nickname,
                                null
                        )
                )
        );
    }
}
