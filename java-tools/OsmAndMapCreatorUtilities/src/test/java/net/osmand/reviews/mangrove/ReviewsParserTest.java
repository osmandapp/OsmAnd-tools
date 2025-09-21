package net.osmand.reviews.mangrove;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class ReviewsParserTest {
    @Test
    public void parsesRealApiOutput() throws IOException, ReviewsParser.ParseException {
        File input = new File("src/test/resources/mangrove200.json");
        ReviewsParser parser = new ReviewsParser();
        Stream<Review> reviewStream = parser.parse(input);
        List<Review> reviewList = reviewStream.toList();

        // sanity checks
        assertEquals(200, reviewList.size());
    }
}
