package net.osmand.reviews.mangrove;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ReviewsParserTest {
    @Test
    public void parsesRealApiOutput() throws IOException, ReviewsParser.ParseException {
        File input = new File("src/test/resources/mangrove100.json");
        ReviewsParser parser = new ReviewsParser();
        Stream<Review> output = parser.parse(input);
        System.err.println(output.collect(Collectors.toList()));
    }
}
