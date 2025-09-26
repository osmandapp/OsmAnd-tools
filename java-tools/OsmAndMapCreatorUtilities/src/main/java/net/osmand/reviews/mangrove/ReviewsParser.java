package net.osmand.reviews.mangrove;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.IOException;
import java.util.stream.Stream;

import static com.fasterxml.jackson.core.JsonToken.*;

final class ReviewsParser {
    private enum State {
        Initial,
        TopLevel,
        ReviewList,
        Review,
        Payload,
        ImageList,
        Image,
        Metadata,
        Geo,
        Coordinates,
        Final
    }

    public static final class ParseException extends RuntimeException {
        ParseException(State state, String message) {
            super(String.format("%s: %s", state, message));
        }
    }

    private Review nextReview = null;
    private JsonParser parser = null;
    private State state = null;

    public Stream<Review> parse(File input) throws IOException {
        JsonFactory jsonFactory = JsonFactory.builder().build();
        parser = jsonFactory.createParser(input);
        state = State.Initial;
        readNext();
        if (hasNext()) {
            return Stream.iterate(nextReview, this::streamHasNext, this::streamNext);
        }
        return Stream.empty();
    }

    private boolean streamHasNext(Review ignored) {
        return hasNext();
    }

    private boolean hasNext() {
        return nextReview != null;
    }

    private Review streamNext(Review ignored) {
        Review currentReview = nextReview;
        try {
            readNext();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return currentReview;
    }

    @SuppressWarnings("SwitchStatementWithTooFewBranches")
    private void readNext() throws IOException {
        Review.Builder review = null;
        Review.Payload.Builder payload = null;
        ImmutableList.Builder<Review.Payload.Image> images = null;
        Review.Payload.Image.Builder image = null;
        Review.Payload.Metadata.Builder metadata = null;
        Review.Geo.Builder geo = null;
        Review.Geo.Coordinates.Builder coordinates = null;

        while (!parser.isClosed()) {
            JsonToken token = parser.nextToken();
            switch (state) {
                case Initial -> {
                    switch (token) {
                        case START_OBJECT -> state = State.TopLevel;
                        default -> unexpectedToken(state, token);
                    }
                }
                case TopLevel -> {
                    switch (token) {
                        case FIELD_NAME -> {
                            switch (parser.currentName()) {
                                case "reviews" -> {
                                    consume(parser, state, START_ARRAY);
                                    state = State.ReviewList;
                                }
                                case "issuers" -> skip(parser);
                                case "maresi_subjects" -> skip(parser);
                                default -> unexpectedField(state, parser.currentName());
                            }
                        }
                        case END_OBJECT -> parser.close();
                        default -> unexpectedToken(state, token);
                    }
                }
                case ReviewList -> {
                    switch (token) {
                        case START_OBJECT -> {
                            review = Review.builder();
                            state = State.Review;
                        }
                        case END_ARRAY -> {
                            state = State.TopLevel;
                        }
                        default -> unexpectedToken(state, token);
                    }
                }
                case Review -> {
                    switch (token) {
                        case FIELD_NAME -> {
                            switch (parser.currentName()) {
                                case "signature" -> review.withSignature(parser.nextTextValue());
                                case "jwt" -> skip(parser);
                                case "kid" -> review.withKid(parser.nextTextValue());
                                case "payload" -> {
                                    consume(parser, state, START_OBJECT);
                                    payload = Review.Payload.builder();
                                    state = State.Payload;
                                }
                                case "scheme" -> review.withScheme(parser.nextTextValue());
                                case "geo" -> {
                                    consume(parser, state, START_OBJECT);
                                    geo = Review.Geo.builder();
                                    state = State.Geo;
                                }
                                default -> unexpectedField(state, parser.currentName());
                            }
                        }
                        case END_OBJECT -> {
                            nextReview = review.build();
                            state = State.ReviewList;
                            return;
                        }
                        default -> unexpectedToken(state, token);
                    }
                }
                case Payload -> {
                    switch (token) {
                        case FIELD_NAME -> {
                            switch (parser.currentName()) {
                                case "iat" -> payload.withIat(nextLongValue(parser, state));
                                case "sub" -> payload.withSub(parser.nextTextValue());
                                case "rating" -> payload.withRating(nextIntValue(parser, state));
                                case "opinion" -> payload.withOpinion(parser.nextTextValue());
                                case "action" -> payload.withAction(parser.nextTextValue());
                                case "images" -> {
                                    if (!isNull(parser, state, START_ARRAY)) {
                                        images = ImmutableList.builder();
                                        state = State.ImageList;
                                    }
                                }
                                case "metadata" -> {
                                    consume(parser, state, START_OBJECT);
                                    metadata = Review.Payload.Metadata.builder();
                                    state = State.Metadata;
                                }
                                default -> unexpectedField(state, parser.currentName());
                            }
                        }
                        case END_OBJECT -> {
                            review.withPayload(payload);
                            payload = null;
                            state = State.Review;
                        }
                        default -> unexpectedToken(state, token);
                    }
                }
                case ImageList -> {
                    switch (token) {
                        case START_OBJECT -> {
                            image = Review.Payload.Image.builder();
                            state = State.Image;
                        }
                        case END_ARRAY -> {
                            payload.withImages(images.build());
                            images = null;
                            state = State.Payload;
                        }
                        default -> unexpectedToken(state, token);
                    }
                }
                case Image -> {
                    switch (token) {
                        case FIELD_NAME -> {
                            switch (parser.currentName()) {
                                case "label" -> image.withLabel(parser.nextTextValue());
                                case "src" -> image.withSrc(parser.nextTextValue());
                                default -> unexpectedField(state, parser.currentName());
                            }
                        }
                        case END_OBJECT -> {
                            images.add(image.build());
                            image = null;
                            state = State.ImageList;
                        }
                        default -> unexpectedToken(state, token);
                    }
                }
                case Metadata -> {
                    switch (token) {
                        case FIELD_NAME -> {
                            switch (parser.currentName()) {
                                case "age" -> skip(parser);
                                case "birthdate" -> skip(parser);
                                case "client_id" -> metadata.withClientId(parser.nextTextValue());
                                case "data_source" -> skip(parser);
                                case "experience_context" -> skip(parser);
                                case "family_name" -> skip(parser);
                                case "gender" -> skip(parser);
                                case "given_name" -> skip(parser);
                                case "is_affiliated" -> metadata.withIsAffiliated(nextBooleanValue(parser));
                                case "is_generated" -> metadata.withIsGenerated(nextBooleanValue(parser));
                                case "is_personal_experience" -> metadata.withIsPersonalExperience(nextBooleanValue(parser));
                                case "nickname" -> metadata.withNickname(parser.nextTextValue());
                                case "preferred_username" -> metadata.withPreferredUsername(parser.nextTextValue());
                                default -> unexpectedField(state, parser.currentName());
                            }
                        }
                        case END_OBJECT -> {
                            payload.withMetadata(metadata);
                            metadata = null;
                            state = State.Payload;
                        }
                        default -> unexpectedToken(state, token);
                    }
                }
                case Geo -> {
                    switch (token) {
                        case FIELD_NAME -> {
                            switch (parser.currentName()) {
                                case "coordinates" -> {
                                    if (!isNull(parser, state, START_OBJECT)) {
                                        coordinates = Review.Geo.Coordinates.builder();
                                        state = State.Coordinates;
                                    }
                                }
                                case "uncertainty" -> geo.withUncertainty(nextIntValueOrNull(parser, state));
                                default -> unexpectedField(state, parser.currentName());
                            }
                        }
                        case END_OBJECT -> {
                            review.withGeo(geo);
                            geo = null;
                            state = State.Review;
                        }
                        default -> unexpectedToken(state, token);
                    }
                }
                case Coordinates -> {
                    switch (token) {
                        case FIELD_NAME -> {
                            switch (parser.currentName()) {
                                case "x" -> coordinates.withX(nextDoubleValue(parser, state));
                                case "y" -> coordinates.withY(nextDoubleValue(parser, state));
                                case "srid" -> coordinates.withSrid(nextIntValue(parser, state));
                                default -> unexpectedField(state, parser.currentName());
                            }
                        }
                        case END_OBJECT -> {
                            geo.withCoordinates(coordinates);
                            coordinates = null;
                            state = State.Geo;
                        }
                        default -> unexpectedToken(state, token);
                    }
                }
                default -> throw new RuntimeException(String.format("TODO: %s", state));
            }
        }
        // end of review list
        nextReview = null;
    }

    private static boolean isNull(JsonParser parser, State state, JsonToken expectedToken) throws IOException, ParseException {
        JsonToken actualToken = parser.nextToken();
        if (actualToken == VALUE_NULL) {
            return true;
        }
        if (actualToken != expectedToken) {
            throw new ParseException(state, String.format("expected %s, got %s", expectedToken, actualToken));
        }
        return false;
    }

    private static void consume(JsonParser parser, State state, JsonToken expectedToken) throws IOException, ParseException {
        if (isNull(parser, state, expectedToken)) {
            throw new ParseException(state, String.format("expected %s, got NULL", expectedToken));
        }
    }

    private static int nextIntValue(JsonParser parser, State state) throws IOException, ParseException {
        JsonToken token = parser.nextToken();
        if (token != VALUE_NUMBER_INT) throw new ParseException(state, String.format("expected INT, got %s", token));
        return parser.getIntValue();
    }

    private static long nextLongValue(JsonParser parser, State state) throws IOException, ParseException {
        Long value = nextLongValueOrNull(parser, state);
        if (value == null) {
            throw new ParseException(state, "expected INT, got NULL");
        }
        return value;
    }

    private static Integer nextIntValueOrNull(JsonParser parser, State state) throws IOException, ParseException {
        JsonToken token = parser.nextToken();
        switch (token) {
            case VALUE_NULL -> { return null; }
            case VALUE_NUMBER_INT -> { return parser.getIntValue(); }
            default -> throw new ParseException(state, String.format("expected INT, got %s", token));
        }
    }

    private static Long nextLongValueOrNull(JsonParser parser, State state) throws IOException, ParseException {
        JsonToken token = parser.nextToken();
        switch (token) {
            case VALUE_NULL -> { return null; }
            case VALUE_NUMBER_INT -> { return parser.getLongValue(); }
            default -> throw new ParseException(state, String.format("expected INT, got %s", token));
        }
    }

    private static double nextDoubleValue(JsonParser parser, State state) throws IOException, ParseException {
        JsonToken token = parser.nextToken();
        if (token != VALUE_NUMBER_FLOAT) throw new ParseException(state, String.format("expected FLOAT, got %s", token));
        return parser.getDoubleValue();
    }

    private static boolean nextBooleanValue(JsonParser parser) throws IOException {
        return Boolean.parseBoolean(parser.nextTextValue());
    }

    private static void skip(JsonParser parser) throws IOException {
        parser.nextToken();
    }

    private static void unexpectedToken(State state, JsonToken token) throws ParseException {
        throw new ParseException(state, String.format("unexpected token: %s", token));
    }

    private static void unexpectedField(State state, String fieldName) throws ParseException {
        throw new ParseException(state, String.format("unexpected field: '%s'", fieldName));
    }
}
