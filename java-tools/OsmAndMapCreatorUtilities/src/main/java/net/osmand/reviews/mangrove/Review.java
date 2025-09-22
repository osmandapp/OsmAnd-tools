package net.osmand.reviews.mangrove;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * A Mangrove API representation of a review. See <a href="https://docs.mangrove.reviews/#operation/get_reviews_route">Mangrove API docs</a>.
 */
record Review(
        @NotNull String signature,
        @NotNull String kid,
        @NotNull Payload payload,
        String scheme,
        Geo geo
) {
    public record Payload(
            long iat,
            @NotNull String sub,
            Integer rating,
            String opinion,
            String action,
            List<Image> images,
            Metadata metadata
    ) {
        public record Image(
                String label,
                String src
        ) {
            public static final class Builder {
                private String label = null;
                private String src = null;

                public Builder withLabel(String label) {
                    this.label = label;
                    return this;
                }

                public Builder withSrc(String src) {
                    this.src = src;
                    return this;
                }

                public Image build() {
                    return new Image(label, src);
                }
            }

            public static Builder builder() {
                return new Builder();
            }
        }
        public record Metadata(
                String clientId,
                String familyName,
                String givenName,
                Boolean isAffiliated,
                Boolean isGenerated,
                Boolean isPersonalExperience,
                String nickname,
                String preferredUsername
        ) {
            public static final class Builder {
                private String clientId = null;
                private String familyName = null;
                private String givenName = null;
                private Boolean isAffiliated = null;
                private Boolean isGenerated = null;
                private Boolean isPersonalExperience = null;
                private String nickname = null;
                private String preferredUsername = null;

                public Builder withClientId(String clientId) {
                    this.clientId = clientId;
                    return this;
                }

                public Builder withFamilyName(String familyName) {
                    this.familyName = familyName;
                    return this;
                }

                public Builder withGivenName(String givenName) {
                    this.givenName = givenName;
                    return this;
                }

                public Builder withIsAffiliated(boolean isAffiliated) {
                    this.isAffiliated = isAffiliated;
                    return this;
                }

                public Builder withIsGenerated(boolean isGenerated) {
                    this.isGenerated = isGenerated;
                    return this;
                }

                public Builder withIsPersonalExperience(boolean isPersonalExperience) {
                    this.isPersonalExperience = isPersonalExperience;
                    return this;
                }

                public Builder withNickname(String nickname) {
                    this.nickname = nickname;
                    return this;
                }

                public Builder withPreferredUsername(String preferredUsername) {
                    this.preferredUsername = preferredUsername;
                    return this;
                }

                public Metadata build() {
                    return new Metadata(clientId, familyName, givenName, isAffiliated, isGenerated, isPersonalExperience, nickname, preferredUsername);
                }
            }

            public static Builder builder() {
                return new Builder();
            }

            public Metadata withClientId(String clientId) {
                return new Metadata(clientId, familyName, givenName, isAffiliated, isGenerated, isPersonalExperience, nickname, preferredUsername);
            }
        }

        public static final class Builder {
            private Long iat = null;
            private String sub = null;
            private Integer rating = null;
            private String opinion = null;
            private String action = null;
            private List<Image> images = null;
            private Metadata metadata = null;

            public Builder withIat(long iat) {
                this.iat = iat;
                return this;
            }

            public Builder withSub(String sub) {
                this.sub = sub;
                return this;
            }

            public Builder withRating(int rating) {
                this.rating = rating;
                return this;
            }

            public Builder withOpinion(String opinion) {
                this.opinion = opinion;
                return this;
            }

            public Builder withAction(String action) {
                this.action = action;
                return this;
            }

            public Builder withImages(List<Image> images) {
                this.images = images;
                return this;
            }

            public Builder withMetadata(Metadata.Builder metadata) {
                this.metadata = metadata.build();
                return this;
            }

            public Payload build() {
                return new Payload(iat, sub, rating, opinion, action, images, metadata);
            }
        }

        public static Builder builder() {
            return new Builder();
        }

        public Payload withSub(@NotNull String sub) {
            return new Payload(iat, sub, rating, opinion, action, images, metadata);
        }

        public Payload withMetadata(Metadata metadata) {
            return new Payload(iat, sub, rating, opinion, action, images, metadata);
        }
    }

    public record Geo(
            Coordinates coordinates,
            Long uncertainty
    ) {
        public record Coordinates(
                double x,
                double y,
                int srid
        ) {
            public static final class Builder {
                private Double x = null;
                private Double y = null;
                private Integer srid = null;

                public Builder withX(double x) {
                    this.x = x;
                    return this;
                }

                public Builder withY(double y) {
                    this.y = y;
                    return this;
                }

                public Builder withSrid(int srid) {
                    this.srid = srid;
                    return this;
                }

                public Coordinates build() {
                    return new Coordinates(x, y, srid);
                }
            }

            public static Builder builder() {
                return new Builder();
            }
        }

        public static final class Builder {
            private Coordinates coordinates = null;
            private Long uncertainty = null;

            public Builder withCoordinates(Coordinates.Builder coordinates) {
                this.coordinates = coordinates.build();
                return this;
            }

            public Builder withUncertainty(Long uncertainty) {
                this.uncertainty = uncertainty;
                return this;
            }

            public Geo build() {
                return new Geo(coordinates, uncertainty);
            }
        }

        public static Builder builder() {
            return new Builder();
        }
    }

    public static final class Builder {
        private String signature = null;
        private String kid = null;
        private Payload payload = null;
        private String scheme = null;
        private Geo geo = null;

        public Builder withSignature(String signature) {
            this.signature = signature;
            return this;
        }

        public Builder withKid(String kid) {
            this.kid = kid;
            return this;
        }

        public Builder withPayload(Payload.Builder payload) {
            this.payload = payload.build();
            return this;
        }

        public Builder withScheme(String scheme) {
            this.scheme = scheme;
            return this;
        }

        public Builder withGeo(Geo.Builder geo) {
            this.geo = geo.build();
            return this;
        }

        public Review build() {
            return new Review(signature, kid, payload, scheme, geo);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public Review withPayload(Payload payload) {
        return new Review(signature, kid, payload, scheme, geo);
    }

    public Review withScheme(String scheme) {
        return new Review(signature, kid, payload, scheme, geo);
    }

    public Review withGeo(Geo geo) {
        return new Review(signature, kid, payload, scheme, geo);
    }
}
