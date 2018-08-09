package net.osmand.server.mapillary;

public class CameraPlace {
    private final String type;
    private final double lat;
    private final double lon;
    private final String timestamp;
    private final String key;
    private final String title;
    private final String url;
    private final boolean externalLink;
    private final double ca;
    private final String username;
    private final double distance;
    private final double bearing;
    private final String imageUrl;
    private final String imageHiresUrl;
    private final String topIcon;
    private final String buttonIcon;
    private final String buttonText;
    private final String buttonIconColor;
    private final String buttonColor;
    private final String buttonTextColor;

    private CameraPlace(String type, double lat, double lon, String timestamp, String key, String title, String url,
                        boolean externalLink, double ca, String username, double distance, double bearing,
                        String imageUrl, String imageHiresUrl, String topIcon, String buttonIcon, String buttonText,
                        String buttonIconColor, String buttonColor, String buttonTextColor) {
        this.type = type;
        this.lat = lat;
        this.lon = lon;
        this.timestamp = timestamp;
        this.key = key;
        this.title = title;
        this.url = url;
        this.externalLink = externalLink;
        this.ca = ca;
        this.username = username;
        this.distance = distance;
        this.bearing = bearing;
        this.imageUrl = imageUrl;
        this.imageHiresUrl = imageHiresUrl;
        this.topIcon = topIcon;
        this.buttonIcon = buttonIcon;
        this.buttonText = buttonText;
        this.buttonIconColor = buttonIconColor;
        this.buttonColor = buttonColor;
        this.buttonTextColor = buttonTextColor;
    }

    public String getType() {
        return type;
    }

    public double getLat() {
        return lat;
    }

    public double getLon() {
        return lon;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getKey() {
        return key;
    }

    public String getTitle() {
        return title;
    }

    public String getUrl() {
        return url;
    }

    public boolean isExternalLink() {
        return externalLink;
    }

    public double getCa() {
        return ca;
    }

    public String getUsername() {
        return username;
    }

    public double getDistance() {
        return distance;
    }

    public double getBearing() {
        return bearing;
    }

    public String getImageUrl() {
        return imageUrl;
    }

    public String getImageHiresUrl() {
        return imageHiresUrl;
    }

    public String getTopIcon() {
        return topIcon;
    }

    public String getButtonIcon() {
        return buttonIcon;
    }

    public String getButtonText() {
        return buttonText;
    }

    public String getButtonIconColor() {
        return buttonIconColor;
    }

    public String getButtonColor() {
        return buttonColor;
    }

    public String getButtonTextColor() {
        return buttonTextColor;
    }

    public static class CameraPlaceBuilder {
        private String type;
        private double lat;
        private double lon;
        private String timestamp;
        private String key;
        private String title;
        private String url;
        private boolean externalLink = false;
        private double ca = -1.0;
        private String username;
        private double distance;
        private double bearing;
        private String imageUrl = null;
        private String imageHiresUrl = null;
        private String topIcon = null;
        private String buttonIcon = null;
        private String buttonText = null;
        private String buttonIconColor = null;
        private String buttonColor = null;
        private String buttonTextColor = null;

        public CameraPlaceBuilder setType(String type) {
            this.type = type;
            return this;
        }

        public CameraPlaceBuilder setLat(double lat) {
            this.lat = lat;
            return this;
        }

        public CameraPlaceBuilder setLon(double lon) {
            this.lon = lon;
            return this;
        }

        public CameraPlaceBuilder setTimestamp(String timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public CameraPlaceBuilder setKey(String key) {
            this.key = key;
            return this;
        }

        public CameraPlaceBuilder setTitle(String title) {
            this.title = title;
            return this;
        }

        public CameraPlaceBuilder setUrl(String url) {
            this.url = url;
            return this;
        }

        public CameraPlaceBuilder setExternalLink(boolean externalLink) {
            this.externalLink = externalLink;
            return this;
        }

        public CameraPlaceBuilder setCa(double ca) {
            this.ca = ca;
            return this;
        }

        public CameraPlaceBuilder setUsername(String username) {
            this.username = username;
            return this;
        }

        public CameraPlaceBuilder setDistance(double distance) {
            this.distance = distance;
            return this;
        }

        public CameraPlaceBuilder setBearing(double bearing) {
            this.bearing = bearing;
            return this;
        }

        public CameraPlaceBuilder setImageUrl(String imageUrl) {
            this.imageUrl = imageUrl;
            return this;
        }

        public CameraPlaceBuilder setImageHiresUrl(String imageHiresUrl) {
            this.imageHiresUrl = imageHiresUrl;
            return this;
        }

        public CameraPlaceBuilder setTopIcon(String topIcon) {
            this.topIcon = topIcon;
            return this;
        }

        public CameraPlaceBuilder setButtonIcon(String buttonIcon) {
            this.buttonIcon = buttonIcon;
            return this;
        }

        public CameraPlaceBuilder setButtonText(String buttonText) {
            this.buttonText = buttonText;
            return this;
        }

        public CameraPlaceBuilder setButtonIconColor(String buttonIconColor) {
            this.buttonIconColor = buttonIconColor;
            return this;
        }

        public CameraPlaceBuilder setButtonColor(String buttonColor) {
            this.buttonColor = buttonColor;
            return this;
        }

        public CameraPlaceBuilder setButtonTextColor(String buttonTextColor) {
            this.buttonTextColor = buttonTextColor;
            return this;
        }

        public CameraPlace createCameraPlace() {
            return new CameraPlace(type, lat, lon, timestamp, key, title, url, externalLink, ca, username, distance,
                    bearing, imageUrl, imageHiresUrl, topIcon, buttonIcon, buttonText, buttonIconColor, buttonColor,
                    buttonTextColor);
        }
    }
}
