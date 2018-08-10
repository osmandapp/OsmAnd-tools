package net.osmand.server.mapillary;

import java.util.List;

public class CameraPlaceCollection {

    private final List<CameraPlace> features;

    public CameraPlaceCollection(List<CameraPlace> features) {
        this.features = features;
    }

    public List<CameraPlace> getFeatures() {
        return features;
    }
}