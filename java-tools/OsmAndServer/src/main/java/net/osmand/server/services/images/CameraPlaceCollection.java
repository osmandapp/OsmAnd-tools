package net.osmand.server.services.images;

import java.util.ArrayList;
import java.util.List;

public class CameraPlaceCollection {

    private final List<CameraPlace> features;

    public CameraPlaceCollection() {
        this.features = new ArrayList<>();
    }

    public CameraPlaceCollection(List<CameraPlace> features) {
        this.features = features;
    }

    public List<CameraPlace> getFeatures() {
        return features;
    }
}