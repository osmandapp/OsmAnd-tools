package net.osmand.server.mapillary;

import java.util.List;

public class CameraPlaceHolder {

    private final List<CameraPlace> features;

    public CameraPlaceHolder(List<CameraPlace> features) {
        this.features = features;
    }

    public List<CameraPlace> getFeatures() {
        return features;
    }
}