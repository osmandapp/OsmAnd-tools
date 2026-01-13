package net.osmand.obf;

import net.osmand.shared.api.KStringMatcherMode;
import net.osmand.shared.api.OsmAndContext;
import net.osmand.shared.api.SettingsAPI;
import net.osmand.shared.data.KLatLon;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxTrackAnalysis;
import net.osmand.shared.io.KFile;
import net.osmand.shared.settings.enums.AltitudeMetrics;
import net.osmand.shared.settings.enums.MetricsConstants;
import net.osmand.shared.settings.enums.SpeedConstants;
import net.osmand.shared.util.KStringMatcher;

import java.io.IOException;
import java.io.InputStream;

import kotlin.Unit;
import kotlin.jvm.functions.Function1;

public class ToolsOsmAndContextImpl implements OsmAndContext {

    private final String resourcesDirectoryAsAssets = "resources"; // filled by collectActivities

    @Override
    public String getAssetAsString(String name) {
        try {
			String resourcePath = "/" + resourcesDirectoryAsAssets + "/" + name;
			InputStream inputStream = this.getClass().getResourceAsStream(resourcePath);
			if (inputStream == null) {
			    throw new IOException("Resource not found: " + name);
			}

			// Read the input stream into a string (requires Java 9 or later)
			String content = new String(inputStream.readAllBytes());
			return content;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
    }

    @Override
    public KFile getAppDir() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public KFile getCacheDir() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public KFile getGpxDir() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public KFile getGpxImportDir() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public KFile getGpxRecordedDir() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public SettingsAPI getSettings() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public SpeedConstants getSpeedSystem() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public MetricsConstants getMetricSystem() {
        throw new UnsupportedOperationException("Not yet implemented");
    }
    
    @Override
    public AltitudeMetrics getAltitudeMetric() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public boolean isGpxFileVisible(String path) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public GpxFile getSelectedFileByPath(String path) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public KStringMatcher getNameStringMatcher(String name, KStringMatcherMode mode) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public GpxTrackAnalysis.TrackPointsAnalyser getTrackPointsAnalyser() {
        throw new UnsupportedOperationException("Not yet implemented");
    }


	@Override
	public void searchNearestCityName(KLatLon arg0, Function1<? super String, Unit> arg1) {
		throw new UnsupportedOperationException("Not yet implemented");
	}
}
