package net.osmand.obf;

import net.osmand.shared.api.KStringMatcherMode;
import net.osmand.shared.api.OsmAndContext;
import net.osmand.shared.api.SettingsAPI;
import net.osmand.shared.data.KLatLon;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxTrackAnalysis;
import net.osmand.shared.gpx.SmartFolderHelper;
import net.osmand.shared.io.KFile;
import net.osmand.shared.settings.enums.AltitudeMetrics;
import net.osmand.shared.settings.enums.MetricsConstants;
import net.osmand.shared.settings.enums.AngularConstants;
import net.osmand.shared.settings.enums.SpeedConstants;
import net.osmand.shared.units.TemperatureUnits
import net.osmand.shared.util.KStringMatcher;

import java.io.IOException;
import java.io.InputStream;

import kotlin.Unit;
import kotlin.jvm.functions.Function1;

import java.nio.charset.StandardCharsets;

public class ToolsOsmAndContextImpl implements OsmAndContext {

    private final String resourcesDirectoryAsAssets = "resources"; // filled by collectActivities

    @Override
    public String getAssetAsString(String name) {
        try {
			try (InputStream inputStream = openAssetStream(name)) {
				if (inputStream == null) {
					throw new IOException("Resource not found: " + name);
				}
				return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
    }

	private InputStream openAssetStream(String name) {
		String[] resourceCandidates = {
				resourcesDirectoryAsAssets + "/" + name,
				name,
				"/" + resourcesDirectoryAsAssets + "/" + name,
				"/" + name
		};
		ClassLoader[] classLoaders = {
				Thread.currentThread().getContextClassLoader(),
				getClass().getClassLoader()
		};
		for (ClassLoader classLoader : classLoaders) {
			if (classLoader == null) {
				continue;
			}
			for (String resourceCandidate : resourceCandidates) {
				InputStream inputStream = classLoader.getResourceAsStream(stripLeadingSlash(resourceCandidate));
				if (inputStream != null) {
					return inputStream;
				}
			}
		}
		for (String resourceCandidate : resourceCandidates) {
			InputStream inputStream = getClass().getResourceAsStream(resourceCandidate);
			if (inputStream != null) {
				return inputStream;
			}
		}
		return null;
	}

	private String stripLeadingSlash(String value) {
		return value.startsWith("/") ? value.substring(1) : value;
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
    public KFile getColorPaletteDir() {
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
    public AngularConstants getAngularSystem() {
        throw new UnsupportedOperationException("Not yet implemented");
    }
    
    @Override
    public TemperatureUnits getTemperatureUnits() {
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

    @Override
    public SmartFolderHelper getSmartFolderHelper() {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
