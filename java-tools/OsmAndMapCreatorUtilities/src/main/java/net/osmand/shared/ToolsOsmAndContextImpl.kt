package net.osmand.shared

import net.osmand.shared.api.CityNameCallback
import net.osmand.shared.api.KStringMatcherMode
import net.osmand.shared.api.OsmAndContext
import net.osmand.shared.api.SettingsAPI
import net.osmand.shared.data.KLatLon
import net.osmand.shared.gpx.GpxFile
import net.osmand.shared.gpx.GpxTrackAnalysis
import net.osmand.shared.io.KFile
import net.osmand.shared.settings.enums.MetricsConstants
import net.osmand.shared.settings.enums.SpeedConstants
import net.osmand.shared.util.KStringMatcher
import okio.FileNotFoundException

class ToolsOsmAndContextImpl() : OsmAndContext {
	val resourcesDirectoryAsAssets = "resources" // filled by collectActivities

	override fun getAssetAsString(name: String): String? {
		val resourcePath = "/" + resourcesDirectoryAsAssets + "/" + name
		return this::class.java.getResource(resourcePath)?.readText() ?: throw FileNotFoundException(name)
	}

	override fun getAppDir(): KFile {
		TODO("Not yet implemented")
	}

	override fun getGpxDir(): KFile {
		TODO("Not yet implemented")
	}

	override fun getGpxImportDir(): KFile {
		TODO("Not yet implemented")
	}

	override fun getGpxRecordedDir(): KFile {
		TODO("Not yet implemented")
	}

	override fun getSettings(): SettingsAPI {
		TODO("Not yet implemented")
	}

	override fun getSpeedSystem(): SpeedConstants? {
		TODO("Not yet implemented")
	}

	override fun getMetricSystem(): MetricsConstants? {
		TODO("Not yet implemented")
	}

	override fun isGpxFileVisible(path: String): Boolean {
		TODO("Not yet implemented")
	}

	override fun getSelectedFileByPath(path: String): GpxFile? {
		TODO("Not yet implemented")
	}

	override fun getNameStringMatcher(name: String, mode: KStringMatcherMode): KStringMatcher {
		TODO("Not yet implemented")
	}

	override fun getTrackPointsAnalyser(): GpxTrackAnalysis.TrackPointsAnalyser? {
		TODO("Not yet implemented")
	}

	override fun searchNearestCityName(latLon: KLatLon, callback: CityNameCallback) {
		TODO("Not yet implemented")
	}
}
