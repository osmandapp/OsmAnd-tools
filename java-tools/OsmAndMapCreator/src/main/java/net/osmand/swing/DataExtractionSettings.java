package net.osmand.swing;

import java.awt.Rectangle;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.prefs.Preferences;

import org.xmlpull.v1.XmlPullParserException;

import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.CachedOsmandIndexes;
import net.osmand.binary.MapZooms;
import net.osmand.data.LatLon;
import net.osmand.router.RoutingConfiguration;
import net.osmand.router.RoutingConfiguration.Builder;
import net.osmand.util.Algorithms;


public class DataExtractionSettings {

	// public static final String INDEXES_CACHE = "";
	public static final String INDEXES_CACHE = CachedOsmandIndexes.INDEXES_DEFAULT_FILENAME;
	private static DataExtractionSettings settings = null;
	public static DataExtractionSettings getSettings(){
		if(settings == null){
			settings = new DataExtractionSettings();
		}
		return settings;

	}

	
	Map<String, BinaryMapIndexReaderReference> obfFiles = new ConcurrentHashMap<>();
	
	Preferences preferences = Preferences.userRoot();

	long lastSaved = 0;
	
	public static class BinaryMapIndexReaderReference { 
		File file;
		BinaryMapIndexReader reader;
	}
	
	
	public BinaryMapIndexReader[] getObfReaders() throws IOException {
		List<BinaryMapIndexReader> files = new ArrayList<>();
		File mapsFolder = new File(getBinaryFilesDir());
		CachedOsmandIndexes cache = null;
		File cacheFile = new File(mapsFolder, INDEXES_CACHE);
		if (mapsFolder.exists() && obfFiles.isEmpty() && INDEXES_CACHE.length() > 0) {
			cache = new CachedOsmandIndexes();
			if (cacheFile.exists()) {
				cache.readFromFile(cacheFile);
			}
		}
		File[] sortedFiles = Algorithms.getSortedFilesVersions(mapsFolder);
		if (sortedFiles != null) {
			for (File obf : sortedFiles) {
				if (obf.getName().endsWith(".obf")) {
					BinaryMapIndexReaderReference ref = obfFiles.get(obf.getAbsolutePath());
					if (ref == null || ref.reader == null) {
						ref = new BinaryMapIndexReaderReference();
						ref.file = obf;
						if (cache == null) {
							RandomAccessFile raf = new RandomAccessFile(obf, "r"); //$NON-NLS-1$ //$NON-NLS-2$
							ref.reader = new BinaryMapIndexReader(raf, obf);
						} else {
							ref.reader = cache.getReader(obf, true);
						}
						obfFiles.put(obf.getAbsolutePath(), ref);
					}
					files.add(ref.reader);
				}
			}
		}
		if (cache != null && !files.isEmpty()) {
			cache.writeToFile(cacheFile);
		}
		return files.toArray(new BinaryMapIndexReader[files.size()]);
	}

	public void saveLocation(double lat, double lon, int zoom, boolean save) {
		long ms = System.currentTimeMillis();
		if (ms - lastSaved > 5000 || save) {
			saveDefaultLocation(lat, lon);
			saveDefaultZoom(zoom);
			lastSaved = ms;
		}
	}

	public File getTilesDirectory(){
		return new File(getDefaultWorkingDir(), "tiles");
	}

	public File getDefaultWorkingDir(){
		String workingDir = preferences.get("working_dir", System.getProperty("user.home"));
		if(workingDir.equals(System.getProperty("user.home"))){
			workingDir += "/osmand";
			new File(workingDir).mkdir();
		}
		return new File(workingDir);
	}

	public void saveDefaultWorkingDir(File path){
		preferences.put("working_dir", path.getAbsolutePath());
	}

	public LatLon getDefaultLocation(){
		double lat = preferences.getDouble("default_lat",  53.9);
		double lon = preferences.getDouble("default_lon",  27.56);
		return new LatLon(lat, lon);
	}

	public LatLon getStartLocation(){
		double lat = preferences.getDouble("start_lat",  53.9);
		double lon = preferences.getDouble("start_lon",  27.56);
		return new LatLon(lat, lon);
	}

	public LatLon getEndLocation(){
		double lat = preferences.getDouble("end_lat",  53.9);
		double lon = preferences.getDouble("end_lon",  27.56);
		return new LatLon(lat, lon);
	}

	public void saveDefaultLocation(double lat, double lon){
		preferences.putDouble("default_lat",  lat);
		preferences.putDouble("default_lon",  lon);
	}

	public void saveStartLocation(double lat, double lon){
		preferences.putDouble("start_lat",  lat);
		preferences.putDouble("start_lon",  lon);
	}

	public void saveEndLocation(double lat, double lon){
		preferences.putDouble("end_lat",  lat);
		preferences.putDouble("end_lon",  lon);
	}

	public MapZooms getMapZooms(){
		String value = preferences.get("map_zooms", MapZooms.MAP_ZOOMS_DEFAULT);
		return MapZooms.parseZooms(value);
	}

	public String getMapZoomsValue(){
		return preferences.get("map_zooms", MapZooms.MAP_ZOOMS_DEFAULT);
	}

	public void setMapZooms(String zooms){
		// check string
		MapZooms.parseZooms(zooms);
		preferences.put("map_zooms", zooms);
	}

	public String getLineSmoothness(){
		return preferences.get("line_smoothness", "2");
	}

	public void setLineSmoothness(String smooth){
		// check string
		Integer.parseInt(smooth);
		preferences.put("line_smoothness", smooth);
	}

	
	public String getPoiTypesFile(){
		return preferences.get("poi_types_file", "");
	}
	
	public void setPoiTypesFile(String fileName){
		preferences.put("poi_types_file", fileName);
	}

	public String getMapRenderingTypesFile(){
		return preferences.get("rendering_types_file", "");
	}
	
	public void setMapRenderingTypesFile(String fileName){
		preferences.put("rendering_types_file", fileName);
	}
	
	public int getDefaultZoom(){
		return preferences.getInt("default_zoom",  5);
	}

	public void saveDefaultZoom(int zoom){
		preferences.putInt("default_zoom",  zoom);
	}

	public boolean getLoadEntityInfo(){
		return preferences.getBoolean("load_entity_info",  true);
	}

	public void setLoadEntityInfo(boolean loadEntityInfo){
		preferences.putBoolean("load_entity_info",  loadEntityInfo);
	}

	public Rectangle getWindowBounds(){
		Rectangle r = new Rectangle();
		r.x = preferences.getInt("window_x",  0);
		r.y = preferences.getInt("window_y",  0);
		r.width = preferences.getInt("window_width",  800);
		r.height = preferences.getInt("window_height",  600);
		return r;
	}

	public void saveWindowBounds(Rectangle r) {
		preferences.putInt("window_x", r.x);
		preferences.putInt("window_y", r.y);
		preferences.putInt("window_width", r.width);
		preferences.putInt("window_height", r.height);
	}



	public boolean useAdvancedRoutingUI(){
		return preferences.getBoolean("use_advanced_routing_ui", false);
	}

	public void setUseAdvancedRoutingUI(boolean b){
		preferences.putBoolean("use_advanced_routing_ui", b);
	}

	public boolean useInternetToLoadImages(){
		return preferences.getBoolean("use_internet", true);
	}

	public void setUseInterentToLoadImages(boolean b){
		preferences.putBoolean("use_internet", b);
	}


	public String getSearchLocale(){
		return preferences.get("searchLocale", "");
	}

	public void setSearchLocale(String s){
		preferences.put("searchLocale", s);
	}
	
	public String getRouteMode(){
		return preferences.get("routeMode", "car,short_way");
	}

	public void setRouteMode(String mode){
		preferences.put("routeMode", mode);
	}

	public String getNativeLibFile(){
		String fl = preferences.get("nativeLibFile", null);
		if(fl != null) {
			return fl;
		}
		return "";
	}

	public String getQtLibFolder(){
		String fl = preferences.get("qtLibFolder", null);
		if(fl != null) {
			return fl;
		}
		return "";
	}

	public void setNativeLibFile(String file){
		preferences.put("nativeLibFile", file);
	}

	public void setQtLibFolder(String file){
		preferences.put("qtLibFolder", file);
	}



	public String getRenderXmlPath(){
		return preferences.get("renderXmlPath", "default.render.xml");
	}

	public void setRenderXmlPath(String file){
		preferences.put("renderXmlPath", file);
	}

	public String getRenderGenXmlPath(){
		return preferences.get("renderGenXmlPath", "");
	}

	public void setRenderGenXmlPath(String path){
		preferences.put("renderGenXmlPath", path);
	}

	public String getRoutingXmlPath(){
		return preferences.get("routingXmlPath", "routing.xml");
	}
	
	public Builder getRoutingConfig() {
		Builder builder;
		String xmlPath = getRoutingXmlPath();
		if(xmlPath.equals("routing.xml")){
			builder = RoutingConfiguration.getDefault() ;
		} else{
			try {
				builder = RoutingConfiguration.parseFromInputStream(new FileInputStream(xmlPath));
			} catch (IOException e) {
				throw new IllegalArgumentException("Error parsing routing.xml file",e);
			} catch (XmlPullParserException e) {
				throw new IllegalArgumentException("Error parsing routing.xml file",e);
			}
		}
		return builder;
	}
	

	public void setRoutingXmlPath(String file){
		preferences.put("routingXmlPath", file);
	}



	public String getBinaryFilesDir(){
		String fl = preferences.get("binaryFilesDir", null);
		if(fl != null) {
			return fl;
		}
		return getDefaultWorkingDir().getAbsolutePath();
	}
	
	

	public void setBinaryFilesDir(String file){
		preferences.put("binaryFilesDir", file);
	}

	public String getLastUsedDir(){
		String fl = preferences.get("lastUsedDir", null);
		if(fl != null) {
			return fl;
		}
		return getDefaultWorkingDir().getAbsolutePath();
	}

	public void setLastUsedDir(String file){
		preferences.put("lastUsedDir", file);
	}


	public String getOsrmServerAddress(){
		return preferences.get("osrmServerAddress", "http://127.0.0.1:5000");
	}

	public void setOsrmServerAddress(String s){
		preferences.put("osrmServerAddress", s);
	}

	public boolean isSupressWarningsForDuplicatedId(){
		return preferences.getBoolean("supress_duplicated_id", true);
	}

	public void setSupressWarningsForDuplicatedId(boolean b){
		preferences.putBoolean("supress_duplicated_id", b);
	}

	public boolean isAnimateRouting(){
		return preferences.getBoolean("animate_routing", false);
	}

	public boolean useNativeRouting(){
		return preferences.getBoolean("native_routing", false);
	}

	public void setAnimateRouting(boolean b){
		preferences.putBoolean("animate_routing", b);
	}

	public void setNativeRouting(boolean b){
		preferences.putBoolean("native_routing", b);
	}

	public void preferHousenumber(boolean b){
		preferences.putBoolean("prefer_housenumber", b);
	}

	public boolean isHousenumberPrefered(){
		return preferences.getBoolean("prefer_housenumber", true);
	}

	public void AdditionalInfo(boolean b){
		preferences.putBoolean("additional_address_info", b);
	}

	public boolean isAdditionalInfo(){
		return preferences.getBoolean("additional_address_info", true);
	}



	public void setRenderingProperties(String renderingProperties) {
		preferences.put("rendering_props", renderingProperties);
	}

	public String getRenderingProperties() {
		return preferences.get("rendering_props", "nightMode=false, appMode=default, noPolygons=false, hmRendered=false");
	}

}
