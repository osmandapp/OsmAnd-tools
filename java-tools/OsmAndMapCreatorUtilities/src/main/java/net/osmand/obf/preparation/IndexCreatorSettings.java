package net.osmand.obf.preparation;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import net.osmand.data.Multipolygon;

public class IndexCreatorSettings {
	
	public boolean indexMap;
	public boolean indexPOI;
	public boolean indexTransport;
	public boolean indexAddress;
	public boolean indexRouting;
	public boolean addRegionTag = false;
	
	// use Sqlite in RAM instead of normal Sqlite (speeds up process but takes a lot of RAM)  
	public boolean processInRam;
	
	// maximum tiles to use in RAM
	public int maxHeightTilesInRam = -1;

	// generate low level roads and maps
	public boolean generateLowLevel = true;
	
	// zoom smoothness for low level roads
	public int zoomWaySmoothness = 2;
	
	// srtm data folder to amend roads with height profile (could be s3://, https:// url)
	public String srtmDataFolderUrl;
	
	// gtfs data for public transport
	public File gtfsData;
	
	// for example file with low emissions polygons 
	public List<File> extraRelations = new ArrayList<>();
	
	// path to rendering_types.xml
	public String renderingTypesFile;
	
	// for seamarks generation
	public boolean keepOnlySeaObjects;
	
	// limit entities by multipolygon (used by srtm)  
	public Multipolygon boundary;
	
	// adds additional info to house name
	public boolean houseNameAddAdditionalInfo = false;
	
	public boolean houseNumberPreferredOverName = true;

	// should be documented
	public boolean suppressWarningsForDuplicateIds = true;

	// make by default
	public boolean poiZipLongStrings = true;
	
	public int poiZipStringLimit = 100;
	
	public int charsToBuildPoiNameIndex = 4;
	
	public int charsToBuildAddressNameIndex = 4;
	
	public boolean keepOnlyRouteRelationObjects;
	
    public boolean indexByProximity = true;
	

	public String getString(String key) {
		// IndexCreator.INDEX_LO_LEVEL_WAYS
		String k = key.substring(key.indexOf('.')+1).toLowerCase().replace(' ', ' ');
		return k;
	}
}
