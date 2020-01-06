package net.osmand.obf.preparation;

import java.io.File;

import net.osmand.data.Multipolygon;

public class IndexCreatorSettings {
	
	public boolean indexMap;
	public boolean indexPOI;
	public boolean indexTransport;
	public boolean indexAddress;
	public boolean indexRouting;
	public boolean addRegionTag = false;

	// generate low level roads and maps
	public boolean generateLowLevel = true;
	
	// zoom smoothness for low level roads
	public int zoomWaySmoothness = 2;
	
	// srtm data folder to amend roads with height profile
	public File srtmDataFolder;
	
	// gtfs data for public transport
	public File gtfsData;
	
	// for seamarks generation
	public boolean keepOnlySeaObjects;
	
	
	// limit entities by multipolygon (used by srtm)  
	public Multipolygon boundary;
	
	// adds additional info to house name
	public boolean houseNameAddAdditionalInfo = false;
	
	public boolean houseNumberPreferredOverName = true;

	// should be documented
	public boolean backwardCompatibleIds = false;
	
	public boolean suppressWarningsForDuplicateIds = true;

	public boolean poiZipLongStrings = false;
	
	public int poiZipStringLimit = 100;
	
	

	public String getString(String key) {
		// IndexCreator.INDEX_LO_LEVEL_WAYS
		String k = key.substring(key.indexOf('.')+1).toLowerCase().replace(' ', ' ');
		return k;
	}
}
