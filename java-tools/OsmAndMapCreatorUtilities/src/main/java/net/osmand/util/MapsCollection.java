package net.osmand.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import net.osmand.map.OsmandRegions;
import net.osmand.map.WorldRegion;

public class MapsCollection {
    private static final Log log = LogFactory.getLog(MapsCollection.class);
    
	private final OsmandRegions osmandRegions;
	private final boolean filterDuplicates;
	private final List<File> allFiles = new ArrayList<File>();
	private List<File> filesToUse;
    
    public MapsCollection(boolean filterDuplicates) throws IOException {
    	this.filterDuplicates = filterDuplicates;
    	if(filterDuplicates) {
    		osmandRegions = new OsmandRegions();
    		osmandRegions.prepareFile();
    	} else {
    		osmandRegions = null;
    	}
    }
    
    public void add(File obf) {
    	allFiles.add(obf);
	}
    
    
	public List<File> getFilesToUse() throws IOException {
		if (filesToUse != null) {
			return filesToUse;
		}

		filesToUse = new ArrayList<>();
		if (!filterDuplicates) {
			filesToUse.addAll(allFiles);
			return filesToUse;
		}
		TreeSet<String> allDwNames = new TreeSet<>();
		for (File file : allFiles) {
			String dwName = getDownloadNameByFileName(file.getName());
			WorldRegion wr = osmandRegions.getRegionDataByDownloadName(dwName);
			if (wr != null) {
				allDwNames.add(wr.getRegionDownloadName());
			}
		}
		for (File file : allFiles) {
			String dwName = getDownloadNameByFileName(file.getName());
			WorldRegion wr = osmandRegions.getRegionDataByDownloadName(dwName);
			if (wr == null) {
				// unknown map add to use
				filesToUse.add(file);
			} else {
				if (wr.getSuperregion() != null && 
						allDwNames.contains(wr.getSuperregion().getRegionDownloadName())) {
					log.debug("SKIP initializing cause bigger map is present: " + file.getName());
				} else {
					filesToUse.add(file);
				}
			}
		}
		return filesToUse;
	}
	
	private String getDownloadNameByFileName(String fileName) {
		String dwName = fileName.substring(0, fileName.indexOf('.')).toLowerCase();
		if (dwName.endsWith("_2")) {
			dwName = dwName.substring(0, dwName.length() - 2);
		}
		return dwName;
	}
    
}
