package net.osmand.util;

import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.map.OsmandRegions;
import net.osmand.map.WorldRegion;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class UtilityToExcludeDuplicatedMaps {
    private static final Log log = LogFactory.getLog(UtilityToExcludeDuplicatedMaps.class);
    
    public boolean checkBiggerMapExist(List<BinaryMapIndexReader> files, List<String> regionNameList, String fileName, OsmandRegions osmandRegions) throws IOException {
        String name = getRegionName(fileName);
        WorldRegion wr = osmandRegions.getRegionDataByDownloadName(name);
        String parentRegionName = wr.getSuperregion().getRegionDownloadName();
        
        if (!containsBiggerMap(regionNameList, parentRegionName) || parentRegionName == null) {
            if (!regionNameList.isEmpty()) {
                List<File> fileList = files.stream()
                        .map(BinaryMapIndexReader::getFile)
                        .collect(Collectors.toList());
                removeSubregionsIfExist(wr, regionNameList, fileList);
                files.removeIf(file -> !fileList.contains(file.getFile()));
            }
            regionNameList.add(name);
            return false;
        }
        return true;
    }
    
    public void checkBiggerMapExistNative(File file, List<String> regionNameList,
                                          List<File> files, TreeSet<String> allFileNames, boolean filterDuplicates, OsmandRegions osmandRegions) throws IOException {
        String name = getRegionName(file.getName());
        WorldRegion wr = osmandRegions.getRegionDataByDownloadName(name);
        if (wr == null) {
            //check basemap
            boolean basemapPresent = checkMoreGenericMapPresent(allFileNames, file.getName());
            if (filterDuplicates && !basemapPresent) {
                files.add(file);
            } else {
                log.debug("SKIP initializing cause bigger map is present: " + file.getName());
            }
        } else {
            String parentRegionName = wr.getSuperregion().getRegionDownloadName();
            if (!containsBiggerMap(regionNameList, parentRegionName) || parentRegionName == null) {
                if (!regionNameList.isEmpty()) {
                    removeSubregionsIfExist(wr, regionNameList, files);
                }
                regionNameList.add(name);
                files.add(file);
            }
        }
    }
    
    private boolean containsBiggerMap(List<String> regionNameList, String parentName) {
        for (String name : regionNameList) {
            if (name.equalsIgnoreCase(parentName)) {
                return true;
            }
        }
        return false;
    }
    
    private void removeSubregionsIfExist(WorldRegion wr, List<String> regionNameList, List<File> files) {
        for (WorldRegion subr : wr.getSubregions()) {
            String subrName = subr.getRegionDownloadName();
            if (regionNameList.contains(subrName)) {
                regionNameList.remove(subrName);
                files.removeIf(file -> getRegionName(file.getName()).equals(subrName));
            }
        }
    }
    
    private String getRegionName(String fileName) {
        String name = fileName.split("\\.")[0];
        return name.substring(0, name.length() - 2).toLowerCase();
    }
    
    private boolean checkMoreGenericMapPresent(TreeSet<String> allFileNames, String file) {
        String[] splitParts = file.split("_");
        boolean presentBaseFile = false;
        for (int i = 0; i < splitParts.length - 1 && !presentBaseFile; i++) {
            StringBuilder baseFile = new StringBuilder();
            for (int j = 0; j < splitParts.length; j++) {
                if (i == j) {
                    continue;
                }
                if (baseFile.length() > 0) {
                    baseFile.append("_");
                }
                baseFile.append(splitParts[j]);
            }
            if (allFileNames.contains(baseFile.toString())) {
                return true;
            }
        }
        return false;
    }
}
