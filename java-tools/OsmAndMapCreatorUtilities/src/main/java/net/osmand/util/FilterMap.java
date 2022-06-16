package net.osmand.util;

import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.map.OsmandRegions;
import net.osmand.map.WorldRegion;

import java.io.File;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class FilterMap {
    
    FilterMap() {
    }
    
    public static boolean checkBiggerMapExist(List<BinaryMapIndexReader> files, List<String> regionNameList, String fileName, OsmandRegions osmandRegions) {
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
    
    public static void checkBiggerMapExistNative(File file, List<String> regionNameList, String fileName, OsmandRegions osmandRegions,
                                                 List<File> files, TreeSet<String> allFileNames, boolean filterDuplicates) {
        String name = getRegionName(fileName);
        WorldRegion wr = osmandRegions.getRegionDataByDownloadName(name);
        if (wr == null) {
            //check basemap
            boolean basemapPresent = checkMoreGenericMapPresent(allFileNames, file.getName());
            if (filterDuplicates && !basemapPresent) {
                files.add(file);
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
    
    public static boolean containsBiggerMap(List<String> regionNameList, String parentName) {
        for (String name : regionNameList) {
            if (name.equalsIgnoreCase(parentName)) {
                return true;
            }
        }
        return false;
    }
    
    public static void removeSubregionsIfExist(WorldRegion wr, List<String> regionNameList, List<File> files) {
        for (WorldRegion subr : wr.getSubregions()) {
            String subrName = subr.getRegionDownloadName();
            if (regionNameList.contains(subrName)) {
                regionNameList.remove(subrName);
                files.removeIf(file -> getRegionName(file.getName()).equals(subrName));
            }
        }
    }
    
    public static String getRegionName(String fileName) {
        String name = fileName.split("\\.")[0];
        return name.substring(0, name.length() - 2).toLowerCase();
    }
    
    private static boolean checkMoreGenericMapPresent(TreeSet<String> allFileNames, String file) {
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
