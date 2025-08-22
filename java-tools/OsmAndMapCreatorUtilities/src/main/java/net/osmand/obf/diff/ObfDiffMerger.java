package net.osmand.obf.diff;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

import gnu.trove.map.hash.TLongObjectHashMap;
import net.osmand.binary.*;
import net.osmand.data.Amenity;
import org.apache.commons.logging.Log;

import net.osmand.PlatformUtil;
import net.osmand.util.Algorithms;
import rtree.RTreeException;

public class ObfDiffMerger {
	static SimpleDateFormat day = new SimpleDateFormat("yyyy_MM_dd");
	static SimpleDateFormat month = new SimpleDateFormat("yyyy_MM");
	private static final Log LOG = PlatformUtil.getLog(ObfDiffMerger.class);
	static {
		day.setTimeZone(TimeZone.getTimeZone("UTC"));
		month.setTimeZone(TimeZone.getTimeZone("UTC"));
	}
	private static final String OSMAND_CHANGE_VALUE = "delete";
	private static final String OSMAND_CHANGE_TAG = "osmand_change";

	public static void main(String[] args) {
		try {
			if(args.length == 1 && args[0].equals("test")) {
				args = new String[4];
				List<String> s = new ArrayList<String>();
				s.add("/Users/victorshcherb/Desktop/Belarus_gomel_europe_merge.obf");
				s.add("/Users/victorshcherb/Desktop/Belarus_gomel_europe_07_10.obf");
				s.add("/Users/victorshcherb/Desktop/Belarus_gomel_europe_07_30.obf");
				s.add("/Users/victorshcherb/Desktop/Belarus_gomel_europe_09_00.obf");
				args = s.toArray(new String[0]);
			} else if (args.length == 1 && args[0].equals("mergeRelationTest")) {
				args = new String[3];
				List<String> s = new ArrayList<String>();
				s.add("/Users/macmini/OsmAnd/overpass/test7/23_04_23_23_20_diff_rel.obf");
				s.add("/Users/macmini/OsmAnd/overpass/test7/23_04_23_23_20_diff.obf");
				s.add("/Users/macmini/OsmAnd/overpass/test7/23_04_23_23_20_merged.obf");
				args = s.toArray(new String[0]);
				mergeRelationOsmLive(args);
				return;
			}
			ObfDiffMerger merger = new ObfDiffMerger();
			merger.mergeChanges(args);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}


	public static void mergeBulkOsmLiveDay(String[] args) {
		try {
			String location = args[0];
			File folder = new File(location);
			for (File region : getSortedFiles(folder)) {
				if (!region.isDirectory()) {
					continue;
				}
				String regionName = Algorithms.capitalizeFirstLetter(region.getName());
				if (regionName.startsWith("_")) {
					continue;
				}
				LOG.info("Processing " + regionName);
				if (regionName.equals("_diff")) {
					regionName = "World";
				}
				for (File date : getSortedFiles(region)) {
					if (!date.isDirectory()) {
						continue;
					}

					File flToMerge = new File(region, regionName + "_" + date.getName() + ".obf.gz");
					boolean processed = new ObfDiffMerger().process(flToMerge, Arrays.asList(date), true);
					if(processed) {
						System.out.println("Processed " + region + " " + date + " .");
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public static void mergeRelationOsmLive(String[] args) {
		try {
			if (args.length < 3) {
				System.out.println("Usage: <path to relation_osm_live.obf> <path to common_osm_live.obf> " +
						"<path to merged_osm_live.obf>");
				System.exit(1);
			}
			ObfDiffMerger merger = new ObfDiffMerger();
			merger.mergeRelationDiff(args);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	private void mergeRelationDiff(String[] args) throws IOException {
		File rel = new File(args[0]);
		File common = new File(args[1]);
		ObfFileInMemory relObf = new ObfFileInMemory();
		relObf.readObfFiles(Collections.singletonList(rel));
		ObfFileInMemory commonObf = new ObfFileInMemory();
		commonObf.readObfFiles(Collections.singletonList(common));

		File result = new File(args[2]);

		// Map section
		BinaryMapIndexReader.MapIndex mi = commonObf.getMapIndex();
		Integer commonDelIdRule = mi.getRule(OSMAND_CHANGE_TAG, OSMAND_CHANGE_VALUE);
		int commDelRule = commonDelIdRule == null ? -1 : commonDelIdRule;

        BinaryMapIndexReader.MapIndex mi2 = relObf.getMapIndex();
        Integer relDelIdRule = mi2.getRule(OSMAND_CHANGE_TAG, OSMAND_CHANGE_VALUE);
        int relDelRule = relDelIdRule == null ? -1 : relDelIdRule;

		int cnt = 0;
		for (MapZooms.MapZoomPair mz : relObf.getZooms()) {
			TLongObjectHashMap<BinaryMapDataObject> relMapData = relObf.get(mz);
			TLongObjectHashMap<BinaryMapDataObject> commonMapData = commonObf.get(mz);
			for (Long id : relMapData.keys()) {
				BinaryMapDataObject relObj = relMapData.get(id);
				BinaryMapDataObject commonObj = commonMapData.get(id);
				if (commonObj == null) {
                    if (relDelRule == -1 || !relObj.containsType(relDelRule)) {
                        commonMapData.put(id, mi.adoptMapObject(relObj));
                        cnt++;
                    }
				} else if (commDelRule == -1 || !commonObj.containsType(commDelRule)) {
					commonMapData.remove(id);
					commonMapData.put(id, mi.adoptMapObject(relObj));
					cnt++;
				}
				if (commonObj != null && commonObj.containsType(commDelRule)) {
					System.out.println("Map id:" + relObj.getId() + " has osmand_change=delete tag (" + relObj.toString() + ")");
				}
			}
		}
		System.out.println("Map section. Merged " + cnt);

		//Route section
		BinaryMapRouteReaderAdapter.RouteRegion ri = commonObf.getRouteIndex();
        commDelRule = ri.searchRouteEncodingRule(OSMAND_CHANGE_TAG, OSMAND_CHANGE_VALUE);

        BinaryMapRouteReaderAdapter.RouteRegion ri2 = commonObf.getRouteIndex();
        relDelRule = ri2.searchRouteEncodingRule(OSMAND_CHANGE_TAG, OSMAND_CHANGE_VALUE);

		TLongObjectHashMap<RouteDataObject> relRouteData = relObf.getRoutingData();
		TLongObjectHashMap<RouteDataObject> commonRouteData = commonObf.getRoutingData();
		cnt = 0;
		for (Long id : relRouteData.keys()) {
			RouteDataObject relObj = relRouteData.get(id);
			RouteDataObject commonObj = commonRouteData.get(id);
			if (commonObj == null) {
                if (relDelRule == -1 || !relObj.containsType(relDelRule)) {
                    commonRouteData.put(id, ri.adopt(relObj));
                    cnt++;
                }
			} else if (commDelRule == -1 || !commonObj.containsType(commDelRule)) {
				commonRouteData.remove(id);
				commonRouteData.put(id, ri.adopt(relObj));
				cnt++;
			}
			if (commonObj != null && commonObj.containsType(commDelRule)) {
				System.out.println("Route id:" + relObj.getId() + " has osmand_change=delete tag (" + relObj.toString() + ")");
			}
		}
		System.out.println("Route section. Merged " + cnt);

		//POI section
		TLongObjectHashMap<Map<String, Amenity>> relPoiSource = relObf.getPoiObjects();
		TLongObjectHashMap<Map<String, Amenity>> commonPoiSource = commonObf.getPoiObjects();
		ObfDiffGenerator obfDiffGenerator = new ObfDiffGenerator();
		Map<String, Amenity> relPoi = obfDiffGenerator.buildPoiMap(relPoiSource);
		Map<String, Amenity> commonPoi = obfDiffGenerator.buildPoiMap(commonPoiSource);
		cnt = 0;
		for (String id : relPoi.keySet()) {
			Amenity relObj = relPoi.get(id);
			if (commonPoiSource.get(relObj.getId()) == null) {
				commonPoiSource.put(relObj.getId(), new TreeMap<String, Amenity>());
				commonPoiSource.get(relObj.getId()).put(relObj.getType().getKeyName(), relObj);
				cnt++;
			} else {
				Amenity commonObj = commonPoi.get(id);
				if (commonObj == null || commonObj.getAdditionalInfo(OSMAND_CHANGE_TAG) == null) {
					commonPoiSource.get(relObj.getId()).remove(relObj.getType().getKeyName());
					commonPoiSource.get(relObj.getId()).put(relObj.getType().getKeyName(), relObj);
					cnt++;
				}
				if (commonObj != null && commonObj.getAdditionalInfo(OSMAND_CHANGE_TAG) != null) {
					System.out.println("Amenity id:" + relObj.getId() + " has osmand_change=delete tag");
				}
			}
		}
		System.out.println("POI section. Merged " + cnt);

		// Don't merge transport section, because data in relation diff is not complete

		// Write results
		try {
			System.out.println("Saving merged file");
			commonObf.writeFile(result, true);
			System.out.println("SUCCESS");
		} catch (RTreeException e) {
			e.printStackTrace();
		} catch (SQLException throwables) {
			throwables.printStackTrace();
		}
	}

	private static List<File> getSortedFiles(File region) {
		List<File> f = new ArrayList<>();
		for(File l : region.listFiles()) {
			f.add(l);
		}
		f.sort(new Comparator<File>() {

			@Override
			public int compare(File o1, File o2) {
				return o1.getName().compareTo(o2.getName());
			}

		});

		return f;
	}


	public static void mergeBulkOsmLiveMonth(String[] args) {
		try {
			String location = args[0];
			Date currentDate = new Date();
			String cdate = day.format(currentDate).substring(2);
			String pdate = day.format(new Date(System.currentTimeMillis() - 1000 * 24 * 60 * 60 * 20)).substring(2);
			String ppdate = day.format(new Date(System.currentTimeMillis() - 1000 * 24 * 60 * 60 * 40)).substring(2);
			System.out.println("Current date: " + cdate + ", file ends with will be ignored: " + cdate + ".obf.gz");
			Set<String> allowedMonths = new TreeSet<String>();
			allowedMonths.add(cdate.substring(0, cdate.length() - 2) +"00");
			allowedMonths.add(pdate.substring(0, pdate.length() - 2) +"00");
			allowedMonths.add(ppdate.substring(0, ppdate.length() - 2) +"00");
			System.out.println("Process following months: " +  allowedMonths);

			File folder = new File(location);
			for (File region : getSortedFiles(folder)) {
				if (!region.isDirectory()) {
					continue;
				}
				String regionName = Algorithms.capitalizeFirstLetter(region.getName());
				if (regionName.startsWith("_")) {
					continue;
				}
				List<File> days = getSortedFiles(region);

				Map<String, List<File>> fls = groupFilesByMonth(regionName, days, cdate, allowedMonths);
				for (String fl : fls.keySet()) {
					File flToMerge = new File(region, fl);
					boolean processed = new ObfDiffMerger().process(flToMerge, fls.get(fl), true);
					if(processed) {
						String s = "";
						for(File f: fls.get(fl)) {
							s += f.getName() + " ";
						}
						System.out.println("Processed " + flToMerge + " with " + s);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	private static Map<String, List<File>> groupFilesByMonth(String regionName, List<File> days, String cdate,
			Set<String> allowedMonths) {
		Map<String, List<File>> grpFiles = new LinkedHashMap<String, List<File>>();
		for (File d : days) {
			if (!d.isFile() || !d.getName().startsWith(regionName + "_") || !d.getName().endsWith(".obf.gz")) {
				continue;
			}
			// month
			if (d.getName().endsWith("_00.obf.gz")) {
				continue;
			}
			// current date
			if (d.getName().endsWith(cdate + ".obf.gz")) {
				continue;
			}
			String date = d.getName().substring(regionName.length() + 1, d.getName().length() - ".obf.gz".length());
			String mnth = date.substring(0, date.length() - 2) + "00";
			String mnthFile = regionName + "_" + mnth +".obf.gz";
			if(!allowedMonths.contains(mnth)) {
				continue;
			}
			if(!grpFiles.containsKey(mnthFile)) {
				grpFiles.put(mnthFile, new ArrayList<File>());
			}
			grpFiles.get(mnthFile).add(d);

		}
		return grpFiles;
	}


	private void sortByDate(List<File> f) {
		f.sort(new Comparator<File>() {

			@Override
			public int compare(File o1, File o2) {
				long l1 = o1.lastModified();
				long l2 = o2.lastModified();
				return Long.compare(l1, l2);
			}
		});
	}

	private void mergeChanges(String[] args) throws IOException, RTreeException, SQLException {
		File result = new File(args[0]);
		List<File> inputDiffs = new ArrayList<>();
		boolean checkTimestamps = false;
		for (int i = 1; i < args.length; i++) {
			if(args[i].equals("--check-timestamp")) {
				checkTimestamps = true;
				continue;
			}
			File fl = new File(args[i]);
			if(!fl.exists()) {
				throw new IllegalArgumentException("File not found: " + fl.getAbsolutePath());
			}
			inputDiffs.add(fl);
		}
		process(result, inputDiffs, checkTimestamps);
	}

	public boolean process(File result, List<File> inputDiffs, boolean checkTimestamps) throws IOException,
			RTreeException, SQLException {
		List<File> diffs = new ArrayList<>();
		for (File fl : inputDiffs) {
			if (fl.isDirectory()) {
				File[] lf = fl.listFiles();
				List<File> odiffs = new ArrayList<>();
				if (lf != null) {
					for (File f : lf) {
						if (f.getName().endsWith(".obf") || f.getName().endsWith(".obf.gz")) {
							odiffs.add(f);
						}
					}
				}
				sortByDate(odiffs);
				diffs.addAll(odiffs);
			} else {
				diffs.add(fl);
			}
		}
		if (checkTimestamps && result.exists()) {
			boolean skipEditing = true;
			long lastModified = result.lastModified();
			for (File f : diffs) {
				if (f.lastModified() > lastModified) {
					LOG.info("Process " + result.getName() + " because of " + f.getName() + " " + lastModified + " != "
							+ f.lastModified());
					skipEditing = false;
					break;
				}
			}
			if (skipEditing) {
				return false;
			}
		}
		ObfFileInMemory context = new ObfFileInMemory();
		context.readObfFiles(diffs);
		context.writeFile(result, true);
		return true;
	}




}
