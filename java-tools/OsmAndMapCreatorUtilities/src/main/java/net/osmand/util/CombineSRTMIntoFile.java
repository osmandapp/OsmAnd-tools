package net.osmand.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;

import net.osmand.IndexConstants;
import net.osmand.PlatformUtil;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexReader.MapIndex;
import net.osmand.binary.MapZooms;
import net.osmand.data.LatLon;
import net.osmand.data.Multipolygon;
import net.osmand.data.MultipolygonBuilder;
import net.osmand.data.QuadRect;
import net.osmand.data.Ring;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.map.OsmandRegions;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.obf.preparation.IndexCreator;
import net.osmand.obf.preparation.IndexCreatorSettings;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;
import rtree.RTree;

public class CombineSRTMIntoFile {
	private static final Log log = PlatformUtil.getLog(CombineSRTMIntoFile.class);
	private static final int NUMBER_OF_FILES_TO_PROCESS_ON_DISK = 50;
	private static final long SIZE_GB_TO_COMBINE_INRAM = 8l << 30; // 8GB

	public static void main(String[] args) throws IOException {
		File directoryWithSRTMFiles = new File(args[0]);
		File directoryWithTargetFiles = new File(args[1]);
		boolean dryRun = false;
		boolean feet = false;
		String filter = null; // mauritius
		int limit = 1000;
		for(int i = 2; i < args.length; i++ ){
			if("--dry-run".equals(args[i])) {
				dryRun = true;
			} else if("--feet".equals(args[i])) {
				feet = true;
			} else if(args[i].startsWith("--filter=")) {
				filter = args[i].substring("--filter=".length());
				if(filter.length() == 0) {
					filter = null;
				}
			} else if(args[i].startsWith("--limit=")) {
				limit = Integer.parseInt(args[i].substring("--limit=".length())); 
			}
		}
		OsmandRegions or = new OsmandRegions();
		BinaryMapIndexReader fl = or.prepareFile();
		Map<String, LinkedList<BinaryMapDataObject>> allCountries = or.cacheAllCountries();
		MapIndex mapIndex = fl.getMapIndexes().get(0);
		int srtm = mapIndex.getRule("region_srtm", "yes");
		int downloadName = mapIndex.getRule("download_name", null);
		int boundary = mapIndex.getRule("osmand_region", "boundary");
		int cnt = 1;
		Set<String> failedCountries = new HashSet<String>();
		for(String fullName : allCountries.keySet()) {
			LinkedList<BinaryMapDataObject> lst = allCountries.get(fullName);
			if (fullName == null || (filter != null && !fullName.contains(filter))) {
				continue;
			}
			BinaryMapDataObject rc = null;
			for(BinaryMapDataObject r : lst) {
				if(!r.containsType(boundary)) {
					rc = r;
					break;
				}
			}
			System.out.println(fullName);
			if(rc != null && rc.containsAdditionalType(srtm)) {
				String dw = rc.getNameByType(downloadName);
				System.out.println("Region " + fullName + " " + cnt++ + " out of " + allCountries.size());
				try {
					process(rc, lst, dw, directoryWithSRTMFiles, directoryWithTargetFiles, dryRun, limit, feet);
				} catch(Exception e) {
					failedCountries.add(fullName);
					e.printStackTrace();
				}
			}
		}
		if(!failedCountries.isEmpty()) {
			throw new IllegalStateException("Failed countries " + failedCountries);
		}
	}

	private static void process(BinaryMapDataObject country, List<BinaryMapDataObject> boundaries, String downloadName,
			File directoryWithSRTMFiles, File directoryWithTargetFiles, boolean dryRun, int limit, boolean feet)
			throws Exception {
		final String suffix = "_" + IndexConstants.BINARY_MAP_VERSION + 
				(feet ? IndexConstants.BINARY_SRTM_FEET_MAP_INDEX_EXT : IndexConstants.BINARY_SRTM_MAP_INDEX_EXT);
		String name = country.getName();
		String dwName = Algorithms.capitalizeFirstLetterAndLowercase(downloadName + suffix);
		final File targetFile = new File(directoryWithTargetFiles, dwName);
		if (targetFile.exists()) {
			System.out.println("Already processed " + name);
			return;
		}

		Set<String> srtmFileNames = new TreeSet<String>();
		QuadRect qr = new QuadRect(180, -90, -180, 90);
		MultipolygonBuilder bld = new MultipolygonBuilder();
		if(boundaries != null) {
			for (BinaryMapDataObject o : boundaries) {
				bld.addOuterWay(convertToWay(o));
				updateBbox(o, qr);
			}
		} else {
			bld.addOuterWay(convertToWay(country));
			updateBbox(country, qr);			
		}
		Multipolygon polygon  = bld.build();
		int rightLon = (int) Math.floor(qr.right);
		int leftLon = (int) Math.floor(qr.left);
		int bottomLat = (int) Math.floor(qr.bottom);
		int topLat = (int) Math.floor(qr.top);
		boolean onetile = leftLon == rightLon && bottomLat == topLat;
		for(int lon = leftLon; lon <= rightLon; lon++) {
			for(int lat = bottomLat; lat <= topLat; lat++) {
				boolean isOut = !polygon.containsPoint(lat + 0.5, lon + 0.5) && !onetile;
				if (isOut) {
					LatLon bl = new LatLon(lat, lon);
					LatLon br = new LatLon(lat, lon + 1);
					LatLon tr = new LatLon(lat + 1, lon + 1);
					LatLon tl = new LatLon(lat + 1, lon);
					for (Ring r : polygon.getOuterRings()) {
						List<Node> border = r.getBorder();
						Node prev = border.get(border.size() - 1);
						for (int i = 0; i < border.size() && isOut; i++) {
							Node n = border.get(i);
							if(MapAlgorithms.linesIntersect(prev.getLatLon(), n.getLatLon(), tr, tl)) {
								isOut = false;
							} else if(MapAlgorithms.linesIntersect(prev.getLatLon(), n.getLatLon(), tr, br)) {
								isOut = false;
							} else if(MapAlgorithms.linesIntersect(prev.getLatLon(), n.getLatLon(), bl, tl)) {
								isOut = false;
							} else if(MapAlgorithms.linesIntersect(prev.getLatLon(), n.getLatLon(), br, bl)) {
								isOut = false;
							}
							prev = n;
						}
						if(!isOut) {
							break;
						}
					}
				}
				if(!isOut) {
					final String filename = getFileName(lon, lat);
					srtmFileNames.add(filename);
				}
			}
		}
		System.out.println();
		System.out.println("-----------------------------");
		System.out.println("PROCESSING "+name + " lon [" + leftLon + " - " + rightLon + "] lat [" + bottomLat + " - " + topLat
				+ "] TOTAL " + srtmFileNames.size() + " files " + srtmFileNames);
		if(dryRun) {
			return;
		}
		if(srtmFileNames.size() > limit) {
			System.out.println("\n\n!!!!!!!! WARNING BECAUSE LIMIT OF FILES EXCEEDED !!!!!!!!!\n\n");
			return;
		}
		File procFile = new File(directoryWithTargetFiles, dwName + ".proc");
		boolean created = procFile.createNewFile();
		if (!created) {
			// Lock file exists; check if the process is still alive
			long existingPid = readPidFromFile(procFile);
			if (existingPid != -1 && isProcessAlive(existingPid)) {
				System.out.println("\n\n!!!!!!!! WARNING FILE IS BEING PROCESSED !!!!!!!!!\n\n");
				return;
			} else {
				// Stale lock file found; delete and retry
				if (!procFile.delete()) {
					System.out.println("Failed to delete stale lock file.");
					return;
				}
				created = procFile.createNewFile();
				if (!created) {
					System.out.println("\n\n!!!!!!!! WARNING FILE IS BEING PROCESSED !!!!!!!!!\n\n");
					return;
				}
			}
		}
		writePidToFile(procFile);
		
//		final File work = new File(directoryWithTargetFiles, "work");
//		Map<File, String> mp = new HashMap<File, String>();
		List<File> files = new ArrayList<File>();
		for(String file : srtmFileNames) {
			final File fl = new File(directoryWithSRTMFiles, file + ".osm.bz2");
			if(!fl.exists()) {
				System.err.println("!! Missing " + name + " because " + file + " doesn't exist");
			} else {
				files.add(fl);
//				File ttf = new File(fl.getParentFile(), Algorithms.capitalizeFirstLetterAndLowercase(file) + "_"+ name + ".obf");
//				mp.put(ttf, null);
			}
		}
		if (files.isEmpty()) {
			System.err.println("!!! WARNING " + name + " because no files are present to index !!!");
		} else {
			// speedup processing by using fast disk
			File genFile = new File(targetFile.getName());
			IndexCreatorSettings settings = new IndexCreatorSettings();
			settings.indexMap = true;
			settings.zoomWaySmoothness = 2;
			settings.boundary = polygon;
			IndexCreator ic = new IndexCreator(genFile.getParentFile(), settings);

//			if (srtmFileNames.size() > NUMBER_OF_FILES_TO_PROCESS_ON_DISK || length > SIZE_GB_TO_COMBINE_INRAM) {
//				ic.setDialects(DBDialect.SQLITE, DBDialect.SQLITE);
//				System.out.println("SQLITE on disk is used.");
//			} else {
				ic.setDialects(DBDialect.SQLITE, DBDialect.SQLITE_IN_MEMORY);
//				System.out.println("SQLITE in memory used: be aware whole database is stored in memory.");
//			}
			ic.setRegionName(name + " contour lines");
			ic.setMapFileName(genFile.getName());
			File nodesDB = new File(genFile.getParentFile(), dwName + "." + IndexCreator.TEMP_NODES_DB);
			ic.setNodesDBFile(nodesDB);
			ic.generateIndexes(files.toArray(new File[files.size()]), new ConsoleProgressImplementation(1), null,
					MapZooms.parseZooms("11-12;13-"), new MapRenderingTypesEncoder(genFile.getName()), log, true);
			nodesDB.delete();
			// reduce memory footprint for single thread generation
			// Remove it if it is called in multithread
			RTree.clearCache();
			
			genFile.renameTo(targetFile);
		}
		procFile.delete();
//		if(length > Integer.MAX_VALUE) {
//			System.err.println("!! Can't process " + name + " because too big");
//		} else {
//			BinaryInspector.combineParts(targetFile, mp);
//		}
//		for(String file : srtmFileNames) {
//			final File fl = new File(work, file);
//			fl.delete();
//		}
	}
	
	private static long readPidFromFile(File procFile) {
		try (BufferedReader reader = new BufferedReader(new FileReader(procFile))) {
			String line = reader.readLine();
			return line != null ? Long.parseLong(line.trim()) : -1;
		} catch (IOException | NumberFormatException e) {
			return -1;
		}
	}

	private static boolean isProcessAlive(long pid) {
		Optional<ProcessHandle> handle = ProcessHandle.of(pid);
		return handle.map(ProcessHandle::isAlive).orElse(false);
	}

	private static void writePidToFile(File procFile) throws IOException {
		try (FileWriter writer = new FileWriter(procFile)) {
			long pid = ProcessHandle.current().pid();
			writer.write(Long.toString(pid));
		}
	}

	private static Way convertToWay(BinaryMapDataObject o) {
		Way w = new Way(-1);
		for(int i = 0; i < o.getPointsLength(); i++) {
			double lat = MapUtils.get31LatitudeY(o.getPoint31YTile(i));
			double lon = MapUtils.get31LongitudeX(o.getPoint31XTile(i));
			w.addNode(new Node(lat, lon, -1));
		}
		return w;
	}



	private static void updateBbox(BinaryMapDataObject country, QuadRect qr) {
		for(int i = 0; i < country.getPointsLength(); i++) {
			double lat = MapUtils.get31LatitudeY(country.getPoint31YTile(i));
			double lon = MapUtils.get31LongitudeX(country.getPoint31XTile(i));
			qr.left = Math.min(lon, qr.left);
			qr.right = Math.max(lon, qr.right);
			qr.top = Math.max(lat, qr.top);
			qr.bottom = Math.min(lat, qr.bottom);
		}
	}

	private static String getFileName(int lon, int lat) {
		String fn = lat >= 0 ? "N" : "S";
		if(Math.abs(lat) < 10) {
			fn += "0";
		}
		fn += Math.abs(lat);
		fn += lon >= 0 ? "E" : "W";
		if(Math.abs(lon) < 10) {
			fn += "00";
		} else if(Math.abs(lon) < 100) {
			fn += "0";
		}
		fn += Math.abs(lon);
		return fn;
	}

}
