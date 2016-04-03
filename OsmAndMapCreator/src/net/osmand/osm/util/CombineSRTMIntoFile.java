package net.osmand.osm.util;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.stream.XMLStreamException;

import net.osmand.IndexConstants;
import net.osmand.PlatformUtil;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.data.LatLon;
import net.osmand.data.Multipolygon;
import net.osmand.data.MultipolygonBuilder;
import net.osmand.data.QuadRect;
import net.osmand.data.Ring;
import net.osmand.data.preparation.DBDialect;
import net.osmand.data.preparation.IndexCreator;
import net.osmand.data.preparation.MapZooms;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.map.OsmandRegions;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;
import net.osmand.util.Algorithms;
import net.osmand.util.MapAlgorithms;
import net.osmand.util.MapUtils;

import org.apache.commons.logging.Log;
import org.xmlpull.v1.XmlPullParserException;

import rtree.RTree;

public class CombineSRTMIntoFile {
	private static final Log log = PlatformUtil.getLog(CombineSRTMIntoFile.class);

	public static void main(String[] args) throws IOException {
		File directoryWithSRTMFiles = new File(args[0]);
		File directoryWithTargetFiles = new File(args[1]);
		String ocbfFile = args[2];
//		File directoryWithSRTMFiles = new File("/Users/victorshcherb/osmand/maps/srtm/");
//		File directoryWithTargetFiles = new File("/Users/victorshcherb/osmand/maps/srtm/");
//		String ocbfFile = "/Users/victorshcherb/osmand/repos/resources/countries-info/regions.ocbf";
		OsmandRegions or = new OsmandRegions();
		or.prepareFile(ocbfFile);
		or.cacheAllCountries();

		List<BinaryMapDataObject> r = or.queryBbox(0, Integer.MAX_VALUE, 0, Integer.MAX_VALUE);
		Integer srtm = null;
		Integer regionFullName = null;
		Integer osmandRegionBnd = null;
		Integer downloadName = null;
		Map<String, List<BinaryMapDataObject> > bnds = new HashMap<String, List<BinaryMapDataObject>>();
		for(BinaryMapDataObject rc : r) {
			if(srtm == null) {
				srtm = rc.getMapIndex().getRule("region_srtm", "yes");
				regionFullName = rc.getMapIndex().getRule("region_full_name", null);
				osmandRegionBnd= rc.getMapIndex().getRule("osmand_region", "boundary");
				downloadName = rc.getMapIndex().getRule("download_name", null);
			}
			if(rc.containsAdditionalType(osmandRegionBnd)) {
				String fullName = rc.getNameByType(regionFullName);
				if(!bnds.containsKey(fullName)) {
					bnds.put(fullName, new ArrayList<BinaryMapDataObject>());
				}
				bnds.get(fullName).add(rc);
			}
		}
		int cnt = 1;
		Set<String> failedCountries = new HashSet<String>();
		for(BinaryMapDataObject rc : r) {
			if(rc.containsAdditionalType(srtm)) {
				String dw = rc.getNameByType(downloadName);
				String fullName = rc.getNameByType(regionFullName);
				System.out.println("Region " + fullName +" " + cnt++ + " out of " + r.size());
				try {
					process(rc, bnds.get(fullName), dw, directoryWithSRTMFiles, directoryWithTargetFiles);
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

	private static void process(BinaryMapDataObject country, List<BinaryMapDataObject> boundaries,
			String downloadName, File directoryWithSRTMFiles, File directoryWithTargetFiles) throws IOException, SQLException, InterruptedException, IllegalArgumentException, XmlPullParserException {
		final String suffix = "_" + IndexConstants.BINARY_MAP_VERSION + IndexConstants.BINARY_SRTM_MAP_INDEX_EXT;
		String name = country.getName();
		final File targetFile = new File(directoryWithTargetFiles, Algorithms.capitalizeFirstLetterAndLowercase(downloadName+suffix));
		if(targetFile.exists()) {
			System.out.println("Already processed "+ name);
			return;
		}

		Set<String> srtmFileNames = new TreeSet<String>();
		QuadRect qr = new QuadRect(180, -90, -180, 90);
		MultipolygonBuilder bld = new MultipolygonBuilder();
		bld.addOuterWay(convertToWay(country));
		updateBbox(country, qr);
		if(boundaries != null) {
			for (BinaryMapDataObject o : boundaries) {
				bld.addOuterWay(convertToWay(o));
				updateBbox(o, qr);
			}
		}
		Multipolygon polygon  = bld.build();
		System.out.println("RINGS OF MULTIPOLYGON ARE " + polygon.areRingsComplete());
		int rightLon = (int) Math.floor(qr.right);
		int leftLon = (int) Math.floor(qr.left);
		int bottomLat = (int) Math.floor(qr.bottom);
		int topLat = (int) Math.floor(qr.top);
		for(int lon = leftLon; lon <= rightLon; lon++) {
			for(int lat = bottomLat; lat <= topLat; lat++) {
				boolean isOut = !polygon.containsPoint(lat + 0.5, lon + 0.5);
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
		System.out.println("PROCESSING "+name + " lon [" + leftLon + " - " + rightLon + "] lat [" + bottomLat + " - " + topLat
				+ "] TOTAL " + srtmFileNames.size() + " files");
		System.out.println("-----------------------------");
//		final File work = new File(directoryWithTargetFiles, "work");
//		Map<File, String> mp = new HashMap<File, String>();
//		long length = 0;
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
		// be independent of previous results
		new File(targetFile.getParentFile(), IndexCreator.TEMP_NODES_DB).delete();
		RTree.clearCache();
		IndexCreator ic = new IndexCreator(targetFile.getParentFile());
		ic.setDialects(DBDialect.SQLITE_IN_MEMORY, DBDialect.SQLITE_IN_MEMORY);
		ic.setIndexMap(true);
		ic.setRegionName(name +" contour lines");
		ic.setMapFileName(targetFile.getName());
		ic.setBoundary(polygon);
		ic.setZoomWaySmothness(2);
		ic.generateIndexes(files.toArray(new File[files.size()]), new ConsoleProgressImplementation(1), null, MapZooms.parseZooms("11-12;13-"),
				new MapRenderingTypesEncoder(targetFile.getName()), log, true);
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
