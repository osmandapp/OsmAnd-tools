package net.osmand.osm.util;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.stream.XMLStreamException;

import net.osmand.IndexConstants;
import net.osmand.PlatformUtil;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.data.Multipolygon;
import net.osmand.data.MultipolygonBuilder;
import net.osmand.data.QuadRect;
import net.osmand.data.preparation.DBDialect;
import net.osmand.data.preparation.IndexCreator;
import net.osmand.data.preparation.MapZooms;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.map.OsmandRegions;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

import org.apache.commons.logging.Log;
import org.xml.sax.SAXException;

import rtree.RTree;

public class CombineSRTMIntoFile {
	private static final Log log = PlatformUtil.getLog(CombineSRTMIntoFile.class);

	public static void main(String[] args) throws IOException, SAXException, XMLStreamException, SQLException, InterruptedException {
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
		for(BinaryMapDataObject rc : r) {
			if(rc.containsAdditionalType(srtm)) {
				String dw = rc.getNameByType(downloadName);
				String fullName = rc.getNameByType(regionFullName);
				process(rc, bnds.get(fullName), dw, directoryWithSRTMFiles, directoryWithTargetFiles);
			}
		}
	}

	private static void process(BinaryMapDataObject country, List<BinaryMapDataObject> boundaries,
			String downloadName, File directoryWithSRTMFiles, File directoryWithTargetFiles) throws IOException, SAXException, SQLException, InterruptedException {
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
		System.out.println(polygon.areRingsComplete());
		int rightLon = (int) Math.floor(qr.right);
		int leftLon = (int) Math.floor(qr.left);
		int bottomLat = (int) Math.floor(qr.bottom);
		int topLat = (int) Math.floor(qr.top);
		for(int lon = leftLon; lon <= rightLon; lon++) {
			for(int lat = bottomLat; lat <= topLat; lat++) {
				final String filename = getFileName(lon, lat);
				srtmFileNames.add(filename);
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
		ic.setDialects(DBDialect.SQLITE_IN_MEMORY, DBDialect.SQLITE);
		ic.setIndexMap(true);
		ic.setRegionName(name +" contour lines");
		ic.setMapFileName(targetFile.getName());
		ic.setBoundary(polygon);
		ic.generateIndexes(files.toArray(new File[files.size()]), new ConsoleProgressImplementation(1), null, MapZooms.getDefault(),
				MapRenderingTypesEncoder.getDefault(), log);
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
