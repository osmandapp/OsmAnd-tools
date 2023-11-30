package net.osmand.router;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;

import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;
import net.osmand.PlatformUtil;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexReader.MapIndex;
import net.osmand.binary.BinaryMapIndexReader.MapRoot;
import net.osmand.data.QuadRect;
import net.osmand.map.OsmandRegions;
import net.osmand.obf.preparation.AbstractIndexPartCreator;
import net.osmand.obf.preparation.BinaryMapIndexWriter;
import net.osmand.obf.preparation.IndexVectorMapCreator;
import net.osmand.router.HHRoutingPreparationDB.NetworkDBPointPrep;
import net.osmand.util.MapUtils;
import rtree.Element;
import rtree.IllegalValueException;
import rtree.LeafElement;
import rtree.RTree;
import rtree.RTreeException;
import rtree.RTreeInsertException;
import rtree.Rect;

public class HHRoutingOBFWriter {
	final static Log LOG = PlatformUtil.getLog(HHRoutingOBFWriter.class);
	private static final int BLOCK_SEGMENTS_SIZE = 24;
	
	public static void main(String[] args) throws IOException, SQLException, IllegalValueException {
		File dbFile;
		File obfPolyFile = null;
		String subFolder = "";
		if (args.length == 0) {
			String mapName = "Germany_car.chdb";
			mapName = "Netherlands_europe_car.chdb";
			mapName = "__europe_car.chdb";
//			mapName = "_road_car.chdb";
			String polyFile = "Netherlands_europe_2.road.obf";
			polyFile = "_split";
			dbFile = new File(System.getProperty("maps.dir"), mapName);
			obfPolyFile = new File(System.getProperty("maps.dir"), polyFile);
		} else {
			if (args.length > 0) {
				obfPolyFile = new File(args[1]);
			}
			if (args.length > 1) {
				subFolder = "/" + args[2];
			}
			dbFile = new File(args[0]);
		}
		new HHRoutingOBFWriter().writeFile(dbFile, obfPolyFile, subFolder);
	}
	
	public void writeFile(File dbFile, File obfPolyFileIn, String subFolder) throws IOException, SQLException, IllegalValueException {
		
		long edition = dbFile.lastModified(); // System.currentTimeMillis();
		HHRoutingPreparationDB db = new HHRoutingPreparationDB(dbFile);
		TLongObjectHashMap<NetworkDBPointPrep> points = db.loadNetworkPoints((short)0, NetworkDBPointPrep.class);
		if (obfPolyFileIn == null) {
			File outFile = new File(dbFile.getParentFile(),
					dbFile.getName().substring(0, dbFile.getName().lastIndexOf('.')) + ".obf");
			writeFileBbox(db, points, outFile, edition, null, null);
		} else {
			OsmandRegions or = new OsmandRegions();
			or.prepareFile();
			or.cacheAllCountries();
			List<File> obfPolyFiles = new ArrayList<>();
			if (obfPolyFileIn.isDirectory()) {
				for (File o : obfPolyFileIn.listFiles()) {
					if (o.getName().endsWith(".road.obf") || o.getName().endsWith("_2.obf")) {
						obfPolyFiles.add(o);
					}
				}
			} else {
				obfPolyFiles.add(obfPolyFileIn);
			}
			Map<String, List<NetworkDBPointPrep>> pointsByDownloadName = new LinkedHashMap<String, List<NetworkDBPointPrep>>();
			int index = 0;
			System.out.println("Indexing points...");
			for (NetworkDBPointPrep p : points.valueCollection()) {
				List<BinaryMapDataObject> l = or.query(p.midX(), p.midY());
				if (++index % 100000 == 0) {
					System.out.printf("Indexed %d of %d - %s \n", index, points.valueCollection().size(), new Date());
				}
				for (BinaryMapDataObject b : l) {
					if (OsmandRegions.contain(b, p.midX(), p.midY())) {
						String dw = or.getDownloadName(b);
						if (!pointsByDownloadName.containsKey(dw)) {
							pointsByDownloadName.put(dw, new ArrayList<NetworkDBPointPrep>());
						}
						pointsByDownloadName.get(dw).add(p);
					}
				}
			}
			for (File obfPolyFile : obfPolyFiles) {
				File outFile = new File(obfPolyFile.getParentFile() + subFolder,
						obfPolyFile.getName().substring(0, obfPolyFile.getName().lastIndexOf('.')) + ".hh.obf");
				outFile.getParentFile().mkdirs();
				QuadRect bbox31 = new QuadRect();
				List<NetworkDBPointPrep> filteredPoints = null;
				String countryName = obfPolyFile.getName().substring(0, obfPolyFile.getName().lastIndexOf('_'))
						.toLowerCase();
				
				if (or.getRegionDataByDownloadName(countryName) != null) {
					filteredPoints = pointsByDownloadName.get(countryName);
					if(filteredPoints == null) {
						System.out.printf("Skip %s as it has no points\n", countryName);
						continue;
					}
					System.out.printf("Use native boundary %s - %d\n", countryName, filteredPoints.size());
				} else {
					BinaryMapIndexReader reader = new BinaryMapIndexReader(new RandomAccessFile(obfPolyFile, "r"),
							obfPolyFile);
					// use map index as it more reasonable size
					// for (RouteRegion r : reader.getRoutingIndexes()) {
					// for (RouteSubregion subregion : r.getSubregions()) {
					// bbox31.expand(subregion.left, subregion.top, subregion.right,
					// subregion.bottom);
					// }
					// }
					for (MapIndex mi : reader.getMapIndexes()) {
						for (MapRoot rt : mi.getRoots()) {
							// use first
							bbox31.expand(rt.getLeft(), rt.getTop(), rt.getRight(), rt.getBottom());
							break;
						}
					}
					reader.close();
					System.out.printf("Using polygon for %s %.5f %.5f - %.5f %.5f\n", outFile.getName(),
							MapUtils.get31LongitudeX((int) bbox31.left), MapUtils.get31LatitudeY((int) bbox31.top),
							MapUtils.get31LongitudeX((int) bbox31.right), MapUtils.get31LatitudeY((int) bbox31.bottom));
				}
				writeFileBbox(db, points, outFile, edition, bbox31, filteredPoints);
			}
		}
	}

	private void writeFileBbox(HHRoutingPreparationDB db, TLongObjectHashMap<NetworkDBPointPrep> points, File outFile,
			long edition, QuadRect bbox31, List<NetworkDBPointPrep> filteredPoints)
			throws SQLException, IOException, IllegalValueException {
		String rTreeFile = outFile.getAbsolutePath() + ".rtree";
		String rpTreeFile = outFile.getAbsolutePath() + ".rptree";
		try {
			String profile = db.getRoutingProfile();
			TIntObjectHashMap<String> routingProfiles = db.getRoutingProfiles();
			int pInd = 0;
			String[] profileParams = new String[routingProfiles.size()];
			int[] profileParamsKeys = new int[routingProfiles.size()];
			for (int p : routingProfiles.keys()) {
				profileParamsKeys[pInd] = p;
				profileParams[pInd] = routingProfiles.get(p);
				pInd++;
			}
			BinaryMapIndexWriter bmiw = new BinaryMapIndexWriter(new RandomAccessFile(outFile, "rw"), edition);
			bmiw.startHHRoutingIndex(edition, profile, profileParams);
			RTree routeTree = new RTree(rTreeFile);
			
			for (NetworkDBPointPrep p : points.valueCollection()) {
				p.mapId = 0;
				p.fileId = 0;
			}
			boolean useBbox = !bbox31.hasInitialState() || filteredPoints != null;
			if (filteredPoints != null) {
				for (NetworkDBPointPrep pnt : filteredPoints) {
					pnt.mapId = 1;
					routeTree.insert(new LeafElement(new Rect(pnt.midX(), pnt.midY(), pnt.midX(), pnt.midY()), pnt.index));
				}
			} else {
				for (NetworkDBPointPrep pnt : points.valueCollection()) {
					boolean contains = !useBbox;
					if (!contains && !bbox31.hasInitialState()) {
						contains = bbox31.contains(pnt.midX(), pnt.midY(), pnt.midX(), pnt.midY());
					}
					if (contains) {
						pnt.mapId = 1;
						routeTree.insert(new LeafElement(new Rect(pnt.midX(), pnt.midY(), pnt.midX(), pnt.midY()), pnt.index));
					}
				}
			}
			if (useBbox) {
				TLongHashSet clusters = new TLongHashSet();
//				TLongHashSet clustersOut = new TLongHashSet();
				for (NetworkDBPointPrep pnt : points.valueCollection()) {
					if (pnt.mapId > 0) {
						clusters.add(pnt.clusterId);
//						clustersOut.add(pnt.dualPoint.clusterId);
					}
				}

				int pointsInc = 0, partial = 0, completeInc = 0;
				for (NetworkDBPointPrep pnt : points.valueCollection()) {
					pointsInc++;
					if (pnt.mapId == 0) {
						if (clusters.contains(pnt.clusterId) || clusters.contains(pnt.dualPoint.clusterId)) {
							pnt.mapId = 2;
							partial++;
							routeTree.insert(new LeafElement(new Rect(pnt.midX(), pnt.midY(), pnt.midX(), pnt.midY()), pnt.index));
						}
					} else {
						completeInc++;
					}
				}
				System.out.printf("Total points %d: included %d (complete clusters), %d (partial clusters) \n",
						pointsInc, completeInc, partial);

			}
			routeTree = AbstractIndexPartCreator.packRtreeFile(routeTree, rTreeFile, rpTreeFile);
			
			long rootIndex = routeTree.getFileHdr().getRootIndex();
			rtree.Node root = routeTree.getReadNode(rootIndex);
			Rect rootBounds = IndexVectorMapCreator.calcBounds(root);
			if (rootBounds != null) {
				List<NetworkDBPointPrep> pntsList = writeBinaryRouteTree(root, rootBounds, routeTree, bmiw, points, new int[] {0});
				pntsList.sort(new Comparator<NetworkDBPointPrep>() {
					@Override
					public int compare(NetworkDBPointPrep o1, NetworkDBPointPrep o2) {
						return Integer.compare(o1.fileId, o2.fileId);
					}
				});
				for (int i = 0; i < profileParamsKeys.length; i++) {
					writeSegments(db, i, profileParamsKeys[i], bmiw, pntsList, 0);
				}
			}
			bmiw.endHHRoutingIndex();
			bmiw.close();

			RandomAccessFile file = routeTree.getFileHdr().getFile();
			file.close();
		} catch (RTreeException | RTreeInsertException e) {
			throw new IOException(e);
		} finally {
			new File(rTreeFile).delete();
			new File(rpTreeFile).delete();
		}
		RTree.clearCache();
	}


	private void writeSegments(HHRoutingPreparationDB db, int profile, int dbProfile, BinaryMapIndexWriter writer, 
			List<NetworkDBPointPrep> pntsList, int shift) throws IOException, SQLException {
		writer.startHHRouteBlockSegments(shift, pntsList.size(), profile);
		if (pntsList.size() > BLOCK_SEGMENTS_SIZE) {
			int range = (pntsList.size() - 1) / BLOCK_SEGMENTS_SIZE + 1;
			for (int i = 0; i < pntsList.size(); i += range) {
				int start = i;
				int end = Math.min(i + range, pntsList.size());
				writeSegments(db, profile, dbProfile, writer, pntsList.subList(start, end), shift + start);
			}
		} else {
			for (NetworkDBPointPrep ind : pntsList) {
				if (shift != ind.fileId) {
					throw new IllegalStateException(shift + " != " + ind.fileId);
				}
				byte[][] res = new byte[2][];
				db.loadSegmentPointInternal(ind.index, dbProfile, res);
				writer.writePointSegments(res[0], res[1]);
				shift++;
			}
		}
		writer.endHHRouteBlockSegments();
	}

	private List<NetworkDBPointPrep> writeBinaryRouteTree(rtree.Node parent, Rect re, RTree r, BinaryMapIndexWriter writer,
			TLongObjectHashMap<NetworkDBPointPrep> points, int[] pntId)
			throws IOException, RTreeException {
		Element[] es = parent.getAllElements();
		writer.startHHRouteTreeElement(re.getMinX(), re.getMaxX(), re.getMinY(), re.getMaxY());
		List<NetworkDBPointPrep> l = new ArrayList<>();
		boolean leaf = false;
		for (int i = 0; i < parent.getTotalElements(); i++) {
			Element e = es[i];
			if (e.getElementType() != rtree.Node.LEAF_NODE) {
				if (leaf) {
					throw new IllegalStateException();
				}
				rtree.Node chNode = r.getReadNode(e.getPtr());
				List<NetworkDBPointPrep> ps = writeBinaryRouteTree(chNode, e.getRect(), r, writer, points, pntId);
				l.addAll(ps);
			} else {
				leaf = true;
				NetworkDBPointPrep pnt = points.get(e.getPtr());
				pnt.fileId = pntId[0]++;
				l.add(pnt);
			}
		}
		if (l.size() > 0 && leaf) {
			writer.writeHHRoutePoints(l);
		}
		writer.endRouteTreeElement();
		return l;
	}
		

}