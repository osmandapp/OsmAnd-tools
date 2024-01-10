package net.osmand.router;


import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;

import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;
import net.osmand.PlatformUtil;
import net.osmand.binary.BinaryHHRouteReaderAdapter.HHRouteRegion;
import net.osmand.binary.BinaryIndexPart;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexReader.MapIndex;
import net.osmand.binary.BinaryMapIndexReader.MapRoot;
import net.osmand.data.QuadRect;
import net.osmand.map.OsmandRegions;
import net.osmand.obf.BinaryInspector;
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
	private static final int BLOCK_SEGMENTS_AVG_BLOCKS_SIZE = 10;
	private static final int BLOCK_SEGMENTS_AVG_BUCKET_SIZE = 7;
	public static final int BUFFER_SIZE = 1 << 20;
	public static boolean PREINDEX_POINTS_BY_COUNTRIES = true;
	
	/**
	 * @param args
	 * @throws IOException
	 * @throws SQLException
	 * @throws IllegalValueException
	 */
	public static void main(String[] args) throws IOException, SQLException, IllegalValueException {
		File dbFile = null;
		File obfPolyFile = null;
		File outFolder = null;
		boolean updateExistingFiles = false;
		if (args.length == 0) {
			String mapName = "Germany_car.chdb";
			mapName = "Netherlands_europe_car.chdb";
//			mapName = "__europe_car.chdb";
			mapName = "1/hh-routing_car.chdb";
			String polyFile = "1/Spain_europe_2.road.obf";
			PREINDEX_POINTS_BY_COUNTRIES = false;
//			polyFile = "_split";
			updateExistingFiles = true;
			dbFile = new File(System.getProperty("maps.dir"), mapName);
			obfPolyFile = new File(System.getProperty("maps.dir"), polyFile);
		} else {
			for (String arg : args) {
				if (arg.startsWith("--db=")) {
					dbFile = new File(arg.substring("--db=".length()));
				} else if (arg.startsWith("--outfolder=")) {
					outFolder = new File(arg.substring("--outfolder=".length()));
				} else if (arg.startsWith("--update-existing-files")) {
					updateExistingFiles = true;
				} else if (arg.startsWith("--obf=")) {
					obfPolyFile = new File(arg.substring("--obf=".length()));
				}
			}
		}
		new HHRoutingOBFWriter().writeFile(dbFile, obfPolyFile, outFolder, updateExistingFiles);
	}
	
	public void writeFile(File dbFile, File obfPolyFileIn, File outFolder, boolean updateExistingFiles) throws IOException, SQLException, IllegalValueException {
		long edition = dbFile.lastModified(); // System.currentTimeMillis();
		HHRoutingPreparationDB db = new HHRoutingPreparationDB(dbFile);
		TLongObjectHashMap<NetworkDBPointPrep> points = db.loadNetworkPoints((short) 0, NetworkDBPointPrep.class);
		if (obfPolyFileIn == null) {
			File outFile = new File(dbFile.getParentFile(),
					dbFile.getName().substring(0, dbFile.getName().lastIndexOf('.')) + ".obf");
			if (outFile.exists()) {
				outFile.delete();
			}
			writeFileBbox(db, points, outFile, edition, new QuadRect(), null);
		} else {
			if (outFolder == null) {
				outFolder = obfPolyFileIn.isDirectory() ? obfPolyFileIn : obfPolyFileIn.getParentFile();
			}
			OsmandRegions or = new OsmandRegions();
			or.prepareFile();
			Map<String, LinkedList<BinaryMapDataObject>> downloadNames = or.cacheAllCountries();
			List<File> obfPolyFiles = new ArrayList<>();
			if (obfPolyFileIn.isDirectory()) {
				for (File o : obfPolyFileIn.listFiles()) {
					if (o.getName().toLowerCase().contains("world")) {
						continue;
					}
					if (o.getName().endsWith(".road.obf") || o.getName().endsWith("_2.obf")) {
						obfPolyFiles.add(o);
					}
				}
			} else {
				obfPolyFiles.add(obfPolyFileIn);
			}
			Map<String, List<NetworkDBPointPrep>> pointsByDownloadName = new LinkedHashMap<String, List<NetworkDBPointPrep>>();
			int index = 0;
			System.out.printf("Indexing points %d...\n", points.size());

			if (PREINDEX_POINTS_BY_COUNTRIES) {
				for (NetworkDBPointPrep p : points.valueCollection()) {
					List<BinaryMapDataObject> lst = or.query(p.midX(), p.midY());
					if (++index % 100000 == 0) {
						System.out.printf("Indexed %d of %d - %s \n", index, points.size(), new Date());
					}
					for (BinaryMapDataObject b : lst) {
						if (OsmandRegions.contain(b, p.midX(), p.midY())) {
							String dw = or.getDownloadName(b);
							if (!pointsByDownloadName.containsKey(dw)) {
								pointsByDownloadName.put(dw, new ArrayList<NetworkDBPointPrep>());
							}
							pointsByDownloadName.get(dw).add(p);
						}
					}
				}
			} else {
				for (File obfPolyFile : obfPolyFiles) {
					String countryName = getCountryName(obfPolyFile);
					LinkedList<BinaryMapDataObject> boundaries = downloadNames.get(countryName);
					List<NetworkDBPointPrep> lst = new ArrayList<NetworkDBPointPrep>();
					for (NetworkDBPointPrep p : points.valueCollection()) {
						if (++index % 100000 == 0) {
							System.out.printf("Indexed %d of %d - %s \n", index, points.size(), new Date());
						}
						if (boundaries != null) {
							for (BinaryMapDataObject b : boundaries) {
								if (OsmandRegions.contain(b, p.midX(), p.midY())) {
									lst.add(p);
									break;
								}
							}
						}
					}
					
					pointsByDownloadName.put(countryName, lst);

				}
			}
			for (File obfPolyFile : obfPolyFiles) {
				File outFile = new File(outFolder,
						obfPolyFile.getName().substring(0, obfPolyFile.getName().lastIndexOf('.')) + ".hh.obf");
				if (updateExistingFiles) {
					outFile = obfPolyFile;
				}
				outFile.getParentFile().mkdirs();
				QuadRect bbox31 = new QuadRect();
				List<NetworkDBPointPrep> filteredPoints = null;
				String countryName = getCountryName(obfPolyFile);
				
				if (or.getRegionDataByDownloadName(countryName) != null) {
					filteredPoints = pointsByDownloadName.get(countryName);
					if (filteredPoints == null) {
						System.out.printf("Skip %s as it has no points\n", countryName);
						continue;
					}
					// by default
//					System.out.printf("Use native boundary %s - %d\n", countryName, filteredPoints.size());
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

	private String getCountryName(File obfPolyFile) {
		return obfPolyFile.getName().substring(0, obfPolyFile.getName().lastIndexOf('_'))
				.toLowerCase();
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
			BinaryMapIndexReader reader = null;
			File writeFile = outFile; 
			if (outFile.exists()) {
				reader = new BinaryMapIndexReader(new RandomAccessFile(outFile, "rw"), outFile);
				long profileEdition = -1;
				for (HHRouteRegion h : reader.getHHRoutingIndexes()) {
					if (h.profile.equals(profile)) {
						profileEdition = h.edition;
						break;
					}
				}
				if (edition == profileEdition) {
					System.out.printf("Skip file %s as same hh routing profile (%s) already exist\n", outFile.getName(),
							new Date(edition));
					// regenerate in case there is an issue with writer itself
					return;
				}
				System.out.println((profileEdition > 0 ? "Replace" : "Augment") +" file with hh routing: " + outFile.getName());
				writeFile = new File(outFile.getParentFile(), outFile.getName() + ".tmp");
			}
			long timestamp = reader != null ? reader.getDateCreated() : edition;
			BinaryMapIndexWriter bmiw = new BinaryMapIndexWriter(new RandomAccessFile(writeFile, "rw"), timestamp);
			if (reader != null) {
				byte[] BUFFER_TO_READ = new byte[BUFFER_SIZE];
				for (int i = 0; i < reader.getIndexes().size(); i++) {
					BinaryIndexPart part = reader.getIndexes().get(i);
					if (part instanceof HHRouteRegion && ((HHRouteRegion) part).profile.equals(profile)) {
						// ignore same
						continue;
					}
					bmiw.getCodedOutStream().writeTag(part.getFieldNumber(), WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
					BinaryInspector.writeInt(bmiw.getCodedOutStream(), part.getLength());
					BinaryInspector.copyBinaryPart(bmiw.getCodedOutStream(), BUFFER_TO_READ, reader.getRaf(), part.getFilePointer(), part.getLength());
				}
			}
			bmiw.startHHRoutingIndex(edition, profile, profileParams);
			RTree routeTree = new RTree(rTreeFile);
			
			TLongObjectHashMap<List<NetworkDBPointPrep>> validateClusterIn = new TLongObjectHashMap<>();
			TLongObjectHashMap<List<NetworkDBPointPrep>> validateClusterOut = new TLongObjectHashMap<>();
			for (NetworkDBPointPrep p : points.valueCollection()) {
				p.mapId = 0;
				p.fileId = 0;
				if (validateClusterIn.get(p.clusterId) == null) {
					validateClusterIn.put(p.clusterId, new ArrayList<>());
				}
				if (validateClusterOut.get(p.dualPoint.clusterId) == null) {
					validateClusterOut.put(p.dualPoint.clusterId, new ArrayList<>());
				}
				validateClusterIn.get(p.clusterId).add(p);
				validateClusterOut.get(p.dualPoint.clusterId).add(p);
			}
			
			if (filteredPoints != null) {
				for (NetworkDBPointPrep pnt : filteredPoints) {
					pnt.mapId = 1;
					routeTree.insert(new LeafElement(new Rect(pnt.midX(), pnt.midY(), pnt.midX(), pnt.midY()), pnt.index));
				}
			} else {
				boolean initialState = bbox31.hasInitialState();
				for (NetworkDBPointPrep pnt : points.valueCollection()) {
					if (initialState || bbox31.contains(pnt.midX(), pnt.midY(), pnt.midX(), pnt.midY())) {
						pnt.mapId = 1;
						routeTree.insert(new LeafElement(new Rect(pnt.midX(), pnt.midY(), pnt.midX(), pnt.midY()), pnt.index));
					}
				}
			}
			addIncompletePointsToFormClusters("Prepare ", points, routeTree);
			// Expand points for 1 more cluster: here we could expand points (or delete) to 1 more cluster to make maps "bigger"
			for (NetworkDBPointPrep pnt : points.valueCollection()) {
				if (pnt.mapId == 2) {
					pnt.mapId = 1;
				}
				//				if (// pnt.dualPoint.mapId == 1 && 
				//				pnt.index % 5 == 2) {
				//			pnt.mapId = -1;
				//		}

			}
			addIncompletePointsToFormClusters("Final ", points, routeTree);
				
			routeTree = AbstractIndexPartCreator.packRtreeFile(routeTree, rTreeFile, rpTreeFile);
			
			long rootIndex = routeTree.getFileHdr().getRootIndex();
			rtree.Node root = routeTree.getReadNode(rootIndex);
			Rect rootBounds = IndexVectorMapCreator.calcBounds(root);
			if (rootBounds != null) {
				List<NetworkDBPointPrep> pntsList = writeBinaryRouteTree(root, rootBounds, routeTree, bmiw, points, new int[] {0});
				// validate number of clusters
				validateClusterSizeMatch(db, validateClusterIn, validateClusterOut, pntsList);
				pntsList.sort(new Comparator<NetworkDBPointPrep>() {
					@Override
					public int compare(NetworkDBPointPrep o1, NetworkDBPointPrep o2) {
						return Integer.compare(o1.fileId, o2.fileId);
					}
				});
				List<Integer> blocks = new ArrayList<Integer>();
				int numberOfBlocks = (pntsList.size() - 1) / BLOCK_SEGMENTS_AVG_BUCKET_SIZE + 1;
				while (numberOfBlocks > 1) {
					blocks.add(numberOfBlocks);
					numberOfBlocks = (numberOfBlocks - 1) / BLOCK_SEGMENTS_AVG_BLOCKS_SIZE + 1;
				}
				Collections.reverse(blocks);
				List<Integer> ranges = new ArrayList<Integer>();
				for (int i = 0; i < blocks.size(); i++) {
					ranges.add((pntsList.size() - 1) / blocks.get(i) + 1);
				}
				System.out.printf("Tree of points %d: ranges - %s, number of subblocks - %s\n", points.size(), ranges, blocks);
				for (int i = 0; i < profileParamsKeys.length; i++) {
					writeSegments(db, i, profileParamsKeys[i], bmiw, pntsList, ranges, 0);
				}
			}
			bmiw.endHHRoutingIndex();
			bmiw.close();

			RandomAccessFile file = routeTree.getFileHdr().getFile();
			file.close();
			if (reader != null) {
				reader.close();
				writeFile.renameTo(outFile);
//				outFile.setLastModified(timestamp); // don't update timestamp to use to compare with latest files
			}
		} catch (RTreeException | RTreeInsertException e) {
			throw new IOException(e);
		} finally {
			new File(rTreeFile).delete();
			new File(rpTreeFile).delete();
		}
		RTree.clearCache();
	}

	private void addIncompletePointsToFormClusters(String msg, TLongObjectHashMap<NetworkDBPointPrep> points, RTree routeTree)
			throws RTreeInsertException, IllegalValueException {
		// IMPORTANT: same(pnt.clusterId) - forms a shape where segments look outward the shape
		TLongHashSet clusterDualPointsForInNeeded = new TLongHashSet();
		TLongHashSet clusterPointsForOutNeeded = new TLongHashSet();
		for (NetworkDBPointPrep pnt : points.valueCollection()) {
			if (pnt.mapId > 0) {
				clusterPointsForOutNeeded.add(pnt.dualPoint.clusterId);
				clusterDualPointsForInNeeded.add(pnt.clusterId);
			}
		}
		int pointsInc = 0, partial = 0, completeInc = 0;
		for (NetworkDBPointPrep pnt : points.valueCollection()) {
			pointsInc++;
			if (pnt.mapId <= 0) {
				if (clusterPointsForOutNeeded.contains(pnt.dualPoint.clusterId) || 
						clusterDualPointsForInNeeded.contains(pnt.clusterId)) {
					partial++;
					if (pnt.mapId == 0) {
						routeTree.insert(
								new LeafElement(new Rect(pnt.midX(), pnt.midY(), pnt.midX(), pnt.midY()), pnt.index));
					}
					pnt.mapId = 2;
				}
			} else {
				completeInc++;
			}
		}
		System.out.printf("%s - total points %d: included %d (complete clusters), %d (partial clusters) \n", msg, pointsInc,
				completeInc, partial);
	}

	private void validateClusterSizeMatch(HHRoutingPreparationDB db,
			TLongObjectHashMap<List<NetworkDBPointPrep>> validateClusterIn,
			TLongObjectHashMap<List<NetworkDBPointPrep>> validateClusterOut, List<NetworkDBPointPrep> pntsList)
			throws SQLException, IOException {
		for (NetworkDBPointPrep p : pntsList) {
			if (p.mapId != 1) {
				continue;
			}
			byte[][] res = new byte[2][];
			db.loadSegmentPointInternal(p.index, 0, res);
			int sizeIn = 0, sizeOut = 0;
			ByteArrayInputStream str = new ByteArrayInputStream(res[0]);
			while (str.available() > 0) {
				CodedInputStream.readRawVarint32(str);
				sizeIn++;
			}
			str = new ByteArrayInputStream(res[1]);
			while (str.available() > 0) {
				CodedInputStream.readRawVarint32(str);
				sizeOut++;
			}
			int sizeTIn = 0, sizeTOut = 0;
			for (NetworkDBPointPrep l : validateClusterIn.get(p.clusterId)) {
				if (l.mapId > 0) {
					sizeTIn++;
				} else {
					throw new IllegalStateException(String.format("Into %s <- %s is missing", p, l));
				}
			}
			for (NetworkDBPointPrep l : validateClusterOut.get(p.dualPoint.clusterId)) {
				if (l.mapId > 0) {
					sizeTOut++;
				} else {
					throw new IllegalStateException(String.format("From %s -> %s is missing", p, l));
				}
			}
			if (sizeTIn != sizeIn || sizeOut != sizeTOut) {
				throw new IllegalArgumentException(String.format("Point [%d] %d  in %d>=%d out %d>=%d\n ", p.mapId, p.index, sizeIn,
						sizeTIn, sizeOut, sizeTOut));
			}
		}
	}


	private void writeSegments(HHRoutingPreparationDB db, int profile, int dbProfile, BinaryMapIndexWriter writer, 
			List<NetworkDBPointPrep> pntsList, List<Integer> ranges, int shift) throws IOException, SQLException {
		writer.startHHRouteBlockSegments(shift, pntsList.size(), profile);
		if (ranges.size() > 0) {
			int range = ranges.get(0);
//			System.out.printf(" BLOCK d-%d range-%d totalpoints-%d\n", ranges.size(),  range, pntsList.size());
			for (int i = 0; i < pntsList.size(); i += range) {
				int start = i;
				int end = Math.min(i + range, pntsList.size());
				writeSegments(db, profile, dbProfile, writer, pntsList.subList(start, end), ranges.subList(1, ranges.size()), shift + start);
			}
		} else {
//			System.out.println("   POINTS " +pntsList.size());
			for (NetworkDBPointPrep ind : pntsList) {
				if (shift != ind.fileId) {
					throw new IllegalStateException(shift + " != " + ind.fileId);
				}
				byte[][] res = new byte[2][];
				if (ind.mapId <= 1) {
					// don't write mapId=2 point segments as their clusters anyway incomplete
					db.loadSegmentPointInternal(ind.index, dbProfile, res);
				} else {
					res[1] = res[0] = new byte[0];
				}
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
				if (pnt.mapId > 0) {
					pnt.fileId = pntId[0]++;
					l.add(pnt);
				} else {
					System.out.println("Deleted point " + pnt);
				}
			}
		}
		if (l.size() > 0 && leaf) {
			writer.writeHHRoutePoints(l);
		}
		writer.endRouteTreeElement();
		return l;
	}
		

}