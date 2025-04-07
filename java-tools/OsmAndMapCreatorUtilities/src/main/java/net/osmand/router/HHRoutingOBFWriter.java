package net.osmand.router;


import static net.osmand.router.HHRoutingUtilities.logf;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.procedure.TLongObjectProcedure;
import gnu.trove.procedure.TLongProcedure;
import gnu.trove.set.hash.TLongHashSet;
import net.osmand.PlatformUtil;
import net.osmand.binary.BinaryHHRouteReaderAdapter.HHRouteRegion;
import net.osmand.binary.BinaryIndexPart;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexReader.MapIndex;
import net.osmand.binary.BinaryMapIndexReader.MapRoot;
import net.osmand.binary.BinaryMapIndexReader.TagValuePair;
import net.osmand.data.QuadRect;
import net.osmand.map.OsmandRegions;
import net.osmand.obf.BinaryInspector;
import net.osmand.obf.preparation.AbstractIndexPartCreator;
import net.osmand.obf.preparation.BinaryMapIndexWriter;
import net.osmand.obf.preparation.IndexVectorMapCreator;
import net.osmand.router.HHRouteDataStructure.NetworkDBPoint;
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
	protected static boolean PREINDEX_POINTS_BY_COUNTRIES = true;
	protected static boolean WRITE_TAG_VALUES = true;
	protected static int THREAD_POOL = 1;
	protected static boolean VALIDATE_CLUSTER_SIZE = false;
	
	
	private static final String IGNORE_ROUTE = "route_";
	private static final String IGNORE_ROAD = "road_";
	private static final String IGNORE_OSMAND_ELE = "osmand_ele_";
	private static final String IGNORE_TURN_LANES = "turn:lanes";
	private static final String IGNORE_DESCRIPTION = ":description";
	private static final String IGNORE_NOTE = ":note";
	
	TLongObjectHashMap<NetworkDBPoint> points ;
	private HHRoutingPreparationDB db;
	private long edition;
	private File dbFile;
	private String profile;
	private String[] profileParams;
	private int[] dbProfileParamsKeys;
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
//			THREAD_POOL = 4;
			updateExistingFiles = true;
			String mapName = "Germany_car.chdb";
//			mapName = "Netherlands_europe_car.chdb";
//			mapName = "Montenegro_europe_2.road.obf_car.chdb";
//			mapName = "__europe_car.chdb";
//			mapName = "1/hh-routing_car.chdb";
			String polyFile = null;
//			polyFile = "1/Spain_europe_2.road.obf";
//			PREINDEX_POINTS_BY_COUNTRIES = false;
			polyFile = "_split";
//			updateExistingFiles = true;
			obfPolyFile = new File(System.getProperty("maps.dir"), polyFile);
			dbFile = new File(System.getProperty("maps.dir"), mapName);
		} else {
			for (String arg : args) {
				if (arg.startsWith("--db=")) {
					dbFile = new File(arg.substring("--db=".length()));
				} else if (arg.startsWith("--threads=")) {
					THREAD_POOL = Integer.parseInt(arg.substring("--threads=".length()));
				} else if (arg.startsWith("--outfolder=")) {
					outFolder = new File(arg.substring("--outfolder=".length()));
				} else if (arg.startsWith("--update-existing-files")) {
					updateExistingFiles = true;
				} else if (arg.startsWith("--obf=")) {
					obfPolyFile = new File(arg.substring("--obf=".length()));
				}
			}
		}
		new HHRoutingOBFWriter(dbFile).writeFile(obfPolyFile, outFolder, updateExistingFiles);
	}

	
	public HHRoutingOBFWriter(File dbFile) throws SQLException {
		this.dbFile = dbFile;
		this.edition = dbFile.lastModified(); // System.currentTimeMillis();
		this.db = new HHRoutingPreparationDB(dbFile);
		this.points = db.loadNetworkPoints((short) 0, NetworkDBPoint.class);
		profile = db.getRoutingProfile();
		TIntObjectHashMap<String> routingProfiles = db.getRoutingProfiles();
		int pInd = 0;
		profileParams = new String[routingProfiles.size()];
		dbProfileParamsKeys = new int[routingProfiles.size()];
		for (int p : routingProfiles.keys()) {
			dbProfileParamsKeys[pInd] = p;
			profileParams[pInd] = routingProfiles.get(p);
			pInd++;
		}
	}
	
	public void writeFile(File obfPolyFileIn, File outFolder, boolean updateExistingFiles) throws IOException, SQLException, IllegalValueException {
		if (THREAD_POOL > 1) {
			System.err.println("Threads > 1 are not supported cause of current R-Tree limitations ");
			THREAD_POOL = 1;
		}
		if (obfPolyFileIn == null) {
			File outFile = new File(dbFile.getParentFile(),
					dbFile.getName().substring(0, dbFile.getName().lastIndexOf('.')) + ".obf");
			if (outFile.exists()) {
				outFile.delete();
			}
			TLongObjectHashMap<NetworkDBPointWrite> pnts = convertPoints(points);
			writeObfFileByBbox(toList(pnts), pnts, outFile, new QuadRect(), null);
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
			Map<String, TLongArrayList> pointsByDownloadName = new LinkedHashMap<>();
			int index = 0;
			System.out.printf("Indexing points %d...\n", points.size());

			if (PREINDEX_POINTS_BY_COUNTRIES) {
				for (NetworkDBPoint p : points.valueCollection()) {
					List<BinaryMapDataObject> lst = or.query(p.midX(), p.midY());
					if (++index % 100000 == 0) {
						System.out.printf("Indexed %d of %d - %s \n", index, points.size(), new Date());
					}
					for (BinaryMapDataObject b : lst) {
						if (OsmandRegions.contain(b, p.midX(), p.midY())) {
							String dw = or.getDownloadName(b);
							if (!pointsByDownloadName.containsKey(dw)) {
								pointsByDownloadName.put(dw, new TLongArrayList());
							}
							pointsByDownloadName.get(dw).add(p.index);
						}
					}
				}
			} else {
				for (File obfPolyFile : obfPolyFiles) {
					String countryName = getCountryName(obfPolyFile);
					LinkedList<BinaryMapDataObject> boundaries = downloadNames.get(countryName);
					TLongArrayList lst = new TLongArrayList();
					for (NetworkDBPoint p : points.valueCollection()) {
						if (++index % 100000 == 0) {
							System.out.printf("Indexed %d of %d - %s \n", index, points.size(), new Date());
						}
						if (boundaries != null) {
							for (BinaryMapDataObject b : boundaries) {
								if (OsmandRegions.contain(b, p.midX(), p.midY())) {
									lst.add(p.index);
									break;
								}
							}
						}
					}
					pointsByDownloadName.put(countryName, lst);

				}
			}
			ExecutorService service = Executors.newFixedThreadPool(THREAD_POOL);
			List<Future<String>> results = new ArrayList<>();
			TLongObjectHashMap<NetworkDBPointWrite> wPoints = null;
			for (File obfPolyFile : obfPolyFiles) {
				File outFile = new File(outFolder,
						obfPolyFile.getName().substring(0, obfPolyFile.getName().lastIndexOf('.')) + ".hh.obf");
				if (updateExistingFiles) {
					outFile = obfPolyFile;
				}
				outFile.getParentFile().mkdirs();
				QuadRect bbox31 = new QuadRect();
				TLongArrayList filteredPoints = null;
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
//							break;
						}
					}
					reader.close();
					System.out.printf("Using polygon for %s %.5f %.5f - %.5f %.5f\n", outFile.getName(),
							MapUtils.get31LongitudeX((int) bbox31.left), MapUtils.get31LatitudeY((int) bbox31.top),
							MapUtils.get31LongitudeX((int) bbox31.right), MapUtils.get31LatitudeY((int) bbox31.bottom));
				}
				if (THREAD_POOL > 1) {
					AugmentObfTask task = new AugmentObfTask(this, outFile, bbox31, filteredPoints);
					results.add(service.submit(task));
				} else {
					if (wPoints == null) {
						wPoints = convertPoints(points);
					}
					String log = writeObfFileByBbox(toList(wPoints), wPoints, outFile, bbox31, filteredPoints);
					HHRoutingUtilities.logf(log.trim());
				}
			}
			
			service.shutdown();
			try {
				while (!results.isEmpty()) {
					Thread.sleep(3000);
					Iterator<Future<String>> it = results.iterator();
					while (it.hasNext()) {
						Future<String> future = it.next();
						if (future.isDone()) {
							String res = future.get();
							HHRoutingUtilities.logf(res.trim());
							it.remove();
						}
					}
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			} catch (ExecutionException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			} finally {
				List<Runnable> runnable = service.shutdownNow();
				if (!results.isEmpty()) {
					logf("!!! %d runnable were not executed: exception occurred",
							runnable == null ? 0 : runnable.size());
				}
				try {
					service.awaitTermination(5, TimeUnit.MINUTES);
				} catch (InterruptedException e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				}
			}

		}
	}

	private static TLongObjectHashMap<NetworkDBPointWrite> convertPoints(TLongObjectHashMap<NetworkDBPoint> dbPoints) {
		TLongObjectHashMap<NetworkDBPointWrite> points = new TLongObjectHashMap<>();
		dbPoints.forEachEntry(new TLongObjectProcedure<NetworkDBPoint>() {
			@Override
			public boolean execute(long a, NetworkDBPoint b) {
				points.put(a, new NetworkDBPointWrite(b));
				return true;
			}
		});
		return points;
	}
	
	public static List<NetworkDBPointWrite> toList(TLongObjectHashMap<NetworkDBPointWrite> ctxPoints) {
		final List<NetworkDBPointWrite> lst = new ArrayList<>(ctxPoints.size());
		ctxPoints.forEachEntry(new TLongObjectProcedure<NetworkDBPointWrite>() {
			@Override
			public boolean execute(long a, NetworkDBPointWrite b) {
				lst.add(b);
				return true;
			}
		});
		return lst;
	}

	private String getCountryName(File obfPolyFile) {
		return obfPolyFile.getName().substring(0, obfPolyFile.getName().lastIndexOf('_'))
				.toLowerCase();
	}
	
	private List<String> prepareTagValuesDictionary(List<NetworkDBPointWrite> points) {
		Map<String, Integer> tagDict = new HashMap<String, Integer>();
		for (NetworkDBPointWrite p : points) {
			if (p.pnt.tagValues == null || p.includeFlag == 0) {
				continue;
			}
			for (TagValuePair entry : p.pnt.tagValues) {
				String key = entry.tag;
				if (key.startsWith(IGNORE_ROUTE) || key.startsWith(IGNORE_TURN_LANES)
						|| key.startsWith(IGNORE_ROAD) || key.startsWith(IGNORE_OSMAND_ELE)
						|| key.contains(IGNORE_NOTE) || key.contains(IGNORE_DESCRIPTION) ) {
					continue;
				}
				String keyValue = entry.tag + "=" + entry.value;
				Integer n = tagDict.get(keyValue);
				if (n == null) {
					n = 0;
				}
				tagDict.put(keyValue, n + 1);
			}
		}
		List<String> tagDictList = new ArrayList<>(tagDict.keySet());
		Collections.sort(tagDictList, new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				return -Integer.compare(tagDict.get(o1), tagDict.get(o2));
			}
		});
		Map<String, Integer> finalTagDict = new HashMap<String, Integer>();
		for (int i = 0; i < tagDictList.size(); i++) {
			finalTagDict.put(tagDictList.get(i), i);
//			System.out.println(i + ". " + tagDictList.get(i) + " " + tagDict.get(tagDictList.get(i)));
		}
		for (NetworkDBPointWrite p : points) {
			if (p.pnt.tagValues != null && p.pnt.tagValues.size() > 0) {
				TIntArrayList lst = new TIntArrayList();
				for (TagValuePair entry : p.pnt.tagValues) {
					String keyValue = entry.tag + "=" + entry.value;
					Integer ind = finalTagDict.get(keyValue);
					if (ind != null) {
						lst.add(ind);
					}
				}
				p.tagValuesInts = lst.toArray();
			}
		}
		return tagDictList;
	}
	
	
	public static class NetworkDBPointWrite {
		public NetworkDBPoint pnt;
		public int[] tagValuesInts;
		public int includeFlag; // 0 - exclude point, 1 - include with complete cluster, 2 - incomplete 
		public int localId; // id in specific file
		
		public NetworkDBPointWrite(NetworkDBPoint pnt) {
			this.pnt = pnt;
		}
	}
	

	private String writeObfFileByBbox(List<NetworkDBPointWrite> points, TLongObjectHashMap<NetworkDBPointWrite> pntsMap, File outFile, QuadRect bbox31, TLongArrayList filteredPoints)
			throws SQLException, IOException, IllegalValueException {
		StringBuilder log = new StringBuilder();
		String rTreeFile = outFile.getAbsolutePath() + ".rtree";
		String rpTreeFile = outFile.getAbsolutePath() + ".rptree";
		try {
			
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
					log.append(String.format("Skip file %s as same hh routing profile (%s) already exist", outFile.getName(),
							new Date(edition)));
					// regenerate in case there is an issue with writer itself
					return log.toString();
				}
				log.append((profileEdition > 0 ? "Replace" : "Augment") +" file with hh routing: " + outFile.getName()).append("\n");
				writeFile = new File(outFile.getParentFile(), outFile.getName() + ".tmp");
			}
			// clear up to re use
			for (NetworkDBPointWrite p : points) {
				p.includeFlag = 0;
				p.localId = 0;
				p.tagValuesInts = null;
			}
			
			ValidateClusterSizeStructure vc = null;
			if (VALIDATE_CLUSTER_SIZE) {
				vc = new ValidateClusterSizeStructure(points);
			}
			
			final RTree routeTree = new RTree(rTreeFile);
			String logRes = preparePointsToWrite(routeTree, points, pntsMap, bbox31, filteredPoints);
			log.append(logRes);
			RTree packRTree = AbstractIndexPartCreator.packRtreeFile(routeTree, rTreeFile, rpTreeFile);
			long rootIndex = packRTree.getFileHdr().getRootIndex();
			rtree.Node root = packRTree.getReadNode(rootIndex);
			Rect rootBounds = IndexVectorMapCreator.calcBounds(root);

			List<String> tagValuesDictionary = null;
			if (WRITE_TAG_VALUES) {
				tagValuesDictionary = prepareTagValuesDictionary(points);
			}
			
			/// START WRITING
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
			boolean allowLongSize = false; // worldwide maps - 2 profiles by 8M points
			if (profileParams.length * (filteredPoints != null ? filteredPoints.size() : points.size()) > 8 * 1000
					* 1000 * 2) {
				LOG.info(String.format("!!! Use 64-bit allowLongSize = true (%d params, %d points) !!! File could be used only in Java-version ",
						profileParams.length, filteredPoints.size()));
				allowLongSize = true;
			}
			bmiw.startHHRoutingIndex(edition, profile, tagValuesDictionary, allowLongSize, profileParams);
			if (rootBounds != null) {
				long fp = bmiw.getFilePointer();
				List<NetworkDBPointWrite> pntsList = writeBinaryRouteTree(root, rootBounds, packRTree, bmiw, pntsMap, new int[] {0});
				long size = bmiw.getFilePointer() - fp;
				// validate number of clusters
				if (vc != null) {
					vc.validateClusterSizeMatch(db, pntsList);
				}
				pntsList.sort(new Comparator<NetworkDBPointWrite>() {
					@Override
					public int compare(NetworkDBPointWrite o1, NetworkDBPointWrite o2) {
						return Integer.compare(o1.localId, o2.localId);
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
				fp = bmiw.getFilePointer();
				log.append(String.format("Tree of points %d: ranges - %s, number of subblocks - %s\n", points.size(), ranges, blocks));
				for (int i = 0; i < dbProfileParamsKeys.length; i++) {
					writeSegments(db, i, dbProfileParamsKeys[i], bmiw, pntsList, ranges, 0);
				}
				long size2 = bmiw.getFilePointer() - fp;
				log.append(String.format("Points size %d bytes, segments size %d bytes \n", size, size2));
			}
			bmiw.endHHRoutingIndex();
			bmiw.close();

			RandomAccessFile file = packRTree.getFileHdr().getFile();
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
		return log.toString();
	}


	private String preparePointsToWrite(final RTree routeTree, List<NetworkDBPointWrite> points, TLongObjectHashMap<NetworkDBPointWrite> pntsMap,
			QuadRect bbox31, TLongArrayList filteredPoints)
			throws RTreeInsertException, IllegalValueException {
		StringBuilder log = new StringBuilder();
		if (filteredPoints != null) {
			filteredPoints.forEach(new TLongProcedure() {
				@Override
				public boolean execute(long value) {
					NetworkDBPointWrite p = pntsMap.get(value);
					p.includeFlag = 1;
					try {
						routeTree.insert(new LeafElement(new Rect(p.pnt.midX(), p.pnt.midY(), p.pnt.midX(), p.pnt.midY()), p.pnt.index));
					} catch (Exception e) {
						throw new RuntimeException(e); 
					}
					return true;
				}
			});
		} else {
			boolean initialState = bbox31.hasInitialState();
			for (NetworkDBPointWrite p : points) {
				if (initialState || bbox31.contains(p.pnt.midX(), p.pnt.midY(), p.pnt.midX(), p.pnt.midY())) {
					p.includeFlag = 1;
					routeTree.insert(new LeafElement(new Rect(p.pnt.midX(), p.pnt.midY(), p.pnt.midX(), p.pnt.midY()), p.pnt.index));
				}
			}
		}
		String str = addIncompletePointsToFormClusters("Prepare ", points, routeTree);
		log.append(str);
		// Expand points for 1 more cluster: here we could expand points (or delete) to 1 more cluster to make maps "bigger"
		for (NetworkDBPointWrite pnt : points) {
			if (pnt.includeFlag == 2) {
				pnt.includeFlag = 1;
			}
			//				if (// pnt.dualPoint.mapId == 1 && 
			//				pnt.index % 5 == 2) {
			//			pnt.mapId = -1;
			//		}

		}
		str = addIncompletePointsToFormClusters("Final ", points, routeTree);
		log.append(str);
		return log.toString();
	}

	private String addIncompletePointsToFormClusters(String msg, Collection<NetworkDBPointWrite> points, RTree routeTree)
			throws RTreeInsertException, IllegalValueException {
		// IMPORTANT: same(pnt.clusterId) - forms a shape where segments look outward the shape
		TLongHashSet clusterDualPointsForInNeeded = new TLongHashSet();
		TLongHashSet clusterPointsForOutNeeded = new TLongHashSet();
		for (NetworkDBPointWrite pnt : points) {
			if (pnt.includeFlag > 0) {
				clusterPointsForOutNeeded.add(pnt.pnt.dualPoint.clusterId);
				clusterDualPointsForInNeeded.add(pnt.pnt.clusterId);
			}
		}
		int pointsInc = 0, partial = 0, completeInc = 0;
		for (NetworkDBPointWrite p : points) {
			pointsInc++;
			if (p.includeFlag <= 0) {
				if (clusterPointsForOutNeeded.contains(p.pnt.dualPoint.clusterId) || 
						clusterDualPointsForInNeeded.contains(p.pnt.clusterId)) {
					partial++;
					if (p.includeFlag == 0) {
						routeTree.insert(
								new LeafElement(new Rect(p.pnt.midX(), p.pnt.midY(), p.pnt.midX(), p.pnt.midY()), p.pnt.index));
					}
					p.includeFlag = 2;
				}
			} else {
				completeInc++;
			}
		}
		return String.format("%s - total points %d: included %d (complete clusters), %d (partial clusters) \n", msg, pointsInc,
				completeInc, partial);
	}

	


	private void writeSegments(HHRoutingPreparationDB db, int profile, int dbProfile, BinaryMapIndexWriter writer, 
			List<NetworkDBPointWrite> pntsList, List<Integer> ranges, int shift) throws IOException, SQLException {
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
			for (NetworkDBPointWrite p : pntsList) {
				if (shift != p.localId) {
					throw new IllegalStateException(shift + " != " + p.localId);
				}
				byte[][] res = new byte[2][];
				if (p.includeFlag <= 1) {
					// don't write mapId=2 point segments as their clusters anyway incomplete
					db.loadSegmentPointInternalSync(p.pnt.index, dbProfile, res);
				} else {
					res[1] = res[0] = new byte[0];
				}
				writer.writePointSegments(res[0], res[1]);
				shift++;
			}
		}
		writer.endHHRouteBlockSegments();
	}

	private List<NetworkDBPointWrite> writeBinaryRouteTree(rtree.Node parent, Rect re, RTree r, BinaryMapIndexWriter writer,
			TLongObjectHashMap<NetworkDBPointWrite> points, int[] pntId)
			throws IOException, RTreeException {
		Element[] es = parent.getAllElements();
		writer.startHHRouteTreeElement(re.getMinX(), re.getMaxX(), re.getMinY(), re.getMaxY());
		List<NetworkDBPointWrite> l = new ArrayList<>();
		boolean leaf = false;
		for (int i = 0; i < parent.getTotalElements(); i++) {
			Element e = es[i];
			if (e.getElementType() != rtree.Node.LEAF_NODE) {
				if (leaf) {
					throw new IllegalStateException();
				}
				rtree.Node chNode = r.getReadNode(e.getPtr());
				List<NetworkDBPointWrite> ps = writeBinaryRouteTree(chNode, e.getRect(), r, writer, points, pntId);
				l.addAll(ps);
			} else {
				leaf = true;
				NetworkDBPointWrite pnt = points.get(e.getPtr());
				if (pnt.includeFlag > 0) {
					pnt.localId = pntId[0]++;
					l.add(pnt);
				} else {
					System.out.println("Deleted point " + pnt.pnt);
				}
			}
		}
		if (l.size() > 0 && leaf) {
			writer.writeHHRoutePoints(l);
		}
		writer.endRouteTreeElement();
		return l;
	}
	
	private static class AugmentObfTask implements Callable<String> {
		private static ThreadLocal<TLongObjectHashMap<NetworkDBPointWrite>> context = new ThreadLocal<>();
		private static ThreadLocal<List<NetworkDBPointWrite>> contextList = new ThreadLocal<>();
		private File outFile;
		private QuadRect bbox31;
		private TLongArrayList filteredPoints;
		private HHRoutingOBFWriter writer;

		public AugmentObfTask(HHRoutingOBFWriter writer, File outFile, QuadRect bbox31, TLongArrayList filteredPoints) {
			this.writer = writer;
			this.outFile = outFile;
			this.bbox31 = bbox31;
			this.filteredPoints = filteredPoints;
		}

		@Override
		public String call() throws Exception {
			TLongObjectHashMap<NetworkDBPointWrite> ctxPoints = context.get();
			List<NetworkDBPointWrite> ctxPointsList = contextList.get();
			if (ctxPoints == null || ctxPoints.size() != writer.points.size() || 
					ctxPointsList == null || ctxPointsList.size() != writer.points.size()) {
				ctxPoints = convertPoints(writer.points);
				ctxPointsList = toList(ctxPoints);
				context.set(ctxPoints);
			}
			return writer.writeObfFileByBbox(ctxPointsList, ctxPoints, outFile, bbox31, filteredPoints);
		}
	}


	private static class ValidateClusterSizeStructure {
		TLongObjectHashMap<List<NetworkDBPointWrite>> validateClusterIn = new TLongObjectHashMap<>();
		TLongObjectHashMap<List<NetworkDBPointWrite>> validateClusterOut = new TLongObjectHashMap<>();
		
		public ValidateClusterSizeStructure(List<NetworkDBPointWrite> points) {
			for (NetworkDBPointWrite p : points) {
				if (validateClusterIn.get(p.pnt.clusterId) == null) {
					validateClusterIn.put(p.pnt.clusterId, new ArrayList<>());
				}
				if (validateClusterOut.get(p.pnt.dualPoint.clusterId) == null) {
					validateClusterOut.put(p.pnt.dualPoint.clusterId, new ArrayList<>());
				}
				validateClusterIn.get(p.pnt.clusterId).add(p);
				validateClusterOut.get(p.pnt.dualPoint.clusterId).add(p);
			}
		}

		private void validateClusterSizeMatch(HHRoutingPreparationDB db,List<NetworkDBPointWrite> pntsList) throws SQLException, IOException {
			for (NetworkDBPointWrite p : pntsList) {
				if (p.includeFlag != 1) {
					continue;
				}
				byte[][] res = new byte[2][];
				db.loadSegmentPointInternalSync(p.pnt.index, 0, res);
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
				for (NetworkDBPointWrite l : validateClusterIn.get(p.pnt.clusterId)) {
					if (l.includeFlag > 0) {
						sizeTIn++;
					} else {
						throw new IllegalStateException(String.format("Into %s <- %s is missing", p, l));
					}
				}
				for (NetworkDBPointWrite l : validateClusterOut.get(p.pnt.dualPoint.clusterId)) {
					if (l.includeFlag > 0) {
						sizeTOut++;
					} else {
						throw new IllegalStateException(String.format("From %s -> %s is missing", p, l));
					}
				}
				if (sizeTIn != sizeIn || sizeOut != sizeTOut) {
					throw new IllegalArgumentException(String.format("Point [%d] %d  in %d>=%d out %d>=%d\n ", p.includeFlag, p.pnt.index, sizeIn,
							sizeTIn, sizeOut, sizeTOut));
				}
			}
		}
	}	
		

}