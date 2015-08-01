package net.osmand.data.preparation;

import gnu.trove.TIntCollection;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;
import gnu.trove.set.hash.TLongHashSet;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import net.osmand.IProgress;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.OsmandOdb.IdTable;
import net.osmand.binary.OsmandOdb.OsmAndRoutingIndex.RouteDataBlock;
import net.osmand.binary.OsmandOdb.RestrictionData;
import net.osmand.binary.OsmandOdb.RestrictionData.Builder;
import net.osmand.binary.OsmandOdb.RouteData;
import net.osmand.binary.RouteDataObject;
import net.osmand.data.LatLon;
import net.osmand.data.preparation.BinaryMapIndexWriter.RoutePointToWrite;
import net.osmand.osm.MapRenderingTypes;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.MapRoutingTypes;
import net.osmand.osm.MapRenderingTypesEncoder.EntityConvertApplyType;
import net.osmand.osm.MapRoutingTypes.MapPointName;
import net.osmand.osm.MapRoutingTypes.MapRouteType;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.OSMSettings.OSMTagKey;
import net.osmand.osm.edit.OsmMapUtils;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Way;
import net.osmand.osm.util.CheckRoadConnectivity;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

import org.apache.commons.logging.Log;

import rtree.Element;
import rtree.IllegalValueException;
import rtree.LeafElement;
import rtree.RTree;
import rtree.RTreeException;
import rtree.RTreeInsertException;
import rtree.Rect;

public class IndexRouteCreator extends AbstractIndexPartCreator {
	
	private Connection mapConnection;
	private final Log logMapDataWarn;
	private final static int CLUSTER_ZOOM = 15;
	private final static String CONFLICT_NAME = "#CONFLICT";
	private RTree routeTree = null;
	private RTree baserouteTree = null;
	private MapRoutingTypes routeTypes;
	
	private final static float DOUGLAS_PEUKER_DISTANCE = 15;
	
	
	private TLongObjectHashMap<TLongArrayList> highwayRestrictions = new TLongObjectHashMap<TLongArrayList>();
	private TLongObjectHashMap<Long> basemapRemovedNodes = new TLongObjectHashMap<Long>();
	private TLongObjectHashMap<RouteMissingPoints> basemapNodesToReinsert = new TLongObjectHashMap<RouteMissingPoints> ();

	// local purpose to speed up processing cache allocation
	TIntArrayList outTypes = new TIntArrayList();
	TLongObjectHashMap<TIntArrayList> pointTypes = new TLongObjectHashMap<TIntArrayList>();
	TLongObjectHashMap<TIntObjectHashMap<String> >  pointNames = new TLongObjectHashMap<TIntObjectHashMap<String> > ();
	Map<MapRoutingTypes.MapRouteType, String> names = new HashMap<MapRoutingTypes.MapRouteType, String>();

	private TLongHashSet genSpeedCameras = new TLongHashSet();
	
	TLongObjectHashMap<GeneralizedCluster> generalClusters = new TLongObjectHashMap<GeneralizedCluster>();
	private PreparedStatement mapRouteInsertStat;
	private PreparedStatement basemapRouteInsertStat;
	private Map<EntityId, Map<String, String>> propogatedTags = new LinkedHashMap<Entity.EntityId, Map<String, String>>();
	private MapRenderingTypesEncoder renderingTypes;
	private boolean generateLowLevel;


	private class RouteMissingPoints {
		Map<Integer, Long> pointsMap = new TreeMap<Integer, Long>();
		TIntArrayList[] pointsXToInsert = null;
		TIntArrayList[] pointsYToInsert = null;
		
		void buildPointsToInsert(int targetLength){
			pointsXToInsert = new TIntArrayList[targetLength];
			pointsYToInsert = new TIntArrayList[targetLength];
			for(Map.Entry<Integer, Long> p : pointsMap.entrySet()) {
				int insertAfter = p.getKey() & ((1 << SHIFT_INSERT_AT) -1);
				if(pointsXToInsert[insertAfter] == null ) {
					pointsXToInsert[insertAfter] = new TIntArrayList();
					pointsYToInsert[insertAfter] = new TIntArrayList();
				}
				long x = p.getValue() >> 31;
				long y = p.getValue() - (x << 31);
				pointsXToInsert[insertAfter].add((int) x);
				pointsYToInsert[insertAfter].add((int) y);
			}
		}
	}
	
	public IndexRouteCreator(MapRenderingTypesEncoder renderingTypes, Log logMapDataWarn, boolean generateLowLevel) {
		this.renderingTypes = renderingTypes;
		this.logMapDataWarn = logMapDataWarn;
		this.generateLowLevel = generateLowLevel;
		this.routeTypes = new MapRoutingTypes(renderingTypes);
	}
	public void indexRelations(Entity e, OsmDbAccessorContext ctx) throws SQLException {
		indexHighwayRestrictions(e, ctx);
		if(e instanceof Relation) {
			Map<String, String> tags = renderingTypes.transformTags(e.getTags(), EntityType.RELATION, EntityConvertApplyType.ROUTING);
			Map<String, String> propogated = routeTypes.getRouteRelationPropogatedTags(tags);
			if(propogated != null) {
				ctx.loadEntityRelation((Relation) e);
				for(EntityId id : ((Relation) e).getMembersMap().keySet()) {
					if(!propogatedTags.containsKey(id)) {
						propogatedTags.put(id, new LinkedHashMap<String, String>());
					}
					propogatedTags.get(id).putAll(propogated);
				}
			}
			if("enforcement".equals(tags.get("type")) && "maxspeed".equals(tags.get("enforcement"))) {
				ctx.loadEntityRelation((Relation) e);
				Iterator<Entity> from = ((Relation) e).getMembers("from").iterator();
				// mark as speed cameras
				while(from.hasNext()) {
					Entity n = from.next();
					if(n instanceof Node) {
						genSpeedCameras.add(n.getId());
					}
				}
				
			}
			
		}
	}
	

	public void iterateMainEntity(Entity es, OsmDbAccessorContext ctx) throws SQLException {
		if (es instanceof Way) {
			Way e = (Way) es;
			Map<String, String> tags = renderingTypes.transformTags(e.getTags(), EntityType.WAY, EntityConvertApplyType.ROUTING);
			Map<String, String> ptags = propogatedTags.get(EntityId.valueOf(e));
			if (ptags != null) {
				Iterator<Entry<String, String>> iterator = ptags.entrySet().iterator();
				while (iterator.hasNext()) {
					Entry<String, String> ts = iterator.next();
					if (tags.get(ts.getKey()) == null) {
						tags.put(ts.getKey(), ts.getValue());
					}
				}
			}
			
			boolean encoded = routeTypes.encodeEntity(tags, outTypes, names) 
					&& e.getNodes().size() >= 2;
			if (encoded) {
				// Load point with tags!
				ctx.loadEntityWay(e);
				for (Node n : e.getNodes()) {
					if (n != null && genSpeedCameras.contains(n.getId())) {
						n.putTag("highway", "speed_camera");
					}
				}
				routeTypes.encodePointTypes(e, pointTypes, pointNames, false);
				addWayToIndex(e.getId(), e.getNodes(), mapRouteInsertStat, routeTree, outTypes, pointTypes, pointNames, names);
			}
			if (generateLowLevel) {
				encoded = routeTypes.encodeBaseEntity(tags, outTypes, names) && e.getNodes().size() >= 2;
				if (encoded) {
					List<Node> source = e.getNodes();
					long id = e.getId();
					List<Node> result = simplifyRouteForBaseSection(source, id);
					routeTypes.encodePointTypes(e, pointTypes, pointNames, true);
					addWayToIndex(e.getId(), result, basemapRouteInsertStat, baserouteTree, outTypes, pointTypes,
							pointNames, names);
					// generalizeWay(e);

				}
			}
		}
		
	}
	private List<Node> simplifyRouteForBaseSection(List<Node> source, long id) {
		ArrayList<Node> result = new ArrayList<Node>();
		boolean[] kept = OsmMapUtils.simplifyDouglasPeucker(source, 11 /*zoom*/+ 8 + 1 /*smoothness*/, 3, result, false);
		int indexToInsertAt = 0;
		int originalInd = 0;				
		for(int i = 0; i < kept.length; i ++) {
			Node n = source.get(i);
			if(n != null) {
				long y31 = MapUtils.get31TileNumberY(n.getLatitude());
				long x31 = MapUtils.get31TileNumberX(n.getLongitude());
				long point = (x31 << 31) + y31;
				registerBaseIntersectionPoint(point, !kept[i], id, indexToInsertAt, originalInd);
				originalInd++;
				if(kept[i]) {
					indexToInsertAt ++;
				}
			}
		}
		return result;
	}
	
	private static long SHIFT_INSERT_AT = 12;
	private static long SHIFT_ORIGINAL = 16;
	private static long SHIFT_ID = 64 - (SHIFT_INSERT_AT + SHIFT_ORIGINAL);

	private void registerBaseIntersectionPoint(long pointLoc, boolean register, long wayId, int insertAt, int originalInd) {
		Long exNode = basemapRemovedNodes.get(pointLoc);
		if(insertAt > (1l << SHIFT_INSERT_AT)) {
			throw new IllegalStateException("Way index too big");
		}
		if(originalInd > (1l << SHIFT_ORIGINAL)) {
			throw new IllegalStateException("Way index 2 too big");
		}
		if(wayId > (1l << SHIFT_ID)) {
			throw new IllegalStateException("Way id too big");
		}
		long genKey = register ? ((wayId << (SHIFT_ORIGINAL+SHIFT_INSERT_AT)) + (originalInd << SHIFT_INSERT_AT) + insertAt) : -1l; 
		if(exNode == null) {
			basemapRemovedNodes.put(pointLoc, genKey);
		} else {
			if(exNode != -1) {
				putIntersection(pointLoc, exNode);
			}
			basemapRemovedNodes.put(pointLoc, -1l);
			if(genKey != -1) {
				putIntersection(pointLoc, genKey);
			}
		}
		
	}
	
	private void putIntersection(long point, long wayNodeId) {
		if(wayNodeId != -1){
//			long x = point >> 31;
//			long y = point - (x << 31);
//			System.out.println("Put intersection at " + (float) MapUtils.get31LatitudeY((int) y) + " " + (float)MapUtils.get31LongitudeX((int) x));
			long SHIFT = SHIFT_INSERT_AT + SHIFT_ORIGINAL;
			int ind = (int) (wayNodeId & ((1 << SHIFT) - 1));
			long wayId = wayNodeId >> SHIFT;
			if(!basemapNodesToReinsert.containsKey(wayId)) {
				basemapNodesToReinsert.put(wayId, new RouteMissingPoints());
			}
			RouteMissingPoints mp = basemapNodesToReinsert.get(wayId);
			mp.pointsMap.put(ind, point);
		}
		
	}
	private void addWayToIndex(long id, List<Node> nodes, PreparedStatement insertStat, RTree rTree,
			TIntArrayList outTypes,	TLongObjectHashMap<TIntArrayList> pointTypes,	
			TLongObjectHashMap<TIntObjectHashMap<String>> pointNamesRaw, Map<MapRoutingTypes.MapRouteType, String> names ) throws SQLException {
		boolean init = false;
		int minX = Integer.MAX_VALUE;
		int maxX = 0;
		int minY = Integer.MAX_VALUE;
		int maxY = 0;
		

		ByteArrayOutputStream bcoordinates = new ByteArrayOutputStream();
		ByteArrayOutputStream bpointIds = new ByteArrayOutputStream();
		ByteArrayOutputStream bpointTypes = new ByteArrayOutputStream();
		ByteArrayOutputStream btypes = new ByteArrayOutputStream();
		List<MapPointName> pointNamesEmp = new ArrayList<MapPointName>();
		try {
			for (int j = 0; j < outTypes.size(); j++) {
				Algorithms.writeSmallInt(btypes, outTypes.get(j));
			}
			int pointIndex = 0;

			for (Node n : nodes) {
				if (n != null) {
					// write id
					Algorithms.writeLongInt(bpointIds, n.getId());
					// write point type
					TIntArrayList types = pointTypes.get(n.getId());
					if (types != null) {
						for (int j = 0; j < types.size(); j++) {
							Algorithms.writeSmallInt(bpointTypes, types.get(j));
						}
					}
					TIntObjectHashMap<String> namesP = pointNamesRaw.get(n.getId());
					if (namesP != null) {
						TIntObjectIterator<String> it = namesP.iterator();
						while (it.hasNext()) {
							it.advance();
							MapPointName obj = new MapPointName(it.key(), pointIndex, it.value());
							pointNamesEmp.add(obj);
						}
					}
					Algorithms.writeSmallInt(bpointTypes, 0);
					// write coordinates
					int y = MapUtils.get31TileNumberY(n.getLatitude());
					int x = MapUtils.get31TileNumberX(n.getLongitude());
					minX = Math.min(minX, x);
					maxX = Math.max(maxX, x);
					minY = Math.min(minY, y);
					maxY = Math.max(maxY, y);
					init = true;
					Algorithms.writeInt(bcoordinates, x);
					Algorithms.writeInt(bcoordinates, y);
					pointIndex++;
				}
			}

		} catch (IOException est) {
			throw new IllegalStateException(est);
		}
		if (init) {
			// conn.prepareStatement("insert into route_objects(id, types, pointTypes, pointIds, pointCoordinates, name) values(?, ?, ?, ?, ?, ?, ?)");
			insertStat.setLong(1, id);
			insertStat.setBytes(2, btypes.toByteArray());
			insertStat.setBytes(3, bpointTypes.toByteArray());
			insertStat.setBytes(4, bpointIds.toByteArray());
			insertStat.setBytes(5, bcoordinates.toByteArray());
			insertStat.setString(6, encodeNames(names));
			insertStat.setString(7, encodeListNames(pointNamesEmp));

			addBatch(insertStat, false);
			try {
				rTree.insert(new LeafElement(new Rect(minX, minY, maxX, maxY), id));
			} catch (RTreeInsertException e1) {
				throw new IllegalArgumentException(e1);
			} catch (IllegalValueException e1) {
				throw new IllegalArgumentException(e1);
			}
		}
	}
	
	private static long getBaseId(int x31, int y31) {
		long x = x31;
		long y = y31;
		return (x << 31) + y;
	}
	
	private GeneralizedCluster getCluster(GeneralizedWay gw, int ind, GeneralizedCluster helper) {
		int x31 = gw.px.get(ind);
		int y31 = gw.py.get(ind);
		int xc = x31 >> (31 - CLUSTER_ZOOM);
		int yc = y31 >> (31 - CLUSTER_ZOOM);
		if(helper != null && helper.x == xc  &&
				helper.y == yc) {
			return helper;
		}
		long l = (((long)xc) << (CLUSTER_ZOOM+1)) + yc;
		if(!generalClusters.containsKey(l)) {
			generalClusters.put(l, new GeneralizedCluster(xc, yc, CLUSTER_ZOOM));
		}
		return generalClusters.get(l);
	}
	
	
	public void generalizeWay(Way e) throws SQLException {
		List<Node> ns = e.getNodes();
		
		GeneralizedWay w = new GeneralizedWay(e.getId());
		TIntArrayList px = w.px;
		TIntArrayList py = w.py;
		GeneralizedCluster cluster = null;
		for (Node n : ns) {
			if (n != null) {
				int x31 = MapUtils.get31TileNumberX(n.getLongitude());
				int y31 = MapUtils.get31TileNumberY(n.getLatitude());
				px.add(x31);
				py.add(y31);
			}
		}
		if(w.size() < 2) {
			return;
		}
		for (int i = 0; i < w.size(); i++) {
			GeneralizedCluster ncluster = getCluster(w, i, cluster);
			if (ncluster != cluster) {
				cluster = ncluster;
			}
			ncluster.addWayFromLocation(w, i);
		}
		int mt = getMainType(outTypes); // routeTypes.getTypeByInternalId(mt)
		outTypes.remove(mt);
		w.mainType = mt;
		w.addtypes.addAll(outTypes);
		w.names.putAll(names);
	}

	private static final char SPECIAL_CHAR = ((char) 0x60000);

	protected String encodeNames(Map<MapRouteType, String> tempNames) {
		StringBuilder b = new StringBuilder();
		for (Map.Entry<MapRouteType, String> e : tempNames.entrySet()) {
			if (e.getValue() != null) {
				b.append(SPECIAL_CHAR).append((char)e.getKey().getInternalId()).append(e.getValue());
			}
		}
		return b.toString();
	}
	
	protected String encodeListNames(List<MapPointName> tempNames) {
		StringBuilder b = new StringBuilder();
		for (MapPointName e : tempNames) {
				b.append(SPECIAL_CHAR).append((char)e.nameTypeInternalId).append((char) e.pointIndex).append(e.name);
		}
		return b.toString();
	}
	
	protected void decodeListNames(String name, List<MapPointName> tempNames) {
		int i = name.indexOf(SPECIAL_CHAR);
		while (i != -1) {
			int n = name.indexOf(SPECIAL_CHAR, i + 3);
			int ch = (short) name.charAt(i + 1);
			int index = (short) name.charAt(i + 2);
			String pointName = n == -1 ? name.substring(i + 3) : name.substring(i + 3, n);
			MapPointName pn = new MapPointName(ch, index, pointName);
			pn.nameTypeTargetId = routeTypes.getTypeByInternalId(ch).getTargetId();
			tempNames.add(pn);
			i = n;
		}
	}

	protected void decodeNames(String name, Map<MapRouteType, String> tempNames) {
		int i = name.indexOf(SPECIAL_CHAR);
		while (i != -1) {
			int n = name.indexOf(SPECIAL_CHAR, i + 2);
			int ch = (short) name.charAt(i + 1);
			MapRouteType rt = routeTypes.getTypeByInternalId(ch);
			if (n == -1) {
				tempNames.put(rt, name.substring(i + 2));
			} else {
				tempNames.put(rt, name.substring(i + 2, n));
			}
			i = n;
		}
	}



	public Rect calcBounds(rtree.Node n) {
		Rect r = null;
		Element[] e = n.getAllElements();
		for (int i = 0; i < n.getTotalElements(); i++) {
			Rect re = e[i].getRect();
			if (r == null) {
				try {
					r = new Rect(re.getMinX(), re.getMinY(), re.getMaxX(), re.getMaxY());
				} catch (IllegalValueException ex) {
				}
			} else {
				r.expandToInclude(re);
			}
		}
		return r;
	}

	private static final String TABLE_ROUTE = "route_objects";
	private static final String TABLE_BASEROUTE = "baseroute_objects";
	private static final String CREATETABLE = "(id bigint primary key, "
			+ "types binary, pointTypes binary, pointIds binary, pointCoordinates binary, name varchar(4096), pointNames binary)";
	private static final String CREATE_IND = "_ind on route_objects (id)";
	private static final String SELECT_STAT = "SELECT types, pointTypes, pointIds, pointCoordinates, name, pointNames FROM " +TABLE_ROUTE+" WHERE id = ?";
	private static final String SELECT_BASE_STAT = "SELECT types, pointTypes, pointIds, pointCoordinates, name, pointNames FROM "+TABLE_BASEROUTE+" WHERE id = ?";
	private static final String INSERT_STAT = "(id, types, pointTypes, pointIds, pointCoordinates, name, pointNames) values(?, ?, ?, ?, ?, ?, ?)";
	private static final String COPY_BASE = "INSERT INTO " + TABLE_BASEROUTE + " SELECT id, types, pointTypes, pointIds, pointCoordinates, name, pointNames FROM "+TABLE_ROUTE+" WHERE id = ?";

	public void createDatabaseStructure(Connection mapConnection, DBDialect dialect, String rtreeMapIndexNonPackFileName)
			throws SQLException, IOException {
		this.mapConnection = mapConnection;
		Statement stat = mapConnection.createStatement();
		stat.executeUpdate("create table " +TABLE_ROUTE + CREATETABLE);
		stat.executeUpdate("create table " +TABLE_BASEROUTE + CREATETABLE);
		stat.executeUpdate("create index " +TABLE_ROUTE + CREATE_IND);
		stat.executeUpdate("create index " +TABLE_BASEROUTE + CREATE_IND);
		stat.close();
		mapRouteInsertStat = createStatementRouteObjInsert(mapConnection, false);
		try {
			routeTree = new RTree(rtreeMapIndexNonPackFileName);
		} catch (RTreeException e) {
			throw new IOException(e);
		}
		pStatements.put(mapRouteInsertStat, 0);
		if (generateLowLevel) {
			basemapRouteInsertStat = createStatementRouteObjInsert(mapConnection, true);
			try {
				baserouteTree = new RTree(rtreeMapIndexNonPackFileName + "b");
			} catch (RTreeException e) {
				throw new IOException(e);
			}
			pStatements.put(basemapRouteInsertStat, 0);
		}		
	}
	
	

	private PreparedStatement createStatementRouteObjInsert(Connection conn, boolean basemap) throws SQLException {
		return conn.prepareStatement("insert into " + ( basemap ? TABLE_BASEROUTE : TABLE_ROUTE) + INSERT_STAT);
	}

	public void commitAndCloseFiles(String rTreeMapIndexNonPackFileName, String rTreeMapIndexPackFileName, boolean deleteDatabaseIndexes)
			throws IOException, SQLException {
		// delete map rtree files
		deleteRouteTreeFiles(rTreeMapIndexNonPackFileName, rTreeMapIndexPackFileName, deleteDatabaseIndexes, routeTree);
		if(generateLowLevel) {
			deleteRouteTreeFiles(rTreeMapIndexNonPackFileName+"b", rTreeMapIndexPackFileName+"b", deleteDatabaseIndexes, baserouteTree);
		}
		closeAllPreparedStatements();
	}

	private void deleteRouteTreeFiles(String rTreeMapIndexNonPackFileName, String rTreeMapIndexPackFileName, boolean deleteDatabaseIndexes,
			RTree rte) throws IOException {
		if (rte != null) {
			RandomAccessFile file = rte.getFileHdr().getFile();
			file.close();
		}
		if (rTreeMapIndexNonPackFileName != null) {
			File f = new File(rTreeMapIndexNonPackFileName);
			if (f.exists() && deleteDatabaseIndexes) {
				f.delete();
			}
		}
		if (rTreeMapIndexPackFileName != null) {
			File f = new File(rTreeMapIndexPackFileName);
			if (f.exists() && deleteDatabaseIndexes) {
				f.delete();
			}
		}
	}


	private void indexHighwayRestrictions(Entity e, OsmDbAccessorContext ctx) throws SQLException {
		if (e instanceof Relation && "restriction".equals(e.getTag(OSMTagKey.TYPE))) { //$NON-NLS-1$
			String val = e.getTag("restriction"); //$NON-NLS-1$
			if (val != null) {
				byte type = -1;
				if ("no_right_turn".equalsIgnoreCase(val)) { //$NON-NLS-1$
					type = MapRenderingTypes.RESTRICTION_NO_RIGHT_TURN;
				} else if ("no_left_turn".equalsIgnoreCase(val)) { //$NON-NLS-1$
					type = MapRenderingTypes.RESTRICTION_NO_LEFT_TURN;
				} else if ("no_u_turn".equalsIgnoreCase(val)) { //$NON-NLS-1$
					type = MapRenderingTypes.RESTRICTION_NO_U_TURN;
				} else if ("no_straight_on".equalsIgnoreCase(val)) { //$NON-NLS-1$
					type = MapRenderingTypes.RESTRICTION_NO_STRAIGHT_ON;
				} else if ("only_right_turn".equalsIgnoreCase(val)) { //$NON-NLS-1$
					type = MapRenderingTypes.RESTRICTION_ONLY_RIGHT_TURN;
				} else if ("only_left_turn".equalsIgnoreCase(val)) { //$NON-NLS-1$
					type = MapRenderingTypes.RESTRICTION_ONLY_LEFT_TURN;
				} else if ("only_straight_on".equalsIgnoreCase(val)) { //$NON-NLS-1$
					type = MapRenderingTypes.RESTRICTION_ONLY_STRAIGHT_ON;
				}
				if (type != -1) {
					ctx.loadEntityRelation((Relation) e);
					Collection<EntityId> fromL = ((Relation) e).getMemberIds("from"); //$NON-NLS-1$
					Collection<EntityId> toL = ((Relation) e).getMemberIds("to"); //$NON-NLS-1$
					if (!fromL.isEmpty() && !toL.isEmpty()) {
						EntityId from = fromL.iterator().next();
						EntityId to = toL.iterator().next();
						if (from.getType() == EntityType.WAY) {
							if (!highwayRestrictions.containsKey(from.getId())) {
								highwayRestrictions.put(from.getId(), new TLongArrayList());
							}
							highwayRestrictions.get(from.getId()).add((to.getId() << 3) | (long) type);
						}
					}
				}
			}
		}
	}

	public void createRTreeFiles(String rTreeRouteIndexPackFileName) throws RTreeException {
		routeTree = new RTree(rTreeRouteIndexPackFileName);
		if(generateLowLevel) {
			baserouteTree = new RTree(rTreeRouteIndexPackFileName+"b");
		}
	}

	public void packRtreeFiles(String rTreeRouteIndexNonPackFileName, String rTreeRouteIndexPackFileName) throws IOException {
		routeTree = packRtreeFile(routeTree, rTreeRouteIndexNonPackFileName, rTreeRouteIndexPackFileName);
		if (generateLowLevel) {
			baserouteTree = packRtreeFile(baserouteTree, rTreeRouteIndexNonPackFileName + "b",
					rTreeRouteIndexPackFileName + "b");
		}
	}
	
	public void writeBinaryRouteIndex(File fl, BinaryMapIndexWriter writer, String regionName, boolean generateLowLevel) throws IOException, SQLException {
		closePreparedStatements(mapRouteInsertStat);
		if(basemapRouteInsertStat != null) {
			closePreparedStatements(basemapRouteInsertStat);
		}
		mapConnection.commit();
		
		try {
			writer.startWriteRouteIndex(regionName);
			// write map encoding rules
			// save position
			writer.writeRouteEncodingRules(routeTypes.getEncodingRuleTypes());
			RandomAccessFile raf = writer.getRaf();
			writer.flush();
			long fp = raf.getFilePointer();
			
			// 1st write
			writeRouteSections(writer);
			String fname = null;
			if (baserouteTree != null) {
				// prewrite end of file to read it
				writer.simulateWriteEndRouteIndex();
				writer.preclose();
				writer.flush();

				// use file to recalulate tree
				raf.seek(0);
				appendMissingRoadsForBaseMap(mapConnection, new BinaryMapIndexReader(raf, fl));
				// repack
				fname = baserouteTree.getFileName();
				baserouteTree = packRtreeFile(baserouteTree, fname, fname + "p");

				// seek to previous position
				raf.seek(fp);
				raf.getChannel().truncate(fp);

				// 2nd write
				writeRouteSections(writer);
			}
			writer.endWriteRouteIndex();
			writer.flush();
			if (generateLowLevel) {
				baserouteTree = null;
				new File(fname + "p").delete();
			}
		} catch (RTreeException e) {
			throw new IllegalStateException(e);
		}
	}
	private TLongObjectHashMap<BinaryFileReference> writeRouteSections(BinaryMapIndexWriter writer) throws IOException,
			SQLException, RTreeException {
		TLongObjectHashMap<BinaryFileReference> route = writeBinaryRouteIndexHeader(writer, routeTree, false);
		TLongObjectHashMap<BinaryFileReference> base = null;
		if (baserouteTree != null) {
			base = writeBinaryRouteIndexHeader(writer, baserouteTree, true);
		}
		writeBinaryRouteIndexBlocks(writer, routeTree, false, route);
		if (baserouteTree != null) {
			writeBinaryRouteIndexBlocks(writer, baserouteTree, true, base);
		}
		return base;
	}
	
	private void appendMissingRoadsForBaseMap(Connection conn, BinaryMapIndexReader reader) throws IOException, SQLException {
		TLongObjectHashMap<RouteDataObject> map = new CheckRoadConnectivity().collectDisconnectedRoads(reader);
		// to add 
		PreparedStatement ps = conn.prepareStatement(COPY_BASE);
		for(RouteDataObject rdo : map.valueCollection()) {
			//addWayToIndex(id, nodes, insertStat, rTree)
			int minX = Integer.MAX_VALUE;
			int maxX = 0;
			int minY = Integer.MAX_VALUE;
			int maxY = 0;
			long id = rdo.getId();
			for(int i = 0; i < rdo.getPointsLength(); i++) {
				int x = rdo.getPoint31XTile(i);
				int y = rdo.getPoint31YTile(i);
				minX = Math.min(minX, x);
				maxX = Math.max(maxX, x);
				minY = Math.min(minY, y);
				maxY = Math.max(maxY, y);
				long point = (x << 31) + y;
				registerBaseIntersectionPoint(point, false, id, i, i);
			}
			ps.setLong(1, id);
			ps.execute();
			try {
				baserouteTree.insert(new LeafElement(new Rect(minX, minY, maxX, maxY), id));
			} catch (RTreeInsertException e1) {
				throw new IllegalArgumentException(e1);
			} catch (IllegalValueException e1) {
				throw new IllegalArgumentException(e1);
			}
		}
		ps.close();
	}
	
	private Node convertBaseToNode(long s) {
		long x = s >> 31;
		long y = s - (x << 31);
		return new Node(MapUtils.get31LatitudeY((int) y), 
				MapUtils.get31LongitudeX((int) x), -1);
	}
	
	private String baseOrderValues[] = new String[] { "trunk", "motorway", "ferry", "primary", "secondary", "tertiary", "residential",
			"road", "cycleway", "living_street" };
	
	private int getBaseOrderForType(int intType) {
		if (intType == -1) {
			return Integer.MAX_VALUE;
		}
		MapRouteType rt = routeTypes.getTypeByInternalId(intType);
		int i = 0;
		for (; i < baseOrderValues.length; i++) {
			if (rt.getValue().startsWith(baseOrderValues[i])) {
				return i;
			}
		}
		return i;
	}
	
	private int getMainType(TIntCollection types){
		if(types.isEmpty()){
			return -1;
		}
		TIntIterator tit = types./*keySet().*/iterator();
		int main = tit.next();
		while(tit.hasNext()){
			int rt = tit.next();
			if(getBaseOrderForType(rt) < getBaseOrderForType(main)) {
				main = rt;
			}
		}
		return main;
	}
	
	@SuppressWarnings("rawtypes")
	public void getAdjacentRoads(GeneralizedCluster gcluster, GeneralizedWay gw, int i, Collection<GeneralizedWay> collection){
		gcluster = getCluster(gw, i, gcluster);
		Object o = gcluster.map.get(gw.getLocation(i));
		if (o instanceof LinkedList) {
			Iterator it = ((LinkedList) o).iterator();
			while (it.hasNext()) {
				GeneralizedWay next = (GeneralizedWay) it.next();
				if (next.id != gw.id) {
					collection.add(next);
				}
			}
		}
	}
	
	@SuppressWarnings("rawtypes")
	public int countAdjacentRoads(GeneralizedCluster gcluster, GeneralizedWay gw, int i){
		gcluster = getCluster(gw, i, gcluster);
		Object o = gcluster.map.get(gw.getLocation(i));
		if (o instanceof LinkedList) {
			
			Iterator it = ((LinkedList) o).iterator();
			int cnt = 0;
			while (it.hasNext()) {
				GeneralizedWay next = (GeneralizedWay) it.next();
				if (next.id != gw.id ) {
					cnt++;
				}
			}
			return cnt;
		} else if(o instanceof GeneralizedWay) {
			if(gw.id != ((GeneralizedWay)o).id){
				return 1;
			}
		}
		return 0;
	}
	
	
	
	public void processingLowLevelWays(IProgress progress) {
		if(!generateLowLevel) {
			return;
		}
		pointTypes.clear();
		pointNames.clear();
		Collection<GeneralizedCluster> clusters = new ArrayList<IndexRouteCreator.GeneralizedCluster>(
				generalClusters.valueCollection());
		// 1. roundabouts
		processRoundabouts(clusters);

		// 2. way combination based
		for (GeneralizedCluster cluster : clusters) {
			ArrayList<GeneralizedWay> copy = new ArrayList<GeneralizedWay>(cluster.ways);
			for (GeneralizedWay gw : copy) {
				// already deleted
				if (!cluster.ways.contains(gw)) {
					continue;
				}
				attachWays(gw, true);
				attachWays(gw, false);
			}
		}

		// 3. Douglas peuker simplifications
		douglasPeukerSimplificationStep(clusters);

		// 5. write to db
		TLongHashSet ids = new TLongHashSet();
		for (GeneralizedCluster cluster : clusters) {
			for (GeneralizedWay gw : cluster.ways) {
				if (ids.contains(gw.id)) {
					continue;
				}
				ids.add(gw.id);
				names.clear();
				Iterator<Entry<MapRouteType, String>> its = gw.names.entrySet().iterator();
				while (its.hasNext()) {
					Entry<MapRouteType, String> e = its.next();
					if (e.getValue() != null && !e.getValue().equals(CONFLICT_NAME)) {
						names.put(e.getKey(), e.getValue());
					}
				}
				ArrayList<Node> nodes = new ArrayList<Node>();
				if (gw.size() == 0) {
					System.err.println(gw.id + " empty ? ");
					continue;
				}
				long prev = 0;
				for (int i = 0; i < gw.size(); i++) {
					long loc = gw.getLocation(i);
					if (loc != prev) {
						Node c = convertBaseToNode(loc);
						prev = loc;
						nodes.add(c);
					}
				}
				outTypes.clear();
				outTypes.add(gw.mainType);
				outTypes.addAll(gw.addtypes);
				try {
					addWayToIndex(gw.id, nodes, basemapRouteInsertStat, baserouteTree, outTypes, pointTypes,
							pointNames, names);
				} catch (SQLException e) {
					throw new IllegalStateException(e);
				}
			}
		}
	}


	
	
	private static double scalarMultiplication(double xA, double yA, double xB, double yB, double xC, double yC) {
		// Scalar multiplication between (AB, AC)
		double multiple = (xB - xA) * (xC - xA) + (yB- yA) * (yC -yA);
		return multiple;
	}
	
	public static LatLon getProjection(float y31, float x31, float fromy31, float fromx31, float toy31, float tox31) {
		// not very accurate computation on sphere but for distances < 1000m it is ok
		float mDist = (fromy31 - toy31) * (fromy31 - toy31) + (fromx31 - tox31) * (fromx31 - tox31);
		float projection = (float) scalarMultiplication(fromy31, fromx31, toy31, tox31, y31, x31);
		float prlat;
		float prlon;
		if (projection < 0) {
			prlat = fromy31;
			prlon = fromx31;
		} else if (projection >= mDist) {
			prlat = toy31;
			prlon = tox31;
		} else {
			prlat = fromy31 + (toy31 - fromy31) * (projection / mDist);
			prlon = fromx31 + (tox31 - fromx31) * (projection / mDist);
		}
		return new LatLon(prlat, prlon);
	}
	
	private void simplifyDouglasPeucker(GeneralizedWay gw, float epsilon, Collection<Integer> ints, int start, int end){
		double dmax = -1;
		int index = -1;
		for (int i = start + 1; i <= end - 1; i++) {
			double d = orthogonalDistance(gw, start, end, gw.px.get(i),  gw.py.get(i), false);
			if (d > dmax) {
				dmax = d;
				index = i;
			}
		}
		if(dmax >= epsilon){
			simplifyDouglasPeucker(gw, epsilon, ints, start, index);
			simplifyDouglasPeucker(gw, epsilon, ints, index, end);
		} else {
			ints.add(end);
		}
	}
	
	private double orthogonalDistance(GeneralizedWay gn, int st, int end, int px, int py, boolean returnNanIfNoProjection){
		float fromy31 = gn.py.get(st);
		float fromx31 = gn.px.get(st);
		float toy31 = gn.py.get(end);
		float tox31 = gn.px.get(end);
		float mDist = (fromy31 - toy31) * (fromy31 - toy31) + (fromx31 - tox31) * (fromx31 - tox31);
		float projection = (float) scalarMultiplication(fromy31, fromx31, toy31, tox31, py, px);
		if (returnNanIfNoProjection && (projection < 0 || projection > mDist)) {
			return Double.NaN;
		}
//		float projy31 = fromy31 + (toy31 - fromy31) * (projection / mDist);
//		float projx31 = fromx31 + (tox31 - fromx31) * (projection / mDist);
		double A = MapUtils.convert31XToMeters(px, fromx31);
		double B = MapUtils.convert31YToMeters(py, fromy31);
		double C = MapUtils.convert31XToMeters(tox31, fromx31);
		double D = MapUtils.convert31YToMeters(toy31, fromy31);
		return Math.abs(A * D - C * B) / Math.sqrt(C * C + D * D);
		
	}
	
	private void douglasPeukerSimplificationStep(Collection<GeneralizedCluster> clusters){
		for(GeneralizedCluster cluster : clusters) {
			ArrayList<GeneralizedWay> copy = new ArrayList<GeneralizedWay>(cluster.ways);
			for(GeneralizedWay gw : copy) {
				Set<Integer> res = new HashSet<Integer>();
				simplifyDouglasPeucker(gw, DOUGLAS_PEUKER_DISTANCE, res, 0, gw.size() - 1);
				
				int ind = 1;
				int len = gw.size() - 1;
				for(int j = 1; j < len; j++) {
					if(!res.contains(j) && countAdjacentRoads(cluster, gw, ind) == 0) {
						GeneralizedCluster gcluster = getCluster(gw, ind, cluster);
						gcluster.removeWayFromLocation(gw, ind);
						gw.px.removeAt(ind);
						gw.py.removeAt(ind);
					} else {
						ind++;
					}
				}
			}
		}
	}
	
	public int checkDistanceToLine(GeneralizedWay line, int start, boolean directionPlus, int px, int py, double distThreshold) {
		int j = start;
		int next = directionPlus ? j + 1 : j - 1;
		while (next >= 0 && next < line.size()) {
			double od = orthogonalDistance(line, j, next, px, py, false);
			if (od < distThreshold) {
				return j;
			}
			j = next;
			next = directionPlus ? j + 1 : j - 1;
		}
		return -1;
	}
	
	private void processRoundabouts(Collection<GeneralizedCluster> clusters) {
		for(GeneralizedCluster cluster : clusters) {
			ArrayList<GeneralizedWay> copy = new ArrayList<GeneralizedWay>(cluster.ways);
			for (GeneralizedWay gw : copy) {
				// roundabout
				GeneralizedCluster gcluster = cluster;
				if (gw.getLocation(gw.size() - 1) == gw.getLocation(0) && cluster.ways.contains(gw)) {
					removeWayAndSubstituteWithPoint(gw, gcluster);
				}
			}
		}
	}
	
	private void removeGeneratedWay(GeneralizedWay gw, GeneralizedCluster gcluster) {
		for (int i = 0; i < gw.size(); i++) {
			gcluster = getCluster(gw, i, gcluster);
			gcluster.removeWayFromLocation(gw, i, true);
		}
	}

	
	@SuppressWarnings("rawtypes")
	private void removeWayAndSubstituteWithPoint(GeneralizedWay gw, GeneralizedCluster gcluster) {
		// calculate center location
		long pxc = 0;
		long pyc = 0;
		for (int i = 0; i < gw.size(); i++) {
			pxc += gw.px.get(i);
			pyc += gw.py.get(i);
		}
		pxc /= gw.size();
		pyc /= gw.size();

		// attach additional point to other roads
		for (int i = 0; i < gw.size(); i++) {
			gcluster = getCluster(gw, i, gcluster);
			Object o = gcluster.map.get(gw.getLocation(i));
			// something attachedpxc
			if (o instanceof LinkedList) {
				Iterator it = ((LinkedList)o).iterator();
				while(it.hasNext()) {
					GeneralizedWay next = (GeneralizedWay) it.next();
					replacePointWithAnotherPoint(gcluster, gw, (int) pxc, (int) pyc, i, next);
				}
			} else if (o instanceof GeneralizedWay) {
				replacePointWithAnotherPoint(gcluster, gw, (int) pxc, (int) pyc, i, (GeneralizedWay) o);
			}
		}
		// remove roundabout
		removeGeneratedWay(gw, gcluster);
	}

	private void replacePointWithAnotherPoint(GeneralizedCluster gcluster, GeneralizedWay gw, int pxc, int pyc, int i, GeneralizedWay next) {
		if (next.id != gw.id) {
			for (int j = 0; j < next.size(); j++) {
				if (next.getLocation(j) == gw.getLocation(i)) {
					if (j == next.size() - 1) {
						next.px.add(pxc);
						next.py.add(pyc);
						gcluster = getCluster(next, next.size() - 1, gcluster);
						gcluster.addWayFromLocation(next, next.size() - 1);
					} else {
						next.px.insert(j, pxc);
						next.py.insert(j, pyc);
						gcluster = getCluster(next, j, gcluster);
						gcluster.addWayFromLocation(next, j);
					}
					break;
				}
			}
		}
	}
	
	private boolean compareRefs(GeneralizedWay gw, GeneralizedWay gn){
		String ref1 = gw.names.get(routeTypes.getRefRuleType());
		String ref2 = gn.names.get(routeTypes.getRefRuleType());
		String name1 = gw.names.get(routeTypes.getNameRuleType());
		String name2 = gn.names.get(routeTypes.getNameRuleType());
		return equalsIfNotEmpty(ref1, ref2) && equalsIfNotEmpty(name1, name2);
	}
	
	private boolean equalsIfNotEmpty(String s1, String s2) {
		if(Algorithms.isEmpty(s1) || Algorithms.isEmpty(s2)) {
			return true;
		}
		return s1.equalsIgnoreCase(s2);
	}
	
	private void mergeName(MapRouteType rt, GeneralizedWay from, GeneralizedWay to){
		String rfFrom = from.names.get(rt);
		String rfTo = to.names.get(rt);
		if (rfFrom != null) {
			if (!rfFrom.equalsIgnoreCase(rfTo) && !Algorithms.isEmpty(rfTo)) {
				to.names.put(rt, CONFLICT_NAME);
			} else {
				to.names.put(rt, from.names.get(rt));
			}
		}
	}
	
	private void mergeAddTypes(GeneralizedWay from, GeneralizedWay to){
		TIntIterator it = to.addtypes.iterator();
		while(it.hasNext()) {
			int n = it.next();
			// maxspeed could be merged better
			if(!from.addtypes.contains(n)) {
				it.remove();
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	private GeneralizedWay selectBestWay(GeneralizedCluster cluster, GeneralizedWay gw, int ind) {
		long loc = gw.getLocation(ind);
		Object o = cluster.map.get(loc);
		GeneralizedWay res = null;
		if (o instanceof GeneralizedWay) {
			if (o != gw) {
				GeneralizedWay m = (GeneralizedWay) o;
				if (m.id != gw.id && m.mainType == gw.mainType && compareRefs(gw, m)) {
					return m;
				}
				
			}
		} else if (o instanceof LinkedList) {
			LinkedList<GeneralizedWay> l = (LinkedList<GeneralizedWay>) o;
			double bestDiff = Math.PI / 2;
			for (GeneralizedWay m : l) {
				if (m.id != gw.id && m.mainType == gw.mainType && compareRefs(gw, m)) {
					double init = gw.directionRoute(ind, ind == 0);
					double dir;
					if (m.getLocation(0) == loc) {
						dir = m.directionRoute(0, true);
					} else if (m.getLocation(m.size() - 1) == loc) {
						dir = m.directionRoute(m.size() - 1, false);
					} else {
						return null;
					}
					double angleDiff = Math.abs(MapUtils.alignAngleDifference(Math.PI + dir - init));
					if (angleDiff < bestDiff) {
						bestDiff = angleDiff;
						res = m;
					}
				}
			}
		}
		return res;
	}

	
	private void attachWays(GeneralizedWay gw, boolean first) {
		GeneralizedCluster cluster = null;
		while(true) {
			int ind = first? 0 : gw.size() - 1;
			cluster = getCluster(gw, ind, cluster);
			GeneralizedWay prev = selectBestWay(cluster, gw, ind);
			if(prev == null) {
				break;
			}
			for (int i = 0; i < prev.size(); i++) {
				cluster = getCluster(prev, i, cluster);
				cluster.replaceWayFromLocation(prev, i, gw);
			}
			mergeAddTypes(prev, gw);
			for(MapRouteType rt : new ArrayList<MapRouteType>(gw.names.keySet())) {
				mergeName(rt, prev, gw);	
			}
			for(MapRouteType rt : new ArrayList<MapRouteType>(prev.names.keySet())) {
				if(!gw.names.containsKey(rt)){
					mergeName(rt, prev, gw);
				}
			}
			
			TIntArrayList ax = first? prev.px : gw.px;
			TIntArrayList ay = first? prev.py : gw.py;
			TIntArrayList bx = !first? prev.px : gw.px;
			TIntArrayList by = !first? prev.py : gw.py;
			if(first) {
				if(gw.getLocation(0) == prev.getLocation(0)) {
					ax.reverse();
					ay.reverse();
				}
			} else {
				if(gw.getLocation(ind) == prev.getLocation(prev.size() - 1)) {
					bx.reverse();
					by.reverse();
				}
			}
			bx.removeAt(0);
			by.removeAt(0);
			ax.addAll(bx);
			ay.addAll(by);
			gw.px = ax;
			gw.py = ay;
		}
	}

	private static class RouteWriteContext {
		PreparedStatement selectData ;
		TLongObjectHashMap<BinaryFileReference> treeHeader;
		Map<String, Integer> tempStringTable = new LinkedHashMap<String, Integer>();
		Map<MapRouteType, String> tempNames = new LinkedHashMap<MapRoutingTypes.MapRouteType, String>();
		List<MapPointName> tempPointNames = new ArrayList<MapRoutingTypes.MapPointName>();
		TLongArrayList wayMapIds = new TLongArrayList();
		TLongArrayList pointMapIds = new TLongArrayList();
	}

	private void writeBinaryRouteIndexBlocks(BinaryMapIndexWriter writer, RTree rte, boolean basemap,
			TLongObjectHashMap<BinaryFileReference> treeHeader) throws IOException, SQLException, RTreeException {

		// write map levels and map index
		long rootIndex = rte.getFileHdr().getRootIndex();
		rtree.Node root = rte.getReadNode(rootIndex);
		Rect rootBounds = calcBounds(root);
		if (rootBounds != null) {
				PreparedStatement selectData = mapConnection.prepareStatement(basemap ? SELECT_BASE_STAT : SELECT_STAT);
				RouteWriteContext wc = new RouteWriteContext();
				wc.selectData = selectData;
				wc.treeHeader = treeHeader;
				writeBinaryMapBlock(root, rootBounds, rte, writer, wc, basemap);
				selectData.close();
		}
	}
	
	private TLongObjectHashMap<BinaryFileReference> writeBinaryRouteIndexHeader(BinaryMapIndexWriter writer,  
			RTree rte, boolean basemap) throws IOException, SQLException, RTreeException {
		// write map levels and map index
		TLongObjectHashMap<BinaryFileReference> treeHeader = new TLongObjectHashMap<BinaryFileReference>();
		long rootIndex = rte.getFileHdr().getRootIndex();
		rtree.Node root = rte.getReadNode(rootIndex);
		Rect rootBounds = calcBounds(root);
		if (rootBounds != null) {
			writeBinaryRouteTree(root, rootBounds, rte, writer, treeHeader, basemap);
		}
		return treeHeader;
	}	
	
	private int registerId(TLongArrayList ids, long id) {
		for (int i = 0; i < ids.size(); i++) {
			if (ids.getQuick(i) == id) {
				return i;
			}
		}
		ids.add(id);
		return ids.size() - 1;
	}
	
	private void writeBinaryMapBlock(rtree.Node parent, Rect parentBounds, RTree r, BinaryMapIndexWriter writer, RouteWriteContext wc, boolean basemap)
					throws IOException, RTreeException, SQLException {
		Element[] e = parent.getAllElements();

		RouteDataBlock.Builder dataBlock = null;
		BinaryFileReference ref = wc.treeHeader.get(parent.getNodeIndex());
		wc.wayMapIds.clear();
		wc.pointMapIds.clear();
		for (int i = 0; i < parent.getTotalElements(); i++) {
			if (e[i].getElementType() == rtree.Node.LEAF_NODE) {
				long id = e[i].getPtr();
				// IndexRouteCreator.SELECT_STAT;
				// "SELECT types, pointTypes, pointIds, pointCoordinates, name FROM route_objects WHERE id = ?"
				wc.selectData.setLong(1, id);

				ResultSet rs = wc.selectData.executeQuery();
				if (rs.next()) {
					if (dataBlock == null) {
						dataBlock = RouteDataBlock.newBuilder();
						wc.tempStringTable.clear();
						wc.wayMapIds.clear();
						wc.pointMapIds.clear();
					}
					int cid = registerId(wc.wayMapIds, id);
					wc.tempNames.clear();
					decodeNames(rs.getString(5), wc.tempNames);
					byte[] types = rs.getBytes(1);
					int[] typeUse = new int[types.length / 2];
					for (int j = 0; j < types.length; j += 2) {
						int ids = Algorithms.parseSmallIntFromBytes(types, j);
						typeUse[j / 2] = routeTypes.getTypeByInternalId(ids).getTargetId();
					}
					TLongArrayList restrictions = highwayRestrictions.get(id);
					if(!basemap && restrictions != null){
						for(int li = 0; li<restrictions.size(); li++){
							Builder restriction = RestrictionData.newBuilder();
							restriction.setFrom(cid);
							int toId = registerId(wc.wayMapIds, restrictions.get(li) >> 3);
							restriction.setTo(toId);
							restriction.setType((int) (restrictions.get(li) & 0x7));
							dataBlock.addRestrictions(restriction.build());
						}
					}
					byte[] pointTypes = rs.getBytes(2);
					//byte[] pointIds = rs.getBytes(3);
					byte[] pointCoordinates = rs.getBytes(4);
					String pointNames = rs.getString(6);
					wc.tempPointNames.clear();
					decodeListNames(pointNames, wc.tempPointNames);
					
					int pointsLength = pointCoordinates.length / 8;
					RouteMissingPoints missingPoints = null;
					if(basemap && basemapNodesToReinsert.containsKey(id) ) {
						missingPoints = basemapNodesToReinsert.get(id);
						missingPoints.buildPointsToInsert(pointsLength);
					}
					
					int typeInd = 0;
					List<RoutePointToWrite> points = new ArrayList<RoutePointToWrite>(pointsLength);
					for (int j = 0; j < pointsLength; j++) {
						if(missingPoints != null && missingPoints.pointsXToInsert[j] != null) {
							for(int k = 0; k < missingPoints.pointsXToInsert[j].size(); k++) {
								RoutePointToWrite point = new RoutePointToWrite();
								points.add(point);
								point.x = missingPoints.pointsXToInsert[j].get(k);
								point.y = missingPoints.pointsYToInsert[j].get(k);
							}
						}
						RoutePointToWrite point = new RoutePointToWrite();
						points.add(point);
						point.x = Algorithms.parseIntFromBytes(pointCoordinates, j * 8);
						point.y = Algorithms.parseIntFromBytes(pointCoordinates, j * 8 + 4);
						// not supported any more because basemap could change point types and order
//						if(WRITE_POINT_ID) {
//							points[j].id = registerId(pointMapIds, Algorithms.parseLongFromBytes(pointIds, j * 8));
//						}
						int type = 0;
						do {
							type = Algorithms.parseSmallIntFromBytes(pointTypes, typeInd);
							typeInd += 2;
							if (type != 0) {
								point.types.add(routeTypes.getTypeByInternalId(type).getTargetId());
							}
						} while (type != 0);
					}

					RouteData routeData = writer.writeRouteData(cid, parentBounds.getMinX(), parentBounds.getMinY(), typeUse, 
							points.toArray(new RoutePointToWrite[points.size()]),
							wc.tempNames, wc.tempStringTable, wc.tempPointNames, dataBlock, true, false);
					if (routeData != null) {
						dataBlock.addDataObjects(routeData);
					}
				} else {
					logMapDataWarn.error("Something goes wrong with id = " + id); //$NON-NLS-1$
				}
			}
		}
		if (dataBlock != null) {
			IdTable.Builder idTable = IdTable.newBuilder();
			long prev = 0;
			for (int i = 0; i < wc.wayMapIds.size(); i++) {
				idTable.addRouteId(wc.wayMapIds.getQuick(i) - prev);
				prev = wc.wayMapIds.getQuick(i);
			}
//			if (WRITE_POINT_ID) {
//				prev = 0;
//				for (int i = 0; i < pointMapIds.size(); i++) {
//					prev = pointMapIds.getQuick(i);
//				}
//			}
			dataBlock.setIdTable(idTable.build());
			writer.writeRouteDataBlock(dataBlock, wc.tempStringTable, ref);
		}
		for (int i = 0; i < parent.getTotalElements(); i++) {
			if (e[i].getElementType() != rtree.Node.LEAF_NODE) {
				long ptr = e[i].getPtr();
				rtree.Node ns = r.getReadNode(ptr);
				writeBinaryMapBlock(ns, e[i].getRect(), r, writer, wc, basemap);
			}
		}
	}

	private void writeBinaryRouteTree(rtree.Node parent, Rect re, RTree r, BinaryMapIndexWriter writer,
			TLongObjectHashMap<BinaryFileReference> bounds, boolean basemap)
			throws IOException, RTreeException {
		Element[] e = parent.getAllElements();
		boolean containsLeaf = false;
		for (int i = 0; i < parent.getTotalElements(); i++) {
			if (e[i].getElementType() == rtree.Node.LEAF_NODE) {
				containsLeaf = true;
			}
		}
		BinaryFileReference ref = writer.startRouteTreeElement(re.getMinX(), re.getMaxX(), re.getMinY(), re.getMaxY(), containsLeaf,
				basemap);
		if (ref != null) {
			bounds.put(parent.getNodeIndex(), ref);
		}
		for (int i = 0; i < parent.getTotalElements(); i++) {
			if (e[i].getElementType() != rtree.Node.LEAF_NODE) {
				rtree.Node chNode = r.getReadNode(e[i].getPtr());
				writeBinaryRouteTree(chNode, e[i].getRect(), r, writer, bounds, basemap);
			}
		}
		writer.endRouteTreeElement();
	}
	

	/*private*/ static class GeneralizedCluster {
		public final int x;
		public final int y;
		public final int zoom;
		
		public GeneralizedCluster(int x, int y, int z){
			this.x = x;
			this.y = y;
			this.zoom = z;
		}
		
		public final Set<GeneralizedWay> ways = new HashSet<IndexRouteCreator.GeneralizedWay>();
		// either LinkedList<GeneralizedWay> or GeneralizedWay
		public final TLongObjectHashMap<Object> map = new TLongObjectHashMap<Object>();
		
		@SuppressWarnings({ "rawtypes", "unchecked" })
		public void replaceWayFromLocation(GeneralizedWay delete, int ind, GeneralizedWay toReplace){
			ways.remove(delete);
			long loc = delete.getLocation(ind);
			Object o = map.get(loc);
			if(o instanceof GeneralizedWay){
				if(delete == o) {
					map.put(loc, toReplace);
				} else if(toReplace !=  o){
					addWay(toReplace, loc);
				}
			} else if(o instanceof LinkedList){
				((LinkedList) o).remove(delete);
				if(!((LinkedList) o).contains(toReplace)){
					((LinkedList) o).add(toReplace);
				}
			} else {
				map.put(loc, toReplace);
			}
		}
		
		public void removeWayFromLocation(GeneralizedWay delete, int ind){
			removeWayFromLocation(delete, ind, false);
		}
		@SuppressWarnings("rawtypes")
		public void removeWayFromLocation(GeneralizedWay delete, int ind, boolean deleteAll) {
			long loc = delete.getLocation(ind);
			boolean ex = false;
			if (!deleteAll) {
				for (int t = 0; t < delete.size(); t++) {
					if (t != ind && map.containsKey(delete.getLocation(t))) {
						ex = true;
						break;
					}
				}
			}
			if (!ex || deleteAll) {
				ways.remove(delete);
			}

			Object o = map.get(loc);
			if (o instanceof GeneralizedWay) {
				if (delete == o) {
					map.remove(loc);
				}
			} else if (o instanceof LinkedList) {
				((LinkedList) o).remove(delete);
				if (((LinkedList) o).size() == 1) {
					map.put(loc, ((LinkedList) o).iterator().next());
				} else if (((LinkedList) o).size() == 0) {
					map.remove(loc);
				}
			}
		}
		
		public void addWayFromLocation(GeneralizedWay w, int i) {
			ways.add(w);
			long loc = w.getLocation(i);
			addWay(w, loc);
		}

		@SuppressWarnings("unchecked")
		private void addWay(GeneralizedWay w, long loc) {
			
			if (map.containsKey(loc)) {
				Object o = map.get(loc);
				if (o instanceof LinkedList) {
					if(!((LinkedList<GeneralizedWay>) o).contains(w)){
						((LinkedList<GeneralizedWay>) o).add(w);
					}
				} else if(o != w){
					LinkedList<GeneralizedWay> list = new LinkedList<GeneralizedWay>();
					list.add((GeneralizedWay) o);
					list.add(w);
					map.put(loc, list);
				}
			} else {
				map.put(loc, w);
			}
		}
	}
	/*private*/ static class GeneralizedWay {
		private long id;
		private int mainType;
		private TIntHashSet addtypes = new TIntHashSet();
		private TIntArrayList px = new TIntArrayList();
		private TIntArrayList py = new TIntArrayList();
		
		// TLongObjectHashMap<TIntArrayList> pointTypes = new TLongObjectHashMap<TIntArrayList>();
		private Map<MapRoutingTypes.MapRouteType, String> names = new HashMap<MapRoutingTypes.MapRouteType, String>();
		public GeneralizedWay(long id) {
			this.id = id;
		}
		
		public double getDistance() {
			double dx = 0;
			for (int i = 1; i < px.size(); i++) {
				dx += MapUtils.getDistance(MapUtils.get31LatitudeY(py.get(i - 1)), MapUtils.get31LongitudeX(px.get(i - 1)),
						MapUtils.get31LatitudeY(py.get(i)), MapUtils.get31LongitudeX(px.get(i)));
			}
			return dx;
		}
		
		public long getLocation(int ind) {
			return getBaseId(px.get(ind), py.get(ind));
		}
		
		public int size(){
			return px.size();
		}
		
		// Gives route direction of EAST degrees from NORTH ]-PI, PI]
		public double directionRoute(int startPoint, boolean plus) {
			float dist = 5;
			int x = this.px.get(startPoint);
			int y = this.py.get(startPoint);
			int nx = startPoint;
			int px = x;
			int py = y;
			double total = 0;
			do {
				if (plus) {
					nx++;
					if (nx >= size()) {
						break;
					}
				} else {
					nx--;
					if (nx < 0) {
						break;
					}
				}
				px = this.px.get(nx);
				py = this.py.get(nx);
				// translate into meters
				total += Math.abs(px - x) * 0.011d + Math.abs(py - y) * 0.01863d;
			} while (total < dist);
			return -Math.atan2( x - px, y - py );
		}
	}
	
	
}
