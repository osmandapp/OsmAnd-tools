package net.osmand.obf.preparation;

import gnu.trove.TIntCollection;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;
import gnu.trove.set.hash.TLongHashSet;
import net.osmand.IProgress;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteTypeRule;
import net.osmand.binary.ObfConstants;
import net.osmand.binary.OsmandOdb.IdTable;
import net.osmand.binary.OsmandOdb.OsmAndRoutingIndex.RouteDataBlock;
import net.osmand.binary.OsmandOdb.RestrictionData;
import net.osmand.binary.OsmandOdb.RestrictionData.Builder;
import net.osmand.binary.OsmandOdb.RouteData;
import net.osmand.binary.RouteDataObject;
import net.osmand.binary.RouteDataObject.RestrictionInfo;
import net.osmand.data.*;
import net.osmand.obf.preparation.BinaryMapIndexWriter.RoutePointToWrite;
import net.osmand.osm.MapRenderingTypes;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.MapRenderingTypesEncoder.EntityConvertApplyType;
import net.osmand.osm.MapRoutingTypes;
import net.osmand.osm.MapRoutingTypes.MapPointName;
import net.osmand.osm.MapRoutingTypes.MapRouteType;
import net.osmand.osm.RelationTagsPropagation;
import net.osmand.osm.RelationTagsPropagation.PropagateEntityTags;
import net.osmand.osm.edit.*;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.OSMSettings.OSMTagKey;
import net.osmand.osm.edit.Relation.RelationMember;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import org.apache.commons.logging.Log;
import rtree.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.*;
import java.util.*;
import java.util.Map.Entry;

public class IndexRouteCreator extends AbstractIndexPartCreator {

	private Connection mapConnection;
	private final Log logMapDataWarn;
	private final static int CLUSTER_ZOOM = 15;
	private final static String CONFLICT_NAME = "#CONFLICT";
	private RTree routeTree = null;
	private RTree baserouteTree = null;
	private MapRoutingTypes routeTypes;
	RelationTagsPropagation tagsTransformer = new RelationTagsPropagation();

	private final static float DOUGLAS_PEUKER_DISTANCE = 15;

	// flipped quad tree cause bottom > top
	private QuadTree<Multipolygon> lowEmissionZones = new QuadTree<Multipolygon>(new QuadRect(-180, 90, 180, -90), 8, 0.55f);


	private TLongObjectHashMap<List<RestrictionInfo>> highwayRestrictions = new TLongObjectHashMap<List<RestrictionInfo>>();
	private TLongObjectHashMap<WayNodeId> basemapRemovedNodes = new TLongObjectHashMap<WayNodeId>();
	private TLongObjectHashMap<RouteMissingPoints> basemapNodesToReinsert = new TLongObjectHashMap<RouteMissingPoints> ();

	// local purpose to speed up processing cache allocation
	TIntArrayList outTypes = new TIntArrayList();
	TLongObjectHashMap<TIntArrayList> pointTypes = new TLongObjectHashMap<TIntArrayList>();
	TLongObjectHashMap<TIntObjectHashMap<String> >  pointNames = new TLongObjectHashMap<TIntObjectHashMap<String> > ();
	Map<MapRoutingTypes.MapRouteType, String> names = createTreeMap();



	static TreeMap<MapRoutingTypes.MapRouteType, String> createTreeMap() {
		return new TreeMap<MapRoutingTypes.MapRouteType, String>(new Comparator<MapRoutingTypes.MapRouteType>() {

			@Override
			public int compare(MapRouteType o1, MapRouteType o2) {
				int x = value(o1);
				int y = value(o2);
				return (x < y) ? -1 : ((x == y) ? o1.getTag().compareTo(o2.getTag()) : 1);
			}

			private int value(MapRouteType o1) {
				if(o1.getTag().endsWith(":en")) {
					return 1;
				} else if(o1.getTag().contains(":")) {
					return 2;
				}
				return 0;
			}
		});
	}


	TLongObjectHashMap<GeneralizedCluster> generalClusters = new TLongObjectHashMap<GeneralizedCluster>();
	private PreparedStatement mapRouteInsertStat;
	private PreparedStatement basemapRouteInsertStat;
	private MapRenderingTypesEncoder renderingTypes;
	private IndexCreatorSettings settings;
	private PropagateToNodes propagateToNodes;


	private class RouteMissingPoints {
		List<Map<Integer, Long>> pointsMap = new ArrayList<>();

		private void addPoint(int originalInd, int insertAt, long loc) {
			while(pointsMap.size() <= insertAt) {
				pointsMap.add(null);
			}
			if(pointsMap.get(insertAt) == null) {
				pointsMap.set(insertAt, new TreeMap<>());
			}
			pointsMap.get(insertAt).put(originalInd, loc);
		}

	}

	public IndexRouteCreator(MapRenderingTypesEncoder renderingTypes, Log logMapDataWarn, IndexCreatorSettings settings, PropagateToNodes propagateToNodes) {
		this.renderingTypes = renderingTypes;
		this.logMapDataWarn = logMapDataWarn;
		this.settings = settings;
		this.routeTypes = new MapRoutingTypes(renderingTypes);
		this.propagateToNodes = propagateToNodes;
	}

	public void indexExtraRelations(OsmBaseStorage reader) {
		for (Entity e : reader.getRegisteredEntities().values()) {
			if (e instanceof Relation && "low_emission_zone".equals(e.getTags().get("boundary"))) {
				e.initializeLinks(reader.getRegisteredEntities());
				addLowEmissonZoneRelation((Relation) e);
			}
		}
	}

	public void indexRelations(Entity e, OsmDbAccessorContext ctx) throws SQLException {
		indexHighwayRestrictions(e, ctx);
		if (e instanceof Relation) {
			tagsTransformer.handleRelationPropogatedTags((Relation) e, renderingTypes, ctx, EntityConvertApplyType.ROUTING);
			Map<String, String> tags = renderingTypes.transformTags(e.getTags(), EntityType.RELATION, EntityConvertApplyType.ROUTING);
			if ("enforcement".equals(tags.get("type")) && "maxspeed".equals(tags.get("enforcement"))) {
				ctx.loadEntityRelation((Relation) e);
				Iterator<RelationMember> from = ((Relation) e).getMembers("from").iterator();
				// mark as speed cameras
				while(from.hasNext()) {
					Entity n = from.next().getEntity();
					if (n instanceof Node) {
						PropagateEntityTags pt = tagsTransformer
								.getPropogateTagForEntity(new EntityId(EntityType.NODE, n.getId()));
						pt.putThroughTags.put("highway", "speed_camera");
					}
				}
			}
		}
	}

	public void indexLowEmissionZones(Entity e, OsmDbAccessorContext ctx) throws SQLException {
		if ("low_emission_zone".equals(e.getTags().get("boundary"))) {
			if (e instanceof Relation) {
				ctx.loadEntityRelation((Relation) e);
				addLowEmissonZoneRelation((Relation) e);
			}
			if (e instanceof Way) {
				addLowEmissonZoneWay((Way) e);
			}
		}
	}

	private void addLowEmissonZoneRelation(Relation e) {
		MultipolygonBuilder multipolygonBuilder = new MultipolygonBuilder();
        multipolygonBuilder.setId(e.getId());
        multipolygonBuilder.createInnerAndOuterWays(e);
		Multipolygon lowEmissionZone = multipolygonBuilder.build();
		if (lowEmissionZone != null) {
			QuadRect bbox = lowEmissionZone.getLatLonBbox();
			QuadRect flippedBbox = flipBbox(bbox);
			lowEmissionZones.insert(lowEmissionZone, flippedBbox);
		}
	}

	private void addLowEmissonZoneWay(Way e) {
		List<Way> outer = new ArrayList<>();
		List<Way> inner = new ArrayList<>();
		outer.add(e);
		MultipolygonBuilder multipolygonBuilder = new MultipolygonBuilder(outer, inner);
		multipolygonBuilder.setId(e.getId());
		Multipolygon lowEmissionZone = multipolygonBuilder.build();
		if (lowEmissionZone != null) {
			QuadRect bbox = lowEmissionZone.getLatLonBbox();
			QuadRect flippedBbox = flipBbox(bbox);
			lowEmissionZones.insert(lowEmissionZone, flippedBbox);
		}
	}

	private QuadRect flipBbox(QuadRect bbox) {
		return new QuadRect(bbox.left, bbox.bottom, bbox.right, bbox.top);
	}

	private Map<String, String> addLowEmissionZoneTag(Way e, Map<String, String> tags) {
		Node n = null;
		// get first not null node
		for (int i = 0; i < e.getNodes().size() && n == null; i++) {
			n = e.getNodes().get(i);
		}
		if (n == null) {
			return tags;
		}
		List<Multipolygon> results = lowEmissionZones.queryInBox(
				new QuadRect(n.getLongitude(), n.getLatitude(), n.getLongitude(), n.getLatitude()), new ArrayList<>(0));
		for (Multipolygon m : results) {
			if (m.containsPoint(n.getLatitude(), n.getLongitude())) {
				tags =  new LinkedHashMap<String, String>(tags);
				tags.put("low_emission_zone", "true");
				break;
			}
		}
		return tags;
	}

	public void iterateMainEntity(Entity es, OsmDbAccessorContext ctx) throws SQLException {
		iterateMainEntity(es, ctx, null);
	}

	public void iterateMainEntity(Entity es, OsmDbAccessorContext ctx, IndexCreationContext icc) throws SQLException {
		if (es instanceof Way) {
			Way e = (Way) es;
			if (settings.addRegionTag) {
				icc.calcRegionTag(e, true);
			}
			Map<String, String> tags = e.getTags();
			tags = addLowEmissionZoneTag(e, tags);
			tags = tagsTransformer.addPropogatedTags(renderingTypes, EntityConvertApplyType.ROUTING, e, tags);
			tags = renderingTypes.transformTags(tags, EntityType.WAY, EntityConvertApplyType.ROUTING);
			boolean encoded = routeTypes.encodeEntity(tags, outTypes, names)
					&& e.getNodes().size() >= 2;
			if (encoded) {
				// Load point with tags!
				ctx.loadEntityWay(e);
				if (propagateToNodes != null) {
					propagateToNodes.propagateTagsToWayNodesNoBorderRule(e);
				}
				routeTypes.encodePointTypes(e, pointTypes, pointNames, tagsTransformer, renderingTypes, false);
				addWayToIndex(e.getId(), e.getNodes(), mapRouteInsertStat, routeTree, outTypes, pointTypes, pointNames, names);
			}
			if (settings.generateLowLevel) {
				encoded = routeTypes.encodeBaseEntity(tags, outTypes, names) && e.getNodes().size() >= 2;
				if (encoded) {
					List<Node> source = e.getNodes();
					// NEVER remove this simplify route due to memory limits in routing(task 11770)
					long id = e.getId();
					List<Node> result = simplifyRouteForBaseSection(source, id);
					routeTypes.encodePointTypes(e, pointTypes, pointNames, tagsTransformer, renderingTypes, true);
					addWayToIndex(e.getId(), result, basemapRouteInsertStat, baserouteTree, outTypes, pointTypes,
							pointNames, names);
					// generalizeWay(e);
				}
			}
			if (icc != null) {
				Map<String, String> ntags = renderingTypes.transformTags(e.getModifiableTags(), EntityType.WAY, EntityConvertApplyType.MAP);
				if (e.getModifiableTags() != ntags) {
					e.getModifiableTags().putAll(ntags);
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
				boolean forceKeep =
						registerBaseIntersectionPoint(point, !kept[i], id, indexToInsertAt, originalInd);
				originalInd++;
				if(!kept[i] && forceKeep) {
					kept[i] = true;
					result.add(indexToInsertAt, n);
				}
				if(kept[i]) {
					indexToInsertAt ++;
				}
			}
		}
		return result;
	}

	private static class WayNodeId {
		long wayId;
		int originalInd;
		int insertAt;

		public WayNodeId(long wayId, int originalInd, int insertAt) {
			this.wayId = wayId;
			this.originalInd = originalInd;
			this.insertAt = insertAt;
		}


	}

	private boolean registerBaseIntersectionPoint(long pointLoc, boolean register, long wayId, int insertAt, int originalInd) {
		if(basemapRemovedNodes.containsKey(pointLoc)) {
			WayNodeId exNode = basemapRemovedNodes.get(pointLoc);
			if(exNode != null) {
				if(!basemapNodesToReinsert.containsKey(exNode.wayId)) {
					basemapNodesToReinsert.put(exNode.wayId, new RouteMissingPoints());
				}
				RouteMissingPoints mp = basemapNodesToReinsert.get(exNode.wayId);
				mp.addPoint(exNode.originalInd, exNode.insertAt, pointLoc);
				basemapRemovedNodes.put(pointLoc, null);
			}
			return true;
		}
		WayNodeId genKey = register ?
				new WayNodeId(wayId ,originalInd, insertAt) : null;
		basemapRemovedNodes.put(pointLoc, genKey);
		return false;
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
					if (n.getId() > ObfConstants.PROPAGATE_NODE_BIT) {
						continue;
					}
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
		if (settings.generateLowLevel) {
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
		if(settings.generateLowLevel) {
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
				Relation r = (Relation) e;
				if ("no_u_turn".equalsIgnoreCase(val)) { //$NON-NLS-1$
					List<RelationMember> lfrom = r.getMembers("from"); //$NON-NLS-1$
					List<RelationMember> lto = r.getMembers("to"); //$NON-NLS-1$
					if (lfrom.size() == 1 && lto.size() == 1 &&
							lfrom.get(0).getEntityId().equals(lto.get(0).getEntityId())) {
						// don't index such roads - can't go through issue https://www.openstreetmap.org/way/338099991#map=17/46.86699/-0.20473
						return;
					}
				}

				boolean allowMultipleFrom = false;
				boolean allowMultipleTo = false;
				byte type = -1;
				if ("no_right_turn".equalsIgnoreCase(val)) { //$NON-NLS-1$
					type = MapRenderingTypes.RESTRICTION_NO_RIGHT_TURN;
				} else if ("no_left_turn".equalsIgnoreCase(val)) { //$NON-NLS-1$
					type = MapRenderingTypes.RESTRICTION_NO_LEFT_TURN;
				} else if ("no_u_turn".equalsIgnoreCase(val)) { //$NON-NLS-1$
					type = MapRenderingTypes.RESTRICTION_NO_U_TURN;
				} else if ("no_straight_on".equalsIgnoreCase(val)) { //$NON-NLS-1$
					type = MapRenderingTypes.RESTRICTION_NO_STRAIGHT_ON;
				} else if ("no_entry".equalsIgnoreCase(val)) { //$NON-NLS-1$
					// reuse no straight on
					type = MapRenderingTypes.RESTRICTION_NO_STRAIGHT_ON;
					allowMultipleFrom = true;
				} else if ("no_exit".equalsIgnoreCase(val)) { //$NON-NLS-1$
					// reuse no straight on
					type = MapRenderingTypes.RESTRICTION_NO_STRAIGHT_ON;
					allowMultipleTo = true;
				} else if ("only_right_turn".equalsIgnoreCase(val)) { //$NON-NLS-1$
					type = MapRenderingTypes.RESTRICTION_ONLY_RIGHT_TURN;
				} else if ("only_left_turn".equalsIgnoreCase(val)) { //$NON-NLS-1$
					type = MapRenderingTypes.RESTRICTION_ONLY_LEFT_TURN;
				} else if ("only_straight_on".equalsIgnoreCase(val)) { //$NON-NLS-1$
					type = MapRenderingTypes.RESTRICTION_ONLY_STRAIGHT_ON;
				}
				if (type != -1) {
					ctx.loadEntityRelation(r);
					Collection<RelationMember> fromL = r.getMembers("from"); //$NON-NLS-1$
					Collection<RelationMember> toL = r.getMembers("to"); //$NON-NLS-1$
					Collection<RelationMember> viaL = r.getMembers("via"); //$NON-NLS-1$
					if (!toL.isEmpty()) {
						for (RelationMember from : fromL) {
							if (from.getEntityId().getType() == EntityType.WAY) {
								if (!highwayRestrictions.containsKey(from.getEntityId().getId())) {
									highwayRestrictions.put(from.getEntityId().getId(), new ArrayList<>());
								}

								List<RestrictionInfo> rdList = highwayRestrictions.get(from.getEntityId().getId());
								for (RelationMember to : toL) {
									RestrictionInfo rd = new RestrictionInfo();
									rd.toWay = to.getEntityId().getId();
									rd.type = type;
									if (!viaL.isEmpty()) {
										RelationMember via = viaL.iterator().next();
										if (via.getEntityId().getType() == EntityType.WAY) {
											rd.viaWay = via.getEntityId().getId();
										}
									}
									rdList.add(rd);

									if (!allowMultipleTo) {
										break;
									}
								}

								if (!allowMultipleFrom) {
									break;
								}
							}
						}
					}
				}
			}
		}
	}

	public void createRTreeFiles(String rTreeRouteIndexPackFileName) throws RTreeException {
		routeTree = new RTree(rTreeRouteIndexPackFileName);
		if(settings.generateLowLevel) {
			baserouteTree = new RTree(rTreeRouteIndexPackFileName+"b");
		}
	}

	public void packRtreeFiles(String rTreeRouteIndexNonPackFileName, String rTreeRouteIndexPackFileName) throws IOException {
		routeTree = packRtreeFile(routeTree, rTreeRouteIndexNonPackFileName, rTreeRouteIndexPackFileName);
		if (settings.generateLowLevel) {
			baserouteTree = packRtreeFile(baserouteTree, rTreeRouteIndexNonPackFileName + "b",
					rTreeRouteIndexPackFileName + "b");
		}
	}

	public void writeBinaryRouteIndex(File fl, BinaryMapIndexWriter writer, String regionName, boolean generateLowLevel) throws IOException, SQLException {
		closePreparedStatements(mapRouteInsertStat);
		if (basemapRouteInsertStat != null) {
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
		TLongObjectHashMap<RouteDataObject> map = new ImproveRoadConnectivity().collectDisconnectedRoads(reader);
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
		if(!settings.generateLowLevel) {
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
		int fromy31 = gn.py.get(st);
		int fromx31 = gn.px.get(st);
		int toy31 = gn.py.get(end);
		int tox31 = gn.px.get(end);
		double mDist = ((double)fromy31 - toy31) * ((double)fromy31 - toy31) +
				((double)fromx31 - tox31) * ((double)fromx31 - tox31);
		double projection = scalarMultiplication(fromy31, fromx31, toy31, tox31, py, px);
		if (returnNanIfNoProjection && (projection < 0 || projection > mDist)) {
			return Double.NaN;
		}
//		float projy31 = fromy31 + (toy31 - fromy31) * (projection / mDist);
//		float projx31 = fromx31 + (tox31 - fromx31) * (projection / mDist);
		double A = MapUtils.squareRootDist31(px, py, fromx31, py);
		double B = MapUtils.squareRootDist31(px, py, px, fromy31);
		double C = MapUtils.squareRootDist31(tox31, toy31, fromx31, toy31);
		double D = MapUtils.squareRootDist31(tox31, toy31, tox31, fromy31);
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

	public static class RouteWriteContext {
		PreparedStatement selectData ;
		TLongObjectHashMap<RouteDataObject> objects;

		TLongObjectHashMap<BinaryFileReference> treeHeader;
		Log logMapDataWarn;
		private MapRoutingTypes routeTypes;

		TLongObjectHashMap<List<RestrictionInfo>> highwayRestrictions = new TLongObjectHashMap<List<RestrictionInfo>>();
		TLongObjectHashMap<RouteMissingPoints> basemapNodesToReinsert = new TLongObjectHashMap<RouteMissingPoints> ();

		public RouteWriteContext(Log logMapDataWarn, TLongObjectHashMap<BinaryFileReference> treeHeader, MapRoutingTypes routeTypes,
				PreparedStatement selectData) {
			this.logMapDataWarn = logMapDataWarn;
			this.treeHeader = treeHeader;
			this.routeTypes = routeTypes;
			this.selectData = selectData;

		}

		public RouteWriteContext(Log logMapDataWarn, TLongObjectHashMap<BinaryFileReference> treeHeader, MapRoutingTypes routeTypes,
				TLongObjectHashMap<RouteDataObject> objects) {
			this.logMapDataWarn = logMapDataWarn;
			this.treeHeader = treeHeader;
			this.routeTypes = routeTypes;
			this.objects = objects;

			for(RouteDataObject o : objects.valueCollection()){
				List<RestrictionInfo> list = new ArrayList<>();
				for(int k = 0 ; k < o.getRestrictionLength(); k++) {
					list.add(o.getRestrictionInfo(k));
				}
				highwayRestrictions.put(o.getId(), list);
			}
		}


		Map<String, Integer> stringTable = new LinkedHashMap<String, Integer>();
		Map<MapRouteType, String> wayNames = createTreeMap();
		List<MapPointName> pointNames = new ArrayList<MapRoutingTypes.MapPointName>();
		TLongArrayList wayMapIds = new TLongArrayList();
		TLongObjectHashMap<Integer> wayMapIdsCache = new TLongObjectHashMap<>();

		TLongArrayList pointMapIds = new TLongArrayList();
		int[] wayTypes;
		private ArrayList<RoutePointToWrite> points;

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


		public int registerWayMapId(long id) {
			if(!wayMapIdsCache.contains(id)) {
				wayMapIdsCache.put(id, wayMapIdsCache.size());
				wayMapIds.add(id);
			}
//			int oldId = registerId(wayMapIds, id);
			int newId = wayMapIdsCache.get(id);
//			if(oldId != newId) {
//				throw new IllegalArgumentException();
//			}
			return newId;
		}

		public boolean retrieveObject(long id) throws SQLException {
			if (selectData != null) {
				selectData.setLong(1, id);
				ResultSet rs = selectData.executeQuery();
				boolean next = rs.next();
				if(next) {
					wayNames.clear();
					decodeNames(rs.getString(5), wayNames);

					byte[] types = rs.getBytes(1);
					this.wayTypes = new int[types.length / 2];
					for (int j = 0; j < types.length; j += 2) {
						int ids = Algorithms.parseSmallIntFromBytes(types, j);
						wayTypes[j / 2] = routeTypes.getTypeByInternalId(ids).getTargetId();
					}

					byte[] pointTypes = rs.getBytes(2);
					//byte[] pointIds = rs.getBytes(3);
					byte[] pointCoordinates = rs.getBytes(4);
					String pointNamesString = rs.getString(6);
					pointNames.clear();
					decodeListNames(pointNamesString, pointNames);

					int pointsLength = pointCoordinates.length / 8;
					RouteMissingPoints missingPoints = null;
					if(basemapNodesToReinsert != null && basemapNodesToReinsert.containsKey(id) ) {
						missingPoints = basemapNodesToReinsert.get(id);
					}

					int typeInd = 0;
					points = new ArrayList<RoutePointToWrite>(pointsLength);
					for (int j = 0; j < pointsLength; j++) {
						if(missingPoints != null && j < missingPoints.pointsMap.size() &&
								missingPoints.pointsMap.get(j) != null) {
							for(Long loc: missingPoints.pointsMap.get(j).values()) {
								RoutePointToWrite point = new RoutePointToWrite();
								point.x = (int) (loc >> 31);
								point.y = (int) (loc - (point.x << 31));
								points.add(point);
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
				}
				return next;
			} else {
				RouteDataObject rdo = objects.get(id);
				if (rdo != null) {
					wayTypes = rdo.types;
					pointNames.clear();
					if (rdo.pointNames != null) {
						for (int i = 0; i < rdo.pointNames.length; i++) {
							if (rdo.pointNames[i] != null) {
								for (int j = 0; j < rdo.pointNames[i].length; j++) {
									MapPointName mpn = new MapPointName(rdo.pointNameTypes[i][j], i, rdo.pointNames[i][j]);
									pointNames.add(mpn);
								}
							}
						}
					}

					wayNames.clear();
					if (rdo.names != null) {
						TIntObjectIterator<String> it = rdo.names.iterator();
						while (it.hasNext()) {
							it.advance();
							int vl = it.key();
							String value = it.value();
							RouteTypeRule rtr = rdo.region.quickGetEncodingRule(vl);
							MapRouteType mrt = new MapRouteType(vl, rtr.getTag(), rtr.getValue());
							wayNames.put(mrt, value);
						}
					}

					points = new ArrayList<>(rdo.pointsX.length);
					for (int i = 0; i < rdo.pointsX.length; i++) {
						RoutePointToWrite rw = new RoutePointToWrite();
						rw.x = rdo.pointsX[i];
						rw.y = rdo.pointsY[i];
						if (rdo.pointTypes != null &&
								i < rdo.pointTypes.length && rdo.pointTypes[i] != null) {
							rw.types.addAll(rdo.pointTypes[i]);
						}
						points.add(rw);
					}
					return true;
				}
				return false;
			}

		}

	}

	private void writeBinaryRouteIndexBlocks(BinaryMapIndexWriter writer, RTree rte, boolean basemap,
			TLongObjectHashMap<BinaryFileReference> treeHeader) throws IOException, SQLException, RTreeException {

		// write map levels and map index
		long rootIndex = rte.getFileHdr().getRootIndex();
		rtree.Node root = rte.getReadNode(rootIndex);
		Rect rootBounds = calcBounds(root);
		if (rootBounds != null) {
				PreparedStatement selectData = mapConnection.prepareStatement(basemap ? SELECT_BASE_STAT : SELECT_STAT);
				RouteWriteContext wc = new RouteWriteContext(logMapDataWarn, treeHeader, routeTypes, selectData);
				wc.highwayRestrictions = highwayRestrictions;
				if(basemap) {
					wc.basemapNodesToReinsert = basemapNodesToReinsert;
				}
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



	public static void writeBinaryMapBlock(rtree.Node parent, Rect parentBounds, RTree r, BinaryMapIndexWriter writer, RouteWriteContext wc, boolean basemap)
					throws IOException, RTreeException, SQLException {
		Element[] e = parent.getAllElements();

		RouteDataBlock.Builder dataBlock = null;
		BinaryFileReference ref = wc.treeHeader.get(parent.getNodeIndex());
		wc.wayMapIds.clear();
		wc.wayMapIdsCache.clear();
		wc.pointMapIds.clear();
        List<Long> restrictionVia = new ArrayList<>();
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < parent.getTotalElements(); i++) {
            if (e[i].getElementType() == rtree.Node.LEAF_NODE) {
                long id = e[i].getPtr();
                List<RestrictionInfo> restrictions = wc.highwayRestrictions.get(id);
                if (restrictions != null) {
                    for (int li = 0; li < restrictions.size(); li++) {
                        RestrictionInfo rd = restrictions.get(li);
                        if(rd.viaWay != 0) {
                            restrictionVia.add(rd.viaWay);
                        }
                    }
                }
                ids.add(id);
            }
        }
        if (!restrictionVia.isEmpty() && !ids.isEmpty()) {
            // set restrictionVia to the end of idTable (val.viaWay != 0)
            if (restrictionVia.contains(ids.get(0))) {
                Collections.swap(ids, 0, ids.size() - 1);
            }
        }
        for (long id : ids) {
            // IndexRouteCreator.SELECT_STAT;
            // "SELECT types, pointTypes, pointIds, pointCoordinates, name FROM route_objects WHERE id = ?"
            boolean retrieveObject = wc.retrieveObject(id);
            if (retrieveObject) {
                if (dataBlock == null) {
                    dataBlock = RouteDataBlock.newBuilder();
                    wc.stringTable.clear();
                    wc.wayMapIds.clear();
                    wc.wayMapIdsCache.clear();
                    wc.pointMapIds.clear();
                }
                int cid = wc.registerWayMapId(id);
                List<RestrictionInfo> restrictions = wc.highwayRestrictions.get(id);
                if (!basemap && restrictions != null) {
                    for (int li = 0; li < restrictions.size(); li++) {
                        RestrictionInfo rd = restrictions.get(li);
                        Builder restriction = RestrictionData.newBuilder();
                        restriction.setFrom(cid);
                        int toId = wc.registerWayMapId(rd.toWay);
                        restriction.setTo(toId);
                        restriction.setType(rd.type);
                        if (rd.viaWay != 0) {
                            int viaId = wc.registerWayMapId(rd.viaWay);
                            restriction.setVia(viaId);
                        }
                        dataBlock.addRestrictions(restriction.build());
                    }
                }
                RouteData routeData = writer.writeRouteData(cid, parentBounds.getMinX(), parentBounds.getMinY(), wc.wayTypes,
                        wc.points.toArray(new RoutePointToWrite[wc.points.size()]),
                        wc.wayNames, wc.stringTable, wc.pointNames, dataBlock, true, false);
                if (routeData != null) {
                    dataBlock.addDataObjects(routeData);
                }
            } else {
                if (wc.logMapDataWarn != null) {
                    wc.logMapDataWarn.error("Something goes wrong with id = " + id); //$NON-NLS-1$
                } else {
                    System.err.println("Something goes wrong with id = " + id);
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
			writer.writeRouteDataBlock(dataBlock, wc.stringTable, ref);
		}
		for (int i = 0; i < parent.getTotalElements(); i++) {
			if (e[i].getElementType() != rtree.Node.LEAF_NODE) {
				long ptr = e[i].getPtr();
				rtree.Node ns = r.getReadNode(ptr);
				writeBinaryMapBlock(ns, e[i].getRect(), r, writer, wc, basemap);
			}
		}
	}

	public static void writeBinaryRouteTree(rtree.Node parent, Rect re, RTree r, BinaryMapIndexWriter writer,
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
