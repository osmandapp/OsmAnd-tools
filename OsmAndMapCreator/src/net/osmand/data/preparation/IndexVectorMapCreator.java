package net.osmand.data.preparation;


import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;
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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import net.osmand.IProgress;
import net.osmand.binary.OsmandOdb.MapData;
import net.osmand.binary.OsmandOdb.MapDataBlock;
import net.osmand.data.Multipolygon;
import net.osmand.data.MultipolygonBuilder;
import net.osmand.data.Ring;
import net.osmand.data.preparation.MapZooms.MapZoomPair;
import net.osmand.osm.MapRenderingTypes;
import net.osmand.osm.MapRenderingTypes.MapRulType;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.OSMSettings.OSMTagKey;
import net.osmand.osm.edit.OsmMapUtils;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Way;
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

public class IndexVectorMapCreator extends AbstractIndexPartCreator {

	// map zoom levels <= 2^MAP_LEVELS
	private static final int MAP_LEVELS_POWER = 3;
	private static final int MAP_LEVELS_MAX = 1 << MAP_LEVELS_POWER;
	private MapRenderingTypesEncoder renderingTypes;
	private MapZooms mapZooms;
	final static String SPLIT_VALUE= "SPLITVL";

	Map<Long, TIntArrayList> multiPolygonsWays = new LinkedHashMap<Long, TIntArrayList>();
	
	// local purpose to speed up processing cache allocation
	TIntArrayList typeUse = new TIntArrayList(8);
	List<MapRulType> tempNameUse = new ArrayList<MapRulType>();
	Comparator<MapRulType> comparator = new Comparator<MapRulType>() {

		@Override
		public int compare(MapRulType o1, MapRulType o2) {
			int lhs = o1.getOrder();
			int rhs = o2.getOrder();
			if(lhs == rhs) {
				lhs = o1.getInternalId();
				rhs = o2.getInternalId();
			}
			return lhs < rhs ? -1 : (lhs == rhs ? 0 : 1);
		}
	};
	TreeMap<MapRulType, String> namesUse = new TreeMap<MapRulType, String>(comparator);
	Map<EntityId, Map<String, String>> propogatedTags = new LinkedHashMap<Entity.EntityId, Map<String, String>>();
	TIntArrayList addtypeUse = new TIntArrayList(8);

	private PreparedStatement mapBinaryStat;
	private PreparedStatement mapLowLevelBinaryStat;
	private int lowLevelWays = -1;
	private RTree[] mapTree = null;
	private Connection mapConnection;

	private int zoomWaySmothness = 0;
	private final Log logMapDataWarn;

	public static long GENERATE_OBJ_ID = - (1l << 20l); // million million  

	public IndexVectorMapCreator(Log logMapDataWarn, MapZooms mapZooms, MapRenderingTypesEncoder renderingTypes,
	                             int zoomWaySmothness) {
		this.logMapDataWarn = logMapDataWarn;
		this.mapZooms = mapZooms;
		this.zoomWaySmothness = zoomWaySmothness;
		this.renderingTypes = renderingTypes;
		lowLevelWays = -1;
	}

	public void indexMapRelationsAndMultiPolygons(Entity e, OsmDbAccessorContext ctx) throws SQLException {
		indexMultiPolygon(e, ctx);
		addPropogatedTags(propogatedTags, renderingTypes, e, ctx);
	}

	public static void addPropogatedTags(Map<EntityId, Map<String, String>> propogatedTags, MapRenderingTypesEncoder renderingTypes, Entity e, OsmDbAccessorContext ctx) throws SQLException {
		if(e instanceof Relation) {
			Map<MapRulType, String> propogated = renderingTypes.getRelationPropogatedTags(((Relation)e));
			if(propogated != null && propogated.size() > 0) {
				ctx.loadEntityRelation((Relation) e);
				for(EntityId id : ((Relation) e).getMembersMap().keySet()) {
					if(!propogatedTags.containsKey(id)) {
						propogatedTags.put(id, new LinkedHashMap<String, String>());
					}
					Map<String, String> map = propogatedTags.get(id);
					Iterator<Entry<MapRulType, String>> it = propogated.entrySet().iterator();
					while(it.hasNext()) {
						Entry<MapRulType, String> es = it.next();
						String key = es.getKey().getTag();
						if(es.getKey().isText() && map.containsKey(key)) {
							String res = sortAndAttachUniqueValue(map.get(key), es.getValue());
							map.put(key, res);
						} else {
							map.put(key, es.getValue());
						}
					}
				}
			}
		}
	}

	private static String sortAndAttachUniqueValue(String list, String value) {
		String[] ls = list.split(SPLIT_VALUE);
		Set<String> set = new TreeSet<String>(new Comparator<String>() {

			@Override
			public int compare(String s1, String s2) {
				int i1 = Algorithms.extractFirstIntegerNumber(s1);
				int i2 = Algorithms.extractFirstIntegerNumber(s2);
				if(i1 == i2) {
					String t1 = Algorithms.extractIntegerSuffix(s1);
					String t2 = Algorithms.extractIntegerSuffix(s2);
					return t1.compareTo(t2);
				}
				return i1 - i2;
			}
		});
		set.add(value);
		for(String l : ls) {
			set.add(l.trim());
		}
		String r = "";
		for(String a : set) {
			if(r.length() > 0) {
				r += SPLIT_VALUE;
			}
			r+= a;
		}
		return r;
	}

	/**
	 * index a multipolygon into the database
	 * only multipolygons without admin_level and with type=multipolygon are indexed
	 * broken multipolygons are also indexed, inner ways are sometimes left out, broken rings are split and closed
	 * broken multipolygons will normally be logged
	 * @param e the entity to index
	 * @param ctx the database context
	 * @throws SQLException
	 */
	private void indexMultiPolygon(Entity e, OsmDbAccessorContext ctx) throws SQLException {
		// Don't handle things that aren't multipolygon, and nothing administrative
		if (!(e instanceof Relation) || !"multipolygon".equals(e.getTag(OSMTagKey.TYPE))
				|| e.getTag(OSMTagKey.ADMIN_LEVEL) != null)
			return;
		MultipolygonBuilder original = createMultipolygonBuilder(e, ctx);
		try {
			renderingTypes.encodeEntityWithType(false, e.getModifiableTags(), mapZooms.getLevel(0).getMaxZoom(), typeUse, addtypeUse, namesUse, tempNameUse);
		} catch (RuntimeException es) {
			es.printStackTrace();
			return;
		}
		List<Map<String, String>> splitEntities = renderingTypes.splitTags(e.getModifiableTags(), EntityType.valueOf(e));

		// Don't add multipolygons with an unknown type
		if (typeUse.size() == 0)
			return;
		excludeFromMainIteration(original.getOuterWays());
		excludeFromMainIteration(original.getInnerWays());

		
		// Rings with different types (inner or outer) in one ring will be logged in the previous case
		// The Rings are only composed by type, so if one way gets in a different Ring, the rings will be incomplete
		List<Multipolygon> multipolygons = original.splitPerOuterRing(logMapDataWarn);


		for (Multipolygon m : multipolygons) {
			assert m.getOuterRings().size() == 1;
			// Log the fact that Rings aren't complete, but continue with the relation, try to close it as well as possible
			if (!m.areRingsComplete()) {
				logMapDataWarn.warn("In multipolygon  " + e.getId() + " there are incompleted ways");
			}
			Ring out = m.getOuterRings().get(0);
			if(out.getBorder().size() == 0) {
				logMapDataWarn.warn("Multipolygon has an outer ring that can't be formed: "+e.getId());
				// don't index this
				continue;
			}

			// innerWays are new closed ways 
			List<List<Node>> innerWays = new ArrayList<List<Node>>();

			for (Ring r : m.getInnerRings()) {
				innerWays.add(r.getBorder());
			}

			// don't use the relation ids. Create new onesgetInnerRings
			Map<String, String> stags  = splitEntities == null ? e.getModifiableTags() : splitEntities.get(0);
			createMultipolygonObject(stags, out, innerWays, GENERATE_OBJ_ID --);
			if (splitEntities != null) {
				for (int i = 1; i < splitEntities.size(); i++) {
					Map<String, String> tags = splitEntities.get(i);
					createMultipolygonObject(tags, out, innerWays,	GENERATE_OBJ_ID--);
				}
			}
		}
	}

	protected void createMultipolygonObject(Map<String, String> tags, Ring out, List<List<Node>> innerWays, long baseId)
			throws SQLException {
		nextZoom: for (int level = 0; level < mapZooms.size(); level++) {
			renderingTypes.encodeEntityWithType(false,  tags, mapZooms.getLevel(level).getMaxZoom(), typeUse, addtypeUse, namesUse,
					tempNameUse);
			if (typeUse.isEmpty()) {
				continue;
			}
			long id = convertBaseIdToGeneratedId(baseId, level);
			// simplify route
			List<Node> outerWay = out.getBorder();
			int zoomToSimplify = mapZooms.getLevel(level).getMaxZoom() - 1;
			if (zoomToSimplify < 15) {
				outerWay = simplifyCycleWay(outerWay, zoomToSimplify, zoomWaySmothness);
				if (outerWay == null) {
					continue nextZoom;
				}
				List<List<Node>> newinnerWays = new ArrayList<List<Node>>();
				for (List<Node> ls : innerWays) {
					ls = simplifyCycleWay(ls, zoomToSimplify, zoomWaySmothness);
					if (ls != null) {
						newinnerWays.add(ls);
					}
				}
				innerWays = newinnerWays;
			}
			insertBinaryMapRenderObjectIndex(mapTree[level], outerWay, innerWays, namesUse, id, true, typeUse, addtypeUse, true);

		}
	}

	private MultipolygonBuilder createMultipolygonBuilder(Entity e, OsmDbAccessorContext ctx) throws SQLException {
		ctx.loadEntityRelation((Relation) e);
		Map<Entity, String> entities = ((Relation) e).getMemberEntities();

		// create a multipolygon object for this
		MultipolygonBuilder original = new MultipolygonBuilder();
		original.setId(e.getId());

		// fill the multipolygon with all ways from the Relation
		for (Entity es : entities.keySet()) {
			if (es instanceof Way) {
				boolean inner = "inner".equals(entities.get(es)); //$NON-NLS-1$
				if (inner) {
					original.addInnerWay((Way) es);
				} else if("outer".equals(entities.get(es))){
					original.addOuterWay((Way) es);
				}
			}
		}
		return original;
	}

	private void excludeFromMainIteration(List<Way> l) {
		for(Way w : l) {
			if(!multiPolygonsWays.containsKey(w.getId())) {
				multiPolygonsWays.put(w.getId(), new TIntArrayList());
			}
			multiPolygonsWays.get(w.getId()).addAll(typeUse);
		}
	}
	
	public static List<Node> simplifyCycleWay(List<Node> ns, int zoom, int zoomWaySmothness) throws SQLException {
		if (checkForSmallAreas(ns, zoom + Math.min(zoomWaySmothness / 2, 3), 2, 4)) {
			return null;
		}
		List<Node> res = new ArrayList<Node>();
		// simplification
		OsmMapUtils.simplifyDouglasPeucker(ns, zoom + 8 + zoomWaySmothness, 3, res, false);
		if (res.size() < 2) {
			return null;
		}
		return res;
	}

	public int getLowLevelWays() {
		return lowLevelWays;
	}

	private void loadNodes(byte[] nodes, List<Float> toPut) {
		toPut.clear();
		for (int i = 0; i < nodes.length;) {
			int lat = Algorithms.parseIntFromBytes(nodes, i);
			i += 4;
			int lon = Algorithms.parseIntFromBytes(nodes, i);
			i += 4;
			toPut.add(Float.intBitsToFloat(lat));
			toPut.add(Float.intBitsToFloat(lon));
		}
	}
	
	private void parseAndSort(TIntArrayList ts, byte[] bs) {
		ts.clear();
		if (bs != null && bs.length > 0) {
			for (int j = 0; j < bs.length; j += 2) {
				ts.add(Algorithms.parseSmallIntFromBytes(bs, j));
			}
		}
		ts.sort();
	}
	
	private static class LowLevelWayCandidate {
		public byte[] nodes;
		public long wayId;
		public long otherNodeId;
		public Map<MapRulType, String> names;
		public int namesCount = 0;
		
		
	}
	
	public List<LowLevelWayCandidate> readLowLevelCandidates(ResultSet fs, 
			List<LowLevelWayCandidate> l, TIntArrayList temp, TIntArrayList tempAdd, TLongHashSet visitedWays) throws SQLException {
		l.clear();
		while (fs.next()) {
			if (!visitedWays.contains(fs.getLong(1))) {
				parseAndSort(temp, fs.getBytes(5));
				parseAndSort(tempAdd, fs.getBytes(6));
				if(temp.equals(typeUse) && tempAdd.equals(addtypeUse)){
					LowLevelWayCandidate llwc = new LowLevelWayCandidate();
					llwc.wayId = fs.getLong(1);
					llwc.names = decodeNames(fs.getString(4), new HashMap<MapRulType, String>());
					llwc.nodes = fs.getBytes(3);
					llwc.otherNodeId = fs.getLong(2);
					for(MapRulType mr : namesUse.keySet()) {
						if(Algorithms.objectEquals(namesUse.get(mr), llwc.names.get(mr))) {
							llwc.namesCount++;
						}
					}
					l.add(llwc);
				}
			}
		}	
		return l;
	}

	public void processingLowLevelWays(IProgress progress) throws SQLException {
		mapLowLevelBinaryStat.executeBatch();
		mapLowLevelBinaryStat.close();
		pStatements.remove(mapLowLevelBinaryStat);
		mapLowLevelBinaryStat = null;
		mapConnection.commit();

		PreparedStatement startStat = mapConnection.prepareStatement("SELECT id, end_node, nodes, name, type, addType FROM low_level_map_objects"
				+ " WHERE start_node = ? AND level = ?");
		PreparedStatement endStat = mapConnection.prepareStatement("SELECT id, start_node, nodes, name, type, addType FROM low_level_map_objects"
				+ " WHERE end_node = ? AND level = ?");
		Statement selectStatement = mapConnection.createStatement();
		ResultSet rs = selectStatement.executeQuery("SELECT id, start_node, end_node, nodes, name, type, addType, level FROM low_level_map_objects");
		TLongHashSet visitedWays = new TLongHashSet();
		ArrayList<Float> list = new ArrayList<Float>(100);
		TIntArrayList temp = new TIntArrayList();
		TIntArrayList tempAdd = new TIntArrayList();
		while (rs.next()) {
			if (lowLevelWays != -1) {
				progress.progress(1);
			}
			long id = rs.getLong(1);
			if (visitedWays.contains(id)) {
				continue;
			}
			visitedWays.add(id);
			
			int level = rs.getInt(8);
			int zoom = mapZooms.getLevel(level).getMaxZoom();

			long startNode = rs.getLong(2);
			long endNode = rs.getLong(3);

			namesUse.clear();
			decodeNames(rs.getString(5), namesUse);
			parseAndSort(typeUse, rs.getBytes(6));
			parseAndSort(addtypeUse, rs.getBytes(7));
			
			loadNodes(rs.getBytes(4), list);
			ArrayList<Float> wayNodes = new ArrayList<Float>(list);

			// combine startPoint with EndPoint
			boolean combined = true;
			List<LowLevelWayCandidate> candidates = new ArrayList<LowLevelWayCandidate>();
			Comparator<LowLevelWayCandidate> cmpCandidates = new Comparator<LowLevelWayCandidate>() {
				@Override
				public int compare(LowLevelWayCandidate o1, LowLevelWayCandidate o2) {
					return -Integer.compare(o1.namesCount, o2.namesCount);
				}
			};
			
			while (combined) {
				combined = false;
				endStat.setLong(1, startNode);
				endStat.setShort(2, (short) level);
				ResultSet fs = endStat.executeQuery();
				readLowLevelCandidates(fs, candidates, temp, tempAdd, visitedWays);
				fs.close();
				LowLevelWayCandidate cand = getCandidate(candidates, cmpCandidates);
				if (cand != null) {
					combined = true;
					startNode = cand.otherNodeId;
					visitedWays.add(cand.wayId);
					loadNodes(cand.nodes, list);
					ArrayList<Float> li = new ArrayList<Float>(list);
					// remove first lat/lon point
					wayNodes.remove(0);
					wayNodes.remove(0);
					li.addAll(wayNodes);
					wayNodes = li;
					for (MapRulType rt : new ArrayList<MapRulType>(namesUse.keySet())) {
						if (!Algorithms.objectEquals(namesUse.get(rt), cand.names.get(rt))) {
							namesUse.remove(rt);
						}
					}
				}
			}

			// combined end point
			combined = true;
			while (combined) {
				combined = false;
				startStat.setLong(1, endNode);
				startStat.setShort(2, (short) level);
				ResultSet fs = startStat.executeQuery();
				readLowLevelCandidates(fs, candidates, temp, tempAdd, visitedWays);
				fs.close();
				LowLevelWayCandidate cand = getCandidate(candidates, cmpCandidates);
				if (cand != null) {
					combined = true;
					endNode = cand.otherNodeId;
					visitedWays.add(cand.wayId);
					loadNodes(cand.nodes, list);
					for (int i = 2; i < list.size(); i++) {
						wayNodes.add(list.get(i));
					}
					for (MapRulType rt : new ArrayList<MapRulType>(namesUse.keySet())) {
						if (!Algorithms.objectEquals(namesUse.get(rt), cand.names.get(rt))) {
							namesUse.remove(rt);
						}
					}
				}
			}

			List<Node> wNodes = new ArrayList<Node>();
			int wNsize = wayNodes.size();
			for (int i = 0; i < wNsize; i += 2) {
				wNodes.add(new Node(wayNodes.get(i), wayNodes.get(i + 1), i == 0 ? startNode : endNode));
			}
			boolean skip = false;
			boolean cycle = startNode == endNode;
			if (cycle) {
				skip = checkForSmallAreas(wNodes, zoom  + Math.min(zoomWaySmothness / 2, 3), 3, 4);
			} else {
				// coastline
				if(!typeUse.contains(renderingTypes.getCoastlineRuleType().getInternalId())) {
					skip = checkForSmallAreas(wNodes, zoom  + Math.min(zoomWaySmothness / 2, 3), 2, 8);
				}
			}
			if (!skip) {
				List<Node> res = new ArrayList<Node>();
				OsmMapUtils.simplifyDouglasPeucker(wNodes, zoom - 1 + 8 + zoomWaySmothness, 3, res, false);
				if (res.size() > 0) {
					insertBinaryMapRenderObjectIndex(mapTree[level], res, null, namesUse, id, false, typeUse, addtypeUse, false);
				}
			}
			
			// end cycle 

		}

	}

	private LowLevelWayCandidate getCandidate(List<LowLevelWayCandidate> candidates,
			Comparator<LowLevelWayCandidate> cmpCandidates) {
		if(candidates.size() > 0) {
			Collections.sort(candidates, cmpCandidates);
			LowLevelWayCandidate cand = candidates.get(0);
			if(cand.namesCount > 0) {
				return cand;
			}
			if(cand.names.isEmpty() && namesUse.isEmpty()) {
				return cand;
			}
			
		}
		return null;
	}

	public static boolean checkForSmallAreas(List<Node> nodes, int zoom, int minz, int maxz) {
		int minX = Integer.MAX_VALUE;
		int maxX = Integer.MIN_VALUE;
		int minY = Integer.MAX_VALUE;
		int maxY = Integer.MIN_VALUE;
		int c = 0;
		int nsize = nodes.size();
		for (int i = 0; i < nsize; i++) {
			if (nodes.get(i) != null) {
				c++;
				int x = (int) (MapUtils.getTileNumberX(zoom, nodes.get(i).getLongitude()) * 256.0d);
				int y = (int) (MapUtils.getTileNumberY(zoom, nodes.get(i).getLatitude()) * 256.0d);
				minX = Math.min(minX, x);
				maxX = Math.max(maxX, x);
				minY = Math.min(minY, y);
				maxY = Math.max(maxY, y);
			}
		}
		if (c < 2) {
			return true;
		}
		return ((maxX - minX) <= minz && (maxY - minY) <= maxz) || ((maxX - minX) <= maxz && (maxY - minY) <= minz);

	}

	public void iterateMainEntity(Entity e, OsmDbAccessorContext ctx) throws SQLException {
		if (e instanceof Way || e instanceof Node) {
			EntityId eid = EntityId.valueOf(e);
			Map<String, String> proptags = propogatedTags.get(eid);
			if (proptags != null) {
				Iterator<Entry<String, String>> iterator = proptags.entrySet().iterator();
				while (iterator.hasNext()) {
					Entry<String, String> ts = iterator.next();
					if (e.getTag(ts.getKey()) == null) {
						String vl = ts.getValue();
						if(vl != null) {
							vl = vl.replaceAll(SPLIT_VALUE, ", ");
						}
						e.putTag(ts.getKey(), vl);
					}
				}
			}
			// manipulate what kind of way to load
			long originalId = e.getId();
			long assignedId = e.getId();
			List<Map<String, String>> splitTags = renderingTypes.splitTags(e.getTags(), EntityType.valueOf(e));
			Map<String, String> tags = splitTags == null ?  e.getModifiableTags() : splitTags.get(0);
			for (int level = 0; level < mapZooms.size(); level++) {
				processMainEntity(e, originalId, assignedId, level, tags);
			}
			if (splitTags != null) {
				for (int i = 1; i < splitTags.size(); i++) {
					assignedId = GENERATE_OBJ_ID--;
					Map<String, String> stags = splitTags.get(i);
					for (int level = 0; level < mapZooms.size(); level++) {
						processMainEntity(e, originalId, assignedId, level, stags);
					}
				}
			}
		}
	}

	protected void processMainEntity(Entity e, long originalId, long assignedId, int level, Map<String, String> tags)
			throws SQLException {
		boolean area = renderingTypes.encodeEntityWithType(e instanceof Node, 
				tags, mapZooms.getLevel(level).getMaxZoom(), typeUse, addtypeUse, namesUse,
				tempNameUse);
		if (typeUse.isEmpty()) {
			return;
		}
		boolean hasMulti = e instanceof Way && multiPolygonsWays.containsKey(originalId);
		if (hasMulti) {
			TIntArrayList set = multiPolygonsWays.get(originalId);
			typeUse.removeAll(set);
		}
		if (typeUse.isEmpty()) {

			return;
		}
		long id = convertBaseIdToGeneratedId(assignedId, level);
		List<Node> res = null;
		if (e instanceof Node) {
			res = Collections.singletonList((Node) e);
		} else {
			id |= 1;

			// simplify route id>>1
			boolean mostDetailedLevel = level == 0;
			if (!mostDetailedLevel) {
				int zoomToSimplify = mapZooms.getLevel(level).getMaxZoom() - 1;
				boolean cycle = ((Way) e).getFirstNodeId() == ((Way) e).getLastNodeId();
				if (cycle) {
					res = simplifyCycleWay(((Way) e).getNodes(), zoomToSimplify, zoomWaySmothness);
				} else {
					insertLowLevelMapBinaryObject(level, zoomToSimplify, typeUse, addtypeUse, id, ((Way) e).getNodes(), namesUse);
				}
			} else {
				res = ((Way) e).getNodes();
			}
		}
		if (res != null) {
			insertBinaryMapRenderObjectIndex(mapTree[level], res, null, namesUse, id, area, typeUse, addtypeUse, true);
		}
	}

	

	public void writeBinaryMapIndex(BinaryMapIndexWriter writer, String regionName) throws IOException, SQLException {
		closePreparedStatements(mapBinaryStat, mapLowLevelBinaryStat);
		mapConnection.commit();
		try {
			writer.startWriteMapIndex(regionName);
			// write map encoding rules
			writer.writeMapEncodingRules(renderingTypes.getEncodingRuleTypes());
			
			PreparedStatement selectData = mapConnection
					.prepareStatement("SELECT area, coordinates, innerPolygons, types, additionalTypes, name FROM binary_map_objects WHERE id = ?");

			// write map levels and map index
			TLongObjectHashMap<BinaryFileReference> treeHeader = new TLongObjectHashMap<BinaryFileReference>();
			for (int i = 0; i < mapZooms.size(); i++) {
				RTree rtree = mapTree[i];
				long rootIndex = rtree.getFileHdr().getRootIndex();
				rtree.Node root = rtree.getReadNode(rootIndex);
				Rect rootBounds = calcBounds(root);
				if (rootBounds != null) {
					writer.startWriteMapLevelIndex(mapZooms.getLevel(i).getMinZoom(), mapZooms.getLevel(i).getMaxZoom(),
							rootBounds.getMinX(), rootBounds.getMaxX(), rootBounds.getMinY(), rootBounds.getMaxY());
					writeBinaryMapTree(root, rootBounds, rtree, writer, treeHeader);
					
					writeBinaryMapBlock(root,  rootBounds, rtree, writer, selectData, treeHeader, new LinkedHashMap<String, Integer>(),
								new LinkedHashMap<MapRulType, String>(), mapZooms.getLevel(i));

					writer.endWriteMapLevelIndex();
				}
			}


			selectData.close();

			writer.endWriteMapIndex();
			writer.flush();
		} catch (RTreeException e) {
			throw new IllegalStateException(e);
		}
	}

	

	private long convertBaseIdToGeneratedId(long baseId, int level) {
		if (level >= MAP_LEVELS_MAX) {
			throw new IllegalArgumentException("Number of zoom levels " + level + " exceeds allowed maximum : " + MAP_LEVELS_MAX);
		}
		return ((baseId << MAP_LEVELS_POWER) | level) << 1;
	}

	public long convertGeneratedIdToObfWrite(long id) {
		return (id >> (MAP_LEVELS_POWER)) + (id & 1);
	}

	private static final char SPECIAL_CHAR = ((char) 0x60000);

	private String encodeNames(Map<MapRulType, String> tempNames) {
		StringBuilder b = new StringBuilder();
		for (Map.Entry<MapRulType, String> e : tempNames.entrySet()) {
			if (e.getValue() != null) {
				b.append(SPECIAL_CHAR).append((char)e.getKey().getInternalId()).append(e.getValue());
			}
		}
		return b.toString();
	}

	private Map<MapRulType, String> decodeNames(String name, Map<MapRulType, String> tempNames) {
		int i = name.indexOf(SPECIAL_CHAR);
		while (i != -1) {
			int n = name.indexOf(SPECIAL_CHAR, i + 2);
			int ch = (short) name.charAt(i + 1);
			MapRulType rt = renderingTypes.getTypeByInternalId(ch);
			if (n == -1) {
				tempNames.put(rt, name.substring(i + 2));
			} else {
				tempNames.put(rt, name.substring(i + 2, n));
			}
			i = n;
		}
		return tempNames;
	}

	public void writeBinaryMapBlock(rtree.Node parent, Rect parentBounds, RTree r, BinaryMapIndexWriter writer, PreparedStatement selectData,
			TLongObjectHashMap<BinaryFileReference> bounds, Map<String, Integer> tempStringTable, LinkedHashMap<MapRulType, String> tempNames, MapZoomPair level)
			throws IOException, RTreeException, SQLException {
		Element[] e = parent.getAllElements();

		MapDataBlock.Builder dataBlock = null;
		BinaryFileReference ref = bounds.get(parent.getNodeIndex());
		long baseId = 0;
		for (int i = 0; i < parent.getTotalElements(); i++) {
			if (e[i].getElementType() == rtree.Node.LEAF_NODE) {
				long id = e[i].getPtr();
				selectData.setLong(1, id);
				// selectData = mapConnection.prepareStatement("SELECT area, coordinates, innerPolygons, types, additionalTypes, name FROM binary_map_objects WHERE id = ?");
				ResultSet rs = selectData.executeQuery();
				if (rs.next()) {
					long cid = convertGeneratedIdToObfWrite(id);
					if (dataBlock == null) {
						baseId = cid;
						dataBlock = writer.createWriteMapDataBlock(baseId);
						tempStringTable.clear();

					}
					tempNames.clear();
					decodeNames(rs.getString(6), tempNames);
					byte[] types = rs.getBytes(4);
					int[] typeUse = new int[types.length / 2];
					for (int j = 0; j < types.length; j += 2) {
						int ids = Algorithms.parseSmallIntFromBytes(types, j);
						typeUse[j / 2] = renderingTypes.getTypeByInternalId(ids).getTargetId();
					}
					byte[] addTypes = rs.getBytes(5);
					int[] addtypeUse = null ;
					if (addTypes != null) {
						addtypeUse = new int[addTypes.length / 2];
						for (int j = 0; j < addTypes.length; j += 2) {
							int ids = Algorithms.parseSmallIntFromBytes(addTypes, j);
							addtypeUse[j / 2] = renderingTypes.getTypeByInternalId(ids).getTargetId();
						}
					}
					
					
					MapData mapData = writer.writeMapData(cid - baseId, parentBounds.getMinX(), parentBounds.getMinY(), rs.getBoolean(1), rs.getBytes(2), rs.getBytes(3),
							typeUse, addtypeUse, tempNames, null, tempStringTable, dataBlock, level.getMaxZoom() > 15);
					if(mapData != null) {
						dataBlock.addDataObjects(mapData);
					}
				} else {
					logMapDataWarn.error("Something goes wrong with id = " + id); //$NON-NLS-1$
				}
			}
		}
		if (dataBlock != null) {
			writer.writeMapDataBlock(dataBlock, tempStringTable, ref);
		}
		for (int i = 0; i < parent.getTotalElements(); i++) {
			if (e[i].getElementType() != rtree.Node.LEAF_NODE) {
				long ptr = e[i].getPtr();
				rtree.Node ns = r.getReadNode(ptr);
				writeBinaryMapBlock(ns, e[i].getRect(), r, writer, selectData, bounds, tempStringTable, tempNames,level);
			}
		}
	}

	public static void writeBinaryMapTree(rtree.Node parent, Rect re, RTree r, BinaryMapIndexWriter writer, TLongObjectHashMap<BinaryFileReference> bounds)
			throws IOException, RTreeException {
		Element[] e = parent.getAllElements();
		boolean containsLeaf = false;
		for (int i = 0; i < parent.getTotalElements(); i++) {
			if (e[i].getElementType() == rtree.Node.LEAF_NODE) {
				containsLeaf = true;
			}
		}
		BinaryFileReference ref = writer.startMapTreeElement(re.getMinX(), re.getMaxX(), re.getMinY(), re.getMaxY(), containsLeaf);
		if (ref != null) {
			bounds.put(parent.getNodeIndex(), ref);
		}
		for (int i = 0; i < parent.getTotalElements(); i++) {
			if (e[i].getElementType() != rtree.Node.LEAF_NODE) {
				rtree.Node chNode = r.getReadNode(e[i].getPtr());
				writeBinaryMapTree(chNode, e[i].getRect(), r, writer, bounds);
			}
		}
		writer.endWriteMapTreeElement();
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

	public void createDatabaseStructure(Connection mapConnection, DBDialect dialect, String rtreeMapIndexNonPackFileName)
			throws SQLException, IOException {
		createMapIndexStructure(mapConnection);
		this.mapConnection = mapConnection;
		mapBinaryStat = createStatementMapBinaryInsert(mapConnection);
		mapLowLevelBinaryStat = createStatementLowLevelMapBinaryInsert(mapConnection);
		try {
			mapTree = new RTree[mapZooms.size()];
			for (int i = 0; i < mapZooms.size(); i++) {
				File file = new File(rtreeMapIndexNonPackFileName + i);
				if (file.exists()) {
					file.delete();
				}
				mapTree[i] = new RTree(rtreeMapIndexNonPackFileName + i);
				// very slow
				// mapTree[i].getFileHdr().setBufferPolicy(true);
			}
		} catch (RTreeException e) {
			throw new IOException(e);
		}
		pStatements.put(mapBinaryStat, 0);
		pStatements.put(mapLowLevelBinaryStat, 0);
	}

	private void createMapIndexStructure(Connection conn) throws SQLException {
		Statement stat = conn.createStatement();
		stat.executeUpdate("create table binary_map_objects (id bigint primary key, name varchar(4096), "
				+ "area smallint, types binary, additionalTypes binary, coordinates binary, innerPolygons binary)");
		stat.executeUpdate("create index binary_map_objects_ind on binary_map_objects (id)");

		stat.executeUpdate("create table low_level_map_objects (id bigint primary key, start_node bigint, "
				+ "end_node bigint, name varchar(1024), nodes binary, type binary, addType binary, level smallint)");
		stat.executeUpdate("create index low_level_map_objects_ind on low_level_map_objects (id)");
		stat.executeUpdate("create index low_level_map_objects_ind_st on low_level_map_objects (start_node, type)");
		stat.executeUpdate("create index low_level_map_objects_ind_end on low_level_map_objects (end_node, type)");
		stat.close();
	}

	private PreparedStatement createStatementMapBinaryInsert(Connection conn) throws SQLException {
		return conn
				.prepareStatement("insert into binary_map_objects(id, area, coordinates, innerPolygons, types, additionalTypes, name) values(?, ?, ?, ?, ?, ?, ?)");
	}

	private PreparedStatement createStatementLowLevelMapBinaryInsert(Connection conn) throws SQLException {
		return conn
				.prepareStatement("insert into low_level_map_objects(id, start_node, end_node, name, nodes, type, addType, level) values(?, ?, ?, ?, ?, ?, ?, ?)");
	}

	private void insertLowLevelMapBinaryObject(int level, int zoom, TIntArrayList types, TIntArrayList addTypes, long id, List<Node> in, TreeMap<MapRulType, String> namesUse)
			throws SQLException {
		lowLevelWays++;
		List<Node> nodes = new ArrayList<Node>();
		OsmMapUtils.simplifyDouglasPeucker(in, zoom + 8 + zoomWaySmothness, 3, nodes, false);
		boolean first = true;
		long firstId = -1;
		long lastId = -1;
		ByteArrayOutputStream bNodes = new ByteArrayOutputStream();
		ByteArrayOutputStream bTypes = new ByteArrayOutputStream();
		ByteArrayOutputStream bAddtTypes = new ByteArrayOutputStream();
		try {
			for (Node n : nodes) {
				if (n != null) {
					if (first) {
						firstId = n.getId();
						first = false;
					}
					lastId = n.getId();
					Algorithms.writeInt(bNodes, Float.floatToRawIntBits((float) n.getLatitude()));
					Algorithms.writeInt(bNodes, Float.floatToRawIntBits((float) n.getLongitude()));
				}
			}
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
		if (firstId == -1) {
			return;
		}
		for (int j = 0; j < types.size(); j++) {
			try {
				Algorithms.writeSmallInt(bTypes, types.get(j));
			} catch (IOException e) {
			}
		}
		for (int j = 0; j < addTypes.size(); j++) {
			try {
				Algorithms.writeSmallInt(bAddtTypes, addTypes.get(j));
			} catch (IOException e) {
			}
		}
		mapLowLevelBinaryStat.setLong(1, id);
		mapLowLevelBinaryStat.setLong(2, firstId);
		mapLowLevelBinaryStat.setLong(3, lastId);
		mapLowLevelBinaryStat.setString(4, encodeNames(namesUse));
		mapLowLevelBinaryStat.setBytes(5, bNodes.toByteArray());
		mapLowLevelBinaryStat.setBytes(6, bTypes.toByteArray());
		mapLowLevelBinaryStat.setBytes(7, bAddtTypes.toByteArray());
		mapLowLevelBinaryStat.setShort(8, (short) level);

		addBatch(mapLowLevelBinaryStat);
	}

	private void insertBinaryMapRenderObjectIndex(RTree mapTree, Collection<Node> nodes, List<List<Node>> innerWays,
			Map<MapRulType, String> names, long id, boolean area, TIntArrayList types, TIntArrayList addTypes, boolean commit)
			throws SQLException {
		boolean init = false;
		int minX = Integer.MAX_VALUE;
		int maxX = 0;
		int minY = Integer.MAX_VALUE;
		int maxY = 0;

		ByteArrayOutputStream bcoordinates = new ByteArrayOutputStream();
		ByteArrayOutputStream binnercoord = new ByteArrayOutputStream();
		ByteArrayOutputStream btypes = new ByteArrayOutputStream();
		ByteArrayOutputStream badditionalTypes = new ByteArrayOutputStream();

		try {
			for (int j = 0; j < types.size(); j++) {
				Algorithms.writeSmallInt(btypes, types.get(j));
			}
			for (int j = 0; j < addTypes.size(); j++) {
				Algorithms.writeSmallInt(badditionalTypes, addTypes.get(j));
			}

			for (Node n : nodes) {
				if (n != null) {
					int y = MapUtils.get31TileNumberY(n.getLatitude());
					int x = MapUtils.get31TileNumberX(n.getLongitude());
					minX = Math.min(minX, x);
					maxX = Math.max(maxX, x);
					minY = Math.min(minY, y);
					maxY = Math.max(maxY, y);
					init = true;
					Algorithms.writeInt(bcoordinates, x);
					Algorithms.writeInt(bcoordinates, y);
				}
			}

			if (innerWays != null) {
				for (List<Node> ws : innerWays) {
					boolean exist = false;
					if (ws != null) {
						for (Node n : ws) {
							if (n != null) {
								exist = true;
								int y = MapUtils.get31TileNumberY(n.getLatitude());
								int x = MapUtils.get31TileNumberX(n.getLongitude());
								Algorithms.writeInt(binnercoord, x);
								Algorithms.writeInt(binnercoord, y);
							}
						}
					}
					if (exist) {
						Algorithms.writeInt(binnercoord, 0);
						Algorithms.writeInt(binnercoord, 0);
					}
				}
			}
		} catch (IOException es) {
			throw new IllegalStateException(es);
		}
		if (init) {
			// conn.prepareStatement("insert into binary_map_objects(id, area, coordinates, innerPolygons, types, additionalTypes, name) values(?, ?, ?, ?, ?, ?, ?)");
			mapBinaryStat.setLong(1, id);
			mapBinaryStat.setBoolean(2, area);
			mapBinaryStat.setBytes(3, bcoordinates.toByteArray());
			mapBinaryStat.setBytes(4, binnercoord.toByteArray());
			mapBinaryStat.setBytes(5, btypes.toByteArray());
			mapBinaryStat.setBytes(6, badditionalTypes.toByteArray());
			mapBinaryStat.setString(7, encodeNames(names));
			addBatch(mapBinaryStat, commit);
			try {
				mapTree.insert(new LeafElement(new Rect(minX, minY, maxX, maxY), id));
			} catch (RTreeInsertException e1) {
				throw new IllegalArgumentException(e1);
			} catch (IllegalValueException e1) {
				throw new IllegalArgumentException(e1);
			}
		}
	}

	public void createRTreeFiles(String rTreeMapIndexPackFileName) throws RTreeException {
		mapTree = new RTree[mapZooms.size()];
		for (int i = 0; i < mapZooms.size(); i++) {
			mapTree[i] = new RTree(rTreeMapIndexPackFileName + i);
		}

	}

	public void packRtreeFiles(String rTreeMapIndexNonPackFileName, String rTreeMapIndexPackFileName) throws IOException {
		for (int i = 0; i < mapZooms.size(); i++) {
			mapTree[i] = packRtreeFile(mapTree[i], rTreeMapIndexNonPackFileName + i, rTreeMapIndexPackFileName + i);
		}
	}

	public void commitAndCloseFiles(String rTreeMapIndexNonPackFileName, String rTreeMapIndexPackFileName, boolean deleteDatabaseIndexes)
			throws IOException, SQLException {

		// delete map rtree files
		if (mapTree != null) {
			for (int i = 0; i < mapTree.length; i++) {
				if (mapTree[i] != null) {
					RandomAccessFile file = mapTree[i].getFileHdr().getFile();
					file.close();
				}

			}
			for (int i = 0; i < mapTree.length; i++) {
				File f = new File(rTreeMapIndexNonPackFileName + i);
				if (f.exists() && deleteDatabaseIndexes) {
					f.delete();
				}
				f = new File(rTreeMapIndexPackFileName + i);
				if (f.exists() && deleteDatabaseIndexes) {
					f.delete();
				}
			}
		}
		closeAllPreparedStatements();

	}

	public void setZoomWaySmothness(int zoomWaySmothness) {
		this.zoomWaySmothness = zoomWaySmothness;
	}

	public int getZoomWaySmothness() {
		return zoomWaySmothness;
	}
}
