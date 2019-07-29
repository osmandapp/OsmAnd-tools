package net.osmand.obf.preparation;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import net.osmand.data.LatLon;
import net.osmand.data.QuadRect;
import net.osmand.data.TransportRoute;
import net.osmand.data.TransportSchedule;
import net.osmand.data.TransportStop;
import net.osmand.data.TransportStopExit;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.EntityParser;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.OSMSettings.OSMTagKey;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Relation.RelationMember;
import net.osmand.osm.edit.Way;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import net.sf.junidecode.Junidecode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;
import rtree.Element;
import rtree.IllegalValueException;
import rtree.LeafElement;
import rtree.RTree;
import rtree.RTreeException;
import rtree.RTreeInsertException;
import rtree.Rect;

public class IndexTransportCreator extends AbstractIndexPartCreator {

	private static final Log log = LogFactory.getLog(IndexTransportCreator.class);

	private static final int DISTANCE_THRESHOLD = 50;

	private Set<Long> visitedStops = new HashSet<Long>();
	private PreparedStatement transRouteStat;
	private PreparedStatement transRouteStopsStat;
	private PreparedStatement transStopsStat;
	private PreparedStatement transRouteGeometryStat;
	private RTree transportStopsTree;
	private Map<Long, Relation> masterRoutes = new HashMap<Long, Relation>();
	private Connection gtfsConnection;
	
	
	// Note: in future when we need more information from stop_area relation, it is better to memorize relations itself
	// now we need only specific names of stops and platforms
	private Map<EntityId, Relation> stopAreas = new HashMap<EntityId, Relation>();
	private Map<EntityId, List<TransportStopExit>> exits = new HashMap<EntityId, List<TransportStopExit>>();


	private static Set<String> acceptedRoutes = new HashSet<String>();

	private PreparedStatement gtfsSelectRoute;
	private PreparedStatement gtfsSelectStopTimes;
	
	private GtfsInfoStats gtfsStats = new GtfsInfoStats(); 

	
	static {
		acceptedRoutes.add("bus"); //$NON-NLS-1$
		acceptedRoutes.add("trolleybus"); //$NON-NLS-1$
		acceptedRoutes.add("share_taxi"); //$NON-NLS-1$
		acceptedRoutes.add("funicular"); //$NON-NLS-1$
		
		acceptedRoutes.add("subway"); //$NON-NLS-1$
		acceptedRoutes.add("light_rail"); //$NON-NLS-1$
		acceptedRoutes.add("monorail"); //$NON-NLS-1$
		acceptedRoutes.add("train"); //$NON-NLS-1$

		acceptedRoutes.add("tram"); //$NON-NLS-1$

		acceptedRoutes.add("ferry"); //$NON-NLS-1$
	}

	public IndexTransportCreator(IndexCreatorSettings settings) throws SQLException {
		File gtfs = settings.gtfsData;
		if(gtfs != null && gtfs.exists()) {
			DBDialect dialect = DBDialect.SQLITE;
			gtfsConnection = dialect.getDatabaseConnection(gtfs.getAbsolutePath(), log);
		}
	}


	public void createRTreeFile(String rtreeTransportStopFile) throws RTreeException {
		transportStopsTree = new RTree(rtreeTransportStopFile);
	}

	public void writeBinaryTransportTree(rtree.Node parent, RTree r, BinaryMapIndexWriter writer,
			PreparedStatement selectTransportStop, PreparedStatement selectTransportRouteStop,
			Map<Long, Long> transportRoutes, Map<String, Integer> stringTable) throws IOException, RTreeException, SQLException {
		Element[] e = parent.getAllElements();
		TLongArrayList routesOffsets = null;
		TLongArrayList routesIds = null;
		TLongArrayList deletedRoutes = null;
		for (int i = 0; i < parent.getTotalElements(); i++) {
			Rect re = e[i].getRect();
			if (e[i].getElementType() == rtree.Node.LEAF_NODE) {
				long id = e[i].getPtr();
				selectTransportStop.setLong(1, id);
				selectTransportRouteStop.setLong(1, id);
				ResultSet rs = selectTransportStop.executeQuery();
				if (rs.next()) {
					Gson gson = new Gson();
					Type t = new TypeToken<Map<String, String>>(){}.getType();
					int x24 = (int) MapUtils.getTileNumberX(24, rs.getDouble(3));
					int y24 = (int) MapUtils.getTileNumberY(24, rs.getDouble(2));
					String name = rs.getString(4);
					String nameEn = rs.getString(5);
					Map<String, String> names = gson.fromJson(rs.getString(6),t);
					if (nameEn != null && nameEn.equals(Junidecode.unidecode(name))) {
						nameEn = null;
					}
					String deletedRoutesStr = rs.getString(7);
					if (deletedRoutes == null) {
						deletedRoutes = new TLongArrayList();
					} else {
						deletedRoutes.clear();
					}
					if (!Algorithms.isEmpty(deletedRoutesStr)) {
						long[] deletedRoutesArray = gson.fromJson(deletedRoutesStr, long[].class);
						if (deletedRoutesArray != null && deletedRoutesArray.length > 0) {
							for (long routeId : deletedRoutesArray) {
								deletedRoutes.add(routeId);
							}
						}
					}

					ResultSet rset = selectTransportRouteStop.executeQuery();
					if (routesOffsets == null) {
						routesOffsets = new TLongArrayList();
					} else {
						routesOffsets.clear();
					}
					if (routesIds == null) {
						routesIds = new TLongArrayList();
					} else {
						routesIds.clear();
					}
					while (rset.next()) {
						long routeId = rset.getLong(1);
						if(transportRoutes.get(routeId) == null) {
							log.error("Something goes wrong with transport route id = " + routeId);
						} else {
							routesIds.add(routeId);
						}
					}
					routesIds.sort();
					for(long routeId : routesIds.toArray()) {
						Long routeOffset = transportRoutes.get(routeId);
						routesOffsets.add(routeOffset);
					}
					rset.close();
					writer.writeTransportStop(id, x24, y24, name, nameEn, names, stringTable, routesOffsets, routesIds, deletedRoutes, exits);
				} else {
					log.error("Something goes wrong with transport id = " + id); //$NON-NLS-1$
				}
			} else {
				long ptr = e[i].getPtr();
				rtree.Node ns = r.getReadNode(ptr);

				writer.startTransportTreeElement(re.getMinX(), re.getMaxX(), re.getMinY(), re.getMaxY());
				writeBinaryTransportTree(ns, r, writer, selectTransportStop, selectTransportRouteStop, transportRoutes, stringTable);
				writer.endWriteTransportTreeElement();
			}
		}
	}

	public void writeBinaryTransportTree(rtree.Node parent, RTree r, BinaryMapIndexWriter writer,
										 TLongObjectHashMap<TransportStop> transportStops,
										 Map<String, Integer> stringTable) throws IOException, RTreeException, SQLException {
		Element[] e = parent.getAllElements();
		for (int i = 0; i < parent.getTotalElements(); i++) {
			Rect re = e[i].getRect();
			if (e[i].getElementType() == rtree.Node.LEAF_NODE) {
				long id = e[i].getPtr();
				TransportStop stop = transportStops.get(id);
				if (stop != null) {
					int x24 = (int) MapUtils.getTileNumberX(24, stop.getLocation().getLongitude());
					int y24 = (int) MapUtils.getTileNumberY(24, stop.getLocation().getLatitude());
					TLongArrayList routesOffsets = new TLongArrayList();
					if (stop.getReferencesToRoutes() != null) {
						for (int referencesToRoute : stop.getReferencesToRoutes()) {
							routesOffsets.add((long) referencesToRoute);
						}
					}
					TLongArrayList routesIds = new TLongArrayList();
					if (stop.getRoutesIds() != null) {
						routesIds.addAll(stop.getRoutesIds());
					}
					TLongArrayList deletedRoutes = new TLongArrayList();
					if (stop.getDeletedRoutesIds() != null) {
						deletedRoutes.addAll(stop.getDeletedRoutesIds());
					}
					Map<Entity.EntityId, List<TransportStopExit>> exits = new HashMap<>();
					exits.put(new EntityId(Entity.EntityType.NODE, stop.getId()), stop.getExits());
					writer.writeTransportStop(id, x24, y24, stop.getName(), stop.getEnName(false), stop.getNamesMap(false), 
							stringTable, routesOffsets, routesIds, deletedRoutes, exits);
				} else {
					log.error("Something goes wrong with transport id = " + id);
				}
			} else {
				long ptr = e[i].getPtr();
				rtree.Node ns = r.getReadNode(ptr);

				writer.startTransportTreeElement(re.getMinX(), re.getMaxX(), re.getMinY(), re.getMaxY());
				writeBinaryTransportTree(ns, r, writer, transportStops, stringTable);
				writer.endWriteTransportTreeElement();
			}
		}
	}

	public void packRTree(String rtreeTransportStopsFileName, String rtreeTransportStopsPackFileName) throws IOException {
		transportStopsTree = packRtreeFile(transportStopsTree, rtreeTransportStopsFileName, rtreeTransportStopsPackFileName);
	}

	public void indexRelations(Relation e, OsmDbAccessorContext ctx) throws SQLException {
		if (e.getTag(OSMTagKey.ROUTE_MASTER) != null) {
			ctx.loadEntityRelation(e);
			for (RelationMember child : ((Relation) e).getMembers()) {
				Entity entity = child.getEntity();
				if(entity != null) {
					masterRoutes.put(entity.getId(), (Relation) e);
				}
			}
		}
		if ("stop_area".equals(e.getTag(OSMTagKey.PUBLIC_TRANSPORT))) {
			// save stop area relation members for future processing
			String name = e.getTag(OSMTagKey.NAME);
			if (name == null) return;

			ctx.loadEntityRelation(e);
			for (RelationMember entry : e.getMembers()) {
				String role = entry.getRole();
				if ("platform".equals(role) || "stop".equals(role)) {
					if (entry.getEntity() != null && 
							entry.getEntity().getTag(OSMTagKey.NAME) == null) {
						stopAreas.put(entry.getEntityId(), e);
					}
				}
			}
			List<TransportStopExit> stopExitList = new ArrayList<>();
			for (RelationMember entryAlt : e.getMembers()) {
				if (entryAlt.getEntity() != null && 
						("subway_entrance".equals(entryAlt.getEntity().getTag(OSMTagKey.RAILWAY)))) {
					TransportStopExit exit = new TransportStopExit();
					exit.setId(entryAlt.getEntity().getId());
					if (entryAlt.getEntity().getTag("ref") != null) {
						exit.setRef(entryAlt.getEntity().getTag("ref"));
					}
					exit.setLocation(entryAlt.getEntity().getLatitude(),entryAlt.getEntity().getLongitude());
					stopExitList.add(exit);
				}
			}
			for (RelationMember entry : e.getMembers()) {
				String role = entry.getRole();
				if (entry.getEntity() != null) {
					if (("".equals(role) && "station".equals(entry.getEntity().getTag(OSMTagKey.RAILWAY)))
							|| "stop".equals(role)) {
						exits.put(entry.getEntityId(), stopExitList);
					}
				}
			}
		}
	}

	public void iterateMainEntity(Entity e, OsmDbAccessorContext ctx) throws SQLException {
		if (e instanceof Relation && e.getTag(OSMTagKey.ROUTE) != null) {
			ctx.loadEntityRelation((Relation) e);
			List<TransportRoute> troutes = new ArrayList<>();
			indexTransportRoute((Relation) e, troutes);
			for(TransportRoute route : troutes) {
				insertTransportIntoIndex(route);
			}
		}
	}

	public void createDatabaseStructure(Connection conn, DBDialect dialect, String rtreeStopsFileName) throws SQLException, IOException {
		Statement stat = conn.createStatement();

		stat.executeUpdate("create table transport_route (id bigint primary key, type varchar(1024), operator varchar(1024)," +
				"ref varchar(1024), name varchar(1024), name_en varchar(1024), dist int, color varchar(1024))");
		stat.executeUpdate("create index transport_route_id on transport_route (id)");

		stat.executeUpdate("create table transport_route_stop (stop bigint, route bigint, ord int, primary key (route, ord))");
		stat.executeUpdate("create index transport_route_stop_stop on transport_route_stop (stop)");
		stat.executeUpdate("create index transport_route_stop_route on transport_route_stop (route)");
		
		stat.executeUpdate("create table transport_route_geometry (geometry bytes, route bigint, ind int)");
		stat.executeUpdate("create index transport_route_geometry_route on transport_route_geometry (route)");
		

		stat.executeUpdate("create table transport_stop (id bigint primary key, latitude double, longitude double, name varchar(1024), name_en varchar(1024), names varchar(8096), deleted_routes varchar(1024))");
		stat.executeUpdate("create index transport_stop_id on transport_stop (id)");
		stat.executeUpdate("create index transport_stop_location on transport_stop (latitude, longitude)");

//        if(dialect == DBDialect.SQLITE){
//        	stat.execute("PRAGMA user_version = " + IndexConstants.TRANSPORT_TABLE_VERSION); //$NON-NLS-1$
//        }
		stat.close();

		try {
			File file = new File(rtreeStopsFileName);
			if (file.exists()) {
				file.delete();
			}
			transportStopsTree = new RTree(file.getAbsolutePath());
		} catch (RTreeException e) {
			throw new IOException(e);
		}
		transRouteStat = conn.prepareStatement("insert into transport_route(id, type, operator, ref, name, name_en, dist, color) values(?, ?, ?, ?, ?, ?, ?, ?)");
		transRouteStopsStat = conn.prepareStatement("insert into transport_route_stop(route, stop, ord) values(?, ?, ?)");
		transStopsStat = conn.prepareStatement("insert into transport_stop(id, latitude, longitude, name, name_en, names, deleted_routes) values(?, ?, ?, ?, ?, ?, ?)");
		transRouteGeometryStat = conn.prepareStatement("insert into transport_route_geometry(route, geometry, ind) values(?, ?, ?)");
		pStatements.put(transRouteStat, 0);
		pStatements.put(transRouteStopsStat, 0);
		pStatements.put(transStopsStat, 0);
		pStatements.put(transRouteGeometryStat, 0);
		
		if(gtfsConnection != null) {
			boolean hasTripMetadataLoc = false;
			ResultSet columns = gtfsConnection.createStatement().executeQuery("PRAGMA table_info(trips)");
			while(columns.next()) {
				String name = columns.getString("name");
				if(name.equals("firstStopLat")) {
					hasTripMetadataLoc = true;
				}
			}
			if (!hasTripMetadataLoc) {
				indexBboxForGtfsTrips();
			}
		}
		
	}


	private void indexBboxForGtfsTrips() throws SQLException {
		ResultSet countRs = gtfsConnection.createStatement().executeQuery("SELECT COUNT(*) FROM trips");
		countRs.next();
		int total = countRs.getInt(1);
		countRs.close();
		for (String s : new String[] { "routes:route_id", "trips:trip_id,route_id,shape_id,service_id",
				"shapes:shape_id", "stops:stop_id", "stop_times:trip_id,stop_id", "calendar_dates:service_id" }) {
			int spl = s.indexOf(':');
			String tableName = s.substring(0, spl);
			String[] colNames = s.substring(spl+1).split(",");
			for(String colName : colNames) {
				String indName = tableName + "_" + colName;
				String ddl = "CREATE INDEX IF NOT EXISTS " + indName + " on " + tableName + " (" + colName + ")"; 
				log.info(ddl);
				gtfsConnection.createStatement().execute(ddl);
			}
			
		}
		log.info("Alter table and add columns");
		for (String s : new String[] { "firstStopLat", "firstStopLon", "minLat", "maxLat", "minLon", "maxLon" }) {
			try {
				gtfsConnection.createStatement().execute("ALTER TABLE trips ADD COLUMN " + s + " double");
			} catch(Exception e) {
			}
		}
		log.info(String.format("Indexing %d gtfs trips to compute bbox", total));

		PreparedStatement ins = gtfsConnection
				.prepareStatement("UPDATE trips SET firstStopLat = ?, firstStopLon = ?, minLat = ?, maxLat = ?, minLon = ?, maxLon = ? where trip_id = ?");
		
		ResultSet selectTimes = gtfsConnection.createStatement().executeQuery(
				"SELECT t.trip_id, t.stop_sequence, s.stop_lat, s.stop_lon from stop_times t "
						+ "join stops s on s.stop_id = t.stop_id order by t.trip_id"); // 
		QuadRect bbox = null;
		LatLon start = null;
		String tripId = null;
		int cnt = 0;
		int minStopSeq = 0;
		while (selectTimes.next()) {
			String nTripId = selectTimes.getString(1);
			double lat = selectTimes.getDouble(3);
			double lon = selectTimes.getDouble(4);
			int stopSeq = selectTimes.getInt(2);
			if (!Algorithms.objectEquals(nTripId, tripId)) {
				insertTripIds(ins, tripId, start, bbox, ++cnt);
				minStopSeq = stopSeq;
				start = new LatLon(lat, lon);
				bbox = new QuadRect(lon, lat, lon, lat);
				tripId = nTripId;
			}
			if(stopSeq < minStopSeq) {
				minStopSeq = stopSeq;
				start = new LatLon(lat, lon);
			}
			bbox.left = Math.min(bbox.left, lon);
			bbox.right = Math.max(bbox.right, lon);
			bbox.top = Math.max(bbox.top, lat);
			bbox.bottom = Math.min(bbox.bottom, lat);
		}
		insertTripIds(ins, tripId, start, bbox, 0);
		ins.close();
		selectTimes.close();
	}


	private void insertTripIds(PreparedStatement ins, String tripId, LatLon start, QuadRect bbox, int i)
			throws SQLException {
		if (tripId != null) {
			ins.setDouble(1, start.getLatitude());
			ins.setDouble(2, start.getLongitude());
			ins.setDouble(3, bbox.bottom);
			ins.setDouble(4, bbox.top);
			ins.setDouble(5, bbox.left);
			ins.setDouble(6, bbox.right);
			ins.setString(7, tripId);
			ins.addBatch();
		}
		if (i % 10000 == 0) {
			ins.executeBatch();
			log.info("Progress " + i + " trips");
		}
	}


	private void insertTransportIntoIndex(TransportRoute route) throws SQLException {
		transRouteStat.setLong(1, route.getId());
		transRouteStat.setString(2, route.getType());
		transRouteStat.setString(3, route.getOperator());
		transRouteStat.setString(4, route.getRef());
		transRouteStat.setString(5, route.getName());
		transRouteStat.setString(6, route.getEnName(false));
		transRouteStat.setInt(7, route.getAvgBothDistance());
		transRouteStat.setString(8, route.getColor());
		addBatch(transRouteStat);
		
		ByteArrayOutputStream ous = new ByteArrayOutputStream();
		if (route.getForwardWays() != null) {
			int ind = 0;
			for (Way tr : route.getForwardWays()) {
				addBatch(route, ous, tr, ind++);
			}
		}
		writeRouteStops(route, route.getForwardStops());
	}


	private void addBatch(TransportRoute route, ByteArrayOutputStream ous, Way tr, int ind) throws SQLException {
		if (tr.getNodes().size() == 0) {
			return;
		}
		transRouteGeometryStat.setLong(1, route.getId());
		writeWay(ous, tr);
		transRouteGeometryStat.setBytes(2, ous.toByteArray());
		transRouteGeometryStat.setInt(3, ind);
		addBatch(transRouteGeometryStat);
	}

	public void writeWay(ByteArrayOutputStream ous, Way way) {
		ous.reset();
		for (Node n : way.getNodes()) {
			int y = MapUtils.get31TileNumberY(n.getLatitude());
			int x = MapUtils.get31TileNumberX(n.getLongitude());
			try {
				Algorithms.writeInt(ous, x);
				Algorithms.writeInt(ous, y);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}


	private void writeRouteStops(TransportRoute r, List<TransportStop> stops) throws SQLException {
		int i = 0;
		for (TransportStop s : stops) {
			if (!visitedStops.contains(s.getId())) {
				Gson gson = new Gson();
				transStopsStat.setLong(1, s.getId());
				transStopsStat.setDouble(2, s.getLocation().getLatitude());
				transStopsStat.setDouble(3, s.getLocation().getLongitude());
				transStopsStat.setString(4, s.getName());
				transStopsStat.setString(5, s.getEnName(false));
				transStopsStat.setString(6, gson.toJson(s.getNamesMap(false)));
				transStopsStat.setString(7, gson.toJson(s.getDeletedRoutesIds()));
				int x = (int) MapUtils.getTileNumberX(24, s.getLocation().getLongitude());
				int y = (int) MapUtils.getTileNumberY(24, s.getLocation().getLatitude());
				addBatch(transStopsStat);
				try {
					transportStopsTree.insert(new LeafElement(new Rect(x, y, x, y), s.getId()));
				} catch (RTreeInsertException e) {
					throw new IllegalArgumentException(e);
				} catch (IllegalValueException e) {
					throw new IllegalArgumentException(e);
				}
				visitedStops.add(s.getId());
			}
			transRouteStopsStat.setLong(1, r.getId());
			transRouteStopsStat.setLong(2, s.getId());
			transRouteStopsStat.setInt(3, i++);
			addBatch(transRouteStopsStat);
		}
	}



	public void writeBinaryTransportIndex(BinaryMapIndexWriter writer, String regionName,
			Connection mapConnection) throws IOException, SQLException {
		try {
			closePreparedStatements(transRouteStat, transRouteStopsStat, transStopsStat, transRouteGeometryStat);
			mapConnection.commit();
			transportStopsTree.flush();

			visitedStops = null; // allow gc to collect it
			PreparedStatement selectTransportRouteData = mapConnection.prepareStatement(
					"SELECT id, dist, name, name_en, ref, operator, type, color FROM transport_route"); //$NON-NLS-1$
			PreparedStatement selectTransportData = mapConnection.prepareStatement("SELECT S.stop, " + //$NON-NLS-1$
					"  A.latitude,  A.longitude, A.name, A.name_en, A.names, A.deleted_routes " + //$NON-NLS-1$
					"FROM transport_route_stop S INNER JOIN transport_stop A ON A.id = S.stop WHERE S.route = ? ORDER BY S.ord asc"); //$NON-NLS-1$
			PreparedStatement selectTransportRouteGeometry = mapConnection.prepareStatement("SELECT S.geometry " + 
					"FROM transport_route_geometry S WHERE S.route = ? order by S.ind"); //$NON-NLS-1$

			writer.startWriteTransportIndex(regionName);

			writer.startWriteTransportRoutes();

			// expect that memory would be enough
			Map<String, Integer> stringTable = createStringTableForTransport();
			Map<Long, Long> transportRoutes = new LinkedHashMap<>();

			ResultSet rs = selectTransportRouteData.executeQuery();
			List<TransportStop> directStops = new ArrayList<>();
			List<TransportStop> reverseStops = new ArrayList<>();
			List<byte[]> directGeometry = new ArrayList<>();
			while (rs.next()) {
				long idRoute = rs.getLong(1);
				int dist = rs.getInt(2);
				String routeName = rs.getString(3);
				String routeEnName = rs.getString(4);
				if (routeEnName != null && routeEnName.equals(Junidecode.unidecode(routeName))) {
					routeEnName = null;
				}
				String ref = rs.getString(5);
				String operator = rs.getString(6);
				String type = rs.getString(7);
				String color = rs.getString(8);

				selectTransportData.setLong(1, idRoute);
				ResultSet rset = selectTransportData.executeQuery();
				reverseStops.clear();
				directStops.clear();
				directGeometry.clear();
				while (rset.next()) {
					long idStop = rset.getLong(1);
					String stopName = rset.getString(4);
					String stopEnName = rset.getString(5);
					Gson gson = new Gson();
					String names = rset.getString(6);
					Type t = new TypeToken<Map<String, String>>(){}.getType();
					Map<String, String> map = gson.fromJson(names, t);
					if (stopEnName != null && stopEnName.equals(Junidecode.unidecode(stopName))) {
						stopEnName = null;
					}
					TransportStop st = new TransportStop();
					st.setNames(map);
					st.setId(idStop);
					st.setName(stopName);
					st.setLocation(rset.getDouble(2), rset.getDouble(3));
					if (stopEnName != null) {
						st.setEnName(stopEnName);
					}
					directStops.add(st);
				}
				selectTransportRouteGeometry.setLong(1, idRoute);
				rset = selectTransportRouteGeometry.executeQuery();
				while (rset.next()) {
					byte[] bytes = rset.getBytes(1);
					directGeometry.add(bytes);
				}
				TransportSchedule schedule = readSchedule(ref, directStops);
				writer.writeTransportRoute(idRoute, routeName, routeEnName, ref, operator, type, dist, color, directStops, 
						directGeometry, stringTable, transportRoutes, schedule);
			}
			rs.close();
			selectTransportRouteData.close();
			selectTransportData.close();
			writer.endWriteTransportRoutes();

			PreparedStatement selectTransportStop = mapConnection.prepareStatement(
					"SELECT A.id,  A.latitude,  A.longitude, A.name, A.name_en, A.names, A.deleted_routes FROM transport_stop A where A.id = ?"); //$NON-NLS-1$
			PreparedStatement selectTransportRouteStop = mapConnection.prepareStatement(
					"SELECT DISTINCT S.route FROM transport_route_stop S join transport_route R  on R.id = S.route WHERE S.stop = ? ORDER BY R.type, R.ref "); //$NON-NLS-1$
			long rootIndex = transportStopsTree.getFileHdr().getRootIndex();
			rtree.Node root = transportStopsTree.getReadNode(rootIndex);
			Rect rootBounds = calcBounds(root);
			if (rootBounds != null) {
				writer.startTransportTreeElement(rootBounds.getMinX(), rootBounds.getMaxX(), rootBounds.getMinY(), rootBounds.getMaxY());
				writeBinaryTransportTree(root, transportStopsTree, writer, selectTransportStop, selectTransportRouteStop,
						transportRoutes, stringTable);
				writer.endWriteTransportTreeElement();
			}
			selectTransportStop.close();
			selectTransportRouteStop.close();

			writer.writeTransportStringTable(stringTable);

			writer.endWriteTransportIndex();
			writer.flush();
			log.info(gtfsStats);
		} catch (RTreeException e) {
			throw new IllegalStateException(e);
		}
	}


	private Rect calcBounds(rtree.Node n) {
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

	private int registerString(Map<String, Integer> stringTable, String s) {
		if (stringTable.containsKey(s)) {
			return stringTable.get(s);
		}
		int size = stringTable.size();
		stringTable.put(s, size);
		return size;
	}

	public Map<String, Integer> createStringTableForTransport() {
		Map<String, Integer> stringTable = new LinkedHashMap<String, Integer>();
		registerString(stringTable, "bus"); //$NON-NLS-1$
		registerString(stringTable, "trolleybus"); //$NON-NLS-1$
		registerString(stringTable, "subway"); //$NON-NLS-1$
		registerString(stringTable, "tram"); //$NON-NLS-1$
		registerString(stringTable, "share_taxi"); //$NON-NLS-1$
		registerString(stringTable, "taxi"); //$NON-NLS-1$
		registerString(stringTable, "train"); //$NON-NLS-1$
		registerString(stringTable, "ferry"); //$NON-NLS-1$
		return stringTable;
	}


	public void commitAndCloseFiles(String rtreeStopsFileName, String rtreeStopsPackFileName, boolean deleteDatabaseIndexes) throws IOException, SQLException {
		// delete transport rtree files
		if (transportStopsTree != null) {
			transportStopsTree.getFileHdr().getFile().close();
			File f = new File(rtreeStopsFileName);
			if (f.exists() && deleteDatabaseIndexes) {
				f.delete();
			}
			f = new File(rtreeStopsPackFileName);
			if (f.exists() && deleteDatabaseIndexes) {
				f.delete();
			}
		}
		closeAllPreparedStatements();
	}


	private void indexTransportRoute(Relation rel, List<TransportRoute> troutes) throws SQLException {
		String ref = rel.getTag(OSMTagKey.REF);
		String route = rel.getTag(OSMTagKey.ROUTE);
		String operator = rel.getTag(OSMTagKey.OPERATOR);
		String color = rel.getTag(OSMTagKey.COLOUR);
		Relation master = masterRoutes.get(rel.getId());
		if (master != null) {
			if (ref == null) {
				ref = master.getTag(OSMTagKey.REF);
			}
			if (route == null) {
				route = master.getTag(OSMTagKey.ROUTE);
			}
			if (operator == null) {
				operator = master.getTag(OSMTagKey.OPERATOR);
			}
			if (color == null) {
				color = master.getTag(OSMTagKey.COLOUR);
			}
		}

		if (route == null || ref == null) {
			return;
		}
		if (!acceptedRoutes.contains(route)) {
			return;
		}
		if (color != null) {
			String tmp = MapRenderingTypesEncoder.formatColorToPalette(color, false).replaceAll("_", "");
			color = "subwayText" + Algorithms.capitalizeFirstLetter(tmp) + "Color";
		}
		TransportRoute directRoute = EntityParser.parserRoute(rel, ref);
		directRoute.setOperator(operator);
		directRoute.setColor(color);
		directRoute.setType(route);
		directRoute.setRef(ref);
		directRoute.setId(directRoute.getId() << 1);
		if (processTransportRelationV2(rel, directRoute)) { // try new transport relations first
			troutes.add(directRoute);
		} else {
			TransportRoute backwardRoute = EntityParser.parserRoute(rel, ref);
			backwardRoute.setOperator(operator);
			backwardRoute.setColor(color);
			backwardRoute.setType(route);
			backwardRoute.setRef(ref);
			backwardRoute.setName(reverseName(ref, backwardRoute.getName()));
			if(!Algorithms.isEmpty(backwardRoute.getEnName(false))) {
				backwardRoute.setEnName(reverseName(ref, backwardRoute.getEnName(false)));
			}
			if (processTransportRelationV1(rel, directRoute, backwardRoute)) { // old relation style otherwise
				backwardRoute.setId((backwardRoute.getId() << 1) + 1);
				troutes.add(directRoute);
				troutes.add(backwardRoute);
			}
		}

	}

	private TransportSchedule readSchedule(String ref, List<TransportStop> directStops) throws SQLException {
		if(!Algorithms.isEmpty(ref) && gtfsConnection != null && directStops.size() > 0) {
			if(gtfsSelectRoute == null) {
				// new String[] { "firstStopLat", "firstStopLon", "minLat", "maxLat", "minLon", "maxLon" }
				gtfsSelectRoute = gtfsConnection.prepareStatement(
						"SELECT r.route_id, r.route_short_name, r.route_long_name, "+
						" t.trip_id, t.shape_id, t.service_id, t.firstStopLat, t.firstStopLon from routes r join "+
						" trips t on t.route_id = r.route_id where route_short_name = ? order by r.route_id asc, t.trip_id asc ");
			}
			if(gtfsSelectStopTimes == null) {
				gtfsSelectStopTimes = gtfsConnection.prepareStatement(
						"SELECT arrival_time, departure_time, stop_id, stop_sequence"+
						" from stop_times where trip_id = ? order by stop_sequence ");
			}
			gtfsSelectRoute.setString(1, ref);
			ResultSet rs = gtfsSelectRoute.executeQuery();
			String routeId = null, routeShortName = null, routeLongName = null;
			TransportSchedule schedule = new TransportSchedule();
			TIntArrayList timeDeparturesFirst = new TIntArrayList(); 
			while (rs.next()) {
				String nrouteId = rs.getString(1);
				if (!Algorithms.objectEquals(routeId, nrouteId)) {
					routeId = nrouteId;
					routeShortName = rs.getString(2);
					routeLongName = rs.getString(3);
				}
				String tripId = rs.getString(4);
				// String shapeId = rs.getString(5);
				// String serviceId = rs.getString(6);
				double firstLat = rs.getDouble(7);
				double firstLon = rs.getDouble(8);
				double dist = MapUtils.getDistance(directStops.get(0).getLocation(), firstLat,
						firstLon);
				if (dist < DISTANCE_THRESHOLD) {
					gtfsSelectStopTimes.setString(1, tripId);
					ResultSet nrs = gtfsSelectStopTimes.executeQuery();
					TIntArrayList stopIntervals = new TIntArrayList(directStops.size());
					TIntArrayList waitIntervals = new TIntArrayList(directStops.size());
					int ftime = 0, ptime = 0;
					
					while (nrs.next()) {
						int arrivalTime = parseTime(nrs.getString(1));
						int depTime = parseTime(nrs.getString(2));
						if(arrivalTime == -1 || depTime == -1) {
							gtfsStats.errorsTimeParsing++;
							continue;
						}
						if(ftime == 0) {
							ftime = ptime = depTime;
						} else {
							stopIntervals.add(arrivalTime - ptime);
						}
						waitIntervals.add(depTime - arrivalTime);
						ptime = arrivalTime;
					}
					if(waitIntervals.size() != directStops.size()) {
						gtfsStats.errorsTripsStopCounts++;
						// failed = true;
					} else {
						gtfsStats.successTripsParsing++;
						if(schedule.avgWaitIntervals.isEmpty()) {
							schedule.avgWaitIntervals.addAll(waitIntervals);
						} else {
							// check wait intervals different
							for (int j = 0; j < waitIntervals.size(); j++) {
								if(Math.abs(schedule.avgWaitIntervals.getQuick(j) - waitIntervals.getQuick(j)) > 3) {
									gtfsStats.avgWaitDiff30Sec++;
									break;
								}
							}
						}
						if(schedule.avgStopIntervals.isEmpty()) {
							schedule.avgStopIntervals.addAll(stopIntervals);
						} else {
							for (int j = 0; j < stopIntervals.size(); j++) {
								if(Math.abs(schedule.avgStopIntervals.getQuick(j) - stopIntervals.getQuick(j)) > 3) {
									gtfsStats.avgStopDiff30Sec++;
									break;
								}
							}
						}
						timeDeparturesFirst.add(ftime);
					}
					nrs.close();
				}
			}
			if(timeDeparturesFirst.size() > 0) {
				timeDeparturesFirst.sort();
				int p = 0;
				for(int i = 0; i < timeDeparturesFirst.size(); i++) {
					int x = timeDeparturesFirst.get(i) - p;
					// this is a wrong check cause there should be a check for calendar
					if(x > 0) {
						schedule.tripIntervals.add(x);
						p = timeDeparturesFirst.get(i);
					}
				}
				boolean allZeros = true;
				for (int i = 0; i < schedule.avgWaitIntervals.size(); i++) {
					if(schedule.avgWaitIntervals.getQuick(i) != 0) {
						allZeros = false;
						break;
					}
				}
				if (allZeros) {
					schedule.avgWaitIntervals.clear();
				}
				return schedule;
			}
		}
		return null;
	}
	
	
	
	


	private int parseTime(String str) {
		int f1 = str.indexOf(':');
		int f2 = str.indexOf(':', f1 + 1);
		if (f1 != -1 && f2 != -1) {
			try {
				int h = Integer.parseInt(str.substring(0, f1));
				int m = Integer.parseInt(str.substring(f1 + 1, f2));
				int s = Integer.parseInt(str.substring(f2 + 1));
				return h * 60 * 6 + m * 6 + s / 10;
			} catch (NumberFormatException e) {
				return -1;
			}
		}
		return -1;
	}


	private String reverseName(String ref, String name) {
		int startPos = name.indexOf(ref);
		String fname = "";
		if(startPos != -1) {
			fname = name.substring(0, startPos + ref.length()) + " ";
			name = name.substring(startPos + ref.length()).trim();
		}
		int i = name.indexOf('-');
		if(i != -1) {
			fname += name.substring(i + 1).trim() + " - " + name.substring(0, i).trim();
		} else {
			fname += name;
		}
		return fname;
	}


	private Pattern platforms = Pattern.compile("^(stop|platform)_(entry|exit)_only$");
	private Matcher stopPlatformMatcher = platforms.matcher("");

	private boolean processTransportRelationV2(Relation rel, TransportRoute route) {
		// first, verify we can accept this relation as new transport relation
		// accepted roles restricted to: <empty>, stop, platform, ^(stop|platform)_(entry|exit)_only$
		String version = rel.getTag("public_transport:version");
		try {
			if (Algorithms.isEmpty(version) || Integer.parseInt(version) < 2) {
				for (RelationMember entry : rel.getMembers()) {
					// ignore ways (cause with even with new relations there could be a mix of forward/backward ways)
					if (entry.getEntity() instanceof Way) {
						continue;
					}
					String role = entry.getRole();
					if (role.isEmpty() || "stop".equals(role) || "platform".equals(role)) {
						continue; // accepted roles
					}
					stopPlatformMatcher.reset(role);
					if (stopPlatformMatcher.matches()) {
						continue;
					}
					return false; // there is wrong role in the relation, exit
				}
			}
		} catch (NumberFormatException e) {
			return false;
		}

		List<Entity> platformsAndStops = new ArrayList<Entity>();
		List<Entity> platforms = new ArrayList<Entity>();
		List<Entity> stops = new ArrayList<Entity>();

		Map<EntityId, Entity> platformNames = new LinkedHashMap<>();
		for (RelationMember entry : rel.getMembers()) {
			String role = entry.getRole();
			if (entry.getEntity() == null || entry.getEntity().getLatLon() == null) {
				continue;
			}
			if (role.startsWith("platform")) {
				platformsAndStops.add(entry.getEntity());
				platforms.add(entry.getEntity());
			} else if (role.startsWith("stop")) {
				platformsAndStops.add(entry.getEntity());
				stops.add(entry.getEntity());
			} else {
				if (entry.getEntity() instanceof Way) {
					route.addWay((Way) entry.getEntity());
				}
			}
		}
		mergePlatformsStops(platformsAndStops, platforms, stops, platformNames);
		if (platformsAndStops.isEmpty()) {
			return true; // nothing to get from this relation - there is no stop
		}
		for (Entity s : platformsAndStops) {
			TransportStop stop = EntityParser.parseTransportStop(s);
			Relation stopArea = stopAreas.get(EntityId.valueOf(s));
			// verify name tag, not stop.getName because it may contain unnecessary refs, etc
			Entity genericStopName = null;
			if(stopArea != null && !Algorithms.isEmpty(stopArea.getTag(OSMTagKey.NAME))) {
				genericStopName = stopArea;
			} else if(platformNames.containsKey(EntityId.valueOf(s))){
				genericStopName = platformNames.get(EntityId.valueOf(s));
			}
			if(genericStopName != null) {
				stop.copyNames(genericStopName.getTag(OSMTagKey.NAME), genericStopName.getTag(OSMTagKey.NAME_EN), genericStopName.getNameTags(), true);
			}
			
			route.getForwardStops().add(stop);
		}

		return true;
	}

	private void mergePlatformsStops(List<Entity> platformsAndStopsToProcess, List<Entity> platforms, List<Entity> stops, 
			Map<EntityId, Entity> nameReplacement) {
		// walk through platforms  and verify names from the second:
		for(Entity platform : platforms) {
			Entity replaceStop = null;
			LatLon loc = platform.getLatLon();
			if(loc == null) {
				platformsAndStopsToProcess.remove(platform);
				continue;
			}
			double dist = 300;
			Relation rr = stopAreas.get(EntityId.valueOf(platform));
			for (Entity stop : stops) {
				if (stop.getLatLon() == null) {
					continue;
				}
				if (rr != null && stopAreas.get(EntityId.valueOf(stop)) == rr) {
					replaceStop = stop;
				}
				if (MapUtils.getDistance(stop.getLatLon(), loc) < dist) {
					replaceStop = stop;
					dist = MapUtils.getDistance(stop.getLatLon(), loc);
				}
			}
			if(replaceStop != null) {
				platformsAndStopsToProcess.remove(platform);
				if(!Algorithms.isEmpty(platform.getTag(OSMTagKey.NAME))) {
					nameReplacement.put(EntityId.valueOf(replaceStop), platform);
				}
			}
		}
	}


	private boolean processTransportRelationV1(Relation rel, TransportRoute directRoute, TransportRoute backwardRoute) {
		final Map<TransportStop, Integer> forwardStops = new LinkedHashMap<TransportStop, Integer>();
		final Map<TransportStop, Integer> backwardStops = new LinkedHashMap<TransportStop, Integer>();
		int currentStop = 0;
		int forwardStop = 0;
		int backwardStop = 0;
		for (RelationMember e : rel.getMembers()) {
			if (e.getRole().contains("stop") || e.getRole().contains("platform")) {  //$NON-NLS-1$
				if (e.getEntity() instanceof Node) {
					TransportStop stop = EntityParser.parseTransportStop(e.getEntity());
					Relation stopArea = stopAreas.get(EntityId.valueOf(e.getEntity()));
					if (stopArea != null) {
						stop.copyNames(stopArea.getTag(OSMTagKey.NAME), stopArea.getTag(OSMTagKey.NAME_EN), stopArea.getNameTags(), true);
					}
					boolean forward = e.getRole().contains("forward");  //$NON-NLS-1$
					boolean backward = e.getRole().contains("backward");  //$NON-NLS-1$
					currentStop++;
					if (forward || !backward) {
						forwardStop++;
					}
					if (backward) {
						backwardStop++;
					}
					boolean common = !forward && !backward;
					int index = -1;
					int i = e.getRole().length() - 1;
					int accum = 1;
					while (i >= 0 && Character.isDigit(e.getRole().charAt(i))) {
						if (index < 0) {
							index = 0;
						}
						index = accum * Character.getNumericValue(e.getRole().charAt(i)) + index;
						accum *= 10;
						i--;
					}
					if (index < 0) {
						index = forward ? forwardStop : (backward ? backwardStop : currentStop);
					}
					if (forward || common) {
						forwardStops.put(stop, index);
						directRoute.getForwardStops().add(stop);
					}
					if (backward || common) {
						if (common) {
							// put with negative index
							backwardStops.put(stop, -index);
						} else {
							backwardStops.put(stop, index);
						}

						backwardRoute.getForwardStops().add(stop);
					}

				}

			} else if (e.getEntity() instanceof Way) {
				int dir = e.getRole().equals("backward") ? -1 : (e.getRole().equals("forward") ? 1 : 0);
				if(dir >= 0) {
					directRoute.addWay((Way) e.getEntity());
				}
				if(dir <= 0) {
					backwardRoute.addWay((Way) e.getEntity());
				}
			}
		}
		if (forwardStops.isEmpty() && backwardStops.isEmpty()) {
			return false;
		}
		Collections.sort(directRoute.getForwardStops(), new Comparator<TransportStop>() {
			@Override
			public int compare(TransportStop o1, TransportStop o2) {
				return forwardStops.get(o1) - forwardStops.get(o2);
			}
		});
		// all common stops are with negative index (reeval them)
		for (TransportStop s : new ArrayList<TransportStop>(backwardStops.keySet())) {
			if (backwardStops.get(s) < 0) {
				backwardStops.put(s, backwardStops.size() + backwardStops.get(s) - 1);
			}
		}
		Collections.sort(backwardRoute.getForwardStops(), new Comparator<TransportStop>() {
			@Override
			public int compare(TransportStop o1, TransportStop o2) {
				return backwardStops.get(o1) - backwardStops.get(o2);
			}
		});
		return true;
	}

	
	private static class GtfsInfoStats {
		public int avgWaitDiff30Sec;
		public int avgStopDiff30Sec;
		int errorsTimeParsing = 0;
		int successTripsParsing = 0;
		int errorsTripsStopCounts = 0;
		@Override
		public String toString() {
			return "GtfsInfoStats [avgWaitDiff30Sec=" + avgWaitDiff30Sec + ", avgStopDiff30Sec=" + avgStopDiff30Sec
					+ ", errorsTimeParsing=" + errorsTimeParsing + ", successTripsParsing=" + successTripsParsing
					+ ", errorsTripsStopCounts=" + errorsTripsStopCounts + "]";
		}
		
		
		
		
		
	}
}
