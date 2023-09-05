package net.osmand.router;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.osmand.PlatformUtil;
import net.osmand.data.LatLon;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.router.BinaryRoutePlanner.RouteSegment;
import net.osmand.router.HHRoutingGraphCreator.FullNetwork;
import net.osmand.router.HHRoutingGraphCreator.NetworkIsland;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

public class HHRoutingPreparationDB {
	private static final Log LOG = PlatformUtil.getLog(HHRoutingPreparationDB.class);

	public static final String EXT = ".hhdb";

	private Connection conn;
	private PreparedStatement insertSegment;
	private PreparedStatement insertGeometry;
	private PreparedStatement loadGeometry;


	private static int BATCH = 1000;
	public static final int FULL_RECREATE = 0;
	public static final int RECREATE_SEGMENTS = 1;
	public static final int READ = 2;



	public HHRoutingPreparationDB(File file, int recreate) throws SQLException {
		if (file.exists() && FULL_RECREATE == recreate) {
			file.delete();
		}
		this.conn = DBDialect.SQLITE.getDatabaseConnection(file.getAbsolutePath(), LOG);
		Statement st = conn.createStatement();
		st.execute("CREATE TABLE IF NOT EXISTS points(idPoint, ind, roadId, start, end, sx31, sy31, ex31, ey31, indexes, primary key (idPoint))");
		st.execute("CREATE TABLE IF NOT EXISTS segments(idPoint, idConnPoint, dist, PRIMARY key (idPoint, idConnPoint))");
		st.execute("CREATE TABLE IF NOT EXISTS geometry(idPoint, idConnPoint, geometry, PRIMARY key (idPoint, idConnPoint))");
		if (recreate == RECREATE_SEGMENTS) {
			st.execute("DELETE FROM segments");
		}
		insertSegment = conn.prepareStatement("INSERT INTO segments(idPoint, idConnPoint, dist) " + " VALUES(?, ?, ?)");
		insertGeometry = conn.prepareStatement("INSERT INTO geometry(idPoint, idConnPoint, geometry) " + " VALUES(?, ?, ?)");
		loadGeometry = conn.prepareStatement("SELECT geometry FROM geometry WHERE idPoint = ? AND idConnPoint =? ");
		st.close();
	}

	
	public void updateLevel(NetworkDBPoint pnt) throws SQLException {
		
	}
	
	public void insertSegments(List<NetworkDBSegment> segments) throws SQLException {
		for (NetworkDBSegment s : segments) {
			insertSegment.setLong(1, s.start.index);
			insertSegment.setLong(2, s.end.index);
			insertSegment.setDouble(3, s.dist);
			insertSegment.addBatch();
//			byte[] coordinates = new byte[0];
			byte[] coordinates = new byte[8 * s.geometry.size()];
			for (int t = 0; t < s.geometry.size(); t++) {
				LatLon l = s.geometry.get(t);
				Algorithms.putIntToBytes(coordinates, 8 * t, MapUtils.get31TileNumberX(l.getLongitude()));
				Algorithms.putIntToBytes(coordinates, 8 * t + 4, MapUtils.get31TileNumberY(l.getLatitude()));
			}
			insertGeometry.setLong(1, s.start.index);
			insertGeometry.setLong(2, s.end.index);
			insertGeometry.setBytes(3, coordinates);
			insertGeometry.addBatch();
		}
		insertSegment.executeBatch();
		insertGeometry.executeBatch();
	}
	
	public void loadGeometry(NetworkDBSegment segment, boolean reload) throws SQLException {
		if(!segment.geometry.isEmpty() && !reload) {
			return;
		}
		segment.geometry.clear();
		loadGeometry.setLong(1, segment.start.index);
		loadGeometry.setLong(2, segment.end.index);
		ResultSet rs = loadGeometry.executeQuery();
		if (rs.next()) {
			parseGeometry(segment, rs.getBytes(1));
		}
	}
	
	public int loadNetworkSegments(TLongObjectHashMap<NetworkDBPoint> points) throws SQLException {
		TLongObjectHashMap<NetworkDBPoint> pntsById = new TLongObjectHashMap<>();
		for (NetworkDBPoint p : points.valueCollection()) {
			pntsById.put(p.index, p);
		}
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery("SELECT idPoint, idConnPoint, dist from segments");
		int x = 0;
		while (rs.next()) {
			x++;
			NetworkDBPoint start = pntsById.get(rs.getLong(1));
			NetworkDBPoint end = pntsById.get(rs.getLong(2));
			double dist = rs.getDouble(3);
			NetworkDBSegment segment = new NetworkDBSegment(dist, true, start, end);
			NetworkDBSegment rev = new NetworkDBSegment(dist, false, start, end);
			start.connected.add(segment);
			end.connectedReverse.add(rev);
		}
		rs.close();
		st.close();
		return x;
	}

	private void parseGeometry(NetworkDBSegment segment, byte[] geom) {
		for (int k = 0; k < geom.length; k += 8) {
			int x = Algorithms.parseIntFromBytes(geom, k);
			int y = Algorithms.parseIntFromBytes(geom, k + 4);
			LatLon latlon = new LatLon(MapUtils.get31LatitudeY(y), MapUtils.get31LongitudeX(x));
			segment.geometry.add(latlon);
		}
	}

	public TLongObjectHashMap<NetworkDBPoint> getNetworkPoints(boolean byId) throws SQLException {
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery("SELECT idPoint, ind, roadId, start, end, sx31, sy31, ex31, ey31, indexes from points");
		TLongObjectHashMap<NetworkDBPoint> mp = new TLongObjectHashMap<>();
		while (rs.next()) {
			NetworkDBPoint pnt = new NetworkDBPoint();
			int p = 1;
			pnt.id = rs.getLong(p++);
			pnt.index = rs.getLong(p++);
			pnt.roadId = rs.getLong(p++);
			pnt.start = rs.getInt(p++);
			pnt.end = rs.getInt(p++);
			pnt.startX = rs.getInt(p++);
			pnt.startY = rs.getInt(p++);
			pnt.endX = rs.getInt(p++);
			pnt.endY = rs.getInt(p++);
			pnt.indexes = Algorithms.stringToArray(rs.getString(p++));
			mp.put(byId ? pnt.id : pnt.index, pnt);
		}
		rs.close();
		st.close();
		return mp;
	}
	
	

	public void insertPoints(FullNetwork network) throws SQLException {
		PreparedStatement s = conn
				.prepareStatement("INSERT INTO points(idPoint, ind, roadId, start, end, sx31, sy31, ex31, ey31, indexes) "
						+ " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		int ind = 0;
		TLongObjectIterator<List<NetworkIsland>> it = network.networkPointsCluster.iterator();
		while (it.hasNext()) {
			it.advance();
			int p = 1;
			s.setLong(p++, it.key());
			s.setLong(p++, ind++);
			RouteSegment obj = network.toVisitVertices.get(it.key());
			List<NetworkIsland> islands = it.value();
			s.setLong(p++, obj.getRoad().getId());
			s.setLong(p++, obj.getSegmentStart());
			s.setLong(p++, obj.getSegmentEnd());
			s.setInt(p++, obj.getRoad().getPoint31XTile(obj.getSegmentStart()));
			s.setInt(p++, obj.getRoad().getPoint31YTile(obj.getSegmentStart()));
			s.setInt(p++, obj.getRoad().getPoint31XTile(obj.getSegmentEnd()));
			s.setInt(p++, obj.getRoad().getPoint31YTile(obj.getSegmentEnd()));
			int[] arrs = new int[islands.size()];
			for (int i = 0; i < islands.size(); i++) {
				arrs[i] = islands.get(i).index;
			}
			s.setString(p++, Algorithms.arrayToString(arrs));
			s.addBatch();
			if (ind % BATCH == 0) {
				s.executeBatch();
			}
		}
		s.executeBatch();
		s.close();
	}

	public void close() throws SQLException {
		conn.close();
	}
	
	
	static class NetworkDBSegment {
		final boolean direction;
		final NetworkDBPoint start;
		final NetworkDBPoint end;
		final double dist;
		List<LatLon> geometry = new ArrayList<>();
		
		public NetworkDBSegment(double dist, boolean direction, NetworkDBPoint start, NetworkDBPoint end) {
			this.direction = direction;
			this.start = start;
			this.end = end;
			this.dist = dist;
		}
		
		// routing extra info
		double rtDistanceToEnd; // added once in queue
		double rtCost; // added once in queue
		
		@Override
		public String toString() {
			return String.format("Segment %s -> %s", start, end);
		}
		
	}

	static class NetworkDBPoint {
		long id;
		long index;
		int level = 0;
		public long roadId;
		public int start;
		public int end;
		public int startX;
		public int startY;
		public int endX;
		public int endY;
		public int[] indexes;
		
		List<NetworkDBSegment> connected = new ArrayList<NetworkDBSegment>();
		List<NetworkDBSegment> connectedReverse = new ArrayList<NetworkDBSegment>();
		
		// for routing
		NetworkDBSegment rtRouteToPoint;
		double rtDistanceFromStart;
		
		NetworkDBSegment rtRouteToPointRev;
		double rtDistanceFromStartRev;
		
		// indexing
		int rtCnt = 0;
		
		
		@Override
		public String toString() {
			return String.format("Point %d (%d %d-%d)", index, roadId / 64, start, end);
		}
		
		public LatLon getPoint() {
			return new LatLon(MapUtils.get31LatitudeY(this.startY / 2 + this.endY / 2),
					MapUtils.get31LongitudeX(this.startX / 2 + this.endX / 2));
		}

		public void clearRouting() {
			rtRouteToPoint = null;
			rtDistanceFromStart = 0;
			rtRouteToPointRev = null;
			rtDistanceFromStartRev = 0;
		}
	}


}