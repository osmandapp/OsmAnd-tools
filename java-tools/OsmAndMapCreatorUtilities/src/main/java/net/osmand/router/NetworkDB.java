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
import net.osmand.router.BaseRoadNetworkProcessor.FullNetwork;
import net.osmand.router.BaseRoadNetworkProcessor.NetworkIsland;
import net.osmand.router.BinaryRoutePlanner.RouteSegment;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

class NetworkDB {
	private static final Log LOG = PlatformUtil.getLog(NetworkDB.class);


	private Connection conn;
	private PreparedStatement insertIntoSegment;
	private static int BATCH = 1000;
	public static final int FULL_RECREATE = 0;
	public static final int RECREATE_SEGMENTS = 1;
	public static final int READ = 2;

	public NetworkDB(File file, int recreate) throws SQLException {
		if (file.exists() && FULL_RECREATE == recreate) {
			file.delete();
		}
		this.conn = DBDialect.SQLITE.getDatabaseConnection(file.getAbsolutePath(), LOG);
		Statement st = conn.createStatement();
		st.execute("CREATE TABLE IF NOT EXISTS points(idPoint, ind, roadId, start, end, sx31, sy31, ex31, ey31, indexes)");
		st.execute("CREATE TABLE IF NOT EXISTS segments(idPoint, idConnPoint, dist, geometry)");
		if (recreate == RECREATE_SEGMENTS) {
			insertIntoSegment = conn.prepareStatement(
					"INSERT INTO segments(idPoint, idConnPoint, dist, geometry) " + " VALUES(?, ?, ?, ?)");
			st.execute("DELETE FROM segments");
		}
		st.close();
	}

	public void insertSegments(List<NetworkDBSegment> segments) throws SQLException {
		for (NetworkDBSegment s : segments) {
			insertIntoSegment.setLong(1, s.start.index);
			insertIntoSegment.setLong(2, s.end.index);
			insertIntoSegment.setDouble(3, s.dist);
			byte[] coordinates = new byte[8 * s.geometry.size()];
			for (int t = 0; t < s.geometry.size(); t++) {
				LatLon l = s.geometry.get(t);
				Algorithms.putIntToBytes(coordinates, 8 * t, MapUtils.get31TileNumberX(l.getLongitude()));
				Algorithms.putIntToBytes(coordinates, 8 * t + 4, MapUtils.get31TileNumberY(l.getLatitude()));
			}
			insertIntoSegment.setBytes(4, coordinates);
			insertIntoSegment.addBatch();
		}
		insertIntoSegment.executeBatch();
	}
	
	public void loadNetworkSegments(TLongObjectHashMap<NetworkDBPoint> points) throws SQLException {
		TLongObjectHashMap<NetworkDBPoint> pntsById = new TLongObjectHashMap<>();
		for (NetworkDBPoint p : points.valueCollection()) {
			pntsById.put(p.index, p);
		}
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery("SELECT idPoint, idConnPoint, dist, geometry from segments");
		while (rs.next()) {
			NetworkDBPoint point = pntsById.get(rs.getLong(1));
			NetworkDBSegment segment = new NetworkDBSegment();
			segment.dist = rs.getDouble(3);
			segment.start = point;
			segment.end = pntsById.get(rs.getLong(2));
			byte[] geom = rs.getBytes(4);
			for (int k = 0; k < geom.length; k += 4) {
				int x = Algorithms.parseIntFromBytes(geom, k);
				int y = Algorithms.parseIntFromBytes(geom, k + 4);
				LatLon latlon = new LatLon(MapUtils.get31LatitudeY(y), MapUtils.get31LongitudeX(x));
				segment.geometry.add(latlon);
			}
			point.connected.add(segment);
		}
		rs.close();
		st.close();
	}

	public TLongObjectHashMap<NetworkDBPoint> getNetworkPoints() throws SQLException {
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
			mp.put(pnt.id, pnt);
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
		NetworkDBPoint start;
		NetworkDBPoint end;
		double dist;
		List<LatLon> geometry = new ArrayList<>();
	}

	static class NetworkDBPoint {
		long id;
		long index;
		public long roadId;
		public int start;
		public int end;
		public int startX;
		public int startY;
		public int endX;
		public int endY;
		public int[] indexes;
		List<NetworkDBSegment> connected = new ArrayList<NetworkDBSegment>();
	}


}