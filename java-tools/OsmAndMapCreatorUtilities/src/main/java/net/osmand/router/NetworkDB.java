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

class NetworkDB {
	private static final Log LOG = PlatformUtil.getLog(NetworkDB.class);

	static class NetworkDBSegment {

	}

	static class NetworkDBPoint {
		long id;
		public long roadId;
		public int start;
		public int end;
		public int startX;
		public int startY;
		public int[] indexes;
		List<NetworkDBSegment> connected = new ArrayList<NetworkDBSegment>();
	}

	private Connection conn;
	private static int BATCH = 1000;

	public NetworkDB(File file, boolean recreate) throws SQLException {
		if (file.exists() && recreate) {
			file.delete();
		}
		this.conn = DBDialect.SQLITE.getDatabaseConnection(file.getAbsolutePath(), LOG);
		if (recreate) {
			Statement st = conn.createStatement();
			st.execute("CREATE TABLE points(idPoint, roadId, start, end, x31, y31, lat, lon, indexes)");
			st.close();
		}
	}

	public TLongObjectHashMap<NetworkDBPoint> getNetworkPoints() throws SQLException {
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery("SELECT idPoint, roadId, start, end, x31, y31, indexes from points");
		TLongObjectHashMap<NetworkDBPoint> mp = new TLongObjectHashMap<>();
		while (rs.next()) {
			NetworkDBPoint pnt = new NetworkDBPoint();
			pnt.id = rs.getLong(1);
			pnt.roadId = rs.getLong(2);
			pnt.start = rs.getInt(3);
			pnt.end = rs.getInt(4);
			pnt.startX = rs.getInt(5);
			pnt.startY = rs.getInt(6);
			pnt.indexes = Algorithms.stringToArray(rs.getString(7));
			mp.put(pnt.id, pnt);
		}
		rs.close();
		st.close();
		return mp;
	}

	public void insertPoints(FullNetwork network) throws SQLException {
		PreparedStatement s = conn
				.prepareStatement("INSERT INTO points(idPoint, roadId, start, end, x31, y31, lat, lon, indexes) "
						+ " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)");
		int ind = 0;
		TLongObjectIterator<List<NetworkIsland>> it = network.networkPointsCluster.iterator();
		while (it.hasNext()) {
			it.advance();
			int p = 1;
			s.setLong(p++, it.key());
			RouteSegment obj = network.toVisitVertices.get(it.key());
			List<NetworkIsland> islands = it.value();
			LatLon pnt = BaseRoadNetworkProcessor.getPoint(obj);
			s.setLong(p++, obj.getRoad().getId());
			s.setLong(p++, obj.getSegmentStart());
			s.setLong(p++, obj.getSegmentEnd());
			s.setInt(p++, obj.getRoad().getPoint31XTile(obj.getSegmentStart()));
			s.setInt(p++, obj.getRoad().getPoint31YTile(obj.getSegmentStart()));
			s.setDouble(p++, pnt.getLatitude());
			s.setDouble(p++, pnt.getLongitude());
			int[] arrs = new int[islands.size()];
			for (int i = 0; i < islands.size(); i++) {
				arrs[i] = islands.get(i).index;
			}
			s.setString(p++, Algorithms.arrayToString(arrs));
			s.addBatch();
			if (ind++ > BATCH) {
				s.executeBatch();
				ind = 0;
			}
		}
		s.executeBatch();
		s.close();
	}

	public void close() throws SQLException {
		conn.close();
	}

}