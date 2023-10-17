package net.osmand.router;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteRegion;
import net.osmand.data.LatLon;
import net.osmand.data.QuadRect;
import net.osmand.router.BinaryRoutePlanner.RouteSegment;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

public class HHRoutingPreparationDB extends HHRoutingDB {

	protected PreparedStatement insSegment;
	protected PreparedStatement insGeometry;
	protected PreparedStatement insCluster;
	protected PreparedStatement insPoint;

	public HHRoutingPreparationDB(Connection conn) throws SQLException {
		super(conn);
		if (!compactDB) {
			insPoint = conn
					.prepareStatement("INSERT INTO points(idPoint, ind, roadId, start, end, sx31, sy31, ex31, ey31) "
							+ " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)");
			insCluster = conn.prepareStatement("INSERT INTO clusters(idPoint, indPoint, clusterInd) VALUES(?, ?, ?)");
		}
	}

	public static void compact(Connection src, Connection tgt) throws SQLException {
		Statement st = tgt.createStatement();
		st.execute(
				"CREATE TABLE IF NOT EXISTS points(pointGeoId, id, chInd, roadId, start, end, sx31, sy31, ex31, ey31,  PRIMARY key (id))"); // ind
																																			// unique
		st.execute("CREATE TABLE IF NOT EXISTS segments(id, ins, outs, PRIMARY key (id))");

		PreparedStatement pIns = tgt.prepareStatement(
				"INSERT INTO points(pointGeoId, id, chInd, roadId, start, end, sx31, sy31, ex31, ey31)  "
						+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		ResultSet rs = src.createStatement()
				.executeQuery(" select idPoint, ind, chInd, roadId, start, end, sx31, sy31, ex31, ey31 from points");
		TIntArrayList ids = new TIntArrayList();
		while (rs.next()) {
			ids.add(rs.getInt(2));
			for (int i = 0; i < 10; i++) {
				pIns.setObject(i + 1, rs.getObject(i + 1));
			}
			pIns.addBatch();
		}
		pIns.executeBatch();
		PreparedStatement sIns = tgt.prepareStatement("INSERT INTO segments(id, ins, outs)  VALUES (?, ?, ?)");
		PreparedStatement selOut = src
				.prepareStatement(" select idConnPoint, dist, shortcut from segments where idPoint = ?");
		PreparedStatement selIn = src
				.prepareStatement(" select idPoint, dist, shortcut from segments where idConnPoint = ?");
		for (int id : ids.toArray()) {
			selIn.setInt(1, id);
			selOut.setInt(1, id);
			sIns.setInt(1, id);
			sIns.setBytes(2, prepareSegments(selIn));
			sIns.setBytes(3, prepareSegments(selOut));
			sIns.addBatch();
		}

		sIns.executeBatch();

		tgt.close();

	}

	private static byte[] prepareSegments(PreparedStatement selIn) throws SQLException {
		TIntArrayList bs = new TIntArrayList();
		ResultSet q = selIn.executeQuery();
		while (q.next()) {
			int conn = q.getInt(1);
			bs.add(conn);
			float dist = q.getFloat(2);
			bs.add(Float.floatToIntBits(dist)); // distance
			bs.add(q.getInt(3)); // shortcut
		}

		byte[] bytes = new byte[bs.size() * 4];
		for (int i = 0; i < bs.size(); i++) {
			Algorithms.putIntToBytes(bytes, i * 4, bs.get(i));
		}
		return bytes;
	}

	public void recreateSegments() throws SQLException {
		Statement st = conn.createStatement();
		st.execute("DELETE FROM segments");
		st.execute("DELETE FROM geometry");
		st.close();
	}
	
	


	public void updatePointsCHInd(Collection<NetworkDBPoint> pnts) throws SQLException {
		PreparedStatement updCHInd = conn.prepareStatement("UPDATE  points SET chInd = ? where idPoint = ?");
		int ind = 0;
		for (NetworkDBPoint p : pnts) {
			updCHInd.setLong(1, p.chInd);
			updCHInd.setLong(2, p.pntGeoId);
			updCHInd.addBatch();
			if (ind++ % BATCH_SIZE == 0) {
				updCHInd.executeBatch();
			}
		}
		updCHInd.executeBatch();
		updCHInd.close();
	}
	
	public void insertVisitedVertices(NetworkRouteRegion networkRouteRegion) throws SQLException {
		PreparedStatement ps = conn.prepareStatement("INSERT INTO routeRegionPoints (id, pntId) VALUES (?, ?)");
		int ind = 0;
		for (long k : networkRouteRegion.visitedVertices.keys()) {
			ps.setLong(1, networkRouteRegion.id);
			ps.setLong(2, k);
			ps.addBatch();
			if (ind++ > BATCH_SIZE) {
				ps.executeBatch();
			}
		}
		ps.executeBatch();
		insPoint.executeBatch();
		insCluster.executeBatch();
		
	}
	
	public void deleteShortcuts() throws SQLException {
		Statement st = conn.createStatement();
		st.execute("DELETE from segments where shortcut > 0");
		st.execute("DELETE from geometry where shortcut > 0");
		st.close();
	}
	
	public void insertSegments(List<NetworkDBSegment> segments) throws SQLException {
		if (insSegment == null) {
			insSegment = conn.prepareStatement("INSERT INTO segments(idPoint, idConnPoint, dist, shortcut) VALUES(?, ?, ?, ?)");
		}
		if (insGeometry == null) {
			insGeometry = conn.prepareStatement("INSERT INTO geometry(idPoint, idConnPoint, shortcut, geometry) " + " VALUES(?, ?, ?, ?)");
		}
		int ind= 0;
		for (NetworkDBSegment s : segments) {
			insSegment.setLong(1, s.start.index);
			insSegment.setLong(2, s.end.index);
			insSegment.setDouble(3, s.dist);
			insSegment.setInt(4, s.shortcut ? 1 : 0);
			insSegment.addBatch();
//			byte[] coordinates = new byte[0];
			if (s.geometry.size() > 0) {
				byte[] coordinates = new byte[8 * s.geometry.size()];
				for (int t = 0; t < s.geometry.size(); t++) {
					LatLon l = s.geometry.get(t);
					Algorithms.putIntToBytes(coordinates, 8 * t, MapUtils.get31TileNumberX(l.getLongitude()));
					Algorithms.putIntToBytes(coordinates, 8 * t + 4, MapUtils.get31TileNumberY(l.getLatitude()));
				}
				insGeometry.setBytes(4, coordinates);
			} else if (s.segmentsStartEnd.size() > 0) {
				byte[] coordinates = new byte[4 * s.segmentsStartEnd.size() + 8];
				Algorithms.putIntToBytes(coordinates, 0, XY_SHORTCUT_GEOM);
				Algorithms.putIntToBytes(coordinates, 4, XY_SHORTCUT_GEOM);
				for (int t = 0; t < s.segmentsStartEnd.size(); t++) {
					Algorithms.putIntToBytes(coordinates, 4 * t + 8, s.segmentsStartEnd.getQuick(t));
				}
				insGeometry.setBytes(4, coordinates);
			}
			insGeometry.setLong(1, s.start.index);
			insGeometry.setLong(2, s.end.index);
			insGeometry.setInt(3, s.shortcut ? 1 : 0);
			
			insGeometry.addBatch();
			if (ind++ % BATCH_SIZE == 0) {
				insSegment.executeBatch();
				insGeometry.executeBatch();
			}
		}
		insSegment.executeBatch();
		insGeometry.executeBatch();
	}
	
	public void insertRegions(List<NetworkRouteRegion> regions) throws SQLException {
		PreparedStatement check = conn.prepareStatement("SELECT id from routeRegions where name = ? "); // and filePointer = ?
		PreparedStatement ins = conn
				.prepareStatement("INSERT INTO routeRegions(id, name, filePointer, size, filename, left, right, top, bottom) "
						+ " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)");
		int ind = 0;
		for(NetworkRouteRegion nr : regions) {
			// name is enough
			check.setString(1, nr.region.getName());
//			check.setInt(2, nr.region.getFilePointer());
			ResultSet ls = check.executeQuery();
			if (ls.next()) {
				nr.id = ls.getInt(1);
				continue;
			}
			
			int p = 1;
			nr.id = ind++;
			ins.setLong(p++, nr.id);
			ins.setString(p++, nr.region.getName());
			ins.setLong(p++, nr.region.getFilePointer());
			ins.setLong(p++, nr.region.getLength());
			ins.setString(p++, nr.file.getName());
			ins.setDouble(p++, nr.region.getLeftLongitude());
			ins.setDouble(p++, nr.region.getRightLongitude());
			ins.setDouble(p++, nr.region.getTopLatitude());
			ins.setDouble(p++, nr.region.getBottomLatitude());
			ins.addBatch();
		}
		ins.executeBatch();
		ins.close();
	}

	public void insertCluster(int clusterUniqueIndex, TLongObjectHashMap<? extends RouteSegment> borderPoints, TLongObjectHashMap<Integer> pointDbInd) throws SQLException {
		TLongObjectIterator<? extends RouteSegment> it = borderPoints.iterator();
		while (it.hasNext()) {
			batchInsPoint++;
			it.advance();
			long pntId = it.key();
			RouteSegment obj = it.value();
			int pointInd;
			if (!pointDbInd.contains(pntId)) {
				pointInd = pointDbInd.size();
				pointDbInd.put(pntId, pointInd);
				int p = 1;
				insPoint.setLong(p++, pntId);
				insPoint.setInt(p++, pointInd);
				insPoint.setLong(p++, obj.getRoad().getId());
				insPoint.setLong(p++, obj.getSegmentStart());
				insPoint.setLong(p++, obj.getSegmentEnd());
				insPoint.setInt(p++, obj.getRoad().getPoint31XTile(obj.getSegmentStart()));
				insPoint.setInt(p++, obj.getRoad().getPoint31YTile(obj.getSegmentStart()));
				insPoint.setInt(p++, obj.getRoad().getPoint31XTile(obj.getSegmentEnd()));
				insPoint.setInt(p++, obj.getRoad().getPoint31YTile(obj.getSegmentEnd()));
				insPoint.addBatch();
			} else {
				pointInd = pointDbInd.get(pntId);
			}
			
			int p2 = 1;
			insCluster.setLong(p2++, pntId);
			insCluster.setInt(p2++, pointInd);
			insCluster.setInt(p2++, clusterUniqueIndex);
			insCluster.addBatch();

		}
		if (batchInsPoint > BATCH_SIZE) {
			batchInsPoint = 0;
			insPoint.executeBatch();
			insCluster.executeBatch();
		}

	}
	

	public void loadClusterData(TLongObjectHashMap<NetworkDBPoint> pnts, boolean byId) throws SQLException {
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery("SELECT " + (byId ? "idPoint" : "indPoint") + ", clusterInd from clusters");
		while (rs.next()) {
			NetworkDBPoint pnt = pnts.get(rs.getLong(1));
			if (pnt.clusters == null) {
				pnt.clusters = new TIntArrayList();
			}
			pnt.clusters.add(rs.getInt(2));
		}
		rs.close();
		st.close();		
	}
	
	public boolean hasVisitedPoints(NetworkRouteRegion nrouteRegion) throws SQLException {
		PreparedStatement ps = conn.prepareStatement("SELECT pntId FROM routeRegionPoints WHERE id = ? ");
		ps.setLong(1, nrouteRegion.id);
		ResultSet rs = ps.executeQuery();
		boolean has = false;
		if (rs.next()) {
			has = true;
		}
		rs.close();
		ps.close();
		return has;
	}
	
	public void loadVisitedVertices(NetworkRouteRegion networkRouteRegion) throws SQLException {
		PreparedStatement ps = conn.prepareStatement("SELECT pntId FROM routeRegionPoints WHERE id = ? ");
		ps.setLong(1, networkRouteRegion.id);
		ResultSet rs = ps.executeQuery();
		if(networkRouteRegion.visitedVertices != null) {
			throw new IllegalStateException();
		}
		networkRouteRegion.visitedVertices = new TLongObjectHashMap<>();
		while(rs.next()) {
			networkRouteRegion.visitedVertices.put(rs.getLong(1), null);
		}
		networkRouteRegion.points = -1;		
		rs.close();
	}
	

	static class NetworkRouteRegion {
		int id = 0;
		RouteRegion region;
		File file;
		int points = -1; // -1 loaded points
		TLongObjectHashMap<RouteSegment> visitedVertices = new TLongObjectHashMap<>();

		public NetworkRouteRegion(RouteRegion r, File f) {
			region = r;
			this.file = f;

		}

		public int getPoints() {
			return points < 0 ? visitedVertices.size() : points;
		}

		public QuadRect getRect() {
			return new QuadRect(region.getLeftLongitude(), region.getTopLatitude(), region.getRightLongitude(),
					region.getBottomLatitude());
		}

		public boolean intersects(NetworkRouteRegion nrouteRegion) {
			return QuadRect.intersects(getRect(), nrouteRegion.getRect());
		}

		public void unload() {
			if (this.visitedVertices != null && this.visitedVertices.size() > 1000) {
				this.points = this.visitedVertices.size();
				this.visitedVertices = null;
			}
		}

		public TLongObjectHashMap<RouteSegment> getVisitedVertices(HHRoutingPreparationDB networkDB) throws SQLException {
			if (points > 0) {
				networkDB.loadVisitedVertices(this);
			}
			return visitedVertices;
		}

	}

}