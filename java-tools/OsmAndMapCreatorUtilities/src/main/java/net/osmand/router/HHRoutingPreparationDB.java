package net.osmand.router;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;

import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.list.array.TByteArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.osmand.PlatformUtil;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteRegion;
import net.osmand.data.LatLon;
import net.osmand.data.QuadRect;
import net.osmand.obf.preparation.BinaryMapIndexWriter;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.router.HHRoutingSubGraphCreator.RouteSegmentBorderPoint;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

public class HHRoutingPreparationDB extends HHRoutingDB {
	final static Log LOG = PlatformUtil.getLog(HHRoutingPreparationDB.class);
	
	protected PreparedStatement insSegment;
	protected PreparedStatement insGeometry;
	protected PreparedStatement insPoint;
	protected PreparedStatement updDualPoint;
	protected PreparedStatement insVisitedPoints;
	protected PreparedStatement updateRegionBoundaries;
	private int maxPointDBID;
	private int maxClusterID;


	public HHRoutingPreparationDB(File dbFile) throws SQLException {
		super(DBDialect.SQLITE.getDatabaseConnection(dbFile.getAbsolutePath(), LOG));
		if (!compactDB) {
			Statement st = conn.createStatement();
			st.execute("CREATE TABLE IF NOT EXISTS routeRegions(id, name, filePointer, size, filename, left, right, top, bottom, PRIMARY key (id))");
			st.execute("CREATE TABLE IF NOT EXISTS routeRegionPoints(id, pntId, clusterId)");
			st.execute("CREATE INDEX IF NOT EXISTS routeRegionPointsIndex on routeRegionPoints(id)");
			insPoint = conn.prepareStatement("INSERT INTO points(idPoint, pointGeoUniDir, pointGeoId, clusterId, roadId, start, end, sx31, sy31, ex31, ey31) "
							+ " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
			insVisitedPoints = conn.prepareStatement("INSERT INTO routeRegionPoints (id, pntId, clusterId) VALUES (?, ?, ?)");
			updateRegionBoundaries = conn.prepareStatement("UPDATE routeRegions SET left = ?, right = ?, top = ? , bottom = ? where id = ?");
			updDualPoint = conn.prepareStatement("UPDATE points SET dualIdPoint = ?, dualClusterId = ? WHERE idPoint = ?");
		}
	}
	
	public int loadNetworkPoints(TLongObjectHashMap<NetworkBorderPoint> networkPointsCluster) throws SQLException {
		Statement s = conn.createStatement();
		ResultSet rs = s.executeQuery("SELECT pointGeoUniDir, pointGeoId, idPoint, clusterId,  start, end  FROM points ");
		while (rs.next()) {
			long pointGeoUniDir = rs.getLong(1);
			if (!networkPointsCluster.containsKey(pointGeoUniDir)) {
				networkPointsCluster.put(pointGeoUniDir, new NetworkBorderPoint(pointGeoUniDir));
			}
			int pointId = rs.getInt(3);
			int clusterId = rs.getInt(4);
			networkPointsCluster.get(pointGeoUniDir).set(rs.getLong(2), pointId, rs.getInt(4), rs.getLong(5) < rs.getLong(6), null);
			maxPointDBID = Math.max(pointId, maxPointDBID);
			maxClusterID = Math.max(clusterId, maxClusterID);
		}
		maxClusterID = Math.max(maxClusterID, getMaxClusterId());
		rs.close();
		s.close();
		return maxClusterID;
	}

	private int getMaxClusterId() throws SQLException {
		Statement s = conn.createStatement();
		ResultSet rs = s.executeQuery("select max(clusterId) from routeRegionPoints");
		if (rs.next()) {
			return rs.getInt(1) + 1;
		}
		rs.close();
		s.close();
		return 0;
	}
		  

	public static void compact(Connection src, Connection tgt) throws SQLException, IOException {
		Statement st = tgt.createStatement();
		String columnNames = "pointGeoId, idPoint, clusterId, dualIdPoint, dualClusterId, chInd, roadId, start, end, sx31, sy31, ex31, ey31";
		int columnSize = columnNames.split(",").length;
		st.execute("CREATE TABLE IF NOT EXISTS points("+columnNames+",  PRIMARY key (idPoint))"); 
		st.execute("CREATE TABLE IF NOT EXISTS segments(id, ins, outs, PRIMARY key (id))");
		String insPnts = "";
		for (int k = 0; k < columnSize; k++) {
			if (k > 0) {
				insPnts += ",";
			}
			insPnts += "?";
		}
		HHRoutingDB sourceDB = new HHRoutingDB(src);
		TLongObjectHashMap<NetworkDBPoint> pointsById = sourceDB.loadNetworkPoints();
		TIntObjectHashMap<List<NetworkDBPoint>> outPoints = sourceDB.groupByClusters(pointsById, true);
		TIntObjectHashMap<List<NetworkDBPoint>> inPoints = sourceDB.groupByClusters(pointsById, false);
		PreparedStatement pIns = tgt.prepareStatement("INSERT INTO points(" + columnNames + ") VALUES (" + insPnts + ")");
		ResultSet rs = src.createStatement().executeQuery(" select " + columnNames + " from points");
		while (rs.next()) {
			for (int i = 0; i < columnSize; i++) {
				pIns.setObject(i + 1, rs.getObject(i + 1));
			}
			pIns.addBatch();
		}
		pIns.executeBatch();
		PreparedStatement sIns = tgt.prepareStatement("INSERT INTO segments(id, ins, outs)  VALUES (?, ?, ?)");
		PreparedStatement selOut = src.prepareStatement(" select idConnPoint, dist, shortcut from segments where idPoint = ?");
		PreparedStatement selIn = src.prepareStatement(" select idPoint, dist, shortcut from segments where idConnPoint = ?");
		for(NetworkDBPoint p : pointsById.valueCollection()) {
			selIn.setInt(1, p.index);
			selOut.setInt(1, p.index);
			sIns.setInt(1, p.index);
//			System.out.println(p.index  + " -> cluster " + p.clusterId + " dual clusterId  " + p.dualPoint.clusterId);
//			System.out.println("Incoming Cluster: " + inPoints.get(p.clusterId));
			sIns.setBytes(2, prepareSegments(selIn, pointsById, inPoints.get(p.clusterId)));
//			System.out.println("Outgoing cluster: " + outPoints.get(p.dualPoint.clusterId));
			sIns.setBytes(3, prepareSegments(selOut, pointsById, outPoints.get(p.dualPoint.clusterId)));
			sIns.addBatch();
		}
		sIns.executeBatch();
		tgt.close();

	}
	private static byte[] prepareSegments(PreparedStatement selIn, TLongObjectHashMap<NetworkDBPoint> pointsById, List<NetworkDBPoint> pnts) throws SQLException, IOException {
		TByteArrayList tbs = new TByteArrayList();
		ResultSet q = selIn.executeQuery();
		BinaryMapIndexWriter bmiw = new BinaryMapIndexWriter(null, null);
		for (NetworkDBPoint p : pnts) {
			p.rtCnt = 0;
		}
		while (q.next()) {
			int conn = q.getInt(1);
			int distInt = (int) Math.max(1, q.getFloat(2) * 10);
			NetworkDBPoint connPoint = pointsById.get(conn);
			int ind = Collections.binarySearch(pnts, connPoint, indexComparator);
			if (ind < 0) {
				throw new IllegalStateException();
			}
			pnts.get(ind).rtCnt = distInt;
		}
		for (NetworkDBPoint p : pnts) {
//			if (p.rtCnt == 0) {
//				indZeros++;
//			}
			bmiw.writeRawVarint32(tbs, p.rtCnt);
		}
		return tbs.toArray();
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
	
	public void insertVisitedVerticesBorderPoints(NetworkRouteRegion networkRouteRegion, TLongObjectHashMap<NetworkBorderPoint> borderPoints) throws SQLException {
		int batchInsPoint = 0;
		for (NetworkBorderPoint npnt : borderPoints.valueCollection()) {
			if (npnt.positiveObj == null && npnt.negativeObj == null) {
				continue;
			}
			if (npnt.positiveObj != null) {
				insPoint(npnt.positiveObj, npnt.positiveClusterId, npnt.positiveDbId);
				npnt.positiveObj = null; // inserted
				batchInsPoint++;
			}
			if (npnt.negativeObj != null) {
				insPoint(npnt.negativeObj, npnt.negativeClusterId, npnt.negativeDbId);
				npnt.negativeObj = null; // inserted
				batchInsPoint++;
			}
			if (npnt.positiveDbId > 0 && npnt.negativeDbId > 0) {
				updDualPoint.setInt(1, npnt.negativeDbId);
				updDualPoint.setInt(2, npnt.negativeClusterId);
				updDualPoint.setInt(3, npnt.positiveDbId);
				updDualPoint.addBatch();
				updDualPoint.setInt(1, npnt.positiveDbId);
				updDualPoint.setInt(2, npnt.positiveClusterId);
				updDualPoint.setInt(3, npnt.negativeDbId);
				updDualPoint.addBatch();
			}
			if (batchInsPoint > BATCH_SIZE) {
				batchInsPoint = 0;
				insPoint.executeBatch();
			}
		}
		insPoint.executeBatch();
		updDualPoint.executeBatch();
		int ind = 0;
		TLongIntIterator it = networkRouteRegion.visitedVertices.iterator();
		while (it.hasNext()) {
			it.advance();
			insVisitedPoints.setLong(1, networkRouteRegion.id);
			insVisitedPoints.setLong(2, it.key());
			insVisitedPoints.setInt(3, it.value());
			insVisitedPoints.addBatch();
			if (ind++ > BATCH_SIZE) {
				insVisitedPoints.executeBatch();
				ind = 0;
			}
		}
		insVisitedPoints.executeBatch();
		
		// conn.prepareStatement("UPDATE routeRegions SET left = ?, right = ?, top = ? , bottom = ? routeRegions where id = ?");
		QuadRect r = networkRouteRegion.rect;
		updateRegionBoundaries.setDouble(1, r.left);
		updateRegionBoundaries.setDouble(2, r.right);
		updateRegionBoundaries.setDouble(3, r.top);
		updateRegionBoundaries.setDouble(4, r.bottom);
		updateRegionBoundaries.setInt(5, networkRouteRegion.id);
		updateRegionBoundaries.execute();
		
	}

	private void insPoint(RouteSegmentBorderPoint obj, int clusterIndex, int pointDbId)
			throws SQLException {
		int p = 1;
		insPoint.setInt(p++, pointDbId);
		insPoint.setLong(p++, obj.unidirId);
		insPoint.setLong(p++, obj.uniqueId);
		insPoint.setInt(p++, clusterIndex);
		insPoint.setLong(p++, obj.getRoad().getId());
		insPoint.setLong(p++, obj.getSegmentStart());
		insPoint.setLong(p++, obj.getSegmentEnd());
		insPoint.setInt(p++, obj.getRoad().getPoint31XTile(obj.getSegmentStart()));
		insPoint.setInt(p++, obj.getRoad().getPoint31YTile(obj.getSegmentStart()));
		insPoint.setInt(p++, obj.getRoad().getPoint31XTile(obj.getSegmentEnd()));
		insPoint.setInt(p++, obj.getRoad().getPoint31YTile(obj.getSegmentEnd()));
		insPoint.addBatch();
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
		PreparedStatement check = conn.prepareStatement("SELECT id, left, top, right, bottom from routeRegions where name = ? "); // and filePointer = ?
		PreparedStatement ins = conn
				.prepareStatement("INSERT INTO routeRegions(id, name, filePointer, size, filename, left, right, top, bottom) "
						+ " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)");
		int ind = 0;
		for (NetworkRouteRegion nr : regions) {
			// name is enough
			check.setString(1, nr.region.getName());
//			check.setInt(2, nr.region.getFilePointer());
			ResultSet ls = check.executeQuery();
			if (ls.next()) {
				nr.id = ls.getInt(1);
				nr.rect = new QuadRect(ls.getDouble(2), ls.getDouble(3), ls.getDouble(4), ls.getDouble(5));
				ind = Math.max(ind, nr.id);
			} else {
				nr.id = -1;
			}
		}
		for (NetworkRouteRegion nr : regions) {
			if (nr.id >= 0) {
				continue;
			}
			int p = 1;
			nr.id = ind++;
			ins.setLong(p++, nr.id);
			ins.setString(p++, nr.region.getName());
			ins.setLong(p++, nr.region.getFilePointer());
			ins.setLong(p++, nr.region.getLength());
			ins.setString(p++, nr.file.getName());
			ins.setDouble(p++, nr.rect.left);
			ins.setDouble(p++, nr.rect.right);
			ins.setDouble(p++, nr.rect.top);
			ins.setDouble(p++, nr.rect.bottom);
			ins.addBatch();
		}
		ins.executeBatch();
		ins.close();
	}

	public int prepareBorderPointsToInsert(List<RouteSegmentBorderPoint> borderPoints, TLongObjectHashMap<NetworkBorderPoint> pointDbInd) {
		int clusterIndex = ++maxClusterID;
		for (RouteSegmentBorderPoint obj : borderPoints) {
			if (!pointDbInd.containsKey(obj.unidirId)) {
				pointDbInd.put(obj.unidirId, new NetworkBorderPoint(obj.unidirId));
			}
			NetworkBorderPoint npnt = pointDbInd.get(obj.unidirId);
			int pointDbId = ++maxPointDBID;
			npnt.set(obj.unidirId, pointDbId, clusterIndex, obj.getSegmentStart() < obj.getSegmentEnd(), obj);
		}
		return clusterIndex; 
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
	
	public TLongIntHashMap loadVisitedVertices(int id) throws SQLException {
		PreparedStatement ps = conn.prepareStatement("SELECT pntId, clusterId FROM routeRegionPoints WHERE id = ? ");
		ps.setInt(1, id);
		ResultSet rs = ps.executeQuery();
		TLongIntHashMap pnts = new TLongIntHashMap();
		while (rs.next()) {
			pnts.put(rs.getLong(1), rs.getInt(2));
		}
		rs.close();
		return pnts;
	}
	
	static class NetworkBorderPoint {
		long unidirId;
		long positiveGeoId;
		int positiveDbId;
		int positiveClusterId;
		RouteSegmentBorderPoint positiveObj;
		long negativeGeoId;
		int negativeDbId;
		int negativeClusterId;
		RouteSegmentBorderPoint negativeObj;
		
		public NetworkBorderPoint(long unidirId) {
			this.unidirId = unidirId;
		}
		
		public void set(long geoId, int dbId, int clusterId, boolean positive, RouteSegmentBorderPoint obj) {
			if (positive) {
				if (positiveGeoId != 0) {
					String msg = String.format("Geoid %d was already assigned %d (%d) in cluster %d (%d)  %s",
							positiveGeoId, positiveDbId, dbId, positiveClusterId, clusterId, obj);
					throw new IllegalStateException(msg);
				}
				positiveGeoId = geoId;
				positiveDbId = dbId;
				positiveObj = obj;
				positiveClusterId = clusterId;
			} else {
				if (negativeDbId != 0) {
					String msg = String.format("Geoid %d was already assigned %d (%d) in cluster %d (%d)  %s",
							negativeGeoId, negativeDbId, dbId, negativeClusterId, clusterId, obj);
					throw new IllegalStateException(msg);
				}
				negativeObj = obj;
				negativeGeoId = geoId;
				negativeDbId = dbId;
				negativeClusterId = clusterId;
			}
		}
	}

	static class NetworkRouteRegion {
		int id = 0;
		RouteRegion region;
		File file;
		int points = 0; // -1 loaded points, 0 init, > 0 - visitedVertices = null
		TLongIntHashMap visitedVertices;
		QuadRect rect;
		double regionOverlap = 1; //0.2; // we don't need big overlap cause of visited bbox recalculation
		QuadRect calcRect;

		public NetworkRouteRegion(RouteRegion r, File f) {
			region = r;
			double d = regionOverlap;  
			rect = new QuadRect(Math.max(-180, region.getLeftLongitude() - d),
					Math.min(85, region.getTopLatitude() + d), Math.min(180, region.getRightLongitude() + d),
					Math.max(-85, region.getBottomLatitude() - d));
			this.file = f;

		}
		
		public QuadRect getCalcBbox() {
			if (calcRect == null) {
				return rect;
			}
			QuadRect qr = new QuadRect();
			qr.left = MapUtils.get31LongitudeX((int) calcRect.left);
			qr.right = MapUtils.get31LongitudeX((int) calcRect.right);
			qr.top = MapUtils.get31LatitudeY((int) calcRect.top);
			qr.bottom = MapUtils.get31LatitudeY((int) calcRect.bottom);
			return qr;
		}
		
		public void updateBbox(int x31, int y31) {
			if (calcRect == null) {
				calcRect = new QuadRect();
				calcRect.right = calcRect.left = x31;
				calcRect.top = calcRect.bottom = y31;
			} else {
				if(x31 < calcRect.left ) {
					calcRect.left = x31;
				} else if(x31 > calcRect.right ) {
					calcRect.right = x31;
				}
				if (y31 < calcRect.top) {
					calcRect.top = y31;
				} else if (y31 > calcRect.bottom) {
					calcRect.bottom = y31;
				}
			}
		}

		public int getPoints() {
			return points < 0 ? visitedVertices.size() : points;
		}

		public boolean intersects(NetworkRouteRegion nrouteRegion) {
			return QuadRect.intersects(rect, nrouteRegion.rect);
		}

		public void unload() {
			if (this.visitedVertices != null && this.visitedVertices.size() > 100000) {
				this.points = this.visitedVertices.size();
				this.visitedVertices = null;
			}
		}

		public TLongIntHashMap getVisitedVertices(HHRoutingPreparationDB networkDB) throws SQLException {
			if (points >= 0) {
				if (visitedVertices != null && visitedVertices.size() > 0) {
					throw new IllegalStateException();
				}
				visitedVertices = networkDB.loadVisitedVertices(id);
				points = -1;
				HHRoutingPrepareContext.logf("Loaded visited vertices for %s - %d.", region.getName(),
						visitedVertices.size());
			}
			return visitedVertices;
		}

	}

}