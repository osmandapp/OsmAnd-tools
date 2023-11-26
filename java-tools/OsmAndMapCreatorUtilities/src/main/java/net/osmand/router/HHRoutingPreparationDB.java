package net.osmand.router;


import static net.osmand.router.HHRoutingUtilities.logf;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;

import gnu.trove.iterator.TLongIntIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.list.array.TByteArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;
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
	protected PreparedStatement updMergePoint;
	protected PreparedStatement insVisitedPoints;
	protected PreparedStatement updateRegionBoundaries;
	protected PreparedStatement insertRegionBoundaries;
	protected PreparedStatement insLongRoads;
	private int maxPointDBID;
	private int maxClusterID;

	public HHRoutingPreparationDB(File dbFile) throws SQLException {
		super(dbFile, DBDialect.SQLITE.getDatabaseConnection(dbFile.getAbsolutePath(), LOG));
		if (!compactDB) {
			Statement st = conn.createStatement();
			st.execute("CREATE TABLE IF NOT EXISTS routeLongRoads(id, regionId, roadId, startIndex, points, PRIMARY key (id))");
			st.execute("CREATE TABLE IF NOT EXISTS routeRegions(id, name, filePointer, size, filename, left, right, top, bottom, PRIMARY key (id))");
			st.execute("CREATE TABLE IF NOT EXISTS routeRegionPoints(id, pntId, clusterId)");
			st.execute("CREATE INDEX IF NOT EXISTS routeRegionPointsIndex on routeRegionPoints(id)");
			insPoint = conn.prepareStatement("INSERT INTO points(idPoint, pointGeoUniDir, pointGeoId, clusterId, fileDbId, roadId, start, end, sx31, sy31, ex31, ey31) "
							+ " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
			insLongRoads = conn.prepareStatement("INSERT INTO routeLongRoads (id, regionId, roadId, startIndex, points) VALUES (?, ?, ?, ?, ? )");
			insVisitedPoints = conn.prepareStatement("INSERT INTO routeRegionPoints (id, pntId, clusterId) VALUES (?, ?, ?)");
			updateRegionBoundaries = conn.prepareStatement("UPDATE routeRegions SET left = ?, right = ?, top = ? , bottom = ? where id = ?");
			insertRegionBoundaries = conn.prepareStatement("INSERT INTO routeRegions(left, right, top, bottom, id) VALUES (?, ?, ?, ?, ?)");

			updDualPoint = conn.prepareStatement("UPDATE points SET dualIdPoint = ?, dualClusterId = ? WHERE idPoint = ?");
			updMergePoint = conn.prepareStatement("UPDATE points SET pointGeoUniDir = ?, pointGeoId = ?, roadId = ?, start = ?, end = ?, sx31 = ?, sy31 = ?, ex31 = ?, ey31 = ? WHERE idPoint = ?");
		}
	}
	
	public List<NetworkLongRoad> loadNetworkLongRoads() throws SQLException {
		List<NetworkLongRoad> res = new ArrayList<>();
		Statement s = conn.createStatement();
		ResultSet rs = s.executeQuery("SELECT id, roadId, startIndex, points from routeLongRoads" );
		while(rs.next()) {
			byte[] bytes = rs.getBytes(4);
			int l = bytes.length / 8;
			int[] pointsX = new int[l];
			int[] pointsY = new int[l];
			for (int k = 0; k < l; k++) {
				pointsX[k] = Algorithms.parseIntFromBytes(bytes, k * 8);
				pointsY[k] = Algorithms.parseIntFromBytes(bytes, k * 8 + 4);
			}
			NetworkLongRoad r = new NetworkLongRoad(rs.getLong(2), rs.getInt(3), pointsX, pointsY);
			r.dbId = rs.getInt(1);
			res.add(r);
		}
		rs.close();
		s.close();
		return res;
	}
	
	public int loadNetworkPoints(TLongObjectHashMap<NetworkBorderPoint> networkPointsCluster) throws SQLException {
		Statement s = conn.createStatement();
		ResultSet rs = s.executeQuery("SELECT pointGeoUniDir, pointGeoId, idPoint, clusterId, roadId, start, end, sx31, sy31, ex31, ey31, fileDbId FROM points ");
		while (rs.next()) {
			long pointGeoUniDir = rs.getLong(1);
			if (!networkPointsCluster.containsKey(pointGeoUniDir)) {
				networkPointsCluster.put(pointGeoUniDir, new NetworkBorderPoint(pointGeoUniDir));
			}
//			
			long roadId = rs.getLong(5);
			int st = rs.getInt(6);
			int end = rs.getInt(7);
			int sx = rs.getInt(8), sy = rs.getInt(9), ex = rs.getInt(10), ey = rs.getInt(11);
			RouteSegmentBorderPoint bp = new RouteSegmentBorderPoint(roadId, st, end, sx, sy, ex, ey);
			bp.pointDbId = rs.getInt(3);
			bp.clusterDbId = rs.getInt(4);
			bp.fileDbId = rs.getInt(12);
			networkPointsCluster.get(pointGeoUniDir).set(rs.getLong(2), bp);
			maxPointDBID = Math.max(bp.pointDbId, maxPointDBID);
			maxClusterID = Math.max(bp.clusterDbId, maxClusterID);
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
		  

	public static void compact(File source, File target) throws SQLException, IOException {
		System.out.printf("Compacting %s -> %s...\n", source.getName(), target.getName());
		target.delete();
		Connection src = DBDialect.SQLITE.getDatabaseConnection(source.getAbsolutePath(), LOG);
		Connection tgt = DBDialect.SQLITE.getDatabaseConnection(target.getAbsolutePath(), LOG);
		Statement st = tgt.createStatement();
		String columnNames = "pointGeoId, idPoint, clusterId, dualIdPoint, dualClusterId, chInd, roadId, start, end, sx31, sy31, ex31, ey31";
		int columnSize = columnNames.split(",").length;
		st.execute("CREATE TABLE IF NOT EXISTS profiles(profile, id, params)");
		st.execute("CREATE TABLE IF NOT EXISTS points(" + columnNames + ",  PRIMARY key (idPoint))"); 
		st.execute("CREATE TABLE IF NOT EXISTS segments(id, profile, ins, outs, PRIMARY key (id, profile))");
		PreparedStatement pIns = tgt.prepareStatement("INSERT INTO profiles(profile, id, params) VALUES (?, ?, ?)");
		TIntArrayList profiles = new TIntArrayList();
		ResultSet profileSet = src.createStatement().executeQuery("select profile, id, params from profiles");
		while (profileSet.next()) {
			pIns.setString(1, profileSet.getString(1));
			pIns.setInt(2, profileSet.getInt(2));
			pIns.setString(3, profileSet.getString(3));
			pIns.execute();
			profiles.add(profileSet.getInt(2));
		}
		String insPnts = "";
		for (int k = 0; k < columnSize; k++) {
			if (k > 0) {
				insPnts += ",";
			}
			insPnts += "?";
		}
		
		HHRoutingDB sourceDB = new HHRoutingDB(source, src);
		TLongObjectHashMap<NetworkDBPointPrep> pointsById = sourceDB.loadNetworkPoints(NetworkDBPointPrep.class);
		TIntObjectHashMap<List<NetworkDBPointPrep>> outPoints = HHRoutePlanner.groupByClusters(pointsById, true);
		TIntObjectHashMap<List<NetworkDBPointPrep>> inPoints = HHRoutePlanner.groupByClusters(pointsById, false);
		pIns = tgt.prepareStatement("INSERT INTO points(" + columnNames + ") VALUES (" + insPnts + ")");
		ResultSet rs = src.createStatement().executeQuery(" select " + columnNames + " from points");
		while (rs.next()) {
			for (int i = 0; i < columnSize; i++) {
				pIns.setObject(i + 1, rs.getObject(i + 1));
			}
			pIns.addBatch();
		}
		pIns.executeBatch();
		
		for (int profile : profiles.toArray()) {
			PreparedStatement sIns = tgt.prepareStatement("INSERT INTO segments(id, profile, ins, outs)  VALUES (?, ?, ?, ?)");
			PreparedStatement selOut = src.prepareStatement(
					" select idConnPoint, dist, shortcut from segments where idPoint = ? and profile = " + profile);
			PreparedStatement selIn = src.prepareStatement(
					" select idPoint, dist, shortcut from segments where idConnPoint = ? and profile = " + profile);
			for (NetworkDBPointPrep p : pointsById.valueCollection()) {
				selIn.setInt(1, p.index);
				selOut.setInt(1, p.index);
				sIns.setInt(1, p.index);
				sIns.setInt(2, profile);
//			System.out.println(p.index  + " -> cluster " + p.clusterId + " dual clusterId  " + p.dualPoint.clusterId);
//			System.out.println("Incoming Cluster: " + inPoints.get(p.clusterId));
				sIns.setBytes(3, prepareSegments(selIn, pointsById, inPoints.get(p.clusterId)));
//			System.out.println("Outgoing cluster: " + outPoints.get(p.dualPoint.clusterId));
				sIns.setBytes(4, prepareSegments(selOut, pointsById, outPoints.get(p.dualPoint.clusterId)));
				sIns.addBatch();
			}
			sIns.executeBatch();
		}
		tgt.close();
	}
	
	static class NetworkDBPointPrep extends NetworkDBPoint {
		int distSegment;
		int chIndexEdgeDiff;
		int chFinalInd;
		int chIndexCnt;
		boolean midSaved;
		int midMaxDepth;
		int midProc;
		int midDepth;
		int midPrevMaxDepth;
	}
	
	private static byte[] prepareSegments(PreparedStatement selIn, TLongObjectHashMap<NetworkDBPointPrep> pointsById, List<NetworkDBPointPrep> pnts) throws SQLException, IOException {
		TByteArrayList tbs = new TByteArrayList();
		ResultSet q = selIn.executeQuery();
		BinaryMapIndexWriter bmiw = new BinaryMapIndexWriter(null, null);
		for (NetworkDBPointPrep p : pnts) {
			p.distSegment = 0;
		}
		while (q.next()) {
			int conn = q.getInt(1);
			int distInt = (int) Math.max(1, q.getFloat(2) * 10);
			NetworkDBPoint connPoint = pointsById.get(conn);
			int ind = Collections.binarySearch(pnts, connPoint, indexComparator);
			if (ind < 0) {
				throw new IllegalStateException();
			}
			pnts.get(ind).distSegment = distInt;
		}
		for (NetworkDBPointPrep p : pnts) {
//			if (p.tmpIndex == 0) {
//				indZeros++;
//			}
			bmiw.writeRawVarint32(tbs, p.distSegment);
		}
		return tbs.toArray();
	}

	public void recreateSegments() throws SQLException {
		Statement st = conn.createStatement();
		st.execute("DELETE FROM segments");
		st.execute("DELETE FROM profiles");
		st.execute("DELETE FROM geometry");
		st.close();
	}
	
	

	public void updatePointsCHInd(Collection<NetworkDBPointPrep> pnts) throws SQLException {
		PreparedStatement updCHInd = conn.prepareStatement("UPDATE  points SET chInd = ? where idPoint = ?");
		int ind = 0;
		for (NetworkDBPointPrep p : pnts) {
			updCHInd.setLong(1, p.chFinalInd);
			updCHInd.setLong(2, p.index);
			updCHInd.addBatch();
			if (ind++ % BATCH_SIZE == 0) {
				updCHInd.executeBatch();
			}
		}
		updCHInd.executeBatch();
		updCHInd.close();
	}
	
	public void insertProcessedRegion(NetworkRouteRegion networkRouteRegion, 
			TLongObjectHashMap<NetworkBorderPoint> borderPoints, List<NetworkLongRoad> roads) throws SQLException {
		insertBorderPoints(borderPoints);
		int ins = insertVisitedPoints(networkRouteRegion);
		if (ins > 0) {
			updateRegionBbox(networkRouteRegion);
		}
		updateLongRoads(networkRouteRegion, roads);
		
	}
	
	public void cleanupProcessedRegion(NetworkRouteRegion networkRouteRegion, 
			TLongObjectHashMap<NetworkBorderPoint> borderPoints, List<NetworkLongRoad> longRoads) throws SQLException {
		Iterator<NetworkLongRoad> it = longRoads.iterator();
		while (it.hasNext()) {
			NetworkLongRoad r = it.next();
			if (r.dbId < 0) {
				it.remove();
			}
		}
		TLongObjectIterator<NetworkBorderPoint> its = borderPoints.iterator();
		while (its.hasNext()) {
			its.advance();
			NetworkBorderPoint npnt = its.value();
			if (npnt.positiveObj != null && !npnt.positiveObj.inserted) {
				npnt.positiveObj = null;
			}
			if (npnt.negativeObj != null && !npnt.negativeObj.inserted) {
				npnt.negativeObj = null;
			}
			if (npnt.positiveObj == null && npnt.negativeObj == null) {
				its.remove();
			}
		}

	}

	private void updateLongRoads(NetworkRouteRegion networkRouteRegion, List<NetworkLongRoad> roads)
			throws SQLException {
		int max = -1;
		for (NetworkLongRoad r : roads) {
			max = Math.max(r.dbId, max);
		}
		for (NetworkLongRoad r : roads) {
			if (r.dbId < 0) {
				r.dbId = ++max;
				insLongRoads.setInt(1, r.dbId);
				insLongRoads.setInt(2, networkRouteRegion.id);
				insLongRoads.setLong(3, r.roadId);
				insLongRoads.setInt(4, r.startIndex);
				byte[] ar = new byte[r.pointsX.length * 8];
				for (int k = 0; k < r.pointsX.length; k++) {
					Algorithms.putIntToBytes(ar, k * 8, r.pointsX[k]);
					Algorithms.putIntToBytes(ar, k * 8 + 4, r.pointsY[k]);
				}
				insLongRoads.setBytes(5, ar);
				insLongRoads.execute();
			}
		}
	}

	private void updateRegionBbox(NetworkRouteRegion networkRouteRegion) throws SQLException {
		@SuppressWarnings("resource")
		PreparedStatement p = networkRouteRegion.id < 0 ? insertRegionBoundaries : updateRegionBoundaries;
		QuadRect r = networkRouteRegion.rect;
		p.setDouble(1, r.left);
		p.setDouble(2, r.right);
		p.setDouble(3, r.top);
		p.setDouble(4, r.bottom);
		p.setInt(5, networkRouteRegion.id);
		p.execute();
	}

	private int insertVisitedPoints(NetworkRouteRegion networkRouteRegion) throws SQLException {
		int ind = 0;
		if (networkRouteRegion.visitedVertices.size() == 0) {
			return ind;
		}
		TLongIntIterator it = networkRouteRegion.visitedVertices.iterator();
		while (it.hasNext()) {
			it.advance();
			insVisitedPoints.setLong(1, networkRouteRegion.id);
			insVisitedPoints.setLong(2, it.key());
			insVisitedPoints.setInt(3, it.value());
			insVisitedPoints.addBatch();
			if (ind++ % BATCH_SIZE == 0) {
				insVisitedPoints.executeBatch();
			}
		}
		insVisitedPoints.executeBatch();
		return ind;
	}

	private void insertBorderPoints(TLongObjectHashMap<NetworkBorderPoint> borderPoints) throws SQLException {
		int batchInsPoint = 0;
		for (NetworkBorderPoint npnt : borderPoints.valueCollection()) {
			boolean ins = false;
			if (npnt.positiveObj != null && !npnt.positiveObj.inserted) {
				insPoint(npnt.positiveObj);
				npnt.positiveObj.inserted = true;
				ins = true;
				batchInsPoint++;
			}
			if (npnt.negativeObj != null && !npnt.negativeObj.inserted) {
				insPoint(npnt.negativeObj);
				npnt.negativeObj.inserted = true;
				ins = true;
				batchInsPoint++;
			}
			if (ins && npnt.positiveObj != null && npnt.negativeObj != null) {
				updDualPoint.setInt(1, npnt.negativeObj.pointDbId);
				updDualPoint.setInt(2, npnt.negativeObj.clusterDbId);
				updDualPoint.setInt(3, npnt.positiveObj.pointDbId);
				updDualPoint.addBatch();
				updDualPoint.setInt(1, npnt.positiveObj.pointDbId);
				updDualPoint.setInt(2, npnt.positiveObj.clusterDbId);
				updDualPoint.setInt(3, npnt.negativeObj.pointDbId);
				updDualPoint.addBatch();
				batchInsPoint++;
			}
			if (batchInsPoint > BATCH_SIZE) {
				batchInsPoint = 0;
				insPoint.executeBatch();
				updDualPoint.executeBatch();
			}
		}
		if (batchInsPoint > 0) {
			insPoint.executeBatch();
			updDualPoint.executeBatch();
		}
	}

	private void insPoint(RouteSegmentBorderPoint obj)
			throws SQLException {
		int p = 1;
		insPoint.setInt(p++, obj.pointDbId);
		insPoint.setLong(p++, obj.unidirId);
		insPoint.setLong(p++, obj.uniqueId);
		insPoint.setInt(p++, obj.clusterDbId);
		insPoint.setInt(p++, obj.fileDbId);
		insPoint.setLong(p++, obj.roadId);
		insPoint.setInt(p++, obj.segmentStart);
		insPoint.setInt(p++, obj.segmentEnd);
		insPoint.setInt(p++, obj.sx);
		insPoint.setInt(p++, obj.sy);
		insPoint.setInt(p++, obj.ex);
		insPoint.setInt(p++, obj.ey);
		insPoint.addBatch();
	}
	
	public void deleteShortcuts() throws SQLException {
		Statement st = conn.createStatement();
		st.execute("DELETE from segments where shortcut > 0");
		st.execute("DELETE from geometry where shortcut > 0");
		st.close();
	}
	
	public void insertSegments(List<NetworkDBSegment> segments, int routingProfile) throws SQLException {
		if (insSegment == null) {
			insSegment = conn.prepareStatement("INSERT INTO segments(idPoint, idConnPoint, dist, shortcut, profile) VALUES(?, ?, ?, ?, ?)");
		}
		if (insGeometry == null) {
			insGeometry = conn.prepareStatement("INSERT INTO geometry(idPoint, idConnPoint, shortcut, geometry, profile) VALUES(?, ?, ?, ?, ?)");
		}
		int ind= 0;
		for (NetworkDBSegment s : segments) {
			insSegment.setLong(1, s.start.index);
			insSegment.setLong(2, s.end.index);
			insSegment.setDouble(3, s.dist);
			insSegment.setInt(4, s.shortcut ? 1 : 0);
			insSegment.setInt(5, routingProfile);
			insSegment.addBatch();
//			byte[] coordinates = new byte[0];
			if (s.getGeometry().size() > 0) {
				List<LatLon> geometry = s.getGeometry();
				byte[] coordinates = new byte[8 * geometry.size()];
				for (int t = 0; t < geometry.size(); t++) {
					LatLon l = geometry.get(t);
					Algorithms.putIntToBytes(coordinates, 8 * t, MapUtils.get31TileNumberX(l.getLongitude()));
					Algorithms.putIntToBytes(coordinates, 8 * t + 4, MapUtils.get31TileNumberY(l.getLatitude()));
				}
				insGeometry.setBytes(4, coordinates);
			} else if (s instanceof NetworkDBSegmentPrep && ((NetworkDBSegmentPrep) s).segmentsStartEnd.size() > 0) {
				NetworkDBSegmentPrep ps = ((NetworkDBSegmentPrep) s);
				byte[] coordinates = new byte[4 * ps.segmentsStartEnd.size() + 8];
				Algorithms.putIntToBytes(coordinates, 0, XY_SHORTCUT_GEOM);
				Algorithms.putIntToBytes(coordinates, 4, XY_SHORTCUT_GEOM);
				for (int t = 0; t < ps.segmentsStartEnd.size(); t++) {
					Algorithms.putIntToBytes(coordinates, 4 * t + 8, ps.segmentsStartEnd.getQuick(t));
				}
				insGeometry.setBytes(4, coordinates);
			}
			insGeometry.setLong(1, s.start.index);
			insGeometry.setLong(2, s.end.index);
			insGeometry.setInt(3, s.shortcut ? 1 : 0);
			insGeometry.setInt(5, routingProfile);
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
	
	public void mergePoints(RouteSegmentBorderPoint posMain, RouteSegmentBorderPoint negMerge, RouteSegmentBorderPoint newNeg) {
		// merge to posDir
		try {
			insVisitedPoints.setLong(1, negMerge.fileDbId);
			insVisitedPoints.setLong(2, negMerge.unidirId);
			insVisitedPoints.setInt(3, negMerge.clusterDbId);
			insVisitedPoints.addBatch();
			insVisitedPoints.executeBatch();
			
			int p = 1;
			updMergePoint.setLong(p++, newNeg.unidirId);
			updMergePoint.setLong(p++, newNeg.uniqueId);
			updMergePoint.setLong(p++, newNeg.roadId);
			updMergePoint.setLong(p++, newNeg.segmentStart);
			updMergePoint.setLong(p++, newNeg.segmentEnd);
			updMergePoint.setLong(p++, newNeg.sx);
			updMergePoint.setLong(p++, newNeg.sy);
			updMergePoint.setLong(p++, newNeg.ex);
			updMergePoint.setLong(p++, newNeg.ey);
			updMergePoint.setInt(p++, newNeg.pointDbId);
			updMergePoint.execute();
			
			updDualPoint.setInt(1, newNeg.pointDbId);
			updDualPoint.setInt(2, newNeg.clusterDbId);
			updDualPoint.setInt(3, posMain.pointDbId);
			updDualPoint.addBatch();
			
			updDualPoint.setInt(1, posMain.pointDbId);
			updDualPoint.setInt(2, posMain.clusterDbId);
			updDualPoint.setInt(3, newNeg.pointDbId);
			updDualPoint.addBatch();
			updDualPoint.executeBatch();
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}
	}
	
	public void loadMidPointsIndex(TLongObjectHashMap<NetworkDBPointPrep> pntsMap, Collection<NetworkDBPointPrep> pointsList, boolean update) throws SQLException {
		Statement s = conn.createStatement();
		// rtCnt -> midMaxDepth
		// rtIndex -> midProc
		for (NetworkDBPointPrep p : pointsList) {
			p.midSaved = false;
		}
		PreparedStatement ps = conn.prepareStatement("UPDATE midpoints SET maxMidDepth = ?, proc = ? where ind = ?");
		int batch = 0;
		ResultSet rs = s.executeQuery("SELECT ind, maxMidDepth, proc  FROM midpoints ");
		while (rs.next()) {
			int ind = rs.getInt(1);
			NetworkDBPointPrep pnt = pntsMap.get(ind);
			boolean upd = false;
			if (pnt.midMaxDepth > rs.getInt(2)) {
				upd = true;
			} else {
				pnt.midMaxDepth = rs.getInt(2);
			}
			if (pnt.midProc == 1 && rs.getInt(3) == 0) {
				upd = true;
			} else {
				pnt.midProc = rs.getInt(3);
			}
			if (upd) {
				ps.setLong(1, pnt.midMaxDepth);
				ps.setLong(2, pnt.midProc);
				ps.setLong(3, pnt.index);
				ps.addBatch();
				if (batch++ > 1000) {
					batch = 0;
					ps.executeBatch();
				}
			}
			pnt.midSaved = true;
		}
		ps.executeBatch();
		ps = conn.prepareStatement("INSERT INTO midpoints(ind, maxMidDepth, proc) VALUES(?, ?, ?)");
		batch = 0;
		for (NetworkDBPointPrep p : pointsList) {
			if (!p.midSaved && (p.midMaxDepth > 0 || p.midProc > 0)) {
				ps.setLong(1, p.index);
				ps.setLong(2, p.midMaxDepth);
				ps.setLong(3, p.midProc);
				ps.addBatch();
				if (batch++ > 1000) {
					batch = 0;
					ps.executeBatch();
				}
			}
		}
		ps.executeBatch();
	}

	public int prepareBorderPointsToInsert(int fileId, List<RouteSegmentBorderPoint> borderPoints, TLongObjectHashMap<NetworkBorderPoint> pointDbInd) {
		int clusterIndex = ++maxClusterID;
		for (RouteSegmentBorderPoint obj : borderPoints) {
			if (!pointDbInd.containsKey(obj.unidirId)) {
				pointDbInd.put(obj.unidirId, new NetworkBorderPoint(obj.unidirId));
			}
			NetworkBorderPoint npnt = pointDbInd.get(obj.unidirId);
			obj.pointDbId = ++maxPointDBID;
			obj.clusterDbId = clusterIndex;
			obj.fileDbId = fileId;
			
			npnt.set(obj.unidirId, obj);
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
		RouteSegmentBorderPoint positiveObj;
		RouteSegmentBorderPoint negativeObj;
		
		public NetworkBorderPoint(long unidirId) {
			this.unidirId = unidirId;
		}
		
		public void set(long geoId, RouteSegmentBorderPoint obj) {
			if (obj.isPositive()) {
				if (positiveObj != null) {
					String msg = String.format("Geoid %d was already assigned %d (%d) in cluster %d (%d)  %s",
							geoId, obj.pointDbId, positiveObj.pointDbId, obj.clusterDbId, positiveObj.clusterDbId, obj);
					throw new IllegalStateException(msg);
				}
				positiveObj = obj;
			} else {
				if (negativeObj != null) {
					String msg = String.format("Geoid %d was already assigned %d (%d) in cluster %d (%d)  %s",
							geoId, obj.pointDbId, negativeObj.pointDbId, obj.clusterDbId, negativeObj.clusterDbId, obj);
					throw new IllegalStateException(msg);
				}
				negativeObj = obj;
			}
		}
	}
	
	public static class NetworkLongRoad {
		public int dbId = -1;
		public final long roadId;
		public final int startIndex;
		public final int[] pointsY;
		public final int[] pointsX;
		public List<NetworkLongRoad> connected = new ArrayList<>();
		public TLongHashSet points = new TLongHashSet();

		public NetworkLongRoad(long roadId, int startIndex, int[] pointsX, int[] pointsY) {
			this.roadId = roadId;
			this.startIndex = startIndex;
			this.pointsX = pointsX;
			this.pointsY = pointsY;
			for (int k = 0; k < pointsX.length; k++) {
				points.add(MapUtils.interleaveBits(pointsX[k], pointsY[k]));
			}
		}
		
		public QuadRect getQuadRect() {
			QuadRect qr = null;
			for (int k = 0; k < pointsX.length; k++) {
				double lat = MapUtils.get31LatitudeY(pointsY[k]);
				double lon = MapUtils.get31LongitudeX(pointsX[k]);
				qr = HHRoutingUtilities.expandLatLonRect(qr, lon, lat, lon, lat);
				
			}
			return qr;
		}
		
		public void addConnected(List<NetworkLongRoad> longRoads) {
			for (NetworkLongRoad r : longRoads) {
				if (this != r) {
					boolean intersects = false;
					for (long pnt : r.points.toArray()) {
						if (points.contains(pnt)) {
							intersects = true;
							break;
						}
					}
					if (intersects) {
						connected.add(r);
					}
				}
			}
		}
		
		@Override
		public String toString() {
			return String.format("Long road %d index %d", roadId / 64, startIndex);
		}
	}

	static class NetworkRouteRegion {
		int id = 0;
		RouteRegion region;
		File file;
		int points = 0; // -1 loaded points, 0 init, > 0 - visitedVertices = null
		TLongIntHashMap visitedVertices;
		QuadRect rect;
		QuadRect calcRect;

		public NetworkRouteRegion(RouteRegion r, File f, QuadRect qrect) {
			region = r;
			if (region != null) {
				rect = new QuadRect(region.getLeftLongitude(), region.getTopLatitude(), region.getRightLongitude() ,
						region.getBottomLatitude());
			} else if (qrect != null) {
				rect = qrect;
			} else {
				rect = new QuadRect(-180, 85, 180, -85);
			}
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
				if (x31 < calcRect.left) {
					calcRect.left = x31;
				} else if (x31 > calcRect.right) {
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

		public boolean intersects(NetworkRouteRegion nrouteRegion, double d) {
			QuadRect qr = new QuadRect(Math.max(-180, rect.left - d), Math.min(85, rect.top + d),
					Math.min(180, rect.right + d), Math.max(-85, rect.bottom - d));
			return QuadRect.intersects(qr, nrouteRegion.rect);
		}

		public void unload() {
			if (this.visitedVertices != null && this.visitedVertices.size() > 100000) {
				this.points = this.visitedVertices.size();
				this.visitedVertices = null;
			}
		}

		public TLongIntHashMap loadVisitedVertices(HHRoutingPreparationDB networkDB) throws SQLException {
			if (points >= 0) {
				if (visitedVertices != null && visitedVertices.size() > 0) {
					throw new IllegalStateException();
				}
				visitedVertices = networkDB.loadVisitedVertices(id);
				points = -1;
				logf("Loaded visited vertices for %s - %d.", region.getName(),
						visitedVertices.size());
			}
			return visitedVertices;
		}

		public String getName() {
			return region == null ? "Worldwide" : region.getName();
		}

	}

	static class NetworkDBSegmentPrep extends NetworkDBSegment {

		TIntArrayList segmentsStartEnd = new TIntArrayList();

		public NetworkDBSegmentPrep(NetworkDBPoint start, NetworkDBPoint end, double dist, boolean direction,
				boolean shortcut) {
			super(start, end, dist, direction, shortcut);
		}
	}
	

}