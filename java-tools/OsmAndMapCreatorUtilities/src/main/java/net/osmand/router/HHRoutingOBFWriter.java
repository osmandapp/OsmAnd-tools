package net.osmand.router;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.logging.Log;

import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.osmand.PlatformUtil;
import net.osmand.obf.preparation.AbstractIndexPartCreator;
import net.osmand.obf.preparation.BinaryMapIndexWriter;
import net.osmand.obf.preparation.IndexVectorMapCreator;
import net.osmand.router.HHRoutingPreparationDB.NetworkDBPointPrep;
import rtree.Element;
import rtree.IllegalValueException;
import rtree.LeafElement;
import rtree.RTree;
import rtree.RTreeException;
import rtree.RTreeInsertException;
import rtree.Rect;

public class HHRoutingOBFWriter {
	final static Log LOG = PlatformUtil.getLog(HHRoutingOBFWriter.class);
	private static final int BLOCK_SEGMENTS_SIZE = 24;
	
	public static void main(String[] args) throws IOException, SQLException, IllegalValueException {
		File f;
		if (args.length == 0) {
			String mapName = "Germany_car.chdb";
			mapName = "Netherlands_europe_car.chdb";
			mapName = "__europe_car.chdb";
//			mapName = "_road_car.chdb";
			f = new File(System.getProperty("maps.dir"), mapName);
		} else {
			f = new File(args[0]);
		}
		new HHRoutingOBFWriter().writeFile(f);
	}
	
	public void writeFile(File dbFile) throws IOException, SQLException, IllegalValueException {
		File outFile = new File(dbFile.getParentFile(),
				dbFile.getName().substring(0, dbFile.getName().lastIndexOf('.')) + ".obf");
		String rTreeFile = outFile.getAbsolutePath() + ".rtree";
		String rpTreeFile = outFile.getAbsolutePath() + ".rptree";
		long edition = dbFile.lastModified(); // System.currentTimeMillis();
		try {
			HHRoutingPreparationDB db = new HHRoutingPreparationDB(dbFile);
			String profile = db.getRoutingProfile();
			TIntObjectHashMap<String> routingProfiles = db.getRoutingProfiles();
			int pInd = 0;
			String[] profileParams = new String[routingProfiles.size()];
			int[] profileParamsKeys = new int[routingProfiles.size()];
			for (int p : routingProfiles.keys()) {
				profileParamsKeys[pInd] = p;
				profileParams[pInd] = routingProfiles.get(p);
				pInd++;
			}
			BinaryMapIndexWriter bmiw = new BinaryMapIndexWriter(new RandomAccessFile(outFile, "rw"), edition);
			bmiw.startHHRoutingIndex(edition, profile, profileParams);
			RTree routeTree = new RTree(rTreeFile);
			

			TLongObjectHashMap<NetworkDBPointPrep> points = db.loadNetworkPoints(NetworkDBPointPrep.class);
			for (NetworkDBPointPrep pnt : points.valueCollection()) {
				routeTree.insert(new LeafElement(new Rect(pnt.midX(), pnt.midY(), pnt.midX(), pnt.midY()), pnt.index));
			}
			routeTree = AbstractIndexPartCreator.packRtreeFile(routeTree, rTreeFile, rpTreeFile);
			
			long rootIndex = routeTree.getFileHdr().getRootIndex();
			rtree.Node root = routeTree.getReadNode(rootIndex);
			Rect rootBounds = IndexVectorMapCreator.calcBounds(root);
			if (rootBounds != null) {
				List<NetworkDBPointPrep> pntsList = writeBinaryRouteTree(root, rootBounds, routeTree, bmiw, points, new int[] {0});
				pntsList.sort(new Comparator<NetworkDBPointPrep>() {
					@Override
					public int compare(NetworkDBPointPrep o1, NetworkDBPointPrep o2) {
						return Integer.compare(o1.fileId, o2.fileId);
					}
				});
				for (int i = 0; i < profileParamsKeys.length; i++) {
					writeSegments(db, i, profileParamsKeys[i], bmiw, pntsList, 0);
				}
			}
			bmiw.endHHRoutingIndex();
			bmiw.close();

			RandomAccessFile file = routeTree.getFileHdr().getFile();
			file.close();
		} catch (RTreeException | RTreeInsertException e) {
			throw new IOException(e);
		} finally {
			new File(rTreeFile).delete();
			new File(rpTreeFile).delete();
		}
		RTree.clearCache();
	}


	private void writeSegments(HHRoutingPreparationDB db, int profile, int dbProfile, BinaryMapIndexWriter writer, 
			List<NetworkDBPointPrep> pntsList, int shift) throws IOException, SQLException {
		writer.startHHRouteBlockSegments(shift, pntsList.size(), profile);
		if (pntsList.size() > BLOCK_SEGMENTS_SIZE) {
			int range = (pntsList.size() - 1) / BLOCK_SEGMENTS_SIZE + 1;
			for (int i = 0; i < pntsList.size(); i += range) {
				int start = i;
				int end = Math.min(i + range, pntsList.size());
				writeSegments(db, profile, dbProfile, writer, pntsList.subList(start, end), shift + start);
			}
		} else {
			for (NetworkDBPointPrep ind : pntsList) {
				if (shift != ind.fileId) {
					throw new IllegalStateException(shift + " != " + ind.fileId);
				}
				byte[][] res = new byte[2][];
				db.loadSegmentPointInternal(ind.index, dbProfile, res);
				writer.writePointSegments(res[0], res[1]);
				shift++;
			}
		}
		writer.endHHRouteBlockSegments();
	}

	private List<NetworkDBPointPrep> writeBinaryRouteTree(rtree.Node parent, Rect re, RTree r, BinaryMapIndexWriter writer,
			TLongObjectHashMap<NetworkDBPointPrep> points, int[] pntId)
			throws IOException, RTreeException {
		Element[] es = parent.getAllElements();
		writer.startHHRouteTreeElement(re.getMinX(), re.getMaxX(), re.getMinY(), re.getMaxY());
		List<NetworkDBPointPrep> l = new ArrayList<>();
		boolean leaf = false;
		for (int i = 0; i < parent.getTotalElements(); i++) {
			Element e = es[i];
			if (e.getElementType() != rtree.Node.LEAF_NODE) {
				if (leaf) {
					throw new IllegalStateException();
				}
				rtree.Node chNode = r.getReadNode(e.getPtr());
				List<NetworkDBPointPrep> ps = writeBinaryRouteTree(chNode, e.getRect(), r, writer, points, pntId);
				l.addAll(ps);
			} else {
				leaf = true;
				NetworkDBPointPrep pnt = points.get(e.getPtr());
				pnt.fileId = pntId[0]++;
				l.add(pnt);
			}
		}
		if (l.size() > 0 && leaf) {
			writer.writeHHRoutePoints(l);
		}
		writer.endRouteTreeElement();
		return l;
	}
		

}