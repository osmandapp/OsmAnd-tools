package net.osmand.osm.util;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteRegion;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteSubregion;
import net.osmand.binary.RouteDataObject;
import net.osmand.router.RoutePlannerFrontEnd;
import net.osmand.router.RoutePlannerFrontEnd.RouteCalculationMode;
import net.osmand.router.RoutingConfiguration;
import net.osmand.router.RoutingConfiguration.Builder;
import net.osmand.router.RoutingContext;
import net.osmand.router.RoutingContext.RoutingSubregionTile;

public class CheckRoadConnectivity {
	
	public static void main(String[] args) throws IOException {
		
		CheckRoadConnectivity crc = new CheckRoadConnectivity();
		RandomAccessFile raf = new RandomAccessFile("/home/victor/projects/osmand/osm-gen/Netherlands_europe_2.obf", "r"); //$NON-NLS-1$ //$NON-NLS-2$
		ClusteringContext ctx = new ClusteringContext();
		crc.clustering(ctx, new BinaryMapIndexReader(raf));
		
	}
	
	private static class ClusteringContext {
	}

	
	public void clustering(final ClusteringContext clusterCtx, BinaryMapIndexReader reader)
			throws IOException {
		RoutePlannerFrontEnd router = new RoutePlannerFrontEnd(false);
		Builder builder = RoutingConfiguration.getDefault();
		RoutingConfiguration config = builder.build("car", RoutingConfiguration.DEFAULT_MEMORY_LIMIT * 3);
		RoutingContext ctx = router.buildRoutingContext(config, null, new BinaryMapIndexReader[] { reader },
				RouteCalculationMode.BASE);
		if (reader.getRoutingIndexes().size() != 1) {
			throw new UnsupportedOperationException();
		}
		RouteRegion reg = reader.getRoutingIndexes().get(0);
		List<RouteSubregion> baseSubregions = reg.getBaseSubregions();
		List<RoutingSubregionTile> tiles = new ArrayList<RoutingContext.RoutingSubregionTile>();
		for (RouteSubregion s : baseSubregions) {
			List<RoutingSubregionTile> loadTiles = ctx.loadAllSubregionTiles(reader, s);
			tiles.addAll(loadTiles);
		}
		
		List<Cluster> allClusters = new ArrayList<CheckRoadConnectivity.Cluster>();
		for(RoutingSubregionTile tile : tiles) {
			List<Cluster> clusters = processDataObjects(ctx, tile);
			allClusters.addAll(clusters);
		}
		
		combineClusters(allClusters);
		System.out.println("Tiles " + tiles.size() + " clusters " +allClusters.size());
		for(Cluster c : allClusters) {
			if(c.roadsIncluded > 100) {
				System.out.println("Main Cluster roads = " + c.roadsIncluded + " id = "+ c.initalRoadId);
			} else {
				System.out.println("Cluster roads = " + c.roadsIncluded + " id = "+ c.initalRoadId + " " + c.highways + " " + c.roadIds);
			}
		}

	}
	
	private void combineClusters(List<Cluster> allClusters) {
		boolean change = true;
		int cnt = 0;
		while (change) {
			change = false;
			for(int i = 0; i < allClusters.size(); i++) {
				Cluster c = allClusters.get(i);
				for(int j = i + 1; j < allClusters.size(); ) {
					if((cnt ++) % 1000000 == 0) {
						System.out.println("Cluster to process left  " + allClusters.size() +" ...");
					}
					Cluster toMerge = allClusters.get(j);
					if (c != toMerge && mergeable(c, toMerge)) {
						change = true;
						c.merge(toMerge);
						allClusters.remove(j);
					} else {
						j++;
					}
				}
			}
		}
		
	}

	private boolean mergeable(Cluster a, Cluster b) {
		boolean order = a.points.size() < b.points.size();
		Cluster as = order ? a : b;
		Cluster bs = order ? b : a;
		TLongIterator iterator = as.points.iterator();
		while(iterator.hasNext()) {
			if(bs.points.contains(iterator.next())) {
				return true;
			}
		}
		return false;
	}

	private class Cluster {
		long initalRoadId;
		int roadsIncluded;
		TLongHashSet points = new TLongHashSet();
		TLongHashSet roadIds = new TLongHashSet();
		Set<String> highways = new HashSet<String>();
		public void merge(Cluster toMerge) {
			roadsIncluded += toMerge.roadsIncluded;
			points.addAll(toMerge.points);
			roadIds.addAll(toMerge.roadIds);
			highways.addAll(toMerge.highways);
			
		}
	}

	private List<Cluster> processDataObjects(RoutingContext ctx, RoutingSubregionTile tl) {
		ArrayList<RouteDataObject> dataObjects = new ArrayList<RouteDataObject>();
		ctx.loadSubregionTile(tl, false, dataObjects);
		TLongObjectHashMap<List<RouteDataObject>> clusterPoints = new TLongObjectHashMap<List<RouteDataObject>>();
		for(RouteDataObject rdo : dataObjects) {
			for(int i = 0 ; i < rdo.getPointsLength(); i++) {
				long pointId = calcPointId(rdo, i);
				List<RouteDataObject> list = clusterPoints.get(pointId);
				if(list == null) {
					list = new LinkedList<RouteDataObject>();
					clusterPoints.put(pointId, list);
				}
				list.add(rdo);
			}
		}
		
		HashSet<RouteDataObject> toVisit = new HashSet<RouteDataObject>(dataObjects);
		List<Cluster> clusters = new ArrayList<Cluster>();
		while(!toVisit.isEmpty()) {
			RouteDataObject firstObject = toVisit.iterator().next();
			LinkedList<RouteDataObject> queue = new LinkedList<RouteDataObject>();
			queue.add(firstObject);
			Cluster c = new Cluster();
			c.initalRoadId = firstObject.id;
			clusters.add(c);
			while (!queue.isEmpty()) {
				RouteDataObject rdo = queue.poll();
				if(!toVisit.contains(rdo)) {
					continue;
				}
				c.roadsIncluded++;
				c.roadIds.add(rdo.id);
				String hw = rdo.getHighway();
				if(hw == null) {
					hw = rdo.getRoute();
				}
				c.highways.add(hw);
				toVisit.remove(rdo);
				for (int i = 0; i < rdo.getPointsLength(); i++) {
					long pointId = calcPointId(rdo, i);
					c.points.add(pointId);
					List<RouteDataObject> list = clusterPoints.get(pointId);
					for(RouteDataObject r : list) {
						if(r.id != rdo.id && toVisit.contains(r)) {
							queue.add(r);
						}
					}
				}
			}
		}
		return clusters;
	}

	private long calcPointId(RouteDataObject rdo, int i) {
		return ((long)rdo.getPoint31XTile(i) << 31l) + (long)rdo.getPoint31YTile(i);
	}
	
}	

