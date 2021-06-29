package net.osmand.obf.preparation;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import net.osmand.binary.MapZooms;
import net.osmand.data.TransportRoute;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.RouteActivityType;
import net.osmand.osm.MapRenderingTypesEncoder.EntityConvertApplyType;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Way;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.OSMSettings.OSMTagKey;
import net.osmand.osm.edit.Relation.RelationMember;
import net.osmand.util.MapUtils;

public class IndexRouteRelationCreator {
	private final static Log log = LogFactory.getLog(IndexRouteRelationCreator.class);
	public static long GENERATE_OBJ_ID = - (1l << 20l); // million million
	private static final double DIST_STEP = 25;  
	
	protected final Log logMapDataWarn;
	private final Map<String, Integer> indexRouteRelationTypes = new TreeMap<String, Integer>();
	private final MapRenderingTypesEncoder renderingTypes;
	private final MapZooms mapZooms;
	private final IndexCreatorSettings settings;

	public IndexRouteRelationCreator(Log logMapDataWarn, MapZooms mapZooms, MapRenderingTypesEncoder renderingTypes,
			IndexCreatorSettings settings) {
		this.logMapDataWarn = logMapDataWarn;
		this.mapZooms = mapZooms;
		this.settings = settings;
		this.renderingTypes = renderingTypes;
	}
	
	
	public void iterateRelation(Entity e, OsmDbAccessorContext ctx, IndexCreationContext icc) throws SQLException {
		if (e instanceof Relation) {
			long ts = System.currentTimeMillis();
			processRouteRelation((Relation) e, ctx, icc);
			long tm = (System.currentTimeMillis() - ts) / 1000;
			if (tm > 15) {
				log.warn(String.format("Route relation %d took %d seconds to process", e.getId(), tm));
			}		
		}		
	}
	
	public void iterateMainEntity(Entity e, OsmDbAccessorContext ctx, IndexCreationContext icc) throws SQLException {
		
	}
	
	private void processRouteRelation(Relation e, OsmDbAccessorContext ctx, IndexCreationContext icc) throws SQLException {
		Map<String, String> tags = renderingTypes.transformTags(e.getTags(), EntityType.RELATION, EntityConvertApplyType.MAP);
		String rt = e.getTag(OSMTagKey.ROUTE);
		boolean publicTransport = IndexTransportCreator.acceptedPublicTransportRoute(rt);
		boolean road = "road".equals(rt);
		boolean railway = "railway".equals(rt);
		boolean infra = "power".equals(rt) || "pipeline".equals(rt);
		if (rt != null && !publicTransport && !road && !railway && !infra) {
			ctx.loadEntityRelation(e);
			List<Way> ways = new ArrayList<Way>();
			List<RelationMember> ms = e.getMembers();
			tags = new LinkedHashMap<>(tags);
			RouteActivityType activityType = RouteActivityType.getTypeFromOSMTags(tags);
			for (RelationMember rm : ms) {
				if (rm.getEntity() instanceof Way) {
					Way w = (Way) rm.getEntity();
//					Way newWay = new Way(-e.getId(), w.getNodes()); // duplicates
					Way newWay = new Way(GENERATE_OBJ_ID--, w.getNodes());
					newWay.replaceTags(tags);
					ways.add(newWay);
				}
			}
			TransportRoute.mergeRouteWays(ways);
			for (Way w : ways) {
				addRouteRelationTags(e, w, tags, activityType, icc);
				if (settings.addRegionTag) {
					icc.calcRegionTag(e, true);
				}
				w.replaceTags(tags);
				icc.translitJapaneseNames(e, settings.addRegionTag);
				for (int level = 0; level < mapZooms.size(); level++) {
					icc.getIndexMapCreator().processMainEntity(w, w.getId(), w.getId(), level, tags);
				}
				if (settings.indexPOI) {
					icc.getIndexPoiCreator().iterateEntityInternal(w, ctx, icc);
				}
			}
			String routeKey = activityType == null ? rt : activityType.getName();
			Integer c = indexRouteRelationTypes.get(routeKey);
			indexRouteRelationTypes.put(rt, (c == null ? 0 : c) + 1);
		}
	}

	private void addRouteRelationTags(Relation e, Way w, Map<String, String> tags, RouteActivityType activityType, IndexCreationContext icc) {
		// TODO better tags & id osmc:symbol
		if (tags.get("color") != null) {
			tags.put("colour", tags.get("color"));
		}
		if (activityType != null) {
			String ref = tags.get("ref");
			if (ref == null) {
				ref = String.valueOf(e.getId() % 1000);
			}
			tags.put("ref", ref);
			// red, blue, green, orange, yellow
			int l = Math.max(1, Math.min(6, ref.length()));
			
			tags.put("gpx_bg", activityType.getColor() + "_hexagon_" + l + "_road_shield");
			if (tags.get("colour") == null) {
				tags.put("colour", activityType.getColor());
			}
			tags.put("route_activity_type", activityType.getName().toLowerCase());
		}
		calculateHeightTags(w, tags, icc, DIST_STEP);
		tags.put("route", "segment");
		tags.put("route_type", "track");
		tags.put("route_id", "O-" + e.getId() );
	}

	private void calculateHeightTags(Way w, Map<String, String> tags, IndexCreationContext icc, double DIST_STEP) {
		double minEle = 0, maxEle = 0, avgEle = 0, up = 0, down = 0, dist = 0, ph = 0;
		int hcnt = 0;
		double step = 0;
		Node pnode = null; 
		for (int i = 0; i < w.getNodes().size(); i++) {
			Node node = w.getNodes().get(i);
			if(i > 0) {
				double segment = MapUtils.getDistance(pnode.getLatitude(), pnode.getLongitude(), node.getLatitude(), node.getLongitude());
				dist += segment;
				step += segment;
			}
			double h = icc.getIndexHeightData().getPointHeight(node.getLatitude(), node.getLongitude());
			if (h != IndexHeightData.INEXISTENT_HEIGHT) {
				if (hcnt == 0) {
					ph = minEle = maxEle = avgEle = h;
					hcnt = 1;
				} else {
					minEle = Math.min(h, minEle);
					maxEle = Math.max(h, maxEle);
					avgEle += h;
					hcnt++;
					if (step > DIST_STEP) {
						int extraFragments = (int) (step / DIST_STEP);
						// in case way is very long calculate alt each DIST_STEP
						for (int st = 1; st < extraFragments; st++) {
							double midlat = pnode.getLatitude() + (node.getLatitude()  - pnode.getLatitude()) * st  / ((double) extraFragments);
							double midlon = pnode.getLongitude() + (node.getLongitude()  - pnode.getLongitude()) * st  / ((double) extraFragments);
							double midh = icc.getIndexHeightData().getPointHeight(midlat, midlon);
							if (midh != IndexHeightData.INEXISTENT_HEIGHT) {
								minEle = Math.min(midh, minEle);
								maxEle = Math.max(midh, maxEle);
								avgEle += midh;
								hcnt++;
								if (midh > ph) {
									up += (midh - ph);
								} else {
									down += (ph - midh);
								}
								ph = midh;
							}
						}
						
						if (h > ph) {
							up += (h - ph);
						} else {
							down += (ph - h);
						}
						ph = h;
						step = 0;
					}
					
				}
			} else {
				step = 0;
			}
			pnode = node;
		}
		if (tags.get("distance") == null && dist > 0) {
			tags.put("distance", String.valueOf((int) dist));
		}
		if (hcnt > 0) {
			tags.put("avg_ele", String.valueOf((int) (avgEle / hcnt)));
			tags.put("min_ele", String.valueOf((int) minEle));
			tags.put("max_ele", String.valueOf((int) maxEle));
			tags.put("diff_ele_up", String.valueOf((int) up));
			tags.put("diff_ele_down", String.valueOf((int) down));
		}
	}

	public void closeAllStatements() {
		if (indexRouteRelationTypes.size() > 0) {
			List<String> lst = new ArrayList<>(indexRouteRelationTypes.keySet());
			Collections.sort(lst, new Comparator<String>() {

				@Override
				public int compare(String o1, String o2) {
					Integer i1 = indexRouteRelationTypes.get(o1);
					Integer i2 = indexRouteRelationTypes.get(o2);
					if (i1 == null) {
						return -1;
					}
					if (i2 == null) {
						return -1;
					}
					return -Integer.compare(i1, i2);
				}
			});
			log.info("Indexed route relation types: ");
			for (String tp : lst) {
				log.info(String.format("%s - %d", tp, indexRouteRelationTypes.get(tp)));
			}
		}
	}

	

}
