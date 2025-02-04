package net.osmand.obf.preparation;

import java.sql.SQLException;
import java.util.*;

import net.osmand.binary.ObfConstants;
import net.osmand.data.LatLon;
import net.osmand.osm.edit.*;
import net.osmand.shared.gpx.primitives.Track;
import net.osmand.shared.gpx.primitives.TrkSegment;
import net.osmand.shared.gpx.primitives.WptPt;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import net.osmand.binary.MapZooms;
import net.osmand.data.TransportRoute;
import net.osmand.obf.preparation.IndexHeightData.WayGeneralStats;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.MapRenderingTypesEncoder.EntityConvertApplyType;
import net.osmand.osm.OsmRouteType;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.OSMSettings.OSMTagKey;
import net.osmand.osm.edit.Relation.RelationMember;
import net.osmand.util.MapAlgorithms;

public class IndexRouteRelationCreator {
	private final static Log log = LogFactory.getLog(IndexRouteRelationCreator.class);
	public static long GENERATE_OBJ_ID = - (1l << 20l); // million million
	public static final double DIST_STEP = 25;

	public static final int MAX_GRAPH_SKIP_POINTS_BITS = 3;
    private Map<Long, Integer> syntheticMapRelationIds = new HashMap<>();


	protected final Log logMapDataWarn;
	private final Map<String, Integer> indexRouteRelationTypes = new TreeMap<String, Integer>();
	private final MapRenderingTypesEncoder renderingTypes;
	private final MapZooms mapZooms;
	private final IndexCreatorSettings settings;

    private final double precisionLatLonEquals = 1e-5;

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
		String rt = tags.get(OSMTagKey.ROUTE.name());
		boolean publicTransport = IndexTransportCreator.acceptedPublicTransportRoute(rt);
		boolean road = "road".equals(rt);
		boolean railway = "railway".equals(rt);
		boolean infra = "power".equals(rt) || "pipeline".equals(rt);
		if (rt != null && !publicTransport && !road && !railway && !infra) {
			ctx.loadEntityRelation(e);
			List<Way> ways = new ArrayList<Way>();
			List<RelationMember> ms = e.getMembers();
			tags = new LinkedHashMap<>(tags);
			OsmRouteType activityType = OsmRouteType.getTypeFromOSMTags(tags);
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

    private void joinWaysIntoTrackSegments(JoinedWays joinedWays, List<Way> ways) {
        boolean[] done = new boolean[ways.size()];
        while (true) {
            List<Node> joinedNodes = new ArrayList<>();
            for (int i = 0; i < ways.size(); i++) {
                if (!done[i]) {
                    done[i] = true;
                    addWayToPoints(joinedNodes, false, ways.get(i), false); // "head" way
                    while (true) {
                        boolean stop = true;
                        for (int j = 0; j < ways.size(); j++) {
                            if (!done[j] && considerCandidateToJoin(joinedNodes, ways.get(j))) {
                                done[j] = true;
                                stop = false;
                            }
                        }
                        if (stop) {
                            break; // nothing joined
                        }
                    }
                    break; // segment is done
                }
            }
            if (joinedNodes.isEmpty()) {
                break; // all done
            }
            Way joinedWay = new Way(1234, joinedNodes);
            joinedWays.joinedWays.add(joinedWay);
        }
    }

    private void addWayToPoints(List<Node> joinedNodes, boolean insert, Way way, boolean reverse) {
        List<Node> points = new ArrayList<>(way.getNodes());
        if (reverse) {
            Collections.reverse(points);
        }
        joinedNodes.addAll(insert ? 0 : joinedNodes.size(), points);
    }
    private boolean considerCandidateToJoin(List<Node> joinedNodes, Way candidate) {
        if (joinedNodes.isEmpty() || candidate.getNodes().isEmpty()) {
            return true;
        }

        Node firstWpt = joinedNodes.get(0);
        Node lastWpt = joinedNodes.get(joinedNodes.size() - 1);
        LatLon firstCandidateLL = candidate.getNodes().get(0).getLatLon();
        LatLon lastCandidateLL = candidate.getNodes().get(candidate.getNodes().size() - 1).getLatLon();

        if (eqWptToLatLon(lastWpt, firstCandidateLL)) {
            addWayToPoints(joinedNodes, false, candidate, false); // wpts + Candidate
        } else if (eqWptToLatLon(lastWpt, lastCandidateLL)) {
            addWayToPoints(joinedNodes, false, candidate, true); // wpts + etadidnaC
        } else if (eqWptToLatLon(firstWpt, firstCandidateLL)) {
            addWayToPoints(joinedNodes, true, candidate, true); // etadidnaC + wpts
        } else if (eqWptToLatLon(firstWpt, lastCandidateLL)) {
            addWayToPoints(joinedNodes, true, candidate, false); // Candidate + wpts
        } else {
            return false;
        }

        return true;
    }

    private boolean eqWptToLatLon(Node node, LatLon ll) {
        return MapUtils.areLatLonEqual(node.getLatLon(), ll, precisionLatLonEquals);
    }


    // Way ID for Map section
    private long generateSyntheticId(Relation relation, Way mergedWay) {
        long relationId = relation.getId();
        long sum = 0;
        LatLon l = OsmMapUtils.getWeightCenterForNodes(mergedWay.getNodes());
        int y = MapUtils.get31TileNumberY(l.getLatitude());
        int x = MapUtils.get31TileNumberX(l.getLongitude());
        sum += (x + y);
        Integer countId = syntheticMapRelationIds.get(relationId);
        if (countId == null) {
            countId = 1;
            syntheticMapRelationIds.put(relationId, countId);
        } else {
            countId++;
        }
        return  (1l << (ObfConstants.SHIFT_MULTIPOLYGON_IDS - 1)) + (relationId << 15) + (countId << 6) + (sum % 63);
    }

    // ID for POI section id << 6 (OsmDbCreator.SHIFT_ID)


	private void addRouteRelationTags(Relation e, Way w, Map<String, String> tags, OsmRouteType activityType, IndexCreationContext icc) {
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
		if (icc.getIndexHeightData() != null) {
			WayGeneralStats wg = icc.getIndexHeightData().calculateWayGeneralStats(w, DIST_STEP);
			if (tags.get("distance") == null && wg.dist > 0) {
				tags.put("distance", String.valueOf((int) wg.dist));
			}
			if (wg.eleCount > 0) {
//				int st = (int) wg.startEle;
				tags.put("start_ele", String.valueOf((int) wg.startEle));
//				tags.put("end_ele__start", String.valueOf((int) wg.endEle - st));
//				tags.put("avg_ele__start", String.valueOf((int) (wg.sumEle / wg.eleCount) - st));
//				tags.put("min_ele__start", String.valueOf((int) wg.minEle - st));
//				tags.put("max_ele__start", String.valueOf((int) wg.maxEle - st));
				tags.put("diff_ele_up", String.valueOf((int) wg.up));
				tags.put("diff_ele_down", String.valueOf((int) wg.down));
				tags.put("ele_graph", MapAlgorithms.encodeIntHeightArrayGraph(wg.step, wg.altIncs, MAX_GRAPH_SKIP_POINTS_BITS));
			}
		}
		tags.put("route", "segment");
		tags.put("route_type", "other");
		tags.put("route_id", "O-" + e.getId() );
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

    private class JoinedWays {
        public List<Way> joinedWays = new ArrayList<>();
    }

}
