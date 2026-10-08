package net.osmand.obf.preparation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.hash.TLongHashSet;
import net.osmand.binary.RouteDataObject.RestrictionInfo;
import net.osmand.osm.MapRenderingTypes;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;

// Issue #12537: the router remembers one previous road, so a restriction whose via is a chain of ways is enforced by
// splitting the chain in the route data. Traffic from "from" is moved onto one-way copies of the via ways (same nodes
// and tags, id with the same OSM id) that no other road can enter:
// no_*   - from -> via1 is forbidden (its copy is taken instead), copy of viaN -> to is forbidden;
// only_* - from -> copy of via1 -> ... -> copy of viaN -> to is the only way.
// Traffic that enters the via ways from a side road stays on the originals and is not restricted.
// Restrictions with the same "from" and the same first vias share the copies.
class RestrictionViaCopies {

	private static class ViaCopy {
		final long id;
		final long parentId; // "from" or the copy of the previous via
		final boolean reversed; // the chain goes against the node order of the via way

		ViaCopy(long id, long parentId, boolean reversed) {
			this.id = id;
			this.parentId = parentId;
			this.reversed = reversed;
		}
	}

	private final TLongObjectHashMap<List<RestrictionInfo>> highwayRestrictions;
	private final Map<List<Long>, ViaCopy> copiesByPath = new HashMap<>(); // [from, via1, ..., viaK] -> copy of viaK
	private final TLongObjectHashMap<List<ViaCopy>> copiesByVia = new TLongObjectHashMap<>();
	private final TLongObjectHashMap<List<ViaCopy>> copiesByNode = new TLongObjectHashMap<>();
	private final TLongIntHashMap copiesCount = new TLongIntHashMap();

	RestrictionViaCopies(TLongObjectHashMap<List<RestrictionInfo>> highwayRestrictions) {
		this.highwayRestrictions = highwayRestrictions;
	}

	// returns false if the via ways don't form a chain from "from" (then the restriction is stored the old way)
	boolean addRestriction(Way from, List<Way> vias, long toId, byte type) {
		boolean[] reversed = orientChain(from, vias);
		if (reversed == null) {
			return false;
		}
		boolean onlyRestriction = type >= MapRenderingTypes.RESTRICTION_ONLY_RIGHT_TURN;
		List<Long> path = new ArrayList<>();
		path.add(from.getId());
		long parentId = from.getId();
		for (int i = 0; i < vias.size(); i++) {
			Way via = vias.get(i);
			path.add(via.getId());
			ViaCopy copy = copiesByPath.get(path);
			if (copy == null) {
				copy = new ViaCopy(nextCopyId(via.getId()), parentId, reversed[i]);
				copiesByPath.put(new ArrayList<>(path), copy);
				addCopy(copiesByVia, via.getId(), copy);
				for (long nodeId : via.getNodeIds().toArray()) {
					addCopy(copiesByNode, nodeId, copy);
				}
				addRestrictionInfo(parentId, via.getId(), MapRenderingTypes.RESTRICTION_NO_STRAIGHT_ON);
				addRestrictionInfo(copy.id, via.getId(), MapRenderingTypes.RESTRICTION_NO_STRAIGHT_ON);
			}
			if (onlyRestriction) {
				addRestrictionInfo(parentId, copy.id, type);
			}
			parentId = copy.id;
		}
		addRestrictionInfo(parentId, toId, type);
		return true;
	}

	// writes the copies of a via way with the encoded tags of the original
	void writeCopies(Way via, CopyWriter writer) throws java.sql.SQLException {
		List<ViaCopy> copies = copiesByVia.get(via.getId());
		if (copies == null) {
			return;
		}
		List<RestrictionInfo> viaRestrictions = highwayRestrictions.get(via.getId());
		for (ViaCopy copy : copies) {
			if (viaRestrictions != null) {
				for (RestrictionInfo ri : viaRestrictions) {
					addRestrictionInfo(copy.id, ri.viaWay, ri.toWay, (byte) ri.type);
				}
			}
			List<Node> nodes = new ArrayList<>(via.getNodes());
			if (copy.reversed) {
				Collections.reverse(nodes);
			}
			writer.write(copy.id, nodes);
			addEntryRestrictions(copy.id, nodes);
		}
	}

	// no road except the parent can turn onto a copy at any of its nodes
	void addEntryRestrictions(long wayId, List<Node> nodes) {
		TLongHashSet restricted = new TLongHashSet();
		for (Node n : nodes) {
			List<ViaCopy> copies = n == null ? null : copiesByNode.get(n.getId());
			if (copies != null) {
				for (ViaCopy copy : copies) {
					if (copy.id != wayId && copy.parentId != wayId && restricted.add(copy.id)) {
						addRestrictionInfo(wayId, copy.id, MapRenderingTypes.RESTRICTION_NO_STRAIGHT_ON);
					}
				}
			}
		}
	}

	interface CopyWriter {
		void write(long copyId, List<Node> nodes) throws java.sql.SQLException;
	}

	// for each via: true if the chain goes from its last node to its first one; null if the vias are not a chain
	private static boolean[] orientChain(Way from, List<Way> vias) {
		boolean[] reversed = new boolean[vias.size()];
		long exitNode = 0;
		for (int i = 0; i < vias.size(); i++) {
			Way via = vias.get(i);
			if (via.getNodeIds().size() < 2) {
				return null;
			}
			boolean entersFirst = i == 0 ? from.getNodeIds().contains(via.getFirstNodeId())
					: exitNode == via.getFirstNodeId();
			boolean entersLast = i == 0 ? from.getNodeIds().contains(via.getLastNodeId())
					: exitNode == via.getLastNodeId();
			if (entersFirst == entersLast) {
				return null;
			}
			reversed[i] = entersLast;
			exitNode = entersLast ? via.getFirstNodeId() : via.getLastNodeId();
		}
		return reversed;
	}

	// the copy keeps the OSM id: bits 1..5 of a route id are a hash that ObfConstants.getOsmId drops
	private long nextCopyId(long viaId) {
		int k = copiesCount.adjustOrPutValue(viaId, 1, 1);
		if (k > 31) {
			throw new IllegalStateException("Too many copies of via way " + viaId);
		}
		return viaId ^ ((long) k << 1);
	}

	private static void addCopy(TLongObjectHashMap<List<ViaCopy>> map, long key, ViaCopy copy) {
		if (!map.containsKey(key)) {
			map.put(key, new ArrayList<>());
		}
		map.get(key).add(copy);
	}

	private void addRestrictionInfo(long fromWay, long toWay, byte type) {
		addRestrictionInfo(fromWay, 0, toWay, type);
	}

	private void addRestrictionInfo(long fromWay, long viaWay, long toWay, byte type) {
		if (!highwayRestrictions.containsKey(fromWay)) {
			highwayRestrictions.put(fromWay, new ArrayList<>());
		}
		RestrictionInfo rd = new RestrictionInfo();
		rd.toWay = toWay;
		rd.type = type;
		rd.viaWay = viaWay;
		highwayRestrictions.get(fromWay).add(rd);
	}
}
