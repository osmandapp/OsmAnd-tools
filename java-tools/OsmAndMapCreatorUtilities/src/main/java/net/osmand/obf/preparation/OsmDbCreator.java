package net.osmand.obf.preparation;

import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import net.osmand.binary.ObfConstants;
import net.osmand.data.City.CityType;
import net.osmand.data.LatLon;
import net.osmand.obf.preparation.PropagateToNodes.PropagateFromWayToNode;
import net.osmand.obf.preparation.PropagateToNodes.PropagateWayWithNodes;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.OSMSettings.OSMTagKey;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Relation.RelationMember;
import net.osmand.osm.edit.Way;
import net.osmand.osm.io.IOsmStorageFilter;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.util.MapUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map.Entry;

public class OsmDbCreator implements IOsmStorageFilter {

	public static String OSMAND_DELETE_TAG = "osmand_change";
	public static String OSMAND_DELETE_VALUE = "delete";
	
	private static final Log log = LogFactory.getLog(OsmDbCreator.class);
	public static final int SHIFT_ID = 6;
	public static final int BATCH_SIZE_OSM = 100000;

	// do not store these tags in the database, just ignore them
	final String[] tagsToIgnore= {"created_by","converted_by"};
	
	DBDialect dialect;
	int currentCountNode = 0;
	private PreparedStatement prepNode;
	int allNodes = 0;
	private PreparedStatement selectNode;

	int currentRelationsCount = 0;
	private PreparedStatement prepRelations;
	int allRelations = 0;

	int currentWaysCount = 0;
	private PreparedStatement prepWays;
	int allWays = 0;

	int propagateCount = 0;
	private PreparedStatement prepPropagateNode;
	
	

	
	// not used for now
	private PreparedStatement delNode;
	private PreparedStatement delRelations;
	private PreparedStatement delWays;
	private TLongHashSet nodeIds = new TLongHashSet();
	private TLongHashSet wayIds = new TLongHashSet();
	private TLongHashSet relationIds = new TLongHashSet();;

	private Connection dbConn;

	
	private static boolean VALIDATE_DUPLICATES = false;
	private TLongObjectHashMap<Long> generatedIds = new TLongObjectHashMap<Long>();
	private TLongObjectHashMap<Long> hashes = new TLongObjectHashMap<Long>();
	private TLongSet idSetToValidateDuplicates = new TLongHashSet();
	

	private final int shiftId;
	private final int additionId;
	private final boolean generateNewIds;
	private final boolean addGeoHash;
	
	private long generatedId = -100;

	private PropagateToNodes propagateToNodes;



	public OsmDbCreator(int additionId, int shiftId) {
		this.additionId = additionId;
		this.shiftId = shiftId;
		// Before for basemap it was true but cause too much memory to keep, so it's simplified 
		this.generateNewIds = false;
		this.addGeoHash = true;
	}
	
	public OsmDbCreator(boolean addGeoHash) {
		this.additionId = 0;
		this.shiftId = 0;
		this.generateNewIds = false;
		this.addGeoHash = addGeoHash;
	}
	
	public OsmDbCreator() {
		this.additionId = 0;
		this.shiftId = 0;
		this.generateNewIds = false;
		this.addGeoHash = true;
	}
	
	
	public long convertId(Entity e) {
		long id = e.getId();
		
		int ord = EntityType.valueOf(e).ordinal();
		// for points id > 0 add always geohash (for basemap points)
		if (e instanceof Node) {
			if (!addGeoHash || id < 0) {
				return getSimpleConvertId(id, EntityType.NODE, true);
			}
			int hash = getNodeHash(e);
			return getConvertId(id, ord, hash);
		} else if (e instanceof Way) {
			TLongArrayList lids = ((Way) e).getNodeIds();
			long hash = 0;
			for (int i = 0; i < lids.size(); i++) {
				Long ld;
				if (!addGeoHash || lids.get(i) < 0) {
					ld = getSimpleConvertId(lids.get(i), EntityType.NODE, false);
				} else {
					ld = getGeneratedId(lids.get(i), 0);
					Long hd = getHash(lids.get(i), 0);
					if (hd != null) {
						hash += hd;
					}
				}
				if (ld != null) {
					lids.set(i, ld);
				}
			}
			if (!addGeoHash || id < 0) {
				return getSimpleConvertId(id, EntityType.WAY, true);
			}
			return getConvertId(id, ord, hash);
		} else {
			Relation r = (Relation) e;
			// important keep order of all relation members !!!
			for (RelationMember i : r.getMembers()) {
				long oldId = i.getEntityId().getId().longValue();
				EntityType entityType = i.getEntityId().getType();
				if (i.getEntityId().getType() != EntityType.RELATION) {
					Long newId ;
					if (!addGeoHash || oldId < 0) {
						newId = getSimpleConvertId(oldId, entityType, false);
					} else {
						newId = getGeneratedId(oldId, entityType.ordinal());
					}
					if (newId != null) {
						r.update(i, new EntityId(entityType, newId));
					}
				}
			}
			if (!addGeoHash || id < 0) {
				return getSimpleConvertId(id, EntityType.RELATION, true);
			}
			return id;
		}
	}

	private long getSimpleConvertId(long id, EntityType type, boolean newId) {
		if (generateNewIds) {
			long key = (id << 2) + type.ordinal();
			if (!generatedIds.contains(key) || newId) {
				id = generatedId--;
				generatedIds.put(key, id);
			} else {
				id = generatedIds.get(key);
			}
		}
		if (id < 0) {
			// for negative we do different
			long shiftedPositive = ((-id) << shiftId);
			return -(shiftedPositive + additionId);
		} else {
			// keep original id if it's positive
			return id;
//			return (id << shiftId) + additionId;
		}
		
	}

	private int getNodeHash(Entity e) {
		int y = MapUtils.get31TileNumberY(((Node) e).getLatitude());
		int x = MapUtils.get31TileNumberX(((Node) e).getLongitude());
		int hash = (x + y) >> 10;
		return hash;
	}
	
	
	private Long getHash(long l, int ord) {
		if(l < 0) {
			long lid = (l << shiftId) + additionId;
			long fid = (lid << 2) + ord;
			return hashes.get(fid);
		}
		return hashes.get((l << 2) + ord);
	}

	private Long getGeneratedId(long l, int ord) {
		if(l < 0) {
			long lid = (l << shiftId) + additionId;
			long fid = (lid << 2) + ord;
			return generatedIds.get(fid);
		}
		return generatedIds.get((l << 2) + ord);
	}

	private long getConvertId(long id, int ord, long hash) {
		if(id < 0 && (shiftId > 0 || additionId > 0)) {
			long lid = (id << shiftId) + additionId;
			long fid = (lid << 2) + ord;
			generatedIds.put(fid, lid);
			hashes.put(fid, hash);
			return lid;
		}
		int l = (int) (hash & ((1 << (SHIFT_ID - 1)) - 1));
		long cid = (id << SHIFT_ID) + (ord % 2) + (l << 1);
		long fid = (id << 2) + ord;
		generatedIds.put(fid, cid);
		hashes.put(fid, hash);
		return cid;
	}
	

	public void initDatabase(DBDialect dialect, Object databaseConn, boolean create, OsmDbCreator previous) throws SQLException {

		this.dialect = dialect;
		this.dbConn = (Connection) databaseConn;
		// prepare tables
		Statement stat = dbConn.createStatement();
		if (create) {
			dialect.deleteTableIfExists("node", stat);
			stat.executeUpdate("create table node (id bigint primary key, latitude double, longitude double, tags blob, propagate boolean)"); //$NON-NLS-1$
			stat.executeUpdate("create index IdIndex ON node (id)"); //$NON-NLS-1$
			dialect.deleteTableIfExists("ways", stat);
			stat.executeUpdate("create table ways (id bigint, node bigint, ord smallint, tags blob, boundary smallint, primary key (id, ord))"); //$NON-NLS-1$
			stat.executeUpdate("create index IdWIndex ON ways (id)"); //$NON-NLS-1$
			dialect.deleteTableIfExists("relations", stat);
			stat.executeUpdate("create table relations (id bigint, member bigint, type smallint, role varchar(1024), ord smallint, tags blob, primary key (id, ord))"); //$NON-NLS-1$
			stat.executeUpdate("create index IdRIndex ON relations (id)"); //$NON-NLS-1$
			stat.close();
		} else {
			if (previous != null) {
				nodeIds = previous.nodeIds;
				wayIds = previous.wayIds;
				relationIds = previous.relationIds;
			} else {
				// not used
//				initIds("node", nodeIds);
//				initIds("ways", wayIds);
//				initRelationIds("relations", relationIds);
			}
		}
		prepNode = dbConn.prepareStatement("replace into node(id, latitude, longitude, tags, propagate) values (?, ?, ?, ?, ?)"); //$NON-NLS-1$
		prepWays = dbConn.prepareStatement("replace into ways(id, node, ord, tags, boundary) values (?, ?, ?, ?, ?)"); //$NON-NLS-1$
		prepRelations = dbConn.prepareStatement("replace into relations(id, member, type, role, ord, tags) values (?, ?, ?, ?, ?, ?)"); //$NON-NLS-1$
		prepPropagateNode = dbConn.prepareStatement("update node set propagate=1 where id=?");
		selectNode = dbConn.prepareStatement("select latitude, longitude from node where id=?"); //$NON-NLS-1$
		dbConn.setAutoCommit(false);
	}

	protected void initIds(String table, TLongHashSet col) throws SQLException {
		if(col.isEmpty()) {
			Statement s = dbConn.createStatement();
			ResultSet rs = s.executeQuery("select id from " + table);
			while(rs.next()) {
				col.add(rs.getLong(1));
			}
			s.close();
		}
	}
	

	public void finishLoading() throws SQLException {
		try {
			if (currentCountNode > 0) {
				prepNode.executeBatch();
			}
			prepNode.close();
			if (currentWaysCount > 0) {
				prepWays.executeBatch();
			}
			prepWays.close();
			if (currentRelationsCount > 0) {
				prepRelations.executeBatch();
			}
			if (propagateCount > 0) {
				prepPropagateNode.executeBatch();
			}
		} catch (SQLException ex) {
			log.error("TODO FIX: Could not save in db ", ex); //$NON-NLS-1$
		}
		prepRelations.close();
		prepPropagateNode.close();
		if (delNode != null) {
			delNode.close();
		}
		if (delWays != null) {
			delWays.close();
		}
		if (delRelations != null) {
			delRelations.close();
		}
	}
	
	
	
	protected void checkEntityExists(Entity e, long id) throws SQLException {
		if (delNode == null) {
			delNode = dbConn.prepareStatement("delete from node where id = ?"); //$NON-NLS-1$
			delWays = dbConn.prepareStatement("delete from ways where id = ?"); //$NON-NLS-1$
			delRelations = dbConn.prepareStatement("delete from relations where id = ? "); //$NON-NLS-1$
		}
		boolean present = false;
		if (e instanceof Node) {
			present = !nodeIds.add(id);
		} else if (e instanceof Way) {
			present = !wayIds.add(id);
		} else if (e instanceof Relation) {
			present = !relationIds.add(id);
		}
		if(!present) {
			return;
		}
		prepNode.executeBatch();
		prepWays.executeBatch();
		prepRelations.executeBatch();
		prepPropagateNode.executeBatch();
		currentWaysCount = 0;
		currentCountNode = 0;
		currentRelationsCount = 0;
		propagateCount = 0;
		if (e instanceof Node) {
			delNode.setLong(1, id);
			delNode.execute();
		} else if (e instanceof Way) {
			delWays.setLong(1, id);
			delWays.execute();
		} else if (e instanceof Relation) {
			delRelations.setLong(1, id);
			delRelations.execute();
		}
		dbConn.commit(); // clear memory
	}

	@Override
	public boolean acceptEntityToLoad(OsmBaseStorage storage, EntityId entityId, Entity e) {
		// put all nodes into temporary db to get only required nodes after loading all data
		if (VALIDATE_DUPLICATES) {
			long l = (e.getId() << 2) + entityId.getType().ordinal();
			if (!idSetToValidateDuplicates.add(l)) {
				throw new IllegalStateException("Duplicate id '" + e.getId() + "' " + entityId.getType());
			}
		}
		try {
			e.removeTags(tagsToIgnore);
			ByteArrayOutputStream tags = new ByteArrayOutputStream();
			try {
				for (Entry<String, String> i : e.getTags().entrySet()) {
					// UTF-8 default
					tags.write(i.getKey().getBytes("UTF-8"));
					tags.write(0);
					tags.write(i.getValue().getBytes("UTF-8"));
					tags.write(0);
				}
			} catch (IOException es) {
				throw new RuntimeException(es);
			}
			long id = convertId(e);
			if (propagateToNodes != null && e instanceof Way) {
				boolean firstIteration = propagateToNodes.isNoRegisteredNodes();
				PropagateWayWithNodes pnodes = propagateToNodes.propagateTagsFromWays((Way) e);
				if (pnodes != null && !pnodes.empty) {
					if (firstIteration) {
						executeNodesBatch(true); // commit for proper select node ide
					}
					TLongArrayList nodeIds = ((Way) e).getNodeIds();
					TLongArrayList oldNodeIds = new TLongArrayList(nodeIds);
					nodeIds.clear();
					long newInd = 0;
					for (int i = 0; i < pnodes.points.length; i++) {
						PropagateFromWayToNode pn = pnodes.points[i];
						if (i % 2 == 0) {
							nodeIds.add(oldNodeIds.get(i / 2));
							if (pn != null) {
								prepPropagateNode.setLong(1, pn.id);
								prepPropagateNode.addBatch();
								propagateCount++;
								propagateToNodes.registerNode(pn);
							}
						} else if (pnodes.points[i] != null) { // in between points
							LatLon latLon = pn.getLatLon(getNode(oldNodeIds.get(pn.start)),getNode(oldNodeIds.get(pn.end)));
							if (latLon == null) {
								continue;
							}
							long wayId = e.getId(); 
							if (wayId < 0) {
								wayId = Math.abs(wayId) % (1l << (ObfConstants.SHIFT_PROPAGATED_NODE_IDS - ObfConstants.SHIFT_PROPAGATED_NODES_BITS - 1));
							}
							pn.id = ObfConstants.PROPAGATE_NODE_BIT + (wayId << ObfConstants.SHIFT_PROPAGATED_NODES_BITS) + newInd++; //+ i; - also should work but duplicates should be fixed first
							if (newInd > ObfConstants.MAX_ID_PROPAGATED_NODES) {
								log.error("Maximum number " + ObfConstants.MAX_ID_PROPAGATED_NODES + " of propagated nodes reached for way:" + e.getId());
								nodeIds.clear();
								nodeIds.addAll(oldNodeIds);
								break;
							}
							currentCountNode++;
							prepNode.setLong(1, pn.id);
							prepNode.setDouble(2, latLon.getLatitude());
							prepNode.setDouble(3, latLon.getLongitude());
							prepNode.setBytes(4, new ByteArrayOutputStream().toByteArray());
							prepNode.setBoolean(5, true);
							prepNode.addBatch();

							nodeIds.add(pn.id);
							propagateToNodes.registerNode(pn);
							executeNodesBatch(false);
						}
					}
				}
				if (propagateCount >= BATCH_SIZE_OSM) {
					prepPropagateNode.executeBatch();
					dbConn.commit(); // clear memory
					propagateCount = 0;
				}
			}
			if (e instanceof Node) {
				currentCountNode++;
				if (!e.getTags().isEmpty()) {
					allNodes++;
				}
				prepNode.setLong(1, id);
				prepNode.setDouble(2, ((Node) e).getLatitude());
				prepNode.setDouble(3, ((Node) e).getLongitude());
				prepNode.setBytes(4, tags.toByteArray());
				prepNode.setBoolean(5, false);
				prepNode.addBatch();
				executeNodesBatch(false);
			} else if (e instanceof Way) {
				allWays++;
				int ord = 0;
				TLongArrayList nodeIds = ((Way) e).getNodeIds();
				boolean city = CityType.valueFromEntity(e) != null;
				int boundary = ((Way) e).getTag(OSMTagKey.BOUNDARY) != null || city ? 1 : 0;
				for (int j = 0; j < nodeIds.size(); j++) {
					currentWaysCount++;
					if (ord == 0) {
						prepWays.setBytes(4, tags.toByteArray());
					}
					prepWays.setLong(1, id);
					prepWays.setLong(2, nodeIds.get(j));
					prepWays.setLong(3, ord++);
					prepWays.setInt(5, boundary);
					prepWays.addBatch();
				}
				if (currentWaysCount >= BATCH_SIZE_OSM) {
					prepWays.executeBatch();
					dbConn.commit(); // clear memory
					currentWaysCount = 0;
				}
			} else {
				// osm change can't handle relations properly
				allRelations++;
				short ord = 0;
				for (RelationMember i : ((Relation) e).getMembers()) {
					currentRelationsCount++;
					if (ord == 0) {
						prepRelations.setBytes(6, tags.toByteArray());
					}
					prepRelations.setLong(1, id);
					prepRelations.setLong(2, i.getEntityId().getId());
					prepRelations.setLong(3, i.getEntityId().getType().ordinal());
					prepRelations.setString(4, i.getRole());
					prepRelations.setLong(5, ord++);
					prepRelations.addBatch();
				}
//				System.out.println(id + " " + delete);
				if (currentRelationsCount >= BATCH_SIZE_OSM) {
					prepRelations.executeBatch();
					dbConn.commit(); // clear memory
					currentRelationsCount = 0;
				}
			}

		} catch (SQLException ex) {
			log.error("TODO FIX: Could not save in db (entity " + entityId + ") ", ex); //$NON-NLS-1$
		}
		// do not add to storage
		return false;
	}

	private void executeNodesBatch(boolean force) throws SQLException {
		if (currentCountNode >= BATCH_SIZE_OSM || force) {
			prepNode.executeBatch();
			dbConn.commit(); // clear memory
			currentCountNode = 0;
		}
	}



	private Node getNode(long l) throws SQLException {
		selectNode.setLong(1, l);
		ResultSet q = selectNode.executeQuery();
		if (q.next()) {
			return new Node(q.getDouble(1), q.getDouble(2), l);
		}
		return null;
	}

	public int getAllNodes() {
		return allNodes;
	}

	public int getAllRelations() {
		return allRelations;
	}

	public int getAllWays() {
		return allWays;
	}

	public void setPropagateToNodes(PropagateToNodes propagateToNodes) {
		this.propagateToNodes = propagateToNodes;
	}
	

}
