package net.osmand.obf.diff;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.xml.stream.XMLStreamException;

import net.osmand.PlatformUtil;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.data.LatLon;
import net.osmand.map.OsmandRegions;
import net.osmand.obf.preparation.OsmDbCreator;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.EntityInfo;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Relation.RelationMember;
import net.osmand.osm.edit.Way;
import net.osmand.osm.io.OsmStorageWriter;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

import org.apache.commons.logging.Log;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

public class AugmentedDiffsInspector {
	//  cat query | osm3s/bin/osm3s_query > result.xml
// overpass query example
//	[adiff:"2016-03-01T07:00:00Z","2016-03-01T07:03:00Z"];
//	(
//	node(changed:"2016-03-01T07:00:00Z","2016-03-01T07:03:00Z");
//	way(changed:"2016-03-01T07:00:00Z","2016-03-01T07:03:00Z");
//	relation(changed:"2016-03-01T07:00:00Z","2016-03-01T07:03:00Z");
//	)->.changed;
//	.changed out geom meta;
	private static Log log = PlatformUtil.getLog(AugmentedDiffsInspector.class);


	public static String DEFAULT_REGION = "osmlive_data";
	private static long ID_BASE = -1000;
	public static void main(String[] args) {
		try {
			File inputFile = new File(args[0]);
			File targetDir = new File(args[1]);
			
			AugmentedDiffsInspector inspector = new AugmentedDiffsInspector();
			Context ctx = inspector.parseFile(inputFile);
			OsmandRegions osmandRegions = null;
			osmandRegions = new OsmandRegions();
			osmandRegions.prepareFile();
			osmandRegions.cacheAllCountries();
			inspector.prepareRegions(ctx, ctx.newIds, ctx.regionsNew, osmandRegions);
			inspector.prepareRegions(ctx, ctx.oldIds, ctx.regionsOld, osmandRegions);
			String name = inputFile.getName();
			String date = name.substring(0, name.indexOf('-'));
			String time = name.substring(name.indexOf('-') + 1, name.indexOf('.'));
			
			inspector.write(ctx, targetDir, date, time, inputFile.lastModified());
		} catch (Throwable e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	private void prepareRegions(Context ctx, Map<EntityId, Entity> ids, Map<String, Set<EntityId>> regionsMap,
			OsmandRegions osmandRegions) throws IOException {
		Map<EntityId, Set<String>> mp = new HashMap<Entity.EntityId, Set<String>>();
		for (Entity e : ids.values()) {
			if (e instanceof Node) {
				int y = MapUtils.get31TileNumberY(((Node) e).getLatitude());
				int x = MapUtils.get31TileNumberX(((Node) e).getLongitude());
				EntityId id = EntityId.valueOf(e);
				TreeSet<String> lst = new TreeSet<String>();
				mp.put(id, lst);
				if(osmandRegions == null) {
					addEntityToRegion(regionsMap, id, lst, DEFAULT_REGION);
				} else {
					List<BinaryMapDataObject> l = osmandRegions.query(x, y);
					for (BinaryMapDataObject b : l) {
						if (osmandRegions.contain(b, x, y)) {
							String dw = osmandRegions.getDownloadName(b);
							if (!Algorithms.isEmpty(dw) && osmandRegions.isDownloadOfType(b, OsmandRegions.MAP_TYPE)) {
								addEntityToRegion(regionsMap, id, lst, dw);
							}
						}
					}
				}
			}
		}
		// 2. add ways and complete ways with missing nodes
		for(Entity e : ids.values()) {
			if(e instanceof Way) {
				Way w = (Way) e;
				EntityId wid = EntityId.valueOf(w);
				TreeSet<String> lst = new TreeSet<String>();
				mp.put(wid, lst);
				for(EntityId it : w.getEntityIds()) {
					Set<String> countries = mp.get(it);
					for(String cnt : countries) {
						regionsMap.get(cnt).add(wid);
					}
					lst.addAll(countries);
				}
				// complete ways with missing nodes
				for(EntityId it : w.getEntityIds()) {
					mp.get(it).addAll(lst);
					for(String s : lst) {
						regionsMap.get(s).add(it);
					}
				}
			}
		}
		// 3. add relations (not complete with ways or nodes)
		for(Entity e : ids.values()) {
			if(e instanceof Relation) {
				Relation r = (Relation) e;
				EntityId rid = EntityId.valueOf(r);
				TreeSet<String> lst = new TreeSet<String>();
				mp.put(rid, lst);
				for(RelationMember it : r.getMembers()) {
					Set<String> countries = mp.get(it.getEntityId());
					for(String cnt : countries) {
						regionsMap.get(cnt).add(rid);
					}
					lst.addAll(countries);
				}
			}
		}
	}

	private void addEntityToRegion(Map<String, Set<EntityId>> regionsMap, EntityId id, TreeSet<String> lst, String dw) {
		if (!regionsMap.containsKey(dw)) {
			regionsMap.put(dw, new LinkedHashSet<Entity.EntityId>());
		}
		regionsMap.get(dw).add(id);
		lst.add(dw);
	}

	private void write(Context ctx, File targetDir, String date, String time, long lastModified) throws XMLStreamException, IOException, SQLException, InterruptedException, XmlPullParserException {
		targetDir.mkdirs();
//		writeFile(targetDir, "world", ctx.oldIds, null, ctx.newIds, null);
		for(String reg : ctx.regionsNew.keySet()) {
			File dr = new File(targetDir, reg + "/" + date);
			dr.mkdirs();
			writeFile(dr, reg + "_" + time, ctx.oldIds, ctx.regionsOld.get(reg), ctx.newIds, ctx.regionsNew.get(reg), lastModified);
		}
	}
	
	private File writeFile(File targetDir, String prefix, Map<EntityId, Entity> octx, Set<EntityId> oset,
			Map<EntityId, Entity> nctx, Set<EntityId> nset, long lastModified) throws XMLStreamException,
			IOException, FileNotFoundException {
		List<Node> nodes = new ArrayList<Node>();
		List<Way> ways = new ArrayList<Way>();
		List<Relation> relations = new ArrayList<Relation>();
		groupObjects(octx, oset, nodes, ways, relations);
		groupObjects(nctx, nset, nodes, ways, relations);
		File f = new File(targetDir, prefix + ".osm.gz");
		FileOutputStream fous = new FileOutputStream(f);
		GZIPOutputStream gz = new GZIPOutputStream(fous);
		new OsmStorageWriter().writeOSM(gz, new HashMap<Entity.EntityId, EntityInfo>(),
				nodes, ways, relations, true);
		gz.close();
		fous.close();
		f.setLastModified(lastModified);
		return f;
	}

	private void groupObjects(Map<EntityId, Entity> octx, Set<EntityId> oset, List<Node> nodes, List<Way> ways,
			List<Relation> relations) {
		for(Entity e: octx.values()) {
			if(oset != null &&  oset.contains(EntityId.valueOf(e))) {
				if(e instanceof Node) {
					nodes.add((Node) e);
				} else if(e instanceof Way) {
					ways.add((Way) e);
				} else {
					relations.add((Relation) e);
				}
 			}
		}
	}

	private static class Context {
		Map<EntityId, Entity> oldIds = new LinkedHashMap<Entity.EntityId, Entity>();
		Map<EntityId, Entity> newIds = new LinkedHashMap<Entity.EntityId, Entity>();
		Map<EntityId, Entity> oldOIds = new LinkedHashMap<Entity.EntityId, Entity>();
		Map<EntityId, Entity> newOIds = new LinkedHashMap<Entity.EntityId, Entity>();
		Map<LatLon, Node> oldLocNodes = new LinkedHashMap<LatLon, Node>();
		Map<LatLon, Node> newLocNodes = new LinkedHashMap<LatLon, Node>();
		Map<String, Set<EntityId>> regionsNew = new LinkedHashMap<String, Set<EntityId>>();
		Map<String, Set<EntityId>> regionsOld = new LinkedHashMap<String, Set<EntityId>>();
	}

	private Context parseFile(File file) throws XmlPullParserException, IOException {
		XmlPullParser parser = PlatformUtil.newXMLPullParser();
		InputStream fis = new FileInputStream(file);
		if (file.getName().endsWith(".gz")) {
			fis = new GZIPInputStream(fis);
		}
		parser.setInput(fis, "UTF-8");
		int next;
		int modify = Entity.MODIFY_UNKNOWN;
		Entity currentEntity = null;
		Way currentWay = null;
		Context ctx = new Context();
		boolean old = false;
		while ((next = parser.next()) != XmlPullParser.END_DOCUMENT) {
			if (next == XmlPullParser.END_TAG) {
				String name = parser.getName();
				if (name.equals("node") || name.equals("way") || name.equals("relation")) {
					if (currentEntity != null) {
						if (old) {
							updateTags(currentEntity, "name", "type", "area", "fixme");
							ctx.oldIds.put(EntityId.valueOf(currentEntity), currentEntity);
						} else if (modify != Entity.MODIFY_DELETED) {
							ctx.newIds.put(EntityId.valueOf(currentEntity), currentEntity);
						}
					}
				}
			} else if (next == XmlPullParser.START_TAG) {
				String name = parser.getName();
				if ("action".equals(name)) {
					String type = parser.getAttributeValue("", "type");
					if ("modify".equals(type)) {
						modify = Entity.MODIFY_MODIFIED;
					} else if ("delete".equals(type)) {
						modify = Entity.MODIFY_DELETED;
					} else if ("create".equals(type)) {
						modify = Entity.MODIFY_CREATED;
					}
					old = false;
				} else if (name.equals("old")) {
					old = true;
				} else if (name.equals("new")) {
					old = false;
				} else if (name.equals("tag")) {
					currentEntity.putTag(parser.getAttributeValue("", "k"), parser.getAttributeValue("", "v"));
				} else if (name.equals("node")) {
					if (old || modify != Entity.MODIFY_DELETED) {
						long id = Long.parseLong(parser.getAttributeValue("", "id"));
						currentEntity = new Node(Double.parseDouble(parser.getAttributeValue("", "lat")),
								Double.parseDouble(parser.getAttributeValue("", "lon")), id);
						parseVersion(parser, currentEntity);
					}
				} else if (name.equals("relation")) {
					long id = Long.parseLong(parser.getAttributeValue("", "id"));
					currentEntity = new Relation(id);
					parseVersion(parser, currentEntity);
				} else if (name.equals("way")) {
					long id = Long.parseLong(parser.getAttributeValue("", "id"));
					currentWay = new Way(id);
					currentEntity = currentWay;
					parseVersion(parser, currentEntity);
				} else if (name.equals("member")) {
					String tp = parser.getAttributeValue("", "type");
					long ref = Long.parseLong(parser.getAttributeValue("", "ref"));
					String role = parser.getAttributeValue("", "role");
					EntityType type = tp.equals("node") ? EntityType.NODE : (tp.equals("way") ? EntityType.WAY
							: EntityType.RELATION);
					EntityId nid = new EntityId(type, ref);
					Map<EntityId, Entity> o = old ? ctx.oldIds : ctx.newIds;
					boolean skip = false;
					currentWay = null;
					if (!o.containsKey(nid) || old) {
						if (type == EntityType.NODE) {
							Node nd = registerNewNode(parser, ctx, old, nid);
							nid = EntityId.valueOf(nd);
						} else if (type == EntityType.WAY) {
							currentWay = new Way(ID_BASE--);
							currentWay.putTag("oid", nid.getId().toString());
							registerEntity(ctx, old, currentWay);
							registerByOldId(ctx, old, currentWay, nid);
							nid = EntityId.valueOf(currentWay);
						} else if (type == EntityType.RELATION) {
							// skip subrelations
							// throw new UnsupportedOperationException();
							skip = true;
						}
					}
					if (!skip) {
						((Relation) currentEntity).addMember(nid.getId(), type, role);
					}
				} else if (name.equals("nd") && currentWay != null) {
					String rf = parser.getAttributeValue("", "ref");
					Node nd = null;
					EntityId nid = null;
					if (!Algorithms.isEmpty(rf) && !old) {
						nid = new EntityId(EntityType.NODE, Long.parseLong(rf));
						Map<EntityId, Entity> o = old ? ctx.oldIds : ctx.newIds;
						nd = (Node) o.get(nid);
					}
					if (nd == null) {
						nd = registerNewNode(parser, ctx, old, nid);
					}
					((Way) currentWay).addNode(nd.getId());
				}
			}
		}
		return ctx;
	}

	private void updateTags(Entity currentEntity, String... tags) {
		String[] vls = new String[tags.length];
		for(int i = 0; i < vls.length; i ++) {
			vls[i] = currentEntity.getTag(tags[i]);
		}
		currentEntity.getModifiableTags().clear();
		for(int i = 0; i < vls.length; i ++) {
			if(vls[i] != null) {
				currentEntity.putTag(tags[i], vls[i]);
			}
		}
		currentEntity.putTag(OsmDbCreator.OSMAND_DELETE_TAG, OsmDbCreator.OSMAND_DELETE_VALUE);
	}

	private void parseVersion(XmlPullParser parser, Entity currentEntity) {
		String v = parser.getAttributeValue("", "version");
		if(!Algorithms.isEmpty(v)) {
			currentEntity.setVersion(Integer.parseInt(v));
		}

	}

	private Node registerNewNode(XmlPullParser parser, Context ctx, boolean old, EntityId oid) {
		Node nd = null;
		if (oid != null) {
			nd = (Node) (old ? ctx.oldOIds.get(oid) : ctx.newOIds.get(oid));
			if (nd != null) {
				return nd;
			}
		}
		double lat = Double.parseDouble(parser.getAttributeValue("", "lat"));
		double lon = Double.parseDouble(parser.getAttributeValue("", "lon"));
		LatLon ll = new LatLon(lat, lon);
		nd = old ? ctx.oldLocNodes.get(ll) : ctx.newLocNodes.get(ll);
		if (nd == null) {
			nd = new Node(lat, lon, ID_BASE--);
			if (oid != null) {
				nd.putTag("oid", oid.getId().toString());
			}
			registerEntity(ctx, old, nd);
			if(old) {
				ctx.oldLocNodes.put(ll, nd);
			} else {
				ctx.newLocNodes.put(ll, nd);
			}
		}
		registerByOldId(ctx, old, nd, oid);
		return nd;
	}

	private void registerEntity(Context ctx, boolean old, Entity nd) {
		if(old) {
			ctx.oldIds.put(EntityId.valueOf(nd), nd);
		} else {
			ctx.newIds.put(EntityId.valueOf(nd), nd);
		}
	}

	private void registerByOldId(Context ctx, boolean old, Entity nd, EntityId oid) {
		if(oid != null) {
			if(old) {
				ctx.oldOIds.put(oid, nd);
			} else {
				ctx.newOIds.put(oid, nd);
			}	
		}
	}


}
