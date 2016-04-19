package net.osmand.data.diff;

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
import net.osmand.map.OsmandRegions;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.EntityInfo;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Relation;
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


	private static String OSMAND_DELETE_TAG = "osmand_change";
	private static String OSMAND_DELETE_VALUE = "delete";
	private static long ID_BASE = -1000;
	public static void main(String[] args) throws XmlPullParserException, IOException, XMLStreamException, SQLException, InterruptedException {
		File inputFile = new File(args[0]);
		File targetDir = new File(args[1]);
		File ocbfFile = new File(args[2]);
		
		AugmentedDiffsInspector inspector = new AugmentedDiffsInspector();
		Context ctx = inspector.parseFile(inputFile);
		OsmandRegions or = new OsmandRegions();
		or.prepareFile(ocbfFile.getAbsolutePath());
		or.cacheAllCountries();
		inspector.prepareRegions(ctx, ctx.newIds, ctx.regionsNew, or);
		inspector.prepareRegions(ctx, ctx.oldIds, ctx.regionsOld, or);
		String name = inputFile.getName();
		String date = name.substring(0, name.indexOf('-'));
		String time = name.substring(name.indexOf('-') + 1, name.indexOf('.'));
		
		inspector.write(ctx, targetDir, date, time);
			
	}

	private void prepareRegions(Context ctx, Map<EntityId, Entity> ids, Map<String, Set<EntityId>> regionsMap, 
			OsmandRegions or) throws IOException {
		Map<EntityId, Set<String>> mp = new HashMap<Entity.EntityId, Set<String>>();
		for(Entity e : ids.values()) {
			if(e instanceof Node) {
				int y = MapUtils.get31TileNumberY(((Node) e).getLatitude());
				int x = MapUtils.get31TileNumberX(((Node) e).getLongitude());
				List<BinaryMapDataObject> l = or.query(x, y);
				EntityId id = EntityId.valueOf(e);
				TreeSet<String> lst = new TreeSet<String>();
				mp.put(id, lst);
				for (BinaryMapDataObject b : l) {
					if(or.contain(b, x, y)) {
						String dw = or.getDownloadName(b);
						if(!regionsMap.containsKey(dw)) {
							regionsMap.put(dw, new LinkedHashSet<Entity.EntityId>());
						}
						regionsMap.get(dw).add(id);
						lst.add(dw);
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
				for(EntityId it : r.getMemberIds()) {
					Set<String> countries = mp.get(it);
					for(String cnt : countries) {
						regionsMap.get(cnt).add(rid);
					}
					lst.addAll(countries);
				}
			}
		}
	}

	private void write(Context ctx, File targetDir, String date, String time) throws XMLStreamException, IOException, SQLException, InterruptedException, XmlPullParserException {
		targetDir.mkdirs();
//		writeFile(targetDir, "world", ctx.oldIds, null, ctx.newIds, null);
		for(String reg : ctx.regionsNew.keySet()) {
			File dr = new File(targetDir, reg + "/" + date);
			dr.mkdirs();
			writeFile(dr, reg + "_" + time, ctx.oldIds, ctx.regionsOld.get(reg), ctx.newIds, ctx.regionsNew.get(reg));
		}
	}
	
//	private void indexFile() {
//		IndexCreator ic = new IndexCreator(fl.getParentFile());
//		RTree.clearCache();
//		ic.setIndexAddress(false);
//		ic.setIndexPOI(true);
//		ic.setIndexRouting(true);
//		ic.setIndexMap(true);
//		ic.setGenerateLowLevelIndexes(false);
//		ic.setDialects(DBDialect.SQLITE_IN_MEMORY, DBDialect.SQLITE_IN_MEMORY);
//		//ic.setLastModifiedDate(g.getTimestamp());
//		File tmpFile = new File(fl.getName() + ".tmp.odb");
//		tmpFile.delete();
////		ic.setRegionName(Algorithms.capitalizeFirstLetterAndLowercase(g.dayName));
//		ic.setNodesDBFile(tmpFile);
//		ic.generateIndexes(new File[]{fl}, new ConsoleProgressImplementation(), null, 
//				MapZooms.parseZooms("13-14;15-"), new MapRenderingTypesEncoder(fl.getName()), log, false, true);
//		File targetFile = new File(fl.getParentFile(), ic.getMapFileName());
////		targetFile.setLastModified(g.getTimestamp());
////		FileInputStream fis = new FileInputStream(targetFile);
////		GZIPOutputStream gzout = new GZIPOutputStream(new FileOutputStream(obf));
////		Algorithms.streamCopy(fis, gzout);
////		fis.close();
////		gzout.close();
////		obf.setLastModified(g.getTimestamp());
//		targetFile.delete();
//	}

	private File writeFile(File targetDir, String prefix, Map<EntityId, Entity> octx, Set<EntityId> oset,
			Map<EntityId, Entity> nctx, Set<EntityId> nset) throws XMLStreamException,
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
				nodes, ways, relations);
		fous.close();
		return f;
	}

	private void groupObjects(Map<EntityId, Entity> octx, Set<EntityId> oset, List<Node> nodes, List<Way> ways,
			List<Relation> relations) {
		for(Entity e: octx.values()) {
			if(oset == null || oset.contains(EntityId.valueOf(e))) {
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
							updateTags(currentEntity, "name", "type", "area");
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
							Node nd = registerNewNode(parser, ctx, old);
							nid = EntityId.valueOf(nd);
						} else if (type == EntityType.WAY) {
							currentWay = new Way(ID_BASE--);
							registerEntity(ctx, old, currentWay);
							nid = EntityId.valueOf(currentWay);
						} else if (type == EntityType.RELATION) {
							// skip subrelations
							throw new UnsupportedOperationException();
						}
					}
					if (!skip) {
						((Relation) currentEntity).addMember(nid.getId(), type, role);
					}
				} else if (name.equals("nd") && currentWay != null) {
					String rf = parser.getAttributeValue("", "ref");
					Node nd = null;
					if (!Algorithms.isEmpty(rf) && !old) {
						EntityId nid = new EntityId(EntityType.NODE, Long.parseLong(rf));
						Map<EntityId, Entity> o = old ? ctx.oldIds : ctx.newIds;
						nd = (Node) o.get(nid);
					}
					if (nd == null) {
						nd = registerNewNode(parser, ctx, old);
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
		currentEntity.putTag(OSMAND_DELETE_TAG, OSMAND_DELETE_VALUE);
	}

	private void parseVersion(XmlPullParser parser, Entity currentEntity) {
		String v = parser.getAttributeValue("", "version");
		if(!Algorithms.isEmpty(v)) {
			currentEntity.setVersion(Integer.parseInt(v));
		}
		
	}

	private Node registerNewNode(XmlPullParser parser, Context ctx, boolean old) {
		Node nd;
		nd = new Node(Double.parseDouble(parser.getAttributeValue("", "lat")),
				Double.parseDouble(parser.getAttributeValue("", "lon")), ID_BASE--);
		registerEntity(ctx, old, nd);
		return nd;
	}

	private void registerEntity(Context ctx, boolean old, Entity nd) {
		if(old) {
			ctx.oldIds.put(EntityId.valueOf(nd), nd);
		} else {
			ctx.newIds.put(EntityId.valueOf(nd), nd);
		}
	}

	
}
