package net.osmand.data.diff;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import javax.xml.stream.XMLStreamException;

import net.osmand.PlatformUtil;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Way;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.osm.io.OsmStorageWriter;
import net.osmand.util.Algorithms;

import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

public class AugmentedDiffsInspector {

	private static String OSMAND_DELETE_TAG = "osmand_change";
	private static String OSMAND_DELETE_VALUE = "delete";
	private static long ID_BASE = -1000;
	public static void main(String[] args) throws XmlPullParserException, IOException, XMLStreamException {
		String file = args[0];
		File f = new File(file);
		if (f.isDirectory()) {
			for (File d : f.listFiles()) {
				if (d.isFile()) {
					new AugmentedDiffsInspector().process(d);
				}
			}
		} else {
			Context ctx = new AugmentedDiffsInspector().process(f);
			OsmBaseStorage st = new OsmBaseStorage();
			for(Entity e: ctx.newIds.values()) {
				st.registerEntity(e,  null);
			}
			new OsmStorageWriter().saveStorage(new FileOutputStream("/Users/victorshcherb/osmand/temp/new_temp.osm"), st, null, true);
			new OsmStorageWriter().saveStorage(System.out, st, null, true);
			st = new OsmBaseStorage();
			for(Entity e: ctx.oldIds.values()) {
				st.registerEntity(e,  null);
			}
			new OsmStorageWriter().saveStorage(new FileOutputStream("/Users/victorshcherb/osmand/temp/old_temp.osm"), st, null, true);
		}
	}

	private static class Context {
		Map<EntityId, Entity> oldIds = new LinkedHashMap<Entity.EntityId, Entity>();
		Map<EntityId, Entity> newIds = new LinkedHashMap<Entity.EntityId, Entity>();
	}

	private Context process(File file) throws XmlPullParserException, IOException {
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
							currentEntity.putTag(OSMAND_DELETE_TAG, OSMAND_DELETE_VALUE);
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
					if (!o.containsKey(nid)) {
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
					if (!Algorithms.isEmpty(rf)) {
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
