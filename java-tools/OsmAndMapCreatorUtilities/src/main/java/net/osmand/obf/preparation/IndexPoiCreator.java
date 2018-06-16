package net.osmand.obf.preparation;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.hash.TLongHashSet;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import net.osmand.IProgress;
import net.osmand.IndexConstants;
import net.osmand.binary.BinaryMapPoiReaderAdapter;
import net.osmand.data.Amenity;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.osm.MapPoiTypes;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.MapRenderingTypesEncoder.EntityConvertApplyType;
import net.osmand.osm.PoiType;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.EntityParser;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Relation.RelationMember;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import net.sf.junidecode.Junidecode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class IndexPoiCreator extends AbstractIndexPartCreator {

	private static final Log log = LogFactory.getLog(IndexPoiCreator.class);

	private Connection poiConnection;
	private File poiIndexFile;
	private PreparedStatement poiPreparedStatement;
	private TLongHashSet ids = new TLongHashSet();
	private PreparedStatement poiDeleteStatement;
	private static final int ZOOM_TO_SAVE_END = 16;
	private static final int ZOOM_TO_SAVE_START = 6;
	private static final int ZOOM_TO_WRITE_CATEGORIES_START = 12;
	private static final int ZOOM_TO_WRITE_CATEGORIES_END = 16;
	private static final int CHARACTERS_TO_BUILD = 4;
	private boolean useInMemoryCreator = true;
	public static long GENERATE_OBJ_ID = -(1L << 10L);
	
	private static int SHIFT_MULTIPOLYGON_IDS = 43;
	private static int DUPLICATE_SPLIT = 5;
	public TLongHashSet generatedIds = new TLongHashSet();

	private boolean overwriteIds;
	private List<Amenity> tempAmenityList = new ArrayList<Amenity>();
	TagsTransformer tagsTransform = new TagsTransformer();

	private final MapRenderingTypesEncoder renderingTypes;
	private MapPoiTypes poiTypes;
	private List<PoiAdditionalType> additionalTypesId = new ArrayList<PoiAdditionalType>();
	private Map<String, PoiAdditionalType> additionalTypesByTag = new HashMap<String, PoiAdditionalType>();
	private IndexCreatorSettings settings;


	public IndexPoiCreator(IndexCreatorSettings settings, MapRenderingTypesEncoder renderingTypes, boolean overwriteIds) {
		this.settings = settings;
		this.renderingTypes = renderingTypes;
		this.overwriteIds = overwriteIds;
		this.poiTypes = MapPoiTypes.getDefault();
	}

	public void setPoiTypes(MapPoiTypes poiTypes) {
		this.poiTypes = poiTypes;
	}

	private long assignIdForMultipolygon(Relation orig) {
		long ll = orig.getId();
		return genId(SHIFT_MULTIPOLYGON_IDS, (ll << 6) );
	}
	
	private long genId(int baseShift, long id) {
		long gen = (id << DUPLICATE_SPLIT) +  (1l << (baseShift - 1));
		while (generatedIds.contains(gen)) {
			gen += 2;
		}
		generatedIds.add(gen);
		return gen;
	}

	public void iterateEntity(Entity e, OsmDbAccessorContext ctx, boolean basemap) throws SQLException {
		tempAmenityList.clear();
		tagsTransform.addPropogatedTags(e);
		Map<String, String> tags = e.getTags();
		Map<String, String> etags = renderingTypes.transformTags(tags, EntityType.valueOf(e), EntityConvertApplyType.POI);
		boolean privateReg = "private".equals(e.getTag("access"));
		tempAmenityList = EntityParser.parseAmenities(poiTypes, e, etags, tempAmenityList);
		if (!tempAmenityList.isEmpty() && poiPreparedStatement != null) {
			if (e instanceof Relation) {
				ctx.loadEntityRelation((Relation) e);
			}
			boolean first = true;
			long id = e.getId();
			if (basemap) { 
				id = GENERATE_OBJ_ID--;
			} else if(e instanceof Relation) {
//				id = GENERATE_OBJ_ID--;
				id = assignIdForMultipolygon((Relation) e);
			} else if(id > 0) {
				// keep backward compatibility for ids (osm editing)
				id = e.getId() >> (OsmDbCreator.SHIFT_ID - 1);
				if (id % 2 != (e.getId() % 2)) {
					id ^= 1;
				}
			}
			for (Amenity a : tempAmenityList) {
				if (a.getType().getKeyName().equals("entertainment") && privateReg) {
					// don't index private swimming pools
					continue;
				}
				if (basemap) {
					PoiType st = a.getType().getPoiTypeByKeyName(a.getSubType());
					if (st == null || !a.getType().containsBasemapPoi(st)) {
						continue;
					}
				}
				// do not add that check because it is too much printing for batch creation
				// by statistic < 1% creates maps manually
				// checkEntity(e);
				EntityParser.parseMapObject(a, e, etags);
				a.setId(id);
				if (a.getLocation() != null) {
					// do not convert english name
					// convertEnglishName(a);
					if (overwriteIds && first) {
						if (!ids.add(a.getId())) {
							poiPreparedStatement.executeBatch();
							poiDeleteStatement.setString(1, a.getId() + "");
							poiDeleteStatement.execute();
							first = false;
						}
					}
					insertAmenityIntoPoi(a);
				}
			}
		}
	}

	


	public void iterateRelation(Relation e, OsmDbAccessorContext ctx) throws SQLException {

		Map<String, String> tags = renderingTypes.transformTags(e.getTags(), EntityType.RELATION, EntityConvertApplyType.POI);
		for (String t : tags.keySet()) {
			boolean index = poiTypes.parseAmenity(t, tags.get(t), true, tags) != null;
			if (index) {
				ctx.loadEntityRelation(e);
				for (RelationMember id : ((Relation) e).getMembers()) {
					tagsTransform.registerPropogatedTag(id.getEntityId(), t, tags.get(t));
				}
			}
		}
		tagsTransform.handleRelationPropogatedTags(e, renderingTypes, ctx, EntityConvertApplyType.POI);
	}

	

	public void commitAndClosePoiFile(Long lastModifiedDate) throws SQLException {
		closeAllPreparedStatements();
		if (poiConnection != null) {
			poiConnection.commit();
			poiConnection.close();
			poiConnection = null;
			if (lastModifiedDate != null) {
				poiIndexFile.setLastModified(lastModifiedDate);
			}
		}
	}

	public void removePoiFile() {
		Algorithms.removeAllFiles(poiIndexFile);
	}


	public void insertAmenityIntoPoi(Amenity amenity) throws SQLException {
		assert IndexConstants.POI_TABLE != null : "use constants here to show table usage "; //$NON-NLS-1$
		poiPreparedStatement.setLong(1, amenity.getId());
		poiPreparedStatement.setInt(2, MapUtils.get31TileNumberX(amenity.getLocation().getLongitude()));
		poiPreparedStatement.setInt(3, MapUtils.get31TileNumberY(amenity.getLocation().getLatitude()));
		poiPreparedStatement.setString(4, amenity.getType().getKeyName());
		poiPreparedStatement.setString(5, amenity.getSubType());
		poiPreparedStatement.setString(6, encodeAdditionalInfo(amenity, amenity.getAdditionalInfo(), amenity.getName(), amenity.getEnName(false)));
		addBatch(poiPreparedStatement);
	}

	private PoiAdditionalType getOrCreate(String tag, String value, boolean text) {
		String ks = PoiAdditionalType.getKey(tag, value, text);
		if (additionalTypesByTag.containsKey(ks)) {
			return additionalTypesByTag.get(ks);
		}
		int sz = additionalTypesId.size();
		PoiAdditionalType tp = new PoiAdditionalType(sz, tag, value, text);
		additionalTypesId.add(tp);
		additionalTypesByTag.put(ks, tp);
		return tp;
	}

	private static final char SPECIAL_CHAR = ((char) -1);

	private String encodeAdditionalInfo(Amenity amenity, Map<String, String> tempNames, String name, String nameEn) {
		tempNames = new HashMap<String, String>(tempNames);
		if (!Algorithms.isEmpty(name)) {
			tempNames.put("name", name);
		}
		if (!Algorithms.isEmpty(nameEn) && !Algorithms.objectEquals(name, nameEn)) {
			tempNames.put("name:en", nameEn);
		}
		Iterator<Entry<String, String>> it = amenity.getNamesMap(false).entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, String> next = it.next();
			tempNames.put("name:" + next.getKey(), next.getValue());
		}
		StringBuilder b = new StringBuilder();
		for (Map.Entry<String, String> e : tempNames.entrySet()) {
			boolean text = poiTypes.isTextAdditionalInfo(e.getKey(), e.getValue());
			PoiAdditionalType rulType = getOrCreate(e.getKey(), e.getValue(), text);
			if (!rulType.isText() || !Algorithms.isEmpty(e.getValue())) {
				if (b.length() > 0) {
					b.append(SPECIAL_CHAR);
				}
				if (!rulType.isText() && rulType.getValue() == null) {
					throw new IllegalStateException("Additional rule type '" + rulType.getTag() + "' should be encoded with value '" + e.getValue() + "'");
				}
				// avoid 0 (bug in jdk on macos)
				b.append((char) ((rulType.getId()) + 1)).append(e.getValue());
			}
		}
		return b.toString();
	}


	private Map<PoiAdditionalType, String> decodeAdditionalInfo(String name,
			Map<PoiAdditionalType, String> tempNames) {
		tempNames.clear();
		if (name.length() == 0) {
			return tempNames;
		}
		int i, p = 0;
		while (true) {
			i = name.indexOf(SPECIAL_CHAR, p);
			String t = i == -1 ? name.substring(p) : name.substring(p, i);
			PoiAdditionalType rulType = additionalTypesId.get(t.charAt(0) - 1);
			tempNames.put(rulType, t.substring(1));
			if (!rulType.isText() && rulType.getValue() == null) {
				throw new IllegalStateException("Additional rule type '" + rulType.getTag() + "' should be encoded with value '" + t.substring(1) + "'");
			}
			if (i == -1) {
				break;
			}
			p = i + 1;
		}
		return tempNames;
	}

	public void createDatabaseStructure(File poiIndexFile) throws SQLException {
		this.poiIndexFile = poiIndexFile;
		// delete previous file to save space
		File parentFile = poiIndexFile.getParentFile();
		if(parentFile != null) { 
			parentFile.mkdirs();
		}
		if (poiIndexFile.exists()) {
			Algorithms.removeAllFiles(poiIndexFile);
		}
		// creating connection
		poiConnection = (Connection) DBDialect.SQLITE.getDatabaseConnection(poiIndexFile.getAbsolutePath(), log);

		// create database structure
		Statement stat = poiConnection.createStatement();
		stat.executeUpdate("create table " + IndexConstants.POI_TABLE + //$NON-NLS-1$
				" (id bigint, x int, y int,"
				+ "type varchar(1024), subtype varchar(1024), additionalTags varchar(8096), "
				+ "primary key(id, type, subtype))");
		stat.executeUpdate("create index poi_loc on poi (x, y, type, subtype)");
		stat.executeUpdate("create index poi_id on poi (id, type, subtype)");
		stat.execute("PRAGMA user_version = " + IndexConstants.POI_TABLE_VERSION); //$NON-NLS-1$
		stat.close();

		// create prepared statment
		poiPreparedStatement = poiConnection
				.prepareStatement("INSERT INTO " + IndexConstants.POI_TABLE + "(id, x, y, type, subtype, additionalTags) " + //$NON-NLS-1$//$NON-NLS-2$
						"VALUES (?, ?, ?, ?, ?, ?)");
		poiDeleteStatement = poiConnection.prepareStatement("DELETE FROM " + IndexConstants.POI_TABLE + " where id = ?");
		pStatements.put(poiPreparedStatement, 0);

		poiConnection.setAutoCommit(false);
	}


	private class IntBbox {
		int minX = Integer.MAX_VALUE;
		int maxX = 0;
		int minY = Integer.MAX_VALUE;
		int maxY = 0;
	}

	public static class PoiCreatorCategories {
		Map<String, Set<String>> categories = new HashMap<String, Set<String>>();
		Set<PoiAdditionalType> additionalAttributes = new HashSet<PoiAdditionalType>();


		// build indexes to write
		Map<String, Integer> catIndexes;
		Map<String, Integer> subcatIndexes;

		TIntArrayList cachedCategoriesIds;
		TIntArrayList cachedAdditionalIds;

		// cache for single thread
		TIntArrayList singleThreadVarTypes = new TIntArrayList();


		private String[] split(String subCat) {
			return subCat.split(",|;");
		}

		private boolean toSplit(String subCat) {
			return subCat.contains(";") || subCat.contains(",");
		}

		public void addCategory(String cat, String subCat, Map<PoiAdditionalType, String> additionalTags) {
			for (PoiAdditionalType rt : additionalTags.keySet()) {
				if (!rt.isText() && rt.getValue() == null) {
					throw new NullPointerException("Null value for additional tag =" + rt.getTag());
				}
				additionalAttributes.add(rt);
			}
			if (!categories.containsKey(cat)) {
				categories.put(cat, new TreeSet<String>());
			}
			if (toSplit(subCat)) {
				String[] split = split(subCat);
				for (String sub : split) {
					categories.get(cat).add(sub.trim());
				}
			} else {
				categories.get(cat).add(subCat.trim());
			}
		}

		public TIntArrayList buildTypeIds(String category, String subcategory) {
			singleThreadVarTypes.clear();
			TIntArrayList types = singleThreadVarTypes;
			internalBuildType(category, subcategory, types);
			return types;
		}

		private void internalBuildType(String category, String subcategory, TIntArrayList types) {
			int catInd = catIndexes.get(category);
			if (toSplit(subcategory)) {
				for (String sub : split(subcategory)) {
					Integer subcatInd = subcatIndexes.get(category + SPECIAL_CHAR + sub.trim());
					if (subcatInd == null) {
						throw new IllegalArgumentException("Unknown subcategory " + sub + " category " + category);
					}
					types.add((subcatInd << BinaryMapPoiReaderAdapter.SHIFT_BITS_CATEGORY) | catInd);
				}
			} else {
				Integer subcatInd = subcatIndexes.get(category + SPECIAL_CHAR + subcategory.trim());
				if (subcatInd == null) {
					throw new IllegalArgumentException("Unknown subcategory " + subcategory + " category " + category);
				}
				types.add((subcatInd << BinaryMapPoiReaderAdapter.SHIFT_BITS_CATEGORY) | catInd);
			}
		}

		public void buildCategoriesToWrite(PoiCreatorCategories globalCategories) {
			cachedCategoriesIds = new TIntArrayList();
			cachedAdditionalIds = new TIntArrayList();
			for (Map.Entry<String, Set<String>> cats : categories.entrySet()) {
				for (String subcat : cats.getValue()) {
					String cat = cats.getKey();
					globalCategories.internalBuildType(cat, subcat, cachedCategoriesIds);
				}
			}
			for (PoiAdditionalType rt : additionalAttributes) {
				if (rt.getTargetId() == -1) {
					throw new IllegalStateException("Map rule type is not registered for poi : " + rt);
				}
				cachedAdditionalIds.add(rt.getTargetId());
			}
		}

		public void setSubcategoryIndex(String cat, String sub, int j) {
			if (subcatIndexes == null) {
				subcatIndexes = new HashMap<String, Integer>();
			}
			subcatIndexes.put(cat + SPECIAL_CHAR + sub, j);
		}

		public void setCategoryIndex(String cat, int i) {
			if (catIndexes == null) {
				catIndexes = new HashMap<String, Integer>();
			}
			catIndexes.put(cat, i);
		}
	}

	public void writeBinaryPoiIndex(BinaryMapIndexWriter writer, String regionName, IProgress progress) throws SQLException, IOException {
		if (poiPreparedStatement != null) {
			closePreparedStatements(poiPreparedStatement);
		}
		poiConnection.commit();

		Map<String, Set<PoiTileBox>> namesIndex = new TreeMap<String, Set<PoiTileBox>>();

		int zoomToStart = ZOOM_TO_SAVE_START;
		IntBbox bbox = new IntBbox();
		Tree<PoiTileBox> rootZoomsTree = new Tree<PoiTileBox>();
		// 0. process all entities
		processPOIIntoTree(namesIndex, zoomToStart, bbox, rootZoomsTree);

		// 1. write header
		long startFpPoiIndex = writer.startWritePoiIndex(regionName, bbox.minX, bbox.maxX, bbox.maxY, bbox.minY);

		// 2. write categories table
		PoiCreatorCategories globalCategories = rootZoomsTree.node.categories;
		writer.writePoiCategoriesTable(globalCategories);
		writer.writePoiSubtypesTable(globalCategories);

		// 2.5 write names table
		Map<PoiTileBox, List<BinaryFileReference>> fpToWriteSeeks = writer.writePoiNameIndex(namesIndex, startFpPoiIndex);

		// 3. write boxes
		log.info("Poi box processing finished");
		int level = 0;
		for (; level < (ZOOM_TO_SAVE_END - zoomToStart); level++) {
			int subtrees = rootZoomsTree.getSubTreesOnLevel(level);
			if (subtrees > 8) {
				level--;
				break;
			}
		}
		if (level > 0) {
			rootZoomsTree.extractChildrenFromLevel(level);
			zoomToStart = zoomToStart + level;
		}

		// 3.2 write tree using stack
		for (Tree<PoiTileBox> subs : rootZoomsTree.getSubtrees()) {
			writePoiBoxes(writer, subs, startFpPoiIndex, fpToWriteSeeks, globalCategories);
		}

		// 4. write poi data
		// not so effective probably better to load in memory one time
		PreparedStatement prepareStatement = poiConnection
				.prepareStatement("SELECT id, x, y, type, subtype, additionalTags from poi "
						+ "where x >= ? AND x < ? AND y >= ? AND y < ?");
		for (Map.Entry<PoiTileBox, List<BinaryFileReference>> entry : fpToWriteSeeks.entrySet()) {
			int z = entry.getKey().zoom;
			int x = entry.getKey().x;
			int y = entry.getKey().y;
			writer.startWritePoiData(z, x, y, entry.getValue());

			if (useInMemoryCreator) {
				List<PoiData> poiData = entry.getKey().poiData;

				for (PoiData poi : poiData) {
					int x31 = poi.x;
					int y31 = poi.y;
					String type = poi.type;
					String subtype = poi.subtype;
					int x24shift = (x31 >> 7) - (x << (24 - z));
					int y24shift = (y31 >> 7) - (y << (24 - z));
					writer.writePoiDataAtom(poi.id, x24shift, y24shift, type, subtype, poi.additionalTags,
							globalCategories, settings.poiZipLongStrings ? settings.poiZipStringLimit : -1);
				}

			} else {
				prepareStatement.setInt(1, x << (31 - z));
				prepareStatement.setInt(2, (x + 1) << (31 - z));
				prepareStatement.setInt(3, y << (31 - z));
				prepareStatement.setInt(4, (y + 1) << (31 - z));
				ResultSet rset = prepareStatement.executeQuery();
				Map<PoiAdditionalType, String> mp = new HashMap<PoiAdditionalType, String>();
				while (rset.next()) {
					long id = rset.getLong(1);
					int x31 = rset.getInt(2);
					int y31 = rset.getInt(3);
					int x24shift = (x31 >> 7) - (x << (24 - z));
					int y24shift = (y31 >> 7) - (y << (24 - z));
					String type = rset.getString(4);
					String subtype = rset.getString(5);
					writer.writePoiDataAtom(id, x24shift, y24shift, type, subtype,
							decodeAdditionalInfo(rset.getString(6), mp), globalCategories,
							settings.poiZipLongStrings ? settings.poiZipStringLimit : -1);
				}
				rset.close();
			}
			writer.endWritePoiData();
		}

		prepareStatement.close();

		writer.endWritePoiIndex();

	}

	private PoiAdditionalType retrieveAdditionalType(String key) {
		for (PoiAdditionalType t : additionalTypesId) {
			if (Algorithms.objectEquals(t.getTag(), key)) {
				return t;
			}
		}
		return null;
	}

	private void processPOIIntoTree(Map<String, Set<PoiTileBox>> namesIndex, int zoomToStart, IntBbox bbox,
			Tree<PoiTileBox> rootZoomsTree) throws SQLException {
		ResultSet rs;
		if (useInMemoryCreator) {
			rs = poiConnection.createStatement().executeQuery("SELECT x,y,type,subtype,id,additionalTags from poi");
		} else {
			rs = poiConnection.createStatement().executeQuery("SELECT x,y,type,subtype from poi");
		}
		rootZoomsTree.setNode(new PoiTileBox());

		int count = 0;
		ConsoleProgressImplementation console = new ConsoleProgressImplementation();
		console.startWork(1000000);
		Map<PoiAdditionalType, String> additionalTags = new LinkedHashMap<PoiAdditionalType, String>();
		PoiAdditionalType nameRuleType = retrieveAdditionalType("name");
		PoiAdditionalType nameEnRuleType = retrieveAdditionalType("name:en");
		while (rs.next()) {
			int x = rs.getInt(1);
			int y = rs.getInt(2);
			bbox.minX = Math.min(x, bbox.minX);
			bbox.maxX = Math.max(x, bbox.maxX);
			bbox.minY = Math.min(y, bbox.minY);
			bbox.maxY = Math.max(y, bbox.maxY);
			if (count++ > 10000) {
				count = 0;
				console.progress(10000);
			}

			String type = rs.getString(3);
			String subtype = rs.getString(4);
			decodeAdditionalInfo(rs.getString(6), additionalTags);

			Tree<PoiTileBox> prevTree = rootZoomsTree;
			rootZoomsTree.getNode().categories.addCategory(type, subtype, additionalTags);
			for (int i = zoomToStart; i <= ZOOM_TO_SAVE_END; i++) {
				int xs = x >> (31 - i);
				int ys = y >> (31 - i);
				Tree<PoiTileBox> subtree = null;
				for (Tree<PoiTileBox> sub : prevTree.getSubtrees()) {
					if (sub.getNode().x == xs && sub.getNode().y == ys && sub.getNode().zoom == i) {
						subtree = sub;
						break;
					}
				}
				if (subtree == null) {
					subtree = new Tree<PoiTileBox>();
					PoiTileBox poiBox = new PoiTileBox();
					subtree.setNode(poiBox);
					poiBox.x = xs;
					poiBox.y = ys;
					poiBox.zoom = i;

					prevTree.addSubTree(subtree);
				}
				subtree.getNode().categories.addCategory(type, subtype, additionalTags);

				prevTree = subtree;
			}
			Set<String> otherNames = null;
			Iterator<Entry<PoiAdditionalType, String>> it = additionalTags.entrySet().iterator();
			while (it.hasNext()) {
				Entry<PoiAdditionalType, String> e = it.next();
				if ((e.getKey().getTag().contains("name") || e.getKey().getTag().equals("brand")) 
						&& !"name:en".equals(e.getKey().getTag())) {
					if (otherNames == null) {
						otherNames = new TreeSet<String>();
					}
					otherNames.add(e.getValue());
				}
			}
			addNamePrefix(additionalTags.get(nameRuleType), additionalTags.get(nameEnRuleType), prevTree.getNode(), 
					namesIndex, otherNames);

			if (useInMemoryCreator) {
				if (prevTree.getNode().poiData == null) {
					prevTree.getNode().poiData = new ArrayList<PoiData>();
				}
				PoiData poiData = new PoiData();
				poiData.x = x;
				poiData.y = y;
				poiData.type = type;
				poiData.subtype = subtype;
				poiData.id = rs.getLong(5);
				poiData.additionalTags.putAll(additionalTags);
				prevTree.getNode().poiData.add(poiData);

			}
		}
		log.info("Poi processing finished");
	}

	private void addNamePrefix(String name, String nameEn, PoiTileBox data, Map<String, Set<PoiTileBox>> poiData,
			Set<String> names) {
		if (name != null) {
			parsePrefix(name, data, poiData);
			if (Algorithms.isEmpty(nameEn)) {
				nameEn = Junidecode.unidecode(name);
			}

		}
		if (!Algorithms.objectEquals(nameEn, name) && !Algorithms.isEmpty(nameEn)) {
			parsePrefix(nameEn, data, poiData);
		}
		if (names != null) {
			for (String nk : names) {
				if (!Algorithms.objectEquals(nk, name) && !Algorithms.isEmpty(nk)) {
					parsePrefix(nk, data, poiData);
				}
			}
		}
	}

	private void parsePrefix(String name, PoiTileBox data, Map<String, Set<PoiTileBox>> poiData) {
		int prev = -1;
		for (int i = 0; i <= name.length(); i++) {
			if (i == name.length() || 
					(!Character.isLetter(name.charAt(i)) && !Character.isDigit(name.charAt(i)) )) {
				// && name.charAt(i) != '\''
				if (prev != -1) {
					String substr = name.substring(prev, i);
					if (substr.length() > CHARACTERS_TO_BUILD) {
						substr = substr.substring(0, CHARACTERS_TO_BUILD);
					}
					String val = substr.toLowerCase();
					if (!poiData.containsKey(val)) {
						poiData.put(val, new LinkedHashSet<PoiTileBox>());
					}
					poiData.get(val).add(data);
					prev = -1;
				}
			} else {
				if (prev == -1) {
					prev = i;
				}
			}
		}
	}

	private void writePoiBoxes(BinaryMapIndexWriter writer, Tree<PoiTileBox> tree,
			long startFpPoiIndex, Map<PoiTileBox, List<BinaryFileReference>> fpToWriteSeeks,
			PoiCreatorCategories globalCategories) throws IOException, SQLException {
		int x = tree.getNode().x;
		int y = tree.getNode().y;
		int zoom = tree.getNode().zoom;
		boolean end = zoom == ZOOM_TO_SAVE_END;
		BinaryFileReference fileRef = writer.startWritePoiBox(zoom, x, y, startFpPoiIndex, end);
		if (fileRef != null) {
			if (!fpToWriteSeeks.containsKey(tree.getNode())) {
				fpToWriteSeeks.put(tree.getNode(), new ArrayList<BinaryFileReference>());
			}
			fpToWriteSeeks.get(tree.getNode()).add(fileRef);
		}
		if (zoom >= ZOOM_TO_WRITE_CATEGORIES_START && zoom <= ZOOM_TO_WRITE_CATEGORIES_END) {
			PoiCreatorCategories boxCats = tree.getNode().categories;
			boxCats.buildCategoriesToWrite(globalCategories);
			writer.writePoiCategories(boxCats);
		}

		if (!end) {
			for (Tree<PoiTileBox> subTree : tree.getSubtrees()) {
				writePoiBoxes(writer, subTree, startFpPoiIndex, fpToWriteSeeks, globalCategories);
			}
		}
		writer.endWritePoiBox();
	}

	private static class PoiData {
		int x;
		int y;
		String type;
		String subtype;
		long id;
		Map<PoiAdditionalType, String> additionalTags = new HashMap<PoiAdditionalType, String>();
	}

	public static class PoiTileBox {
		int x;
		int y;
		int zoom;
		PoiCreatorCategories categories = new PoiCreatorCategories();
		List<PoiData> poiData = null;

		public int getX() {
			return x;
		}


		public int getY() {
			return y;
		}

		public int getZoom() {
			return zoom;
		}


	}

	private static class Tree<T> {

		private T node;
		private List<Tree<T>> subtrees = null;

		public List<Tree<T>> getSubtrees() {
			if (subtrees == null) {
				subtrees = new ArrayList<Tree<T>>();
			}
			return subtrees;
		}

		public void addSubTree(Tree<T> t) {
			getSubtrees().add(t);
		}

		public T getNode() {
			return node;
		}

		public void setNode(T node) {
			this.node = node;
		}

		public void extractChildrenFromLevel(int level) {
			List<Tree<T>> list = new ArrayList<Tree<T>>();
			collectChildrenFromLevel(list, level);
			subtrees = list;
		}

		public void collectChildrenFromLevel(List<Tree<T>> list, int level) {
			if (level == 0) {
				if (subtrees != null) {
					list.addAll(subtrees);
				}
			} else if (subtrees != null) {
				for (Tree<T> sub : subtrees) {
					sub.collectChildrenFromLevel(list, level - 1);
				}

			}

		}

		public int getSubTreesOnLevel(int level) {
			if (level == 0) {
				if (subtrees == null) {
					return 0;
				} else {
					return subtrees.size();
				}
			} else {
				int sum = 0;
				if (subtrees != null) {
					for (Tree<T> t : subtrees) {
						sum += t.getSubTreesOnLevel(level - 1);
					}
				}
				return sum;
			}
		}

	}

	public static class PoiAdditionalType {

		private String tag;
		private String value;
		private boolean text;
		private int usage;
		private int targetId;
		private int id;

		public PoiAdditionalType(int id, String tag, String value, boolean text) {
			this.id = id;
			this.tag = tag;
			this.text = text;
			this.value = text ? null : value;
		}

		public int getId() {
			return id;
		}

		public boolean isText() {
			return text;
		}

		public void increment() {
			usage++;
		}

		public int getTargetId() {
			return targetId;
		}

		public int getUsage() {
			return usage;
		}

		public String getTag() {
			return tag;
		}

		public String getValue() {
			return value;
		}


		public static String getKey(String tag, String value, boolean text) {
			return text ? tag : tag + "/" + value;
		}

		public void setTargetPoiId(int catId, int valueId) {
			if (catId <= 31) {
				this.targetId = (valueId << 6) | (catId << 1);
			} else {
				if (catId > (1 << 15)) {
					throw new IllegalArgumentException("Refer source code");
				}
				this.targetId = (valueId << 16) | (catId << 1) | 1;
			}
		}
	}

}
