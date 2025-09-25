package net.osmand.obf.preparation;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.set.hash.TLongHashSet;
import net.osmand.IProgress;
import net.osmand.IndexConstants;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapPoiReaderAdapter;
import net.osmand.binary.GeocodingUtilities;
import net.osmand.binary.GeocodingUtilities.GeocodingResult;
import net.osmand.binary.ObfConstants;
import net.osmand.data.Amenity;
import net.osmand.data.Boundary;
import net.osmand.data.City;
import net.osmand.data.LatLon;
import net.osmand.data.Multipolygon;
import net.osmand.data.MultipolygonBuilder;
import net.osmand.data.QuadRect;
import net.osmand.data.QuadTree;
import net.osmand.data.Ring;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.osm.MapPoiTypes;
import net.osmand.osm.MapRenderingTypes;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.MapRenderingTypesEncoder.EntityConvertApplyType;
import net.osmand.osm.PoiCategory;
import net.osmand.osm.PoiType;
import net.osmand.osm.RelationTagsPropagation;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.EntityParser;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.OSMSettings;
import net.osmand.osm.edit.OsmMapUtils;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Relation.RelationMember;
import net.osmand.osm.edit.Way;
import net.osmand.router.RoutingContext;
import net.osmand.util.Algorithms;
import net.osmand.util.ArabicNormalizer;
import net.osmand.util.MapUtils;
import net.osmand.util.TopTagValuesAnalyzer;
import net.sf.junidecode.Junidecode;

public class IndexPoiCreator extends AbstractIndexPartCreator {


	private static final Log log = LogFactory.getLog(IndexPoiCreator.class);

	private Connection poiConnection;
	private File poiIndexFile;
	private PreparedStatement poiPreparedStatement;
	private PreparedStatement tagGroupsPreparedStatement;
	private static final int ZOOM_TO_SAVE_END = 16;
	private static final int ZOOM_TO_SAVE_START = 6;
	private static final int ZOOM_TO_WRITE_CATEGORIES_START = 12;
	private static final int ZOOM_TO_WRITE_CATEGORIES_END = 16;
	private static final double GEOCODING_DISTANCE = 150;
	
	private boolean useInMemoryCreator = true;
	public static long GENERATE_OBJ_ID = -(1L << 10L);
	private static int DUPLICATE_SPLIT = 5;
	public TLongHashSet generatedIds = new TLongHashSet();

	private List<Amenity> tempAmenityList = new ArrayList<Amenity>();
	RelationTagsPropagation tagsTransform = new RelationTagsPropagation();

	private final MapRenderingTypesEncoder renderingTypes;
	private MapPoiTypes poiTypes;
	private List<PoiAdditionalType> additionalTypesId = new ArrayList<PoiAdditionalType>();
	private Map<String, PoiAdditionalType> additionalTypesByTag = new HashMap<String, PoiAdditionalType>();
	private IndexCreatorSettings settings;
	private Map<String, Set<String>> topIndexAdditional;
	private Set<String> topIndexKeys = new HashSet<>();
	private TLongHashSet excludedRelations = new TLongHashSet();

	private QuadTree<Multipolygon> cityQuadTree;
	private Map<Multipolygon, List<PoiCreatorTagGroup>> cityTagsGroup;
	private Map<Long, List<Integer>> poiTagGroups = new HashMap<>();
	private Map<String, Map<Integer, Integer>> tagGroupIdsByRegion = new HashMap<>();// region => <oldTagGroupId, newTagGroupId>
	private int maxTagGroupId = 0;
	private Map<Integer, PoiCreatorTagGroup> tagGroupsFromDB;

	private final long PROPAGATED_NODE_BIT = 1L << (ObfConstants.SHIFT_PROPAGATED_NODE_IDS - 1);

	// Actual list of brands is constantly regenerated from BrandAnalyzer utlitity
	private static final String ENV_POI_TOP_INDEXES_URL = "POI_TOP_INDEXES_URL";
	public static final int DEFAULT_TOP_INDEX_MIN_COUNT = PoiType.DEFAULT_MIN_COUNT;
	public static final int DEFAULT_TOP_INDEX_MAX_PER_MAP = PoiType.DEFAULT_MAX_PER_MAP;
	public static final int DEFAULT_TOP_INDEX_LIMIT_PER_MAP = 1000;

    private final List<String> WORLD_BRANDS = Arrays.asList("McDonald's", "Starbucks", "Subway", "KFC", "Burger King", "Domino's Pizza",
            "Pizza Hut", "Dunkin'", "Costa Coffee", "Tim Hortons", "7-Eleven", "Żabka", "Shell", "BP", "Chevron",
            "TotalEnergies", "Aral", "Q8", "Petronas", "Caltex", "Esso", "Tesla Supercharger", "Ionity", "Walmart", "Carrefour",
            "Tesco", "Lidl", "Aldi", "Costco", "Auchan", "IKEA", "H&M", "Zara", "Uniqlo", "Nike", "Adidas", "Decathlon", "REI",
            "The North Face", "Apple Store", "Samsung", "Media Markt", "Best Buy", "Barnes & Noble",
            "WHSmith", "Waterstones", "Marriott", "Hilton", "Holiday Inn", "Ibis", "Best Western", "Radisson",
            "Planet Fitness", "Anytime Fitness", "Gold's Gym", "24 Hour Fitness", "Snap Fitness", "Walgreens", "CVS Pharmacy", "Boots", "Watsons",
            "Hertz", "Avis", "Sixt", "Enterprise", "Europcar", "Mountain Warehouse", "Intersport", "Hudson News");

	public IndexPoiCreator(IndexCreatorSettings settings, MapRenderingTypesEncoder renderingTypes) {
		this.settings = settings;
		this.renderingTypes = renderingTypes;
		this.poiTypes = MapPoiTypes.getDefault();
	}

	public void storeCities(CityDataStorage cityDataStorage) {
		if (cityDataStorage != null) {
			cityQuadTree = new QuadTree<Multipolygon>(new QuadRect(0, 0, Integer.MAX_VALUE, Integer.MAX_VALUE),
					8, 0.55f);
			cityTagsGroup = new HashMap<>();
			int id = 1;
			Set<String> allLanguages =new HashSet<>(Arrays.asList(MapRenderingTypes.langs));
			for (Map.Entry<City, Boundary> entry : cityDataStorage.cityBoundaries.entrySet()) {
				Boundary b = entry.getValue();
				City city = entry.getKey();
				if (city.getType() == null || !city.getType().storedAsSeparateAdminEntity()) {
					continue;
				}

				List<String> tags = new ArrayList<>();
				String name = city.getName();
				if (!Algorithms.isEmpty(name)) {
					tags.add("name");
					tags.add(name);
				}
				String enName = city.getEnName(false);
				if (!Algorithms.isEmpty(enName)) {
					tags.add("name:en");
					tags.add(enName);
				}
				Map<String, String> otherNames = city.getNamesMap(true);
				for (Map.Entry<String, String> nameEntry : otherNames.entrySet()) {
					if (allLanguages.contains(nameEntry.getKey())) {
						tags.add("name:" + nameEntry.getKey());
						tags.add(nameEntry.getValue());
					}
				}

				tags.add("place");
				tags.add(city.getType().name().toLowerCase());

				if (tags.isEmpty()) {
					continue;
				}

				PoiCreatorTagGroup poiCreatorTagGroup = new PoiCreatorTagGroup(id, tags);
				List<PoiCreatorTagGroup> tagGroups = cityTagsGroup.computeIfAbsent(b.getMultipolygon(), s -> new ArrayList<>());
				tagGroups.add(poiCreatorTagGroup);
				id++;

				Multipolygon m = b.getMultipolygon();
				QuadRect bboxLatLon = m.getLatLonBbox();
				int left = MapUtils.get31TileNumberX(bboxLatLon.left);
				int right = MapUtils.get31TileNumberX(bboxLatLon.right);
				int top = MapUtils.get31TileNumberY(bboxLatLon.top);
				int bottom = MapUtils.get31TileNumberY(bboxLatLon.bottom);
				QuadRect bbox = new QuadRect(left, top, right, bottom);
				cityQuadTree.insert(m, bbox);
			}
		}
	}

	public void setPoiTypes(MapPoiTypes poiTypes) {
		this.poiTypes = poiTypes;
	}

	private long assignIdForMultipolygon(Relation orig) {
		long ll = orig.getId();
		return genId(ObfConstants.SHIFT_MULTIPOLYGON_IDS, (ll << 6) );
	}

	private long genId(int baseShift, long id) {
		long gen = (id << DUPLICATE_SPLIT) +  (1l << (baseShift - 1));
		while (generatedIds.contains(gen)) {
			gen += 2;
		}
		generatedIds.add(gen);
		return gen;
	}

	public void iterateEntity(Entity e, OsmDbAccessorContext ctx, IndexCreationContext icc) throws SQLException {
		if (e instanceof Relation && excludedRelations.contains(e.getId())) {
			return;
		}
		if (!settings.keepOnlyRouteRelationObjects) {
			iterateEntityInternal(e, ctx, icc);
		}
	}

	void iterateEntityInternal(Entity e, OsmDbAccessorContext ctx, IndexCreationContext icc) throws SQLException {
		tempAmenityList.clear();
		Map<String, String> tags = tagsTransform.addPropogatedTags(renderingTypes, EntityConvertApplyType.POI, e, e.getTags());
		tags = renderingTypes.transformTags(tags, EntityType.valueOf(e), EntityConvertApplyType.POI);
		tempAmenityList = EntityParser.parseAmenities(poiTypes, e, tags, tempAmenityList);
		if (!tempAmenityList.isEmpty() && poiPreparedStatement != null) {
			if (isOutOfRegionBbox(e, icc)) {
                return;
            }
			List<LatLon> centers = Collections.singletonList(null);
			String memberIds = "";
			if (e instanceof Relation relation) {
				ctx.loadEntityRelation(relation);
				boolean isAdministrative = tags.get(OSMSettings.OSMTagKey.ADMIN_LEVEL.getValue()) != null;
				List<Entity> adminCenters = relation.getMemberEntities("admin_centre");
				if (adminCenters.size() == 1) {
					centers = Collections.singletonList(adminCenters.get(0).getLatLon());
				} else if (OsmMapUtils.isMultipolygon(tags) && !isAdministrative) {
					MultipolygonBuilder original = new MultipolygonBuilder();
					original.setId(relation.getId());
					if (MultipolygonBuilder.isClimbingMultipolygon(relation)) {
						Map<Long, Node> allNodes = ctx.retrieveAllRelationNodes((Relation) e);
						original.createClimbingOuterWay(e, new ArrayList<>(allNodes.values()));
					} else {
						original.createInnerAndOuterWays(relation);
					}
					List<Multipolygon> multipolygons = original.splitPerOuterRing(log);
					centers = new ArrayList<>();
					for (Multipolygon m : multipolygons) {
						assert m.getOuterRings().size() == 1;
						if (!m.areRingsComplete()) {
							log.warn("In multipolygon (POI) " + relation.getId() + " there are incompleted ways");
						}
						Ring out = m.getOuterRings().get(0);
						if (out.getBorder().size() == 0) {
							log.warn("Multipolygon (POI) has an outer ring that can't be formed: " + relation.getId());
							// don't index this
							continue;
						}
                        List<List<Node>> innerWays = new ArrayList<>();
                        for (Ring r : m.getInnerRings()) {
                            innerWays.add(r.getBorder());
                        }
                        LatLon l = OsmMapUtils.getComplexPolyCenter(out.getBorder(), innerWays);
                        centers.add(l);
					}
				} else if (OsmMapUtils.isSuperRoute(tags)) {
					for (RelationMember members : relation.getMembers()) {
						if (members.getEntityId().getType() == EntityType.RELATION) {
							if (memberIds.length() > 0) {
								memberIds += " ";
							}
							memberIds += "O" + members.getEntityId().getId(); // OSM route_id start from symbol "O"
							if (centers.get(0) == null) {
								Relation memberRel = (Relation) members.getEntity();
								ctx.loadEntityRelation(memberRel);
								centers = Collections.singletonList(OsmMapUtils.getCenter(memberRel, true));
							}
						}
					}
				}
			}
			long id = e.getId();
			if (icc.basemap && id < 0) {
				id = GENERATE_OBJ_ID--;
			} else if(e instanceof Relation) {
//				id = GENERATE_OBJ_ID--;
				id = assignIdForMultipolygon((Relation) e);
			} else if(id > 0) {
				boolean isPropagated = e.getId() > PROPAGATED_NODE_BIT;
				if (isPropagated) {
					id = e.getId();
				} else {
					// keep backward compatibility for ids (osm editing)
					id = e.getId() >> (OsmDbCreator.SHIFT_ID - 1);
					if (id % 2 != (e.getId() % 2)) {
						id ^= 1;
					}
				}
			}

			for (Amenity a : tempAmenityList) {
				if (icc.basemap) {
					PoiType st = a.getType().getPoiTypeByKeyName(a.getSubType());
					if (st == null || !a.getType().containsBasemapPoi(st)) {
						continue;
					}
				}
				for (int i = 0; i < centers.size(); i++) {
                    LatLon cen = centers.get(i);
					if (cen != null) {
						a.setLocation(cen);
					}
                    if (a.getLocation() == null) {
                        continue;
                    }
    				EntityParser.parseMapObject(a, e, tags);
    				if (centers.size() > 1) {
    					a.setAdditionalInfo(Amenity.ROUTE_ID, "R" + e.getId());
                        long cenId = id + ((long) i * 2);
						a.setId(cenId);
                        generatedIds.add(cenId);
    				} else {
                        a.setId(id);
                        generatedIds.add(id);
                    }
					if (!memberIds.isEmpty()) {
						a.setAdditionalInfo(Amenity.ROUTE_MEMBERS_IDS, memberIds);
					}

   					try {
   						insertAmenityIntoPoi(a);
   					} catch (Exception excpt) {
   						System.out.println("TODO FIX " + a.getId() + " " + excpt);
   						excpt.printStackTrace();
   					}
                }

			}
		}
	}

	public void iterateRelation(Relation e, OsmDbAccessorContext ctx) throws SQLException {
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
		poiPreparedStatement.setString(6, encodeAdditionalInfo(amenity, amenity.getName()));
		poiPreparedStatement.setInt(7, amenity.getOrder());
		String tagGroups = insertMergedTaggroups(amenity);
		poiPreparedStatement.setString(8, tagGroups);
		int topIndex = 9;
		for (Map.Entry<String, PoiType> entry : poiTypes.topIndexPoiAdditional.entrySet()) {
			String val = amenity.getAdditionalInfo(entry.getKey().replace(MapPoiTypes.TOP_INDEX_ADDITIONAL_PREFIX, ""));
			poiPreparedStatement.setString(topIndex, val);
			topIndex++;
		}
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

	

	private String encodeAdditionalInfo(Amenity amenity, String name) {

		Map<String, String> tempNames = new LinkedHashMap<String, String>();
		if (!Algorithms.isEmpty(name)) {
			tempNames.put("name", name);
		}
		Iterator<Entry<String, String>> it = amenity.getNamesMap(true).entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, String> next = it.next();
			tempNames.put("name:" + next.getKey(), next.getValue());
		}
		for(String e : amenity.getAdditionalInfoKeys()) {
			tempNames.put(e, amenity.getAdditionalInfo(e));
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
			String topIndexKey = MapPoiTypes.TOP_INDEX_ADDITIONAL_PREFIX + e.getKey();
			if (poiTypes.topIndexPoiAdditional.containsKey(topIndexKey)) {
				topIndexKeys.add(topIndexKey);
				rulType = getOrCreate(topIndexKey, e.getValue(), false);
				if (rulType.getValue() != null) {
					if (b.length() > 0) {
						b.append(SPECIAL_CHAR);
					}
					b.append((char) ((rulType.getId()) + 1)).append(e.getValue());
				}
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
			if (topIndexAdditional != null && topIndexAdditional.containsKey(rulType.getTag())) {
				Set<String> collection = topIndexAdditional.get(rulType.getTag());
				if (!collection.contains(rulType.getValue())) {
					if (i == -1) {
						break;
					} else {
						p = i + 1;
						continue;
					}
				}
			}
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
				+ "type varchar(1024), subtype varchar(1024), additionalTags varchar(8096), priority int, taggroups varchar(1024), " + getCreateColumnsTopIndexAdditionals()
				+ "primary key(id, type, subtype))");
		stat.executeUpdate("create table taggroups (id int, tagvalues varchar(8096), primary key(id))");
		stat.executeUpdate("create index poi_loc on poi (x, y, type, subtype)");
		stat.executeUpdate("create index poi_id on poi (id, type, subtype)");
		stat.execute("PRAGMA user_version = " + IndexConstants.POI_TABLE_VERSION); //$NON-NLS-1$
		stat.close();

		// create prepared statment
		poiPreparedStatement = poiConnection.prepareStatement("INSERT INTO " + IndexConstants.POI_TABLE
				+ "(id, x, y, type, subtype, additionalTags, priority, taggroups" + getInsertColumnsTopIndexAdditionals() + ") " + //$NON-NLS-2$ //$NON-NLS-2$
				"VALUES (?, ?, ?, ?, ?, ?, ?, ?" + getInsertValuesTopIndexAdditionals() + ")");
		tagGroupsPreparedStatement = poiConnection.prepareStatement("INSERT INTO taggroups (id, tagvalues) VALUES (?, ?)");
		pStatements.put(poiPreparedStatement, 0);
		pStatements.put(tagGroupsPreparedStatement, 0);

		poiConnection.setAutoCommit(false);
	}

	public void excludeFromMainIteration(long id) {
		excludedRelations.add(id);
	}


	private class IntBbox {
		int minX = Integer.MAX_VALUE;
		int maxX = 0;
		int minY = Integer.MAX_VALUE;
		int maxY = 0;
	}

	public static class PoiCreatorTagGroup {
		int id;
		List<String> tagValues;

		PoiCreatorTagGroup(int id, List<String> tagValues) {
			this.id = id;
			this.tagValues = tagValues;
		}
	}

	public static class PoiCreatorTagGroups {
		HashSet<Integer> ids;
		HashSet<PoiCreatorTagGroup> tagGroups;

		public void addTagGroup(List<PoiCreatorTagGroup> poiCreatorTagGroups) {
			for (PoiCreatorTagGroup poiCreatorTagGroup : poiCreatorTagGroups) {
				if (ids == null) {
					ids = new HashSet<>();
					tagGroups = new HashSet<>();
				}
				if (!ids.contains(poiCreatorTagGroup.id)) {
					ids.add(poiCreatorTagGroup.id);
					tagGroups.add(poiCreatorTagGroup);
				}
			}
		}
	}

	public static class PoiCreatorCategories {
		Map<String, Set<String>> categories = new LinkedHashMap<String, Set<String>>();
		Set<PoiAdditionalType> additionalAttributes = new LinkedHashSet<PoiAdditionalType>();


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

	public void writeBinaryPoiIndex(File poiGeocoding, BinaryMapIndexWriter writer, String regionName, 
			IProgress progress) throws SQLException, IOException {
		if (poiPreparedStatement != null) {
			closePreparedStatements(poiPreparedStatement);
		}
		if (tagGroupsPreparedStatement != null) {
			closePreparedStatements(tagGroupsPreparedStatement);
		}
		poiConnection.commit();

		Map<String, Set<PoiTileBox>> namesIndex = new TreeMap<String, Set<PoiTileBox>>();

		int zoomToStart = ZOOM_TO_SAVE_START;
		IntBbox bbox = new IntBbox();
		Tree<PoiTileBox> rootZoomsTree = new Tree<PoiTileBox>();
		collectTopIndexMap();
		collectTagGroups();
		// 0. process all entities
		processPOIIntoTree(poiGeocoding, namesIndex, zoomToStart, bbox, rootZoomsTree);

		// 1. write header
		long startFpPoiIndex = writer.startWritePoiIndex(regionName, bbox.minX, bbox.maxX, bbox.maxY, bbox.minY);

		// 2. write categories table
		PoiCreatorCategories globalCategories = rootZoomsTree.node.categories;
		writer.writePoiCategoriesTable(globalCategories);
		writer.writePoiSubtypesTable(globalCategories, topIndexAdditional);

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
				.prepareStatement("SELECT id, x, y, type, subtype, additionalTags, taggroups from poi "
						+ "where x >= ? AND x < ? AND y >= ? AND y < ?"
						+ " order by priority ");
		for (Map.Entry<PoiTileBox, List<BinaryFileReference>> entry : fpToWriteSeeks.entrySet()) {
			int z = entry.getKey().zoom;
			int x = entry.getKey().x;
			int y = entry.getKey().y;
			writer.startWritePoiData(z, x, y, entry.getValue());

			if (useInMemoryCreator) {
				List<PoiData> poiData = entry.getKey().poiData;
				Collections.sort(poiData, new Comparator<PoiData>() {

					@Override
					public int compare(PoiData o1, PoiData o2) {
						return -Integer.compare(o1.getRating(), o2.getRating());
					}
				});

				for (PoiData poi : poiData) {
					int x31 = poi.x;
					int y31 = poi.y;
					String type = poi.type;
					String subtype = poi.subtype;
					int x24shift = (x31 >> 7) - (x << (24 - z));
					int y24shift = (y31 >> 7) - (y << (24 - z));
					int precisionXY = MapUtils.calculateFromBaseZoomPrecisionXY(24, 27, (x31 >> 4), (y31 >> 4));
					if (poi.id > PROPAGATED_NODE_BIT) {
						continue;
					}
					writer.writePoiDataAtom(poi.id, x24shift, y24shift, type, subtype, poi.additionalTags,
							globalCategories, settings.poiZipLongStrings ? settings.poiZipStringLimit : -1, precisionXY, poi.tagGroups);
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
					int precisionXY = MapUtils.calculateFromBaseZoomPrecisionXY(24, 27, (x31 >> 4), (y31 >> 4));
					String type = rset.getString(4);
					String subtype = rset.getString(5);
					List<Integer> tagGroupIds = poiTagGroups.get(id);
					if (Algorithms.isEmpty(tagGroupIds)) {
						tagGroupIds = parseTaggroups(rset.getString(6));
					}
					if (id > PROPAGATED_NODE_BIT) {
						continue;
					}
					writer.writePoiDataAtom(id, x24shift, y24shift, type, subtype,
							decodeAdditionalInfo(rset.getString(6), mp), globalCategories,
							settings.poiZipLongStrings ? settings.poiZipStringLimit : -1, precisionXY, tagGroupIds);
				}
				rset.close();
			}
			writer.endWritePoiData();
		}

		prepareStatement.close();

		writer.endWritePoiIndex();

	}

	private void collectTopIndexMap() throws SQLException, IOException {
		if (topIndexAdditional != null) {
			return;
		}
		Map<String, Set<String>> providedTopIndexes = null;
		if (settings.poiTopIndexUrl != null || !Algorithms.isEmpty(System.getenv(ENV_POI_TOP_INDEXES_URL))) {
			String url = settings.poiTopIndexUrl != null ? settings.poiTopIndexUrl : System.getenv(ENV_POI_TOP_INDEXES_URL);
			log.info("Using global list of poi additionals - " + url);
			providedTopIndexes = new LinkedHashMap<String, Set<String>>();
			InputStream is = new URL(url).openStream();
			StringBuilder sb = Algorithms.readFromInputStream(is);
			for (String s : sb.toString().split("\n")) {
				String tag = s.substring(0, s.indexOf(','));
				String value = s.substring(s.indexOf(',') + 1);
				Set<String> set = providedTopIndexes.get(tag);
				if (set == null) {
					set = new TreeSet<String>();
					providedTopIndexes.put(tag, set);
				}
				set.add(value);
			}
		} else {
			log.info("Not using global list of poi indexes");
		}
		topIndexAdditional = new LinkedHashMap<>();
		ResultSet rs;
		boolean isBrand = false;
		for (Map.Entry<String, PoiType> entry : poiTypes.topIndexPoiAdditional.entrySet()) {
			if (!topIndexKeys.contains(entry.getKey())) {
				continue;
			}
            String column = entry.getKey();
            if (column.contains(":")) {
                // with lang
                continue;
            }
			if (entry.getKey().equals(MapPoiTypes.TOP_INDEX_ADDITIONAL_PREFIX + "brand")) {
				isBrand = true;
			}
			int minCount = entry.getValue().getMinCount();
			int maxPerMap = entry.getValue().getMaxPerMap();
			minCount = minCount > 0 ? minCount : DEFAULT_TOP_INDEX_MIN_COUNT;
			maxPerMap = maxPerMap > 0 ? maxPerMap : DEFAULT_TOP_INDEX_MAX_PER_MAP;
			if (providedTopIndexes != null) {
				minCount = 0;
				maxPerMap = DEFAULT_TOP_INDEX_LIMIT_PER_MAP;
			}

            rs = poiConnection.createStatement().executeQuery("select count(*) as cnt, \"" + column + "\", *" +
                    " from poi where \"" + column + "\" is not NULL group by \"" + column+ "\" having cnt > " + minCount +
                    " order by cnt desc");
            Set<String> set = new HashSet<>();
			while (rs.next()) {
				String originalValue = rs.getString(2);
				if (Algorithms.isEmpty(originalValue)) {
					continue;
				}
				if (providedTopIndexes != null) {
                    String normalizedValue = TopTagValuesAnalyzer.normalizeTagValue(originalValue);
					String key = entry.getKey().substring(MapPoiTypes.TOP_INDEX_ADDITIONAL_PREFIX.length());
					if (providedTopIndexes.containsKey(key) && providedTopIndexes.get(key).contains(normalizedValue)
							&& maxPerMap-- > 0) {
						set.add(originalValue);
                        addTopIndexWithLang(rs, column, originalValue);
					}
				} else {
					if (maxPerMap-- > 0) {
						set.add(originalValue);
                        addTopIndexWithLang(rs, column, originalValue);
					} else if (isBrand && WORLD_BRANDS.contains(originalValue)) {
						// add world brands anyway
						set.add(originalValue);
                        addTopIndexWithLang(rs, column, originalValue);
					}
				}
			}
			topIndexAdditional.put(column, set);
		}
		return;
	}

    private void addTopIndexWithLang(ResultSet rs, String topIndexKey, String topIndexVal) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        List<String> savedValues = new ArrayList<>();
        for (int i = 3; i < columnCount; i++) {
            String value = rs.getString(i);
            if (Algorithms.isEmpty(value)) {
                continue;
            }
            String columnName = metaData.getColumnName(i);
            if (!columnName.equals(topIndexKey) && columnName.startsWith(topIndexKey)) {
                if (!value.equals(topIndexVal) && !savedValues.contains(value)) {
                    topIndexAdditional
                            .computeIfAbsent(columnName, k -> new HashSet<>())
                            .add(value);
                    savedValues.add(value);
                }
            }
        }
    }

	private PoiAdditionalType retrieveAdditionalType(String key) {
		for (PoiAdditionalType t : additionalTypesId) {
			if (Algorithms.objectEquals(t.getTag(), key)) {
				return t;
			}
		}
		return null;
	}

	private void processPOIIntoTree(File poiGeocoding, Map<String, Set<PoiTileBox>> namesIndex, int zoomToStart, IntBbox bbox,
			Tree<PoiTileBox> rootZoomsTree) throws SQLException, IOException {
		ResultSet rs = poiConnection.createStatement().executeQuery("SELECT x,y,type,subtype,id,additionalTags,taggroups from poi ORDER BY id, priority");
		rootZoomsTree.setNode(new PoiTileBox());
		long geocodingTime = 0, geocodingCnt = 0, geocodingSuccess = 0, geoCitySuccess = 0, geoCityCnt = 0, geoCityTime = 0;
		RoutingContext geocodingCtx = null;
		GeocodingUtilities geocodingUtilities = new GeocodingUtilities();
		BinaryMapIndexReader geoReader = null;
		if (poiGeocoding != null && settings.poiGeocodingEnable) {
			geoReader = new BinaryMapIndexReader(new RandomAccessFile(poiGeocoding, "r"),
					poiGeocoding, false);
			geoReader.init(false);
			if (geoReader.containsAddressData() && geoReader.containsRouteData()) {
				geocodingCtx = GeocodingUtilities.buildDefaultContextForPOI(geoReader);
			}
		}
		if (geocodingCtx == null) {
			log.info("Geocoding for POI is disabled");
		} else {
			log.info("Geocoding for POI is enabled");
		}

		int count = 0;
		ConsoleProgressImplementation console = new ConsoleProgressImplementation();
		console.startWork(1000000);
		Map<PoiAdditionalType, String> additionalTags = new LinkedHashMap<PoiAdditionalType, String>();
		PoiAdditionalType nameRuleType = retrieveAdditionalType("name");
		PoiAdditionalType nameEnRuleType = retrieveAdditionalType("name:en");
		PoiAdditionalType streetRuleType = retrieveAdditionalType(Amenity.ADDR_STREET);
		PoiAdditionalType hnoRuleType = retrieveAdditionalType(Amenity.ADDR_HOUSENUMBER);
		while (rs.next()) {
			int x = rs.getInt(1);
			int y = rs.getInt(2);
			LatLon latLon = new LatLon(MapUtils.get31LatitudeY(y), MapUtils.get31LongitudeX(x));
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
			if (geocodingCtx != null && 
					Algorithms.isEmpty(additionalTags.get(streetRuleType)) && 
					!Algorithms.isEmpty(additionalTags.get(nameRuleType))) {
				long tm = System.currentTimeMillis();
				List<GeocodingResult> res = geocodingUtilities.reverseGeocodingSearch(geocodingCtx,
						latLon.getLatitude(), latLon.getLongitude(), false);
				if (settings.poiGeocodingPrecise) {
					res = geocodingUtilities.sortGeocodingResults(Collections.singletonList(geoReader), res);
				}
				geocodingCnt++;
				if (res.size() > 0 && res.get(0).getDistance() < GEOCODING_DISTANCE) {
					GeocodingResult geoRes = res.get(0);
					if (geoRes.streetName != null) {
						additionalTags.put(streetRuleType, geoRes.streetName);
						if (geoRes.getBuildingString() != null) {
							additionalTags.put(hnoRuleType, geoRes.getBuildingString());
						}
						geocodingSuccess++;
					}
				}
				geocodingTime += (System.currentTimeMillis() - tm);
			}

			List<Integer> tagGroupIds = parseTaggroups(rs.getString(7));
			List<PoiCreatorTagGroup> tagGroups = new ArrayList<>();
			if (cityQuadTree != null) {
				geoCityCnt++;
				long tm = System.currentTimeMillis();
				List<Multipolygon> result = new ArrayList<>();
				cityQuadTree.queryInBox(new QuadRect(x, y, x, y), result);
				boolean ok = false;
				if (result.size() > 0) {
					for (Multipolygon multipolygon : result) {
						if (multipolygon.containsPoint(latLon)) {
							if (!cityTagsGroup.containsKey(multipolygon)) {
								log.error("Multipolygon for POI is not found!!! " + type + " " + subtype + " " + latLon.toString());
							} else {
								ok = true;
								List<PoiCreatorTagGroup> list = cityTagsGroup.get(multipolygon);
								tagGroups.addAll(list);
							}
						}
					}
				}
				if (ok) {
					geoCitySuccess++;
				}
				geoCityTime += (System.currentTimeMillis() - tm);
			} else if (tagGroupsFromDB != null && tagGroupIds.size() > 0) {
				for (int id : tagGroupIds) {
					if (tagGroupsFromDB.containsKey(id)) {
						tagGroups.add(tagGroupsFromDB.get(id));
					}
				}
			}

			Tree<PoiTileBox> prevTree = rootZoomsTree;
			rootZoomsTree.getNode().categories.addCategory(type, subtype, additionalTags);
			if (tagGroups.size() > 0) {
				rootZoomsTree.getNode().tagGroups.addTagGroup(tagGroups);
			}
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
				subtree.getNode().tagGroups.addTagGroup(tagGroups);

				prevTree = subtree;
			}
			Set<String> otherNames = null;
			Set<String> idNames = null;
			Iterator<Entry<PoiAdditionalType, String>> it = additionalTags.entrySet().iterator();
			while (it.hasNext()) {
				Entry<PoiAdditionalType, String> e = it.next();
				String tag = e.getKey().getTag();
				if ((ObfConstants.isTagIndexedForSearchAsName(tag))
						&& !"name:en".equals(tag)) {
					if (otherNames == null) {
						otherNames = new TreeSet<String>();
					}
					otherNames.add(e.getValue());
				}
				if (settings.charsToBuildPoiIdNameIndex > 0 && ObfConstants.isTagIndexedForSearchAsId(tag)) {
					if (idNames == null) {
						idNames = new TreeSet<String>();
					}
					idNames.add(e.getValue());
				}
				if (settings.charsToBuildPoiIdNameIndex > 0 && tag.equals(Amenity.ROUTE_MEMBERS_IDS)) {
					if (idNames == null) {
						idNames = new TreeSet<String>();
					}
					for(String id : e.getValue().split(" ")) {
						idNames.add(id);
					}
				}
			}
			addNamePrefix(additionalTags.get(nameRuleType), additionalTags.get(nameEnRuleType), prevTree.getNode(),
					namesIndex, otherNames, idNames);

			if (tagGroupIds.size() == 0) {
				for (PoiCreatorTagGroup p : tagGroups) {
					tagGroupIds.add(p.id);
				}
			}
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
				poiData.tagGroups.addAll(tagGroupIds);
				prevTree.getNode().poiData.add(poiData);

			} else {
				poiTagGroups.put(rs.getLong(5), tagGroupIds);
			}
		}
		
		log.info(String.format("POI geocoding full address (%d of %d for %.2f sec), city (%d of %d for %.2f sec)",
				geocodingSuccess, geocodingCnt, geocodingTime / 1e3, geoCitySuccess, geoCityCnt, geoCityTime / 1e3));
		log.info("Poi processing finished");
	}

	private void addNamePrefix(String name, String nameEn, PoiTileBox data, Map<String, Set<PoiTileBox>> poiData,
			Set<String> names, Set<String> idNames) {
		if (name != null) {
			parsePrefix(name, data, poiData, settings.charsToBuildPoiNameIndex);
			if (Algorithms.isEmpty(nameEn)) {
				nameEn = Junidecode.unidecode(name);
			}

		}
		if (!Algorithms.objectEquals(nameEn, name) && !Algorithms.isEmpty(nameEn)) {
			parsePrefix(nameEn, data, poiData, settings.charsToBuildPoiNameIndex);
		}
		if (names != null) {
			for (String nk : names) {
				if (!Algorithms.objectEquals(nk, name) && !Algorithms.isEmpty(nk)) {
					parsePrefix(nk, data, poiData, settings.charsToBuildPoiNameIndex);
				}
			}
		}
		if (idNames != null) {
			for (String nk : idNames) {
				if (!Algorithms.isEmpty(nk)) {
					parsePrefix(nk, data, poiData, settings.charsToBuildPoiIdNameIndex);
				}
			}
		}
	}

    private void parsePrefix(String name, PoiTileBox data, Map<String, Set<PoiTileBox>> poiData, int ind) {
        name = Algorithms.normalizeSearchText(name);
        Set<String> splitName = new HashSet<>(Algorithms.splitByWordsLowercase(name));
        if (ArabicNormalizer.isSpecialArabic(name)) {
            String arabic = ArabicNormalizer.normalize(name);
            if (arabic != null && !arabic.equals(name)) {
                splitName.addAll(Algorithms.splitByWordsLowercase(arabic));
            }
        }
        for (String str : splitName) {
            if (str.length() > ind) {
                str = str.substring(0, ind);
            }
            if (!poiData.containsKey(str)) {
                poiData.put(str, new LinkedHashSet<>());
            }
            poiData.get(str).add(data);
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

		PoiCreatorTagGroups tagGroups = tree.getNode().tagGroups;
		writer.writePoiTagGroups(tagGroups);

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
		List<Integer> tagGroups = new ArrayList<>();

		public int getRating() {
			int rt = 0;
			for (PoiAdditionalType t : additionalTags.keySet()) {
				if (t.getTag().equals("travel_elo")) {
					try {
						rt = Integer.parseInt(additionalTags.get(t));
					} catch (NumberFormatException e) {
						e.printStackTrace();
					}
					break;
				}
			}
			if (rt > 0) {
				return rt;
			}
			return additionalTags.size();
		}
	}

	public static class PoiTileBox {
		int x;
		int y;
		int zoom;
		PoiCreatorCategories categories = new PoiCreatorCategories();
		List<PoiData> poiData = null;
		PoiCreatorTagGroups tagGroups = new PoiCreatorTagGroups();

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

	private String getCreateColumnsTopIndexAdditionals() {
		if (poiTypes != null && poiTypes.topIndexPoiAdditional.size() > 0) {
			String sql = "";
			for (Map.Entry<String, PoiType> entry : poiTypes.topIndexPoiAdditional.entrySet()) {
				sql += "\"" + entry.getKey() + "\" varchar(2048), ";
			}
			return sql;
		}
		return "";
	}

	private String getInsertColumnsTopIndexAdditionals() {
		if (poiTypes != null && poiTypes.topIndexPoiAdditional.size() > 0) {
			String sql = "";
			for (Map.Entry<String, PoiType> entry : poiTypes.topIndexPoiAdditional.entrySet()) {
				sql += " ,\"" + entry.getKey() + "\"";
			}
			return sql;
		}
		return "";
	}

	@SuppressWarnings("unused")
	private String getInsertValuesTopIndexAdditionals() {
		if (poiTypes != null && poiTypes.topIndexPoiAdditional.size() > 0) {
			String sql = "";
			for (Map.Entry<String, PoiType> entry : poiTypes.topIndexPoiAdditional.entrySet()) {
				sql += ", ?";
			}
			return sql;
		}
		return "";
	}

	private String insertMergedTaggroups(Amenity amenity) throws SQLException {
		Map<Integer, List<BinaryMapIndexReader.TagValuePair>> tg = amenity.getTagGroups();
		if (tg == null || tg.isEmpty()) {
			return "";
		}
		String result = "";
		String region = amenity.getRegionName();
		Map<Integer, Integer> replaceIDMap = tagGroupIdsByRegion.computeIfAbsent(region, s -> new HashMap<>());
		for (Map.Entry<Integer, List<BinaryMapIndexReader.TagValuePair>> entry : tg.entrySet()) {
			int id = entry.getKey();
			int mergedId;
			if (!replaceIDMap.containsKey(id)) {
				mergedId = maxTagGroupId + 1;
				String allPairs = "";
				for (BinaryMapIndexReader.TagValuePair p : entry.getValue()) {
					if (!allPairs.isEmpty()) {
						allPairs += ",";
					}
					allPairs += p.tag + "," + p.value;
				}
				tagGroupsPreparedStatement.setInt(1, mergedId);
				tagGroupsPreparedStatement.setString(2, allPairs);
				addBatch(tagGroupsPreparedStatement);
				replaceIDMap.put(id, mergedId);
				maxTagGroupId = mergedId;
			} else {
				mergedId = replaceIDMap.get(id);
			}
			if (!result.isEmpty()) {
				result += ",";
			}
			result += String.valueOf(mergedId);
		}
		return result;
	}

	private void collectTagGroups() throws SQLException {
		ResultSet rs;
		rs = poiConnection.createStatement().executeQuery("select id, tagvalues from taggroups");
		while (rs.next()) {
			if (tagGroupsFromDB == null) {
				tagGroupsFromDB = new HashMap<>();
			}
			int id = rs.getInt(1);
			String tg = rs.getString(2);
			String[] strArray = tg.split(",");
			List<String> tagValues = Arrays.asList(strArray);
			PoiCreatorTagGroup poiCreatorTagGroup = new PoiCreatorTagGroup(id, tagValues);
			tagGroupsFromDB.put(id, poiCreatorTagGroup);
		}
	}


	private List<Integer> parseTaggroups(String taggroupsColumn) {
		List<Integer> tagGroupIds = new ArrayList<>();

		String[] strArray = taggroupsColumn.split(",");
		for(int i = 0; i < strArray.length; i++) {
			if (Algorithms.isEmpty(strArray[i])) {
				continue;
			}
			Integer tg = Integer.parseInt(strArray[i]);
			tagGroupIds.add(tg);
		}
		return tagGroupIds;
	}

    public void resetOrder(Amenity amenity) {
        PoiCategory pc = amenity.getType();
        String subtype = amenity.getSubType();
        PoiType pt = pc.getPoiTypeByKeyName(subtype);
        if (pt != null) {
            amenity.setOrder(pt.getOrder());
        }
    }

    private boolean isOutOfRegionBbox(Entity e, IndexCreationContext icc) {
        if (!(e instanceof Relation) && !icc.isInsideRegionBBox(e)) {
            Amenity a = tempAmenityList.get(0);
            String type = a.getType().getKeyName();
            String subtype = a.getSubType();
            String name = a.getName();
            LatLon l = e.getLatLon();
            if (e instanceof Way way && !way.getNodes().isEmpty()) {
                Node node = way.getFirstNode();
                l = node.getLatLon();
            }
            log.warn(String.format("POI out-of-bbox: %s %.4f %.4f %s %s %s",
		            e.getOsmUrl(), l.getLatitude(), l.getLongitude(), type, subtype, name));
            return true;
        }
        return false;
    }

}
