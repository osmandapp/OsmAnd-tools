package net.osmand.osm;

import gnu.trove.list.array.TIntArrayList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
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

import net.osmand.PlatformUtil;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.OSMSettings.OSMTagKey;
import net.osmand.osm.edit.Relation;
import net.osmand.util.Algorithms;

import org.apache.commons.logging.Log;
import org.xmlpull.v1.XmlPullParser;

public class MapRenderingTypesEncoder extends MapRenderingTypes {

	private static Log log = PlatformUtil.getLog(MapRenderingTypesEncoder.class);
	// stored information to convert from osm tags to int type
	private List<MapRouteTag> routeTags = new ArrayList<MapRouteTag>();
	private Map<String, List<EntityConvert>> convertTags = new HashMap<String, List<EntityConvert>>();
	private MapRulType coastlineRuleType;
	private String regionName;
	public static final String OSMAND_REGION_NAME_TAG = "osmand_region_name";


	public MapRenderingTypesEncoder(String fileName, String regionName) {
		super(fileName != null && fileName.length() == 0 ? null : fileName);
		this.regionName = "$" + regionName.toLowerCase() + "^";
	}

	public MapRenderingTypesEncoder(String regionName) {
		super(null);
		this.regionName = "$" + regionName.toLowerCase() + "^";
	}



	@Override
	protected MapRulType registerRuleType(MapRulType rt){
		rt = super.registerRuleType(rt);
		String tag = rt.tagValuePattern.tag;
		String val = rt.tagValuePattern.value;
		if("natural".equals(tag) && "coastline".equals(val)) {
			coastlineRuleType = rt;
		}
		return rt;
	}

	private MapRulType getRelationalTagValue(String tag, String val) {
		MapRulType rType = getRuleType(tag, val);
		if(rType != null && rType.relation) {
			return rType;
		}
		return null;
	}

	public Map<MapRulType, Map<MapRulType, String>> getRelationPropogatedTags(Relation relation,  EntityConvertApplyType at) {
		Map<MapRulType, Map<MapRulType, String>> propogated = new LinkedHashMap<MapRulType, Map<MapRulType, String>>();
		Map<String, String> ts = relation.getTags();
		ts = transformTags(ts, EntityType.RELATION, at);
		ts = processExtraTags(ts);
		Iterator<Entry<String, String>> its = ts.entrySet().iterator();
		while(its.hasNext()) {
			Entry<String, String> ev = its.next();
			MapRulType rule = getRelationalTagValue(ev.getKey(), ev.getValue());
			if(rule != null) {
				String value = ev.getValue();
				LinkedHashMap<MapRulType, String> pr = new LinkedHashMap<MapRulType, String>();

				addRuleToPropogated(pr, ts, rule, value);
				if(pr.size() > 0) {
					propogated.put(rule, pr);
				}
			}
		}
		return propogated;
	}



	protected void addRuleToPropogated(Map<MapRulType, String> propogated, Map<String, String> ts, MapRulType rule,
			String value) {
		if (rule.names != null) {
			for (int i = 0; i < rule.names.length; i++) {
				String tag = rule.names[i].tagValuePattern.tag.substring(rule.namePrefix.length());
				if(ts.containsKey(tag)) {
					propogated.put(rule.names[i], ts.get(tag));
//				} else if(rule.relationGroup) {
//					propogated.put(rule.names[i], "");
				}
			}
		}
		propogated.put(rule, value);
	}


	private MapRulType getRuleType(String tag, String val) {
		return getRuleType(tag, val, false, false);
	}

	private MapRulType getMapRuleType(String tag, String val) {
		return getRuleType(tag, val, false, true);
	}

	public MapRulType getCoastlineRuleType() {
		getEncodingRuleTypes();
		return coastlineRuleType;
	}

	public List<MapRouteTag> getRouteTags() {
		checkIfInitNeeded();
		return routeTags;
	}


	@Override
	protected void parseEntityConvertXML(XmlPullParser parser) {

		String seq = parser.getAttributeValue("", "seq");
		if(Algorithms.isEmpty(seq)) {
			seq = "1:1";
		}
		String[] ls = seq.split(":");
		for (int ind = Integer.parseInt(ls[0]); ind <= Integer.parseInt(ls[1]); ind++) {
			Map<String, String> mp = new HashMap<String, String>();
			for (int i = 0; i < parser.getAttributeCount(); i++) {
				String at = parser.getAttributeName(i);
				mp.put(at, parser.getAttributeValue("", at
						).replace("*", ind+""));
			}
			EntityConvert ec = new EntityConvert();
			String tg = mp.get("if_region_name"); //$NON-NLS-1$
			if (tg != null) {
				ec.ifRegionName.addAll(Arrays.asList(tg.split("\\,")));
			}
			tg = mp.get("if_not_region_name"); //$NON-NLS-1$
			if (tg != null) {
				ec.ifNotRegionName.addAll(Arrays.asList(tg.split("\\,")));
			}
			ec.verbose = "true".equals(mp.get("verbose")); //$NON-NLS-1$
			parseConvertCol(mp, ec.ifTags, "if_");
			parseConvertCol(mp, ec.ifStartsTags, "if_starts_with_");
			parseConvertCol(mp, ec.ifNotStartsTags, "if_not_starts_with_");
			parseConvertCol(mp, ec.ifEndsTags, "if_ends_with_");
			parseConvertCol(mp, ec.ifNotEndsTags, "if_not_ends_with_");
			parseConvertCol(mp, ec.ifContainsTags, "if_contains_");
			parseConvertCol(mp, ec.ifNotContainsTags, "if_not_contains_");
			parseConvertCol(mp, ec.ifNotTags, "if_not_");
			parseConvertCol(mp, ec.ifTagsNotLess, "if_not_less_");
			parseConvertCol(mp, ec.ifTagsLess, "if_less_");
			ec.type = EntityConvertType.valueOf(mp.get("pattern").toUpperCase()); //$NON-NLS-1$
			ec.applyToType = EnumSet.allOf(EntityConvertApplyType.class);
			if ("no".equals(mp.get("routing"))
					|| "false".equals(mp.get("routing"))) {
				ec.applyToType.remove(EntityConvertApplyType.ROUTING);
			}
			if ("no".equals(mp.get("map")) || "false".equals(mp.get("map"))) {
				ec.applyToType.remove(EntityConvertApplyType.MAP);
			}
			if ("no".equals(mp.get("poi")) || "false".equals(mp.get("poi"))) {
				ec.applyToType.remove(EntityConvertApplyType.POI);
			}
			parseConvertCol(mp, ec.toTags, "to_");
			tg = mp.get("from_tag"); //$NON-NLS-1$
			String value = mp.get("from_value"); //$NON-NLS-1$
			if (tg != null) {
				ec.fromTag = new TagValuePattern(tg, "".equals(value) ? null : value);
				if (!convertTags.containsKey(ec.fromTag.tag)) {
					convertTags.put(ec.fromTag.tag, new ArrayList<MapRenderingTypesEncoder.EntityConvert>());
				}
				convertTags.get(ec.fromTag.tag).add(ec);
			}
			String appTo = mp.get("apply_to"); //$NON-NLS-1$
			if (appTo != null) {
				ec.applyTo = EnumSet.noneOf(EntityType.class);
				String[] tps = appTo.split(",");
				for (String t : tps) {
					EntityType et = EntityType.valueOf(t.toUpperCase());
					ec.applyTo.add(et);
				}
			}
		}
	}



	protected void parseConvertCol(Map<String, String> mp, List<TagValuePattern> col, String prefix) {
		for (int i = 1; i <= 15; i++) {
			String tg = mp.get(prefix +"tag" + i); //$NON-NLS-1$
			String value = mp.get(prefix +"value" + i); //$NON-NLS-1$
			if (tg != null) {
				TagValuePattern pt = new TagValuePattern(tg, "".equals(value) ? null : value);
				col.add(pt);
				String substr = mp.get(prefix +"substr" + i); //$NON-NLS-1$
				if(substr != null) {
					String[] ls = substr.split(":");
					if(ls.length > 0) {
						pt.substrSt = Integer.parseInt(ls[0]);
					}
					if(ls.length > 1) {
						pt.substrEnd = Integer.parseInt(ls[0]);
					}
				}
			}
		}
	}

	@Override
	protected void parseRouteTagFromXML(XmlPullParser parser) {
		MapRouteTag rtype = new MapRouteTag();
		String mode = parser.getAttributeValue("", "mode"); //$NON-NLS-1$
		rtype.tag = lc(parser.getAttributeValue("", "tag")); //$NON-NLS-1$
		rtype.value = lc(parser.getAttributeValue("", "value")); //$NON-NLS-1$
		rtype.tag2 = lc(parser.getAttributeValue("", "tag2")); //$NON-NLS-1$
		rtype.value2 = lc(parser.getAttributeValue("", "value2")); //$NON-NLS-1$
		rtype.base = Boolean.parseBoolean(parser.getAttributeValue("", "base"));
		rtype.replace = "replace".equalsIgnoreCase(mode);
		rtype.register = "register".equalsIgnoreCase(mode);
		rtype.type = lc(parser.getAttributeValue("", "type"));
		rtype.amend = "amend".equalsIgnoreCase(mode);
		rtype.text = "text".equalsIgnoreCase(mode);
		rtype.relation = Boolean.parseBoolean(parser.getAttributeValue("", "relation"));
		routeTags.add(rtype);
	}



	private String lc(String a) {
		if(a != null) {
			return a.toLowerCase();
		}
		return a;
	}



	@Override
	protected void parseAndRegisterTypeFromXML(XmlPullParser parser, MapRulType parent) {
		String tag = lc(parser.getAttributeValue("", "tag"));
		MapRulType rtype = parseBaseRuleType(parser, parent, tag);
		registerOnlyMap(parser, rtype);
		if("true".equals(parser.getAttributeValue("", "lang"))) {
			for (String lng : langs) {
				tag = lc(parser.getAttributeValue("", "tag")) + ":" + lng;
				if(!types.containsKey(constructRuleKey(tag, null))){
					MapRulType retype = parseBaseRuleType(parser, parent, tag);
					registerOnlyMap(parser, retype);
				}
			}
		}
	}



	private void registerOnlyMap(XmlPullParser parser, MapRulType rtype) {
		String val = parser.getAttributeValue("", "minzoom"); //$NON-NLS-1$
		if (rtype.isMain()) {
			rtype.minzoom = 15;
		}
		if (val != null) {
			rtype.minzoom = Integer.parseInt(val);
		}
		val = parser.getAttributeValue("", "maxzoom"); //$NON-NLS-1$
		rtype.maxzoom = 31;
		if (val != null) {
			rtype.maxzoom = Integer.parseInt(val);
		}
		registerRuleType(rtype);
	}

	public boolean encodeEntityWithType(Entity e, int zoom, TIntArrayList outTypes,
			TIntArrayList outAddTypes, TreeMap<MapRulType, String> namesToEncode, List<MapRulType> tempListNotUsed) {
		return encodeEntityWithType(e instanceof Node,
				e.getModifiableTags(), zoom, outTypes, outAddTypes, namesToEncode, tempListNotUsed);
	}

	public boolean encodeEntityWithType(boolean node, Map<String, String> tags, int zoom, TIntArrayList outTypes,
			TIntArrayList outAddTypes, TreeMap<MapRulType, String> namesToEncode, List<MapRulType> tempListNotUsed) {
		outTypes.clear();
		outAddTypes.clear();
		namesToEncode.clear();
		tags = transformTags(tags, node ? EntityType.NODE : EntityType.WAY, EntityConvertApplyType.MAP);
		boolean area = "yes".equals(tags.get("area"));
		tags = processExtraTags(tags);


		for (String tag : tags.keySet()) {
			String val = tags.get(tag);
			if(tag.equals("seamark:notice:orientation")){
				val = simplifyValueTo45(val);
			}
			MapRulType rType = getMapRuleType(tag, val);
			if (rType != null) {
				if (rType.minzoom > zoom || rType.maxzoom < zoom) {
					continue;
				}
				if (rType.onlyPoint && !node) {
					continue;
				}
				if(rType == nameEnRuleType && Algorithms.objectEquals(val, tags.get(OSMTagKey.NAME.getValue()))) {
					continue;
				}
				rType.updateFreq();
				if (rType.isMain()) {
					outTypes.add(combineOrderAndId(rType));
				}
				if (rType.isAdditionalOrText() && rType.isMap()) {
					if (rType.isAdditional()) {
						outAddTypes.add(combineOrderAndId(rType));
					} else if (rType.isText()) {
						namesToEncode.put(rType, val);
					}
				}
			}
		}
        // sort to get most important features as first type (important for rendering)
        sortAndUpdateTypes(outTypes);
        sortAndUpdateTypes(outAddTypes);
		return area;
	}


	public Map<String, String> transformTags(Map<String, String> tags, EntityType entity,
			EntityConvertApplyType appType) {
		tags = transformShieldTags(tags, entity, appType);
		tags = transformIntegrityTags(tags, entity, appType);
		EntityConvertType filter = EntityConvertType.TAG_TRANSFORM;
		List<EntityConvert> listToConvert = getApplicableConverts(tags, entity, filter, appType);
		if(listToConvert == null) {
			return tags;
		}
		Map<String, String> rtags = new LinkedHashMap<String, String>(tags);
		for(EntityConvert ec : listToConvert){
			applyTagTransforms(rtags, ec, entity, tags);
		}
		return rtags;
	}


	private Map<String, String> transformIntegrityTags(Map<String, String> tags, EntityType entity,
			EntityConvertApplyType appType) {
		if(tags.containsKey("highway") && entity == EntityType.WAY) {
			tags = new LinkedHashMap<>(tags);
			int integrity = calculateIntegrity(tags);
			int max_integrity = 30;
			int normalised_integrity = (integrity * 10) / max_integrity;
			if(integrity < 100) {
				tags.put("osmand_highway_integrity", normalised_integrity +"");
			}
			if(normalised_integrity > 4 && normalised_integrity <= 10) {
				tags.put("osmand_highway_integrity_low", "yes");
			}
		}
		return tags;
	}

	private Map<String, String> transformShieldTags(Map<String, String> tags, EntityType entity,
			EntityConvertApplyType appType) {
		if(entity == EntityType.WAY && !Algorithms.isEmpty(tags.get("ref")) && tags.containsKey("highway")) {
			String ref = tags.get("ref");
			Set<String> rfs = new LinkedHashSet<String>();
			for(String r : Arrays.asList(ref.split(";"))) {
				rfs.add(r.trim());
			}
			Iterator<Entry<String, String>> it = tags.entrySet().iterator();
			int maxModifier = 1;
			Set<String> exisitingRefs = new LinkedHashSet<String>();
			Map<String, String> missingColors = null;
			while(it.hasNext()) {
				Entry<String, String> e = it.next();
				String tag = e.getKey();
				String vl = e.getValue();
				if(tag.startsWith("road_ref_")) {
					String sf = Algorithms.extractOnlyIntegerSuffix(tag);
					int modifier = -1;
					if(sf.length() > 0) {
						try {
							modifier = Integer.parseInt(sf);
							maxModifier = Math.max(maxModifier, 1 + modifier);
						} catch (NumberFormatException e1) {
							e1.printStackTrace();
						}
					}
					exisitingRefs.add(vl);
					exisitingRefs.add(vl.replaceAll("-", "").replaceAll(" ", "")); // E 17, E-17, E17
					if (tags.get("ref:colour") != null && modifier != -1 && 
							tags.get("road_ref:colour_"+modifier) == null && rfs.contains(vl)) {
						if(missingColors == null) {
							missingColors = new LinkedHashMap<String, String>();
						}
						missingColors.put("road_ref:colour_"+modifier, tags.get("ref:colour"));
					}
				}
			}
			rfs.removeAll(exisitingRefs);
			if(missingColors != null) {
				tags = new LinkedHashMap<String, String>(tags);
				tags.putAll(missingColors);
			}
			if (rfs.size() > 0) {
				tags = new LinkedHashMap<String, String>(tags);
				for (String r : rfs) {
					String s = r.replaceAll("-", "").replaceAll(" ", "");
					if(r.length() == 0 || exisitingRefs.contains(s)) {
						continue;
					}
					tags.put("route_road", "");
					tags.put("road_ref_"+maxModifier, r);
					if (tags.get("ref:colour") != null) {
						tags.put("road_ref:colour_"+maxModifier, tags.get("ref:colour"));
					}
					String network = getNetwork(r);
					if(!Algorithms.isEmpty(network)) {
						tags.put("road_network_"+maxModifier, network);
					}
					maxModifier++;
				}
			}

		}
		if(entity == EntityType.WAY && tags.containsKey("highway") && tags.containsKey("name")) {
			tags = new LinkedHashMap<String, String>(tags);
			String name = tags.get("name");
			if (name.toLowerCase().contains("transcanad") || name.toLowerCase().contains("trans canad") || name.toLowerCase().contains("trans-canad") || name.toLowerCase().contains("yellowhead")) {
				tags.put("tch", "yes");
			}
		}
		if(entity == EntityType.WAY && tags.containsKey("highway")) {
			tags = new LinkedHashMap<String, String>(tags);
			int i = 1;
			while (i < 10) {
				String name = tags.get("road_name_" + i);
				if (name != null && (name.toLowerCase().contains("transcanad") || name.toLowerCase().contains("trans canad") || name.toLowerCase().contains("trans-canad") || name.toLowerCase().contains("yellowhead"))) {
					tags.put("tch", "yes");
				}
				i++;
			}
		}
		tags = transformRouteRoadTags(tags);
		return tags;
	}

	private Map<String, String> transformRouteRoadTags(Map<String, String> tags) {
		if(!tags.containsKey("route") && !"road".equals(tags.get("route"))) {
			return tags;
		}
		Map<String, String> rtags = new LinkedHashMap<String, String>(tags);
		String[] countyArray = new String[] { "abbeville", "acadia", "accomack", "ada", "adair", "adams", "addison", "aiken",
				"aitkin", "alachua", "alamance", "alameda", "alamosa", "albany", "albemarle", "alcona", "alcorn",
				"aleutians east", "aleutians west", "alexander", "alexandria", "alfalfa", "alger", "allamakee",
				"allegan", "allegany", "alleghany", "allegheny", "allen", "allendale", "alpena", "alpine", "amador",
				"amelia", "amherst", "amite", "anchorage", "anderson", "andrew", "andrews", "androscoggin",
				"angelina", "anne arundel", "anoka", "anson", "antelope", "antrim", "apache", "appanoose", "appling",
				"appomattox", "aransas", "arapahoe", "archer", "archuleta", "arenac", "arkansas", "arlington",
				"armstrong", "aroostook", "arthur", "ascension", "ashe", "ashland", "ashley", "ashtabula", "asotin",
				"assumption", "atascosa", "atchison", "athens", "atkinson", "atlantic", "atoka", "attala", "audrain",
				"audubon", "auglaize", "augusta", "aurora", "austin", "autauga", "avery", "avoyelles", "baca", "bacon",
				"bailey", "baker", "baldwin", "ballard", "baltimore", "bamberg", "bandera", "banks", "banner",
				"bannock", "baraga", "barber", "barbour", "barnes", "barnstable", "barnwell", "barren", "barron",
				"barrow", "barry", "bartholomew", "barton", "bartow", "bastrop", "bates", "bath", "baxter", "bay",
				"bayfield", "baylor", "beadle", "bear lake", "beaufort", "beauregard", "beaver", "beaverhead",
				"becker", "beckham", "bedford", "bee", "belknap", "bell", "belmont", "beltrami", "ben hill", "benewah",
				"bennett", "bennington", "benson", "bent", "benton", "benzie", "bergen", "berkeley", "berks",
				"berkshire", "bernalillo", "berrien", "bertie", "bethel", "bexar", "bibb", "bienville", "big horn",
				"big stone", "billings", "bingham", "black hawk", "blackford", "bladen", "blaine", "blair", "blanco",
				"bland", "bleckley", "bledsoe", "blount", "blue earth", "boise", "bolivar", "bollinger", "bon homme",
				"bond", "bonner", "bonneville", "boone", "borden", "bosque", "bossier", "botetourt", "bottineau",
				"boulder", "boundary", "bourbon", "bowie", "bowman", "box butte", "box elder", "boyd", "boyle",
				"bracken", "bradford", "bradley", "branch", "brantley", "braxton", "brazoria", "brazos", "breathitt",
				"breckinridge", "bremer", "brevard", "brewster", "briscoe", "bristol", "bristol", "bristol bay",
				"broadwater", "bronx", "brooke", "brookings", "brooks", "broome", "broomfield", "broward", "brown",
				"brule", "brunswick", "bryan", "buchanan", "buckingham", "bucks", "buena vista", "buffalo",
				"bullitt", "bulloch", "bullock", "buncombe", "bureau", "burke", "burleigh", "burleson", "burlington",
				"burnet", "burnett", "burt", "butler", "butte", "butts", "cabarrus", "cabell", "cache", "caddo",
				"calaveras", "calcasieu", "caldwell", "caledonia", "calhoun", "callahan", "callaway", "calloway",
				"calumet", "calvert", "camas", "cambria", "camden", "cameron", "camp", "campbell", "canadian",
				"candler", "cannon", "canyon", "cape girardeau", "cape may", "carbon", "caribou", "carlisle",
				"carlton", "caroline", "carroll", "carson", "carson city", "carter", "carteret", "carver", "cascade",
				"casey", "cass", "cassia", "castro", "caswell", "catahoula", "catawba", "catoosa", "catron",
				"cattaraugus", "cavalier", "cayuga", "cecil", "cedar", "centre", "cerro gordo", "chaffee", "chambers",
				"champaign", "chariton", "charles", "charles city", "charles mix", "charleston", "charlevoix",
				"charlotte", "charlottesville", "charlton", "chase", "chatham", "chattahoochee", "chattooga",
				"chautauqua", "chaves", "cheatham", "cheboygan", "chelan", "chemung", "chenango", "cherokee", "cherry",
				"chesapeake", "cheshire", "chester", "chesterfield", "cheyenne", "chickasaw", "chicot", "childress",
				"chilton", "chippewa", "chisago", "chittenden", "choctaw", "chouteau", "chowan", "christian",
				"churchill", "cibola", "cimarron", "citrus", "clackamas", "claiborne", "clallam", "clare", "clarendon",
				"clarion", "clark", "clarke", "clatsop", "clay", "clayton", "clear creek", "clearfield", "clearwater",
				"cleburne", "clermont", "cleveland", "clinch", "clinton", "cloud", "coahoma", "coal", "cobb",
				"cochise", "cochran", "cocke", "coconino", "codington", "coffee", "coffey", "coke", "colbert", "cole",
				"coleman", "coles", "colfax", "colleton", "collier", "collin", "collingsworth", "colonial heights",
				"colorado", "colquitt", "columbia", "columbiana", "columbus", "colusa", "comal", "comanche", "concho",
				"concordia", "conecuh", "conejos", "contra costa", "converse", "conway", "cook", "cooke", "cooper",
				"coos", "coosa", "copiah", "corson", "cortland", "coryell", "coshocton", "costilla", "cottle",
				"cotton", "cottonwood", "covington", "coweta", "cowley", "cowlitz", "craig", "craighead", "crane",
				"craven", "crawford", "creek", "crenshaw", "crisp", "crittenden", "crockett", "crook", "crosby",
				"cross", "crow wing", "crowley", "culberson", "cullman", "culpeper", "cumberland", "cuming",
				"currituck", "curry", "custer", "cuyahoga", "dade", "daggett", "dakota", "dale", "dallam", "dallas",
				"dane", "daniels", "danville", "dare", "darke", "darlington", "dauphin", "davidson", "davie",
				"daviess", "davis", "davison", "dawes", "dawson", "day", "de baca", "de soto", "de witt", "deaf smith",
				"dearborn", "decatur", "deer lodge", "defiance", "dekalb", "del norte", "delaware", "delta",
				"denali", "dent", "denton", "denver", "des moines", "deschutes", "desha", "desoto", "deuel",
				"dewey", "dewitt", "dickens", "dickenson", "dickey", "dickinson", "dickson", "dillingham", "dillon",
				"dimmit", "dinwiddie", "district of columbia", "divide", "dixie", "dixon", "doña ana", "doddridge",
				"dodge", "dolores", "doniphan", "donley", "dooly", "door", "dorchester", "dougherty", "douglas",
				"drew", "dubois", "dubuque", "duchesne", "dukes", "dundy", "dunklin", "dunn", "dupage", "duplin",
				"durham", "dutchess", "duval", "dyer", "eagle", "early", "east baton rouge", "east carroll",
				"east feliciana", "eastland", "eaton", "eau claire", "echols", "ector", "eddy", "edgar", "edgecombe",
				"edgefield", "edmonson", "edmunds", "edwards", "effingham", "el dorado", "el paso", "elbert", "elk",
				"elkhart", "elko", "elliott", "ellis", "ellsworth", "elmore", "emanuel", "emery", "emmet", "emmons",
				"emporia", "erath", "erie", "escambia", "esmeralda", "essex", "estill", "etowah", "eureka",
				"evangeline", "evans", "fairbanks north star", "fairfax", "fairfield", "fall river", "fallon", "falls",
				"falls church", "fannin", "faribault", "faulk", "faulkner", "fauquier", "fayette", "fentress",
				"fergus", "ferry", "fillmore", "finney", "fisher", "flagler", "flathead", "fleming", "florence",
				"floyd", "fluvanna", "foard", "fond du lac", "ford", "forest", "forrest", "forsyth", "fort bend",
				"foster", "fountain", "franklin", "frederick", "fredericksburg", "freeborn", "freestone", "fremont",
				"fresno", "frio", "frontier", "fulton", "furnas", "gadsden", "gage", "gaines", "galax", "gallatin",
				"gallia", "galveston", "garden", "garfield", "garland", "garrard", "garrett", "garvin", "garza",
				"gasconade", "gaston", "gates", "geary", "geauga", "gem", "genesee", "geneva", "gentry", "george",
				"georgetown", "gibson", "gila", "gilchrist", "giles", "gillespie", "gilliam", "gilmer", "gilpin",
				"glacier", "glades", "gladwin", "glascock", "glasscock", "glenn", "gloucester", "glynn", "gogebic",
				"golden valley", "goliad", "gonzales", "goochland", "goodhue", "gooding", "gordon", "goshen", "gosper",
				"gove", "grady", "grafton", "graham", "grainger", "grand", "grand forks", "grand isle",
				"grand traverse", "granite", "grant", "granville", "gratiot", "graves", "gray", "grays harbor",
				"grayson", "greeley", "green", "green lake", "greenbrier", "greene", "greenlee", "greensville",
				"greenup", "greenville", "greenwood", "greer", "gregg", "gregory", "grenada", "griggs", "grimes",
				"grundy", "guadalupe", "guernsey", "guilford", "gulf", "gunnison", "guthrie", "gwinnett", "haakon",
				"habersham", "haines", "hale", "halifax", "hall", "hamblen", "hamilton", "hamlin", "hampden",
				"hampshire", "hampton", "hancock", "hand", "hanover", "hansford", "hanson", "haralson", "hardee",
				"hardeman", "hardin", "harding", "hardy", "harford", "harlan", "harmon", "harnett", "harney", "harper",
				"harris", "harrison", "harrisonburg", "hart", "hartford", "hartley", "harvey", "haskell", "hawaii",
				"hawkins", "hayes", "hays", "haywood", "heard", "hemphill", "hempstead", "henderson", "hendricks",
				"hendry", "hennepin", "henrico", "henry", "herkimer", "hernando", "hertford", "hettinger", "hickman",
				"hickory", "hidalgo", "highland", "highlands", "hill", "hillsborough", "hillsdale", "hinds",
				"hinsdale", "hitchcock", "hocking", "hockley", "hodgeman", "hoke", "holmes", "holt", "honolulu",
				"hood", "hood river", "hooker", "hoonah-angoon", "hopewell", "hopkins", "horry", "hot spring",
				"hot springs", "houghton", "houston", "howard", "howell", "hubbard", "hudson", "hudspeth", "huerfano",
				"hughes", "humboldt", "humphreys", "hunt", "hunterdon", "huntingdon", "huntington", "huron",
				"hutchinson", "hyde", "iberia", "iberville", "ida", "idaho", "imperial", "independence",
				"indian river", "indiana", "ingham", "inyo", "ionia", "iosco", "iowa", "iredell", "irion", "iron",
				"iroquois", "irwin", "isabella", "isanti", "island", "isle of wight", "issaquena", "itasca",
				"itawamba", "izard", "jack", "jackson", "james city", "jasper", "jay", "jeff davis", "jefferson",
				"jefferson davis", "jenkins", "jennings", "jerauld", "jerome", "jersey", "jessamine", "jewell",
				"jim hogg", "jim wells", "jo daviess", "johnson", "johnston", "jones", "josephine", "juab",
				"judith basin", "juneau", "juniata", "kalamazoo", "kalawao", "kalkaska", "kanabec", "kanawha",
				"kandiyohi", "kane", "kankakee", "karnes", "kauai", "kaufman", "kay", "kearney", "kearny", "keith",
				"kemper", "kenai peninsula", "kendall", "kenedy", "kennebec", "kenosha", "kent", "kenton", "keokuk",
				"kern", "kerr", "kershaw", "ketchikan gateway", "kewaunee", "keweenaw", "keya paha", "kidder",
				"kimball", "kimble", "king", "king and queen", "king george", "king william", "kingfisher", "kingman",
				"kings", "kingsbury", "kinney", "kiowa", "kit carson", "kitsap", "kittitas", "kittson", "klamath",
				"kleberg", "klickitat", "knott", "knox", "kodiak island", "koochiching", "kootenai", "kosciusko",
				"kossuth", "la crosse", "la paz", "la plata", "la salle", "labette", "lac qui parle", "lackawanna",
				"laclede", "lafayette", "lafourche", "lagrange", "lake", "lake and peninsula", "lake of the woods",
				"lamar", "lamb", "lamoille", "lamoure", "lampasas", "lancaster", "lander", "lane", "langlade",
				"lanier", "lapeer", "laporte", "laramie", "larimer", "larue", "las animas", "lasalle", "lassen",
				"latah", "latimer", "lauderdale", "laurel", "laurens", "lavaca", "lawrence", "le flore", "le sueur",
				"lea", "leake", "leavenworth", "lebanon", "lee", "leelanau", "leflore", "lehigh", "lemhi", "lenawee",
				"lenoir", "leon", "leslie", "letcher", "levy", "lewis", "lewis and clark", "lexington", "liberty",
				"licking", "limestone", "lincoln", "linn", "lipscomb", "litchfield", "little river", "live oak",
				"livingston", "llano", "logan", "long", "lonoke", "lorain", "los alamos", "los angeles", "loudon",
				"loudoun", "louisa", "loup", "love", "loving", "lowndes", "lubbock", "lucas", "luce", "lumpkin",
				"luna", "lunenburg", "luzerne", "lycoming", "lyman", "lynchburg", "lynn", "lyon", "mackinac", "macomb",
				"macon", "macoupin", "madera", "madison", "magoffin", "mahaska", "mahnomen", "mahoning", "major",
				"malheur", "manassas", "manassas park", "manatee", "manistee", "manitowoc", "marathon", "marengo",
				"maricopa", "maries", "marin", "marinette", "marion", "mariposa", "marlboro", "marquette", "marshall",
				"martin", "martinsville", "mason", "massac", "matagorda", "matanuska-susitna", "mathews", "maui",
				"maury", "maverick", "mayes", "mcclain", "mccone", "mccook", "mccormick", "mccracken", "mccreary",
				"mcculloch", "mccurtain", "mcdonald", "mcdonough", "mcdowell", "mcduffie", "mchenry", "mcintosh",
				"mckean", "mckenzie", "mckinley", "mclean", "mclennan", "mcleod", "mcminn", "mcmullen", "mcnairy",
				"mcpherson", "meade", "meagher", "mecklenburg", "mecosta", "medina", "meeker", "meigs", "mellette",
				"menard", "mendocino", "menifee", "menominee", "merced", "mercer", "meriwether", "merrick",
				"merrimack", "mesa", "metcalfe", "miami", "miami-dade", "middlesex", "midland", "mifflin", "milam",
				"millard", "mille lacs", "miller", "mills", "milwaukee", "miner", "mineral", "mingo", "minidoka",
				"minnehaha", "missaukee", "mississippi", "missoula", "mitchell", "mobile", "modoc", "moffat", "mohave",
				"moniteau", "monmouth", "mono", "monona", "monongalia", "monroe", "montague", "montcalm", "monterey",
				"montezuma", "montgomery", "montmorency", "montour", "montrose", "moody", "moore", "mora", "morehouse",
				"morgan", "morrill", "morris", "morrison", "morrow", "morton", "motley", "moultrie", "mountrail",
				"mower", "muhlenberg", "multnomah", "murray", "muscatine", "muscogee", "muskegon", "muskingum",
				"muskogee", "musselshell", "nacogdoches", "nance", "nantucket", "napa", "nash", "nassau",
				"natchitoches", "natrona", "navajo", "navarro", "nelson", "nemaha", "neosho", "neshoba", "ness",
				"nevada", "new castle", "new hanover", "new haven", "new kent", "new london", "new madrid", "new york",
				"newaygo", "newberry", "newport", "newport news", "newton", "nez perce", "niagara", "nicholas",
				"nicollet", "niobrara", "noble", "nobles", "nodaway", "nolan", "nome", "norfolk", "norman",
				"north slope", "northampton", "northumberland", "northwest arctic", "norton", "nottoway", "nowata",
				"noxubee", "nuckolls", "nueces", "nye", "o'brien", "oakland", "obion", "ocean", "oceana", "ochiltree",
				"oconee", "oconto", "ogemaw", "ogle", "oglethorpe", "ohio", "okaloosa", "okanogan", "okeechobee",
				"okfuskee", "oklahoma", "okmulgee", "oktibbeha", "oldham", "oliver", "olmsted", "oneida", "onondaga",
				"onslow", "ontario", "ontonagon", "orange", "orangeburg", "oregon", "orleans", "osage", "osborne",
				"osceola", "oscoda", "oswego", "otero", "otoe", "otsego", "ottawa", "otter tail", "ouachita", "ouray",
				"outagamie", "overton", "owen", "owsley", "owyhee", "oxford", "ozark", "ozaukee", "pacific", "page",
				"palm beach", "palo alto", "palo pinto", "pamlico", "panola", "park", "parke", "parker", "parmer",
				"pasco", "pasquotank", "passaic", "patrick", "paulding", "pawnee", "payette", "payne", "peach",
				"pearl river", "pecos", "pembina", "pemiscot", "pend oreille", "pender", "pendleton", "pennington",
				"penobscot", "peoria", "pepin", "perkins", "perquimans", "perry", "pershing", "person", "petersburg",
				"petroleum", "pettis", "phelps", "philadelphia", "phillips", "piatt", "pickaway", "pickens", "pickett",
				"pierce", "pike", "pima", "pinal", "pine", "pinellas", "pipestone", "piscataquis", "pitkin", "pitt",
				"pittsburg", "pittsylvania", "piute", "placer", "plaquemines", "platte", "pleasants", "plumas",
				"plymouth", "pocahontas", "poinsett", "pointe coupee", "polk", "pondera", "pontotoc", "pope",
				"poquoson", "portage", "porter", "portsmouth", "posey", "pottawatomie", "pottawattamie", "potter",
				"powder river", "powell", "power", "poweshiek", "powhatan", "prairie", "pratt", "preble", "prentiss",
				"presidio", "presque isle", "preston", "price", "prince edward", "prince george", "prince george's",
				"prince of wales-hyder", "prince william", "providence", "prowers", "pueblo", "pulaski", "pushmataha",
				"putnam", "quay", "queen anne's", "queens", "quitman", "rabun", "racine", "radford", "rains",
				"raleigh", "ralls", "ramsey", "randall", "randolph", "rankin", "ransom", "rapides", "rappahannock",
				"ravalli", "rawlins", "ray", "reagan", "real", "red lake", "red river", "red willow", "redwood",
				"reeves", "refugio", "reno", "rensselaer", "renville", "republic", "reynolds", "rhea", "rice", "rich",
				"richardson", "richland", "richmond", "riley", "ringgold", "rio arriba", "rio blanco", "rio grande",
				"ripley", "ritchie", "riverside", "roane", "roanoke", "roberts", "robertson", "robeson", "rock",
				"rock island", "rockbridge", "rockcastle", "rockdale", "rockingham", "rockland", "rockwall",
				"roger mills", "rogers", "rolette", "rooks", "roosevelt", "roscommon", "roseau", "rosebud", "ross",
				"routt", "rowan", "runnels", "rush", "rusk", "russell", "rutherford", "rutland", "sabine", "sac",
				"sacramento", "sagadahoc", "saginaw", "saguache", "salem", "saline", "salt lake", "saluda", "sampson",
				"san augustine", "san benito", "san bernardino", "san diego", "san francisco", "san jacinto",
				"san joaquin", "san juan", "san luis obispo", "san mateo", "san miguel", "san patricio", "san saba",
				"sanborn", "sanders", "sandoval", "sandusky", "sangamon", "sanilac", "sanpete", "santa barbara",
				"santa clara", "santa cruz", "santa fe", "santa rosa", "sarasota", "saratoga", "sargent", "sarpy",
				"sauk", "saunders", "sawyer", "schenectady", "schleicher", "schley", "schoharie", "schoolcraft",
				"schuyler", "schuylkill", "scioto", "scotland", "scott", "scotts bluff", "screven", "scurry", "searcy",
				"sebastian", "sedgwick", "seminole", "seneca", "sequatchie", "sequoyah", "sevier", "seward",
				"shackelford", "shannon", "sharkey", "sharp", "shasta", "shawano", "shawnee", "sheboygan", "shelby",
				"shenandoah", "sherburne", "sheridan", "sherman", "shiawassee", "shoshone", "sibley", "sierra",
				"silver bow", "simpson", "sioux", "siskiyou", "sitka", "skagit", "skagway", "skamania", "slope",
				"smith", "smyth", "snohomish", "snyder", "socorro", "solano", "somerset", "somervell", "sonoma",
				"southampton", "southeast fairbanks", "spalding", "spartanburg", "spencer", "spink", "spokane",
				"spotsylvania", "saint bernard", "saint charles", "saint clair", "saint croix", "saint francis",
				"saint francois", "saint helena", "saint james", "saint john the baptist", "saint johns",
				"saint joseph", "saint landry", "saint lawrence", "saint louis", "saint lucie", "saint martin",
				"saint mary", "saint mary's", "saint tammany", "stafford", "stanislaus", "stanley", "stanly",
				"stanton", "stark", "starke", "starr", "staunton", "sainte genevieve", "stearns", "steele", "stephens",
				"stephenson", "sterling", "steuben", "stevens", "stewart", "stillwater", "stoddard", "stokes", "stone",
				"stonewall", "storey", "story", "strafford", "stutsman", "sublette", "suffolk", "sullivan", "sully",
				"summers", "summit", "sumner", "sumter", "sunflower", "surry", "susquehanna", "sussex", "sutter",
				"sutton", "suwannee", "swain", "sweet grass", "sweetwater", "swift", "swisher", "switzerland",
				"talbot", "taliaferro", "talladega", "tallahatchie", "tallapoosa", "tama", "taney", "tangipahoa",
				"taos", "tarrant", "tate", "tattnall", "taylor", "tazewell", "tehama", "telfair", "teller", "tensas",
				"terrebonne", "terrell", "terry", "teton", "texas", "thayer", "thomas", "throckmorton", "thurston",
				"tift", "tillamook", "tillman", "tioga", "tippah", "tippecanoe", "tipton", "tishomingo", "titus",
				"todd", "tolland", "tom green", "tompkins", "tooele", "toole", "toombs", "torrance", "towner", "towns",
				"traill", "transylvania", "traverse", "travis", "treasure", "trego", "trempealeau", "treutlen",
				"trigg", "trimble", "trinity", "tripp", "troup", "trousdale", "trumbull", "tucker", "tulare", "tulsa",
				"tunica", "tuolumne", "turner", "tuscaloosa", "tuscarawas", "tuscola", "twiggs", "twin falls", "tyler",
				"tyrrell", "uinta", "uintah", "ulster", "umatilla", "unicoi", "union", "upshur", "upson", "upton",
				"utah", "uvalde", "val verde", "valdez-cordova", "valencia", "valley", "van buren", "van wert",
				"van zandt", "vance", "vanderburgh", "venango", "ventura", "vermilion", "vermillion", "vernon",
				"victoria", "vigo", "vilas", "vinton", "virginia beach", "volusia", "wabash", "wabasha", "wabaunsee",
				"wade hampton", "wadena", "wagoner", "wahkiakum", "wake", "wakulla", "waldo", "walker", "walla walla",
				"wallace", "waller", "wallowa", "walsh", "walthall", "walton", "walworth", "wapello", "ward", "ware",
				"warren", "warrick", "wasatch", "wasco", "waseca", "washakie", "washburn", "washington", "washita",
				"washoe", "washtenaw", "watauga", "watonwan", "waukesha", "waupaca", "waushara", "wayne", "waynesboro",
				"weakley", "webb", "weber", "webster", "weld", "wells", "west baton rouge", "west carroll",
				"west feliciana", "westchester", "westmoreland", "weston", "wetzel", "wexford", "wharton", "whatcom",
				"wheatland", "wheeler", "white", "white pine", "whiteside", "whitfield", "whitley", "whitman",
				"wibaux", "wichita", "wicomico", "wilbarger", "wilcox", "wilkes", "wilkin", "wilkinson", "will",
				"willacy", "williams", "williamsburg", "williamson", "wilson", "winchester", "windham", "windsor",
				"winkler", "winn", "winnebago", "winneshiek", "winona", "winston", "wirt", "wise", "wolfe", "wood",
				"woodbury", "woodford", "woodruff", "woods", "woodson", "woodward", "worcester", "worth", "wrangell",
				"wright", "wyandot", "wyandotte", "wyoming", "wythe", "yadkin", "yakima", "yakutat", "yalobusha",
				"yamhill", "yancey", "yankton", "yates", "yavapai", "yazoo", "yell", "yellow medicine", "yellowstone",
				"yoakum", "yolo", "york", "young", "yuba", "yukon-koyukuk", "yuma", "zapata", "zavala", "ziebach",
				"asd", "ash", "ath", "bel", "belt", "but", "cape_may", "car", "chp", "cli", "cos", "cth", "col", "eri",
				"fay", "fra", "ful", "gac", "gea", "gue", "h", "ham", "har", "hen", "hoc", "hol", "hur", "jac", "jef",
				"lic", "lor", "luc", "log", "mah", "med", "mei", "moe", "mrw", "odnr", "ott", "pau", "per", "por",
				"pre", "put", "san", "sen", "sta", "sum", "tus", "uni", "vin", "way", "wil", "winnipeg", "woo",
				"wya" };
		if(rtags.containsKey("network")) {
			String network = rtags.get("network");
			if (network.startsWith("US:")) {
				if (!network.equalsIgnoreCase("US:I") && !network.startsWith("US:I:") && !network.toUpperCase().startsWith("US:US")) {
					if (((network.length() > 7) && network.substring(6,8).equals("CR")) || network.toLowerCase().contains("county") ||
						((network.length() > 7) && Arrays.asList(countyArray).contains((network.substring(6)).toLowerCase())) && ((network.split(":", -1).length-1) < 3))
					{
						rtags.put("us_county_network", "yes");
					} else {
						rtags.put("us_state_network", "yes");
					}
					if (network.length() > 5) {
						network = network.substring(0, 5);
						rtags.put("network", network);
					}
				}
				if (network.equalsIgnoreCase("US:I")) {
					rtags.put("network", "us:i");
				}
				if (network.toUpperCase().startsWith("US:US")) {
					rtags.put("network", "us:us");
				}
			}
		} else {
			String rf = rtags.get("ref");
			if(!Algorithms.isEmpty(rf)) {
				rf = rf.trim();
				String network = getNetwork(rf);
				if(!Algorithms.isEmpty(network)) {
					rtags.put("network", network);
				}
			}
		}
		if(rtags.containsKey("network") && !Algorithms.isEmpty(rtags.get("network"))) {
			rtags.put("network", rtags.get("network").toLowerCase());
		}
		if(rtags.containsKey("modifier") && !Algorithms.isEmpty(rtags.get("modifier"))) {
			rtags.put("modifier", rtags.get("modifier").toLowerCase());
		}
		if(rtags.containsKey("ref") && !Algorithms.isEmpty(rtags.get("ref"))) {
			rtags.put("ref", rtags.get("ref"));
		}
		return rtags;
	}

	private String getNetwork(String rf) {
		boolean numbers = true;
		String network = "";
		if(rf.length() != 0 && !Character.isDigit(rf.charAt(0))) {
			numbers = false;
		}
		if(numbers) {
			network = "#";
		}
// 		 else{
// 			int ind = 0;
// 			for(; ind < rf.length(); ind++) {
// 				if(Character.isDigit(rf.charAt(ind)) ||
// 						rf.charAt(ind) == ' ' || rf.charAt(ind) == '-') {
// 					break;
// 				}
// 			}
// 			network = rf.substring(0, ind).trim();
// 		}
		return network;
	}


	public List<Map<String, String>> splitTags(Map<String, String> tags, EntityType entity) {
		EntityConvertType filter = EntityConvertType.SPLIT;
		List<EntityConvert> listToConvert = getApplicableConverts(tags, entity, filter,
				EntityConvertApplyType.MAP);
		List<Map<String, String>> result = null;
		if(listToConvert == null) {
			return result;
		}
		result = new ArrayList<Map<String,String>>();
		Map<String, String> ctags = new LinkedHashMap<String, String>(tags);
		result.add(ctags);
		for (EntityConvert ec : listToConvert) {
			LinkedHashMap<String, String> mp = new LinkedHashMap<String, String>();
			for (TagValuePattern ift : ec.toTags) {
				String vl = ift.value;
				if (vl == null) {
					vl = ctags.get(ift.tag);
				}
				vl = processSubstr(ift, vl);
				if(vl != null) {
					mp.put(ift.tag, vl);
				}
			}
			ctags.remove(ec.fromTag.tag);
			result.add(mp);
		}
		return result;
	}



	protected List<EntityConvert> getApplicableConverts(Map<String, String> tags, EntityType entity,
			EntityConvertType filter, EntityConvertApplyType appFilter) {
		List<EntityConvert> listToConvert = null;
		for(Map.Entry<String, String> e : tags.entrySet()) {
			List<EntityConvert> list = convertTags.get(e.getKey());
			if (list != null) {
				for (EntityConvert ec : list) {
					if (checkConvertValue(ec.fromTag, e.getValue())) {
						String verbose = null;
						if(ec.verbose) {
							verbose = "Apply entity convert from '"+ec.fromTag+"' to " + tags + " in " +
									appFilter;
						}
						if (checkConvert(tags, ec, entity) && ec.type == filter &&
								ec.applyToType.contains(appFilter)) {
							if (listToConvert == null) {
								listToConvert = new ArrayList<EntityConvert>();
							}
							listToConvert.add(ec);
							if (verbose != null) {
								verbose += " - has succeeded";
							}
						} else {
							if (verbose != null) {
								verbose += " - has failed due to ";
								if(!checkConvert(tags, ec, entity)) {
									verbose += "if conditions;";
								}
								if(ec.type != filter ) {
									verbose += " transform " + filter + "!= " + ec.type + ";";
								}
								if(!ec.applyToType.contains(appFilter)) {
									verbose += " appFilter "+appFilter+";";
								}

							}
						}
						if(verbose != null) {
							log.info(verbose);
						}
					}
				}
			}
		}
		return listToConvert;
	}



	private void applyTagTransforms(Map<String, String> tags, EntityConvert ec, EntityType entity,
			Map<String, String> originaltags) {
		String fromValue =  originaltags.get(ec.fromTag.tag);
		tags.remove(ec.fromTag.tag);
		for(TagValuePattern ift : ec.toTags) {
			String vl = ift.value;
			if(vl == null) {
				vl = fromValue;
			}
			vl = processSubstr(ift, vl);
			tags.put(ift.tag, vl);
		}
	}

	private String processSubstr(TagValuePattern ift, String vl) {
		if (vl == null) {
			return null;
		}
		if (ift.substrSt != 0) {
			int s = ift.substrSt;
			if (s > 0) {
				if (vl.length() < s) {
					vl = "";
				} else {
					vl = vl.substring(s).trim();
				}
			} else {
				if (vl.length() > -s) {
					vl = vl.substring(vl.length() + s).trim();
				}
			}
		}
		if(ift.substrEnd != -1) {
			int s = ift.substrEnd;
			if (s > 0) {
				if (vl.length() > s) {
					vl = vl.substring(0, s).trim();
				}
			} else {
				if (vl.length() < -s) {
					vl = "";
				} else {
					vl = vl.substring(0, vl.length() + s).trim();
				}
			}
		}
		return vl;
	}


	protected boolean checkConvert(Map<String, String> tags, EntityConvert ec, EntityType entity) {
		if(ec.applyTo != null) {
			if(!ec.applyTo.contains(entity)) {
				return false;
			}
		}
		boolean empty = ec.ifRegionName.isEmpty();
		if (!empty) {
			boolean found = false;
			String rg = tags.get(OSMAND_REGION_NAME_TAG);
			if(Algorithms.isEmpty(rg)) {
				rg = regionName;
			} else {
				rg = "$" + rg + "^";
			}
			for (String s : ec.ifRegionName) {
				if (rg.contains(s)) {
					found = true;
					break;
				}
			}
			if (!found) {
				return false;
			}
		}
		for (String s : ec.ifNotRegionName) {
			String rg = tags.get(OSMAND_REGION_NAME_TAG);
			if(Algorithms.isEmpty(rg)) {
				rg = regionName;
			} else {
				rg = "$" + rg + "^";
			}
			if (rg.contains(s)) {
				return false;
			}
		}
		for(TagValuePattern ift : ec.ifTags) {
			String val = tags.get(ift.tag);
			if(!checkConvertValue(ift, val)) {
				return false;
			}
		}
		for(TagValuePattern ift : ec.ifNotTags) {
			String val = tags.get(ift.tag);
			if(checkConvertValue(ift, val)) {
				return false;
			}
		}
		for(TagValuePattern ift : ec.ifStartsTags) {
			String val = tags.get(ift.tag);
			if(!checkStartsWithValue(ift, val)) {
				return false;
			}
		}
		for(TagValuePattern ift : ec.ifNotStartsTags) {
			String val = tags.get(ift.tag);
			if(checkStartsWithValue(ift, val)) {
				return false;
			}
		}

		for(TagValuePattern ift : ec.ifEndsTags) {
			String val = tags.get(ift.tag);
			if(!checkEndsWithValue(ift, val)) {
				return false;
			}
		}
		for(TagValuePattern ift : ec.ifNotEndsTags) {
			String val = tags.get(ift.tag);
			if(checkEndsWithValue(ift, val)) {
				return false;
			}
		}
		for(TagValuePattern ift : ec.ifContainsTags) {
			String val = tags.get(ift.tag);
			if(!checkContainsValue(ift, val)) {
				return false;
			}
		}
		for(TagValuePattern ift : ec.ifNotContainsTags) {
			String val = tags.get(ift.tag);
			if(checkContainsValue(ift, val)) {
				return false;
			}
		}
		for(TagValuePattern ift : ec.ifTagsNotLess) {
			String val = tags.get(ift.tag);
			double nt = Double.parseDouble(ift.value);
			long vl = Algorithms.parseLongSilently(val, 0);
			if(vl < nt) {
				return false;
			}
		}
		for(TagValuePattern ift : ec.ifTagsLess) {
			String val = tags.get(ift.tag);
			double nt = Double.parseDouble(ift.value);
			long vl = Algorithms.parseLongSilently(val, 0);
			if(vl >= nt) {
				return false;
			}
		}
		return true;
	}



	private boolean checkConvertValue(TagValuePattern fromTag, String value) {
		if(value == null ) {
			return false;
		}
		if(fromTag.value == null) {
			return true;
		}
		return fromTag.value.toLowerCase().equals(value.toLowerCase());
	}

	private boolean checkStartsWithValue(TagValuePattern fromTag, String value) {
		if(value == null) {
			return false;
		}
		if(fromTag.value == null) {
			return true;
		}
		return value.toLowerCase().startsWith(fromTag.value.toLowerCase());
	}


	private boolean checkContainsValue(TagValuePattern fromTag, String value) {
		if(value == null) {
			return false;
		}
		if(fromTag.value == null) {
			return true;
		}
		return value.toLowerCase().contains(fromTag.value.toLowerCase());
	}

	private boolean checkEndsWithValue(TagValuePattern fromTag, String value) {
		if(value == null) {
			return false;
		}
		if(fromTag.value == null) {
			return true;
		}
		return value.toLowerCase().endsWith(fromTag.value.toLowerCase());
	}



	protected String simplifyValueTo45(String val) {
		int rad = 8;
		//0, 45, 90, 135, 180, 225, 270, 315
		double circle = 360;
		try {
			double simple01 = Double.parseDouble(val) / circle;
			while (simple01 < 0) {
				simple01++;
			}
			while (simple01 >= 1) {
				simple01--;
			}
			int rnd = (int) (Math.round(simple01 * rad));
			val = "" + (rnd * circle / rad);
		} catch (NumberFormatException e) {
			System.err.println("Wrong value of \"seamark:notice:orientation\" " + val);
		}
		return val;
	}



	private void prepareColorTag(Map<String, String> tags, String tag) {
		String vl = tags.get(tag);
		vl = formatColorToPalette(vl, false);
		tags.put("colour_"+vl, "");
		tags.put("color_"+vl, "");
	}

	private void sortAndUpdateTypes(TIntArrayList outTypes) {
		outTypes.sort();
        for(int i = 0; i < outTypes.size(); i++) {
        	int k = outTypes.get(i) & ((1 << 15) - 1);
        	outTypes.set(i, k);
        }
	}



	private int combineOrderAndId(MapRulType rType) {
		if(rType.id > 1<<15) {
			throw new UnsupportedOperationException();
		}
		return (rType.order << 15) | rType.id;
	}

	public static double[] RGBtoHSV(double r, double g, double b){
		double h, s, v;
		double min, max, delta;
		min = Math.min(Math.min(r, g), b);
		max = Math.max(Math.max(r, g), b);

		// V
		v = max;
		delta = max - min;

		// S
		if( max != 0 )
			s = delta / max;
			else {
				s = 0;
				h = -1;
			return new double[]{h,s,v};
			}

		// H
		if( r == max )
			h = ( g - b ) / delta; // between yellow & magenta
			else if( g == max )
				h = 2 + ( b - r ) / delta; // between cyan & yellow
				else
				h = 4 + ( r - g ) / delta; // between magenta & cyan
			h *= 60;    // degrees

			if( h < 0 )
				h += 360;

		return new double[]{h,s,v};
	}

	public String formatColorToPalette(String vl, boolean palette6){
		vl = vl.toLowerCase();
		int color = -1;
		int r = -1;
		int g = -1;
		int b = -1;
		if (vl.length() > 1 && vl.charAt(0) == '#') {
			try {
				color = Algorithms.parseColor(vl);
				r = (color >> 16) & 0xFF;
				g = (color >> 8) & 0xFF;
				b = (color >> 0) & 0xFF;
			} catch (RuntimeException e) {
			}
		}
		float[] hsv = new float[3];
		java.awt.Color.RGBtoHSB(r,g,b,hsv);
		float h = hsv[0];
		float s = hsv[1];
		float v = hsv[2];
		h *= 360;
		s *= 100;
		v *= 100;

		if ((h < 16 && s > 25 && v > 30) || (h > 326 && s > 25 && v > 30) || (h < 16 && s > 10 && s < 25 && v > 90) || (h > 326 && s > 10 && s < 25 && v > 90) ||
				vl.equals("pink") || vl.contains("red") || vl.equals("pink/white") || vl.equals("white-red") || vl.equals("ff0000") || vl.equals("800000") || vl.equals("red/tan") || vl.equals("tan/red") || vl.equals("rose")) {
			vl = "red";
		} else if ((h >= 16 && h < 50 && s > 25 && v > 20 && v < 60) || vl.equals("brown") || vl.equals("darkbrown") || vl.equals("tan/brown") || vl.equals("tan_brown") || vl.equals("brown/tan") || vl.equals("light_brown") || vl.equals("brown/white") || vl.equals("tan")) {
			vl = palette6 ? "red" : "brown";
		} else if ((h >= 16 && h < 45 && v > 60) || vl.equals("orange") || vl.equals("cream") || vl.equals("gold") || vl.equals("yellow-red") || vl.equals("ff8c00") || vl.equals("peach")) {
			vl = palette6 ? "red" : "orange";
		} else if ((h >= 46 && h < 66 && s > 30 && v > 83) || vl.equals("yellow") || vl.equals("gelb") || vl.equals("ffff00") || vl.equals("beige") || vl.equals("lightyellow") || vl.equals("jaune")) {
			vl = "yellow";
		} else if ((h >= 46 && h < 66 && s > 30 && v > 30 && v < 82)) {
			vl = palette6 ? "yellow" : "darkyellow";
		} else if ((h >= 67 && h < 165 && s > 30 && v > 77) || vl.equals("lightgreen") || vl.equals("lime") || vl.equals("seagreen") || vl.equals("00ff00") || vl.equals("yellow/green")) {
			vl = palette6 ? "green" : "lightgreen";
		} else if ((h >= 74 && h < 174 && s > 30 && v > 30 && v < 77) || vl.contains("green") || vl.equals("darkgreen") || vl.equals("natural") || vl.equals("natur") || vl.equals("mediumseagreen") || vl.equals("green/white") || vl.equals("white/green") || vl.equals("blue/yellow") || vl.equals("vert") || vl.equals("green/blue") || vl.equals("olive")) {
			vl = "green";
		} else if ((h >= 165 && h < 215 && s > 32 && v > 50) || vl.equals("lightblue") || vl.equals("aqua") || vl.equals("cyan") || vl.equals("87ceeb") || vl.equals("turquoise")) {
			vl = palette6 ? "blue" : "lightblue";
		} else if ((h >= 215 && h < 265 && s > 40 && v > 30) || vl.contains("blue") || vl.equals("0000ff") || vl.equals("teal") || vl.equals("darkblue") || vl.equals("blu") || vl.equals("navy")) {
			vl = "blue";
		} else if ((h >= 265 && h < 325 && s > 15 && v >= 27) || (h > 250 && h < 325 && s > 10 && s < 25 && v > 90) || vl.equals("purple") || vl.equals("violet") || vl.equals("magenta") || vl.equals("maroon") || vl.equals("fuchsia") || vl.equals("800080")) {
			vl = palette6 ? "blue" : "purple";
		} else if ((color != -1 & v < 27) || vl.contains("black") || vl.equals("darkgrey")) {
			vl = "black";
		} else if ((s < 32 && v > 30 && v < 90) || vl.equals("gray") || vl.equals("grey") || vl.equals("grey/tan") || vl.equals("silver") || vl.equals("srebrny") || vl.equals("lightgrey") || vl.equals("lightgray") || vl.equals("metal")) {
			vl = palette6 ? "white" : "gray";
		} else if ((s < 5 && v > 95) || vl.contains("white") /*|| vl.equals("white/tan")*/) {
			vl = "white";
		} else if (r != -1 && g != -1 && b != -1) {
			vl = "gray";
		}
		return vl;
	}

	final Set<String> barValues = new HashSet<String>(Arrays.asList(
		new String[]{
			"",
			"bar",
			"stripe",
			"cross",
			"x",
			"slash",
			"rectangle",
			"fork",
			"turned_t",
			"l",
			"lower",
			"corner",
			"backslash",
			"wolfshook",
			"rectangle_line",
	}));
	final java.util.Map<String, String> precolors = new java.util.HashMap<String, String>();
	{
            precolors.put("white_red_diamond","red");
            precolors.put("black_red_diamond","red");
            precolors.put("shell_modern","yellow");
            precolors.put("shell","yellow");
            precolors.put("wolfshook","white");
            precolors.put("ammonit","white");
            precolors.put("mine","white");
            precolors.put("hiker","black");
            precolors.put("heart","red");
            precolors.put("tower","black");
            precolors.put("bridleway","white");
	}

	private boolean isColor(String s) {
		return s.equals("black")
				|| s.equals("blue")
				|| s.equals("green")
				|| s.equals("red")
				|| s.equals("white")
				|| s.equals("yellow")
				|| s.equals("orange")
				|| s.equals("purple")
				|| s.equals("brown");
	}


	private Map<String, String> processExtraTags(Map<String, String> tags) {
		if(tags.containsKey("osmc:symbol")) {
			tags = new LinkedHashMap<String, String>(tags);
			// osmc:symbol=black:red:blue_rectangle ->
			// 1.For backwards compatibility (already done) - osmc_shape=bar, osmc_symbol=black, osmc_symbol_red_blue_name=.
			// 2.New tags: osmc_waycolor=black, osmc_background=red, osmc_foreground=blue_rectangle, osmc_foreground2,
			// osmc_text, osmc_textcolor, osmc_stub_name=. ,
			String value = tags.get("osmc:symbol");
			String[] tokens = value.split(":", 6);
			osmcBackwardCompatility(tags, tokens);
			if (tokens != null) {
				if ((tokens.length == 4 && isColor(tokens[3])) || (tokens.length == 5 && isColor(tokens[4]))) {
					String[] ntokens = new String[] { tokens[0], tokens[1], tokens.length == 4 ? "" : tokens[2], "",
							tokens.length == 4 ? tokens[2] : tokens[3], tokens.length == 4 ? tokens[3] : tokens[4] };
					addOsmcNewTags(tags, ntokens);
				} else {
					addOsmcNewTags(tags, tokens);
				}
			}
		}
		if(tags.containsKey("color")) {
			tags = new LinkedHashMap<String, String>(tags);
			prepareColorTag(tags, "color");
		}
		if(tags.containsKey("colour")) {
			tags = new LinkedHashMap<String, String>(tags);
			prepareColorTag(tags, "colour");
		}
		
		return tags;
	}
	

	private static int calculateIntegrity(Map<String, String> mp) {
		int result = 0;
		String surface = mp.get("surface");
		String smoothness = mp.get("smoothness");
		String tracktype = mp.get("tracktype");
		String highway = mp.get("highway");
		String bicycle = mp.get("bicycle");
		String foot = mp.get("foot");
		if (surface != null) {
			surface = surface.toLowerCase();
		}
		if (smoothness != null) {
			smoothness = smoothness.toLowerCase();
	        }
		if (tracktype != null) {
			tracktype = tracktype.toLowerCase();
		}
		if ("paved".equals(surface) || "concrete".equals(surface) || "concrete:lanes".equals(surface) || "concrete:plates".equals(surface) || "sett".equals(surface) || "paving_stones".equals(surface) || "metal".equals(surface) || "wood".equals(surface)) {
			result += 3;
		} else if ("compacted".equals(surface) || "fine_gravel".equals(surface) || "grass_paver".equals(surface)) {
			result += 4;
		} else if ("unpaved".equals(surface) || "ground".equals(surface) || "earth".equals(surface) || "pebblestone".equals(surface)) {
			result += 9;
		} else if ("grass".equals(surface)) {
			result += 10;
		} else if ("cobblestone".equals(surface)) {
			result += 11;
		} else if ("gravel".equals(surface)) {
			result += 12;
		} else if ("stone".equals(surface) || "rock".equals(surface) || "rocky".equals(surface)) {
			result += 13;
		} else if ("dirt".equals(surface)) {
			result += 14;
		} else if ("salt".equals(surface) || "ice".equals(surface) || "snow".equals(surface)) {
			result += 15;
		} else if ("sand".equals(surface)) {
			result += 16;
		} else if ("mud".equals(surface)) {
			result += 18;
		}
		if ("excellent".equals(smoothness)) {
			result -= 5;
		} else if ("good".equals(smoothness)) {
			if (("track".equals(highway) || ("path".equals(highway))) && (surface == null)) {
				result = 2;
			} else result -= 2;
		} else if ("intermediate".equals(smoothness)) {
			if (("track".equals(highway) || ("path".equals(highway))) && (surface == null)) {
				result = 3;
			}
		} else if ("bad".equals(smoothness)) {
			if (("track".equals(highway) || ("path".equals(highway))) && (surface == null)) {
				result = 4;
			} else if ("asphalt".equals(surface)) {
				result += 7;
		} else result += 6;
		} else if ("very_bad".equals(smoothness)) {
			if (("track".equals(highway) || ("path".equals(highway))) && (surface == null)) {
				result = 5;
			} else if ("asphalt".equals(surface)) {
				result += 12;
		} else result += 7;
		} else if ("horrible".equals(smoothness)) {
			if (("track".equals(highway) || ("path".equals(highway))) && (surface == null)) {
				result = 6;
			} else if ("asphalt".equals(surface)) {
				result += 19;
		} else result += 9;
		} else if ("very_horrible".equals(smoothness)) {
			if (("track".equals(highway) || ("path".equals(highway))) && (surface == null)) {
				result = 7;
			} else if ("asphalt".equals(surface)) {
				result += 22;
		} else result += 11;
		} else if ("impassable".equals(smoothness)) {
			if (("track".equals(highway) || ("path".equals(highway))) && (surface == null)) {
				result = 9;
			} else if ("asphalt".equals(surface)) {
				result += 26;
		} else result += 12;
		}
		if (surface == null) {
			if ("grade1".equals(tracktype)) {
				result += 1;
			} else if ("grade2".equals(tracktype)) {
				result += 3;
			} else if ("grade3".equals(tracktype)) {
				result += 7;
			} else if ("grade4".equals(tracktype)) {
				result += 10;
			} else if ("grade5".equals(tracktype)) {
				result += 15;
			}
		}
		if (("motorway".equals(highway) || ("motorway_link".equals(highway)) || ("trunk".equals(highway)) || ("trunk_link".equals(highway))
			|| ("primary".equals(highway)) || ("primary_link".equals(highway)) || ("secondary".equals(highway))
			|| ("secondary_link".equals(highway)) || ("tertiary".equals(highway)) || ("tertiary_link".equals(highway))
			|| ("unclassified".equals(highway)) || ("residential".equals(highway)) || ("service".equals(highway))
			|| ("pedestrian".equals(highway)) || ("living_street".equals(highway))
			|| ("footway".equals(highway)) || ("cycleway".equals(highway)))
			&& (surface == null) && (smoothness == null)) {
			result = 100;
		}
		if (("track".equals(highway) || "path".equals(highway)) && ((surface == null) && (smoothness == null) && (tracktype == null))) {
			result = 100;
		}
		if ("path".equals(highway)) {
			if ("designated".equals(bicycle)) {
				result = 0;
			} else if ("designated".equals(foot)) {
				result = 2;
			}
		}
		if (result < 0) {
			result = 0;
		}
		return result;
	}


	private void addOsmcNewTags(Map<String, String> propogated, String[] tokens) {
		if (tokens.length > 0) {
			String wayColor = tokens[0]; // formatColorToPalette(tokens[0], true);
			propogated.put("osmc_waycolor", wayColor);
			if (tokens.length > 1) {
				String bgColor = tokens[1]; // formatColorToPalette(tokens[1], true);
				propogated.put("osmc_background", bgColor);
				propogated.put("osmc_stub_name", ".");
				if (tokens.length > 2) {
					String shpVl = tokens[2]; // formatColorToPalette(tokens[1], true);
					propogated.put("osmc_foreground", shpVl);
					if (tokens.length > 3) {
						String shp2Vl = tokens[3];
						propogated.put("osmc_foreground2", shp2Vl);
						if (tokens.length > 4) {
							String txtVl = tokens[4];
							propogated.put("osmc_text", txtVl);
							propogated.put("osmc_text_symbol", txtVl);
							if (tokens.length > 5) {
								String txtcolorVl = tokens[5];
								propogated.put("osmc_textcolor", txtcolorVl);
							}
						}
					}
				}
			}
		}
	}



	private void osmcBackwardCompatility(Map<String, String> propogated, String[] tokens) {
		if (tokens.length > 0) {
			String wayColor = formatColorToPalette(tokens[0], true);
			propogated.put("osmc_symbol_" + wayColor, "");
			propogated.put("osmc_symbol", wayColor);
			if (tokens.length > 1) {
				String bgColor = tokens[1];
				String fgColorPrefix = "";
				String shape = "";
				if (tokens.length > 2) {
					String fgColor = tokens[2];
					if (precolors.containsKey(fgColor)) {
						shape = fgColor;
						fgColor = precolors.get(fgColor);
					} else if (fgColor.indexOf('_') >= 0) {
						final int i = fgColor.indexOf('_');
						shape = fgColor.substring(i + 1).toLowerCase();
						fgColor = fgColor.substring(0, i);
					}
					fgColorPrefix = "_" + fgColor;
				}
				String shpValue = "none";
				if (shape.length() != 0) {
					shpValue = barValues.contains(shape) ? "bar" : "circle";
				}
				propogated.put("osmc_shape", shpValue);
				String symbol = "osmc_symbol_" + formatColorToPalette(bgColor, true) + fgColorPrefix + "_name";
				String name = "."; // "\u00A0";
				// if (tokens.length > 3 && tokens[3].trim().length() > 0) {
				// name = tokens[3];
				// }

				propogated.put(symbol, name);
			}
		}
	}

	public static class MapRouteTag {
		boolean relation;
		String tag;
		String value;
		String tag2;
		String value2;
		String type;
		boolean register;
		boolean amend;
		boolean base;
		boolean text;
		boolean replace;

	}


	public enum EntityConvertApplyType {
		MAP,
		ROUTING,
		POI
	}

	private enum EntityConvertType {
		TAG_TRANSFORM,
		SPLIT
	}
	public static class EntityConvert {
		public boolean verbose;
		public TagValuePattern fromTag ;
		public EntityConvertType type;
		public EnumSet<EntityConvertApplyType> applyToType;
		public EnumSet<EntityType> applyTo ;
		public List<String> ifRegionName = new ArrayList<String>();
		public List<String> ifNotRegionName = new ArrayList<String>();
		public List<TagValuePattern> ifStartsTags = new ArrayList<MapRenderingTypes.TagValuePattern>();
		public List<TagValuePattern> ifNotStartsTags = new ArrayList<MapRenderingTypes.TagValuePattern>();
		public List<TagValuePattern> ifEndsTags = new ArrayList<MapRenderingTypes.TagValuePattern>();
		public List<TagValuePattern> ifNotEndsTags = new ArrayList<MapRenderingTypes.TagValuePattern>();
		public List<TagValuePattern> ifContainsTags = new ArrayList<MapRenderingTypes.TagValuePattern>();
		public List<TagValuePattern> ifNotContainsTags = new ArrayList<MapRenderingTypes.TagValuePattern>();
		public List<TagValuePattern> ifTags = new ArrayList<MapRenderingTypes.TagValuePattern>();
		public List<TagValuePattern> ifTagsLess = new ArrayList<MapRenderingTypes.TagValuePattern>();
		public List<TagValuePattern> ifTagsNotLess = new ArrayList<MapRenderingTypes.TagValuePattern>();
		public List<TagValuePattern> ifNotTags = new ArrayList<MapRenderingTypes.TagValuePattern>();
		public List<TagValuePattern> toTags = new ArrayList<MapRenderingTypes.TagValuePattern>();
	}

}
