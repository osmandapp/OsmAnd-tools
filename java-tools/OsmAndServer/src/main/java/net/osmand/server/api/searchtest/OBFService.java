package net.osmand.server.api.searchtest;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import net.osmand.binary.*;
import net.osmand.binary.BinaryMapIndexReader.TagValuePair;
import net.osmand.data.*;
import net.osmand.osm.MapPoiTypes;
import net.osmand.osm.PoiCategory;
import net.osmand.server.api.services.OsmAndMapsService;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

import java.io.*;
import java.text.Normalizer;
import java.util.*;
import java.util.regex.Pattern;

public interface OBFService extends BaseService {
	int BASE_POI_SHIFT = BinaryMapPoiReaderAdapter.SHIFT_BITS_CATEGORY;
	int FINAL_POI_SHIFT = BinaryMapIndexReader.SHIFT_COORDINATES;
	int BASE_POI_ZOOM = 31 - BASE_POI_SHIFT;
	int FINAL_POI_ZOOM = 31 - FINAL_POI_SHIFT;
	int CATEGORY_MASK = (1 << BinaryMapPoiReaderAdapter.SHIFT_BITS_CATEGORY) - 1;
	int INDEX_TOKEN_CACHE_LIMIT = 32;
	Map<String, CachedIndexTokens> INDEX_TOKENS_CACHE = Collections.synchronizedMap(
			new LinkedHashMap<>(16, 0.75f, true) {
				@Override
				protected boolean removeEldestEntry(Map.Entry<String, CachedIndexTokens> eldest) {
					return size() > INDEX_TOKEN_CACHE_LIMIT;
				}
			});

	Map<String, Map<Integer, InspectorService.ObfFieldSpec>> OBF_MESSAGE_SCHEMA = buildObfMessageSchema();

	Map<Integer, String> OBF_STRUCTURE_FIELD_NAMES = buildObfStructureFieldNames();

	record ObfFieldSpec(String fieldName, String childMessageType, InspectorService.ObfLengthType lengthType, boolean packedVarInt, boolean repeated) {}

	static String toLowerCamelFromUpperUnderscore(String upperUnderscore) {
		String[] parts = upperUnderscore.toLowerCase(Locale.ROOT).split("_");
		if (parts.length == 0) {
			return upperUnderscore;
		}
		StringBuilder sb = new StringBuilder(parts[0]);
		for (int i = 1; i < parts.length; i++) {
			String p = parts[i];
			if (p.isEmpty()) {
				continue;
			}
			sb.append(Character.toUpperCase(p.charAt(0)));
			if (p.length() > 1) {
				sb.append(p.substring(1));
			}
		}
		return sb.toString();
	}
	
	static Map<Integer, String> buildObfStructureFieldNames() {
		Map<Integer, String> out = new HashMap<>();
		try {
			for (java.lang.reflect.Field f : OsmandOdb.OsmAndStructure.class.getDeclaredFields()) {
				if (!java.lang.reflect.Modifier.isStatic(f.getModifiers())) {
					continue;
				}
				if (f.getType() != int.class) {
					continue;
				}
				String name = f.getName();
				if (!name.endsWith("_FIELD_NUMBER")) {
					continue;
				}
				int fieldNumber = f.getInt(null);
				String base = name.substring(0, name.length() - "_FIELD_NUMBER".length());
				String fieldName = toLowerCamelFromUpperUnderscore(base);
				out.put(fieldNumber, fieldName);
			}
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		}
		return out;
	}
	OsmAndMapsService getMapsService();
	String getSearchTestDatasourceUrl();

	default List<String> getOBFs(Double radius, Double lat, Double lon) throws IOException {
		synchronized (INDEX_TOKENS_CACHE) {
			INDEX_TOKENS_CACHE.clear();
		}
		radius = radius == null ? 1.5 : radius;
		if (lat == null || lon == null) {
			return getMapsService().getOBFs();
		}
		double latPlusRadius = lat + radius;
		double lonMinusRadius = lon - radius;
		double latMinusRadius = lat - radius;
		double lonPlusRadius = lon + radius;
		QuadRect points = getMapsService().points(null,
				new LatLon(latPlusRadius, lonMinusRadius),
				new LatLon(latMinusRadius, lonPlusRadius));

		List<String> obfList = new ArrayList<>();
		List<OsmAndMapsService.BinaryMapIndexReaderReference> list = getMapsService().getObfReaders(
				points, OsmAndMapsService.ObfReason.SEARCH_TEST.value());
		for (OsmAndMapsService.BinaryMapIndexReaderReference ref : list)
			obfList.add(ref.getFile().getAbsolutePath());
		return obfList;
	}

	record CachedIndexTokens(String cacheKey, long fileLength, long lastModified, List<IndexToken> tokens) {}

    class BinaryMapIndexReaderExt extends BinaryMapIndexReader {
        BinaryMapIndexReaderExt(final RandomAccessFile raf, File file) throws IOException {
            super(raf, file);
        }

        CodedInputStream getInputStream() {
            return codedIS;
        }
    }

	List<String> OBF_CONTINENTS = List.of(
			"asia",
			"europe",
			"africa",
			"antarctica",
			"australia-oceania",
			"northamerica",
			"centralamerica",
			"southamerica");

	record SuffixMetrics(int dict, int integer, int literal, int extra) {}
	record IndexToken(String name, AddressRef[] addressRefs, int[] poiRefs, int[] poiAtomRefs, int[] poiAtomSizes,
	                  boolean isCommon, boolean isFrequent, String obf, SuffixMetrics suffixMetrics) {
		public IndexToken(String name, AddressRef[] addressRefs, int[] poiRefs, int[] poiAtomRefs, int[] poiAtomSizes, boolean isCommon, boolean isFrequent) {
			this(name, addressRefs, poiRefs, poiAtomRefs, poiAtomSizes, isCommon, isFrequent, null, new SuffixMetrics(0, 0, 0, 0));
		}
		public IndexToken(String name, AddressRef[] addressRefs, int[] poiRefs, int[] poiAtomRefs, int[] poiAtomSizes,
		                  boolean isCommon, boolean isFrequent, SuffixMetrics suffixMetrics) {
			this(name, addressRefs, poiRefs, poiAtomRefs, poiAtomSizes, isCommon, isFrequent, null, suffixMetrics);
		}
	}
	record ObfFileInfo(String path, String name, String continent, String country, String region, long lastModified, long size) {}
	record IndexTokenPage(List<IndexToken> content, int pageToShow, int pageSizeLimit, long totalElements, int totalPages, IndexTokenSummary summary) {}
	record IndexTokenSummary(int poiSum, int addressSum, int commonSum, int frequentSum,
	                         int dictSuffixSum, int integerSuffixSum, int literalSuffixSum, int extraSuffixSum,
	                         int poiMax, int addressMax,
	                         int dictSuffixMax, int integerSuffixMax, int literalSuffixMax, int extraSuffixMax) {}
	record IndexTokenBuilder(String name, int[] addressOffsets, int[] addressSuffixIndexes, AddressRef[] addressRefs,
	                         int[] poiRefs, int[] poiAtomRefs, int[] poiAtomSizes, SuffixMetrics suffixMetrics) {}
	record AddressRef(int shiftToIndex, int shiftToCityIndex, int objectOffset, int cityOffset, int typeIndex, int atomSize) {}

	record ObjectAddress(int sequenceId, String name, LatLon point, Map<String, String> commonTags,
	                     boolean isPoi, boolean isMatched,
	                     boolean isInvalidAtom, boolean isAlone, String type, Long osmId,
	                     String osmType, int payloadOffset, int payloadSize, int sourceOffset, String obf) {
		public ObjectAddress(int sequenceId, String name, LatLon point, Map<String, String> commonTags,
		                     boolean isPoi, boolean isMatched,
		                     boolean isInvalidAtom, boolean isAlone, String type, Long osmId,
		                     String osmType, int payloadOffset, int payloadSize, int sourceOffset) {
			this(sequenceId, name, point, commonTags, isPoi, isMatched, isInvalidAtom, isAlone, type, osmId,
					osmType, payloadOffset, payloadSize, sourceOffset, null);
		}
	}
	record ObjectAddressPage(List<ObjectAddress> content, int pageToShow, int pageSizeLimit, long totalElements, int totalPages, int[] countMetrics, int[] sizeMetrics, int aloneCount, int aloneSize) {}
	record ObjectAddressStats(int size, int count) {}
	record PoiTokenRefs(Set<Integer> offsets, List<Integer> atomSizes) {}
	record PoiCategoryMeta(String type, String subtype) {}
	record GenerateDbProgress(String status, String obfName, int obfIndex, int totalObfs, int processedTokens,
	                          int totalTokens, long elapsedMs, long estimatedMs, String error,
	                          List<GenerateDbObfProgress> obfs) {}
	record GenerateDbObfProgress(String obfName, int obfIndex, int totalTokens, int processedTokens,
	                             long elapsedMs, long estimatedMs, String status) {}
	record GenerateDbObfTokens(String obf, String obfName, int obfIndex, long startMs, List<IndexToken> tokens) {}
	record GenerateDbTokenObjects(String obf, String obfName, int obfIndex, long startMs, IndexToken token, ObjectAddressPage objectsPage) {}
	record GenerateDbTokenChunk(String obf, String obfName, int obfIndex, long startMs, List<GenerateDbTokenObjects> tokens) {}
	record Datasource(String name, long size, long lastModified, boolean valid, String error) {}
	record DbTagName(String name, long objects, boolean isSkipped) {}
	record DbTagValue(String value, long objects_count) {}
	record DbToken(long id, String name, long matched, long alone, boolean isCommon, boolean isFrequent, boolean isGenerated) {}
	record DbTokenSummary(long matchedSum, long aloneSum, long commonSum, long frequentSum, long generatedSum, long matchedMax, long aloneMax) {}
	record DbTokenPage(List<DbToken> content, int pageToShow, int pageSizeLimit, long totalElements, int totalPages, DbTokenSummary summary) {}
	record DbObjectToken(long id, String name, boolean isCommon, boolean isFrequent, boolean isGenerated, String obfName) {}
	record DbObjectTokenPage(List<DbObjectToken> content, int pageToShow, int pageSizeLimit, long totalElements, int totalPages) {}
	record DbObject(int sequenceId, String name, LatLon point, Map<String, String> commonTags,
	                String type, Long osmId, String osmType,
	                boolean isAlone, String obfName, long tokens) {}
	record DbObjectPage(List<DbObject> content, int pageToShow, int pageSizeLimit, long totalElements, int totalPages) {}
	record DbReportDistribution(String bucket, int ord, long tokens, long postings, long tokensNew, long postingsNew) {}
	record DbReportTagHit(String tag, long hits, double sharePct) {}
	record DbReportPruneToken(String name, boolean isCommon, boolean isFrequent, long matched, long alone,
	                          double cumulativePct, List<DbReportTagHit> topTags) {}
	record TestCaseObject(String name, String type, LatLon point, long tokens,
	                      long commonFrequentTokens, long commonTokens, long frequentTokens,
	                      long newTokens, double proneScore, String topCommonFrequentTokens) {}
	record DbReport(long totalTokens, long totalPostings, List<DbReportDistribution> distribution,
	                List<DbReportPruneToken> pruning, List<TestCaseObject> mainWordInconsistency) {}
	@FunctionalInterface
	interface GenerateDbProgressListener {
		void onProgress(GenerateDbProgress progress);
	}
	record CityAddress(String name, LatLon point, List<StreetAddress> streets, int streetsCount, String type, String obf) {
		public CityAddress(String name, LatLon point, List<StreetAddress> streets, int streetsCount, String type) {
			this(name, point, streets, streetsCount, type, null);
		}
	}
	record PoiAddress(String name, LatLon point, String value, String obf) {
		public PoiAddress(String name, LatLon point, String value) {
			this(name, point, value, null);
		}
	}
	record HouseAddress(String name, LatLon point, String obf) {
		public HouseAddress(String name, LatLon point) {
			this(name, point, null);
		}
	}
	record StreetAddress(String name, LatLon point, List<HouseAddress> houses, int houseCount, String obf) {
		public StreetAddress(String name, LatLon point, List<HouseAddress> houses, int houseCount) {
			this(name, point, houses, houseCount, null);
		}
	}

	default List<ObfFileInfo> getObfFileInfos() throws IOException {
		List<ObfFileInfo> result = new ArrayList<>();
		for (String obf : getMapsService().getOBFs()) {
			if (OBFService.getObfFileName(obf).startsWith("World_base")) {
				continue;
			}
			result.add(parseObfFileInfo(obf));
		}
		result.sort(Comparator.comparing(ObfFileInfo::name, String.CASE_INSENSITIVE_ORDER));
		return result;
	}

	default ObfFileInfo parseObfFileInfo(String obf) {
		String name = OBFService.getObfFileName(obf);
		String baseName = stripObfExtension(name);
		String[] rawParts = baseName.split("_");
		List<String> parts = new ArrayList<>();
		for (String rawPart : rawParts) {
			if (!Algorithms.isEmpty(rawPart)) {
				parts.add(rawPart);
			}
		}
		while (!parts.isEmpty() && parts.get(parts.size() - 1).matches("\\d+")) {
			parts.remove(parts.size() - 1);
		}
		String continent = "";
		int continentIndex = -1;
		for (int i = parts.size() - 1; i >= 0; i--) {
			String part = parts.get(i).toLowerCase(Locale.ROOT);
			if (OBF_CONTINENTS.contains(part)) {
				continent = part;
				continentIndex = i;
				break;
			}
		}
		String country = continentIndex > 0 ? parts.get(0) : (!parts.isEmpty() ? parts.get(0) : "");
		String region = "";
		if (continentIndex > 1) {
			region = String.join("_", parts.subList(1, continentIndex));
		} else if (continentIndex < 0 && parts.size() > 1) {
			region = String.join("_", parts.subList(1, parts.size()));
		}
		File file = new File(obf);
		return new ObfFileInfo(obf, name, continent, normalizeObfDisplayName(country), normalizeObfDisplayName(region),
				file.lastModified(), file.length());
	}

	static String getObfFileName(String obf) {
		if (obf == null) {
			return "";
		}
		String normalized = obf.replace('\\', '/');
		int index = normalized.lastIndexOf('/');
		return index >= 0 ? normalized.substring(index + 1) : normalized;
	}

	static String stripObfExtension(String name) {
		if (name == null) {
			return "";
		}
		String lower = name.toLowerCase(Locale.ROOT);
		return lower.endsWith(".obf") ? name.substring(0, name.length() - 4) : name;
	}

	static String normalizeObfDisplayName(String value) {
		if (Algorithms.isEmpty(value)) {
			return "";
		}
		String[] words = value.replace('_', ' ').split("\\s+");
		List<String> normalizedWords = new ArrayList<>();
		for (String word : words) {
			if (Algorithms.isEmpty(word)) {
				continue;
			}
			if ("us".equalsIgnoreCase(word)) {
				normalizedWords.add("US");
			} else {
				normalizedWords.add(word);
			}
		}
		return String.join(" ", normalizedWords);
	}

	final class RawPoiObject {
		String name = "";
		String nameEn = "";
		long id;
		String type = "";
		String subType = "";
		String openingHours = "";
		String site = "";
		String phone = "";
		String description = "";
		final LinkedHashMap<String, List<String>> decodedTextTags = new LinkedHashMap<>();
		final LinkedHashMap<String, List<String>> decodedSubcategories = new LinkedHashMap<>();
		final List<PoiCategoryMeta> decodedCategories = new ArrayList<>();
		final LinkedHashMap<Integer, List<TagValuePair>> tagGroups = new LinkedHashMap<>();
		double lat;
		double lon;

		void addDecodedTextTag(String tag, String value) {
			if (Algorithms.isEmpty(tag) || Algorithms.isEmpty(value)) {
				return;
			}
			decodedTextTags.computeIfAbsent(tag, ignored -> new ArrayList<>()).add(value);
			if (Amenity.NAME.equals(tag) && Algorithms.isEmpty(name)) {
				name = value;
			} else if (("name:en".equals(tag) || "name_en".equals(tag)) && Algorithms.isEmpty(nameEn)) {
				nameEn = value;
			}
		}

		void addDecodedSubcategory(String tag, String value) {
			if (Algorithms.isEmpty(tag) || Algorithms.isEmpty(value)) {
				return;
			}
			decodedSubcategories.computeIfAbsent(tag, ignored -> new ArrayList<>()).add(value);
			addDecodedTextTag(tag, value);
		}
	}

	default PoiAddress findRawValue(RawPoiObject objectRecord, Pattern poiPattern, Pattern normalizedPoiPattern) {
		if (objectRecord == null || poiPattern == null) {
			return null;
		}
		LatLon location = new LatLon(objectRecord.lat, objectRecord.lon);
		String displayName = Algorithms.isEmpty(objectRecord.name) ? objectRecord.nameEn : objectRecord.name;
		if (matchesPattern(objectRecord.name, poiPattern, normalizedPoiPattern)) {
			return new PoiAddress(objectRecord.name, location, "name-> " + objectRecord.name);
		}
		if (matchesPattern(objectRecord.nameEn, poiPattern, normalizedPoiPattern)) {
			return new PoiAddress(objectRecord.nameEn, location, "name:en-> " + objectRecord.nameEn);
		}
		for (Map.Entry<String, List<String>> entry : objectRecord.decodedTextTags.entrySet()) {
			for (String value : entry.getValue()) {
				if (matchesPattern(value, poiPattern, normalizedPoiPattern)) {
					return new PoiAddress(displayName, location, entry.getKey() + "-> " + value);
				}
			}
		}
		return null;
	}

	default List<PoiAddress> findPoiAddressesRaw(RandomAccessFile randomAccessFile,
	                                                    BinaryMapIndexReader index,
	                                                    BinaryMapPoiReaderAdapter.PoiRegion poiRegion,
	                                                    Pattern poiPattern,
	                                                    Pattern normalizedPoiPattern) throws IOException {
		List<PoiAddress> results = new ArrayList<>();
		index.initCategories(poiRegion);
		CodedInputStream codedIS = CodedInputStream.newInstance(randomAccessFile);
		codedIS.setSizeLimit(CodedInputStream.MAX_DEFAULT_SIZE_LIMIT);
		Map<Integer, List<TagValuePair>> tagGroups = preloadPoiTagGroups(codedIS, poiRegion);
		codedIS.seek(poiRegion.getFilePointer());
		long oldLimit = codedIS.pushLimitLong(poiRegion.getLength());
		try {
			while (true) {
				int tagValue = codedIS.readTag();
				int tag = WireFormat.getTagFieldNumber(tagValue);
				switch (tag) {
					case 0:
						return results;
					case OsmandOdb.OsmAndPoiIndex.POIDATA_FIELD_NUMBER: {
						long length = InspectorService.readFixed32Length(codedIS);
						long innerOldLimit = codedIS.pushLimitLong(length);
						findPoiAddressesInBoxData(codedIS, poiRegion, tagGroups, poiPattern, normalizedPoiPattern, results);
						codedIS.popLimit(innerOldLimit);
						break;
					}
					default:
						InspectorService.skipUnknownField(codedIS, tagValue);
						break;
				}
			}
		} finally {
			codedIS.popLimit(oldLimit);
		}
	}

	default void findPoiAddressesInBoxData(CodedInputStream codedIS,
			BinaryMapPoiReaderAdapter.PoiRegion poiRegion,
			Map<Integer, List<TagValuePair>> tagGroups,
			Pattern poiPattern,
			Pattern normalizedPoiPattern,
			List<PoiAddress> results) throws IOException {
		int x = 0;
		int y = 0;
		int zoom = 0;
		while (true) {
			int tagValue = codedIS.readTag();
			int tag = WireFormat.getTagFieldNumber(tagValue);
			switch (tag) {
				case 0:
					return;
				case OsmandOdb.OsmAndPoiBoxData.X_FIELD_NUMBER:
					x = codedIS.readUInt32();
					break;
				case OsmandOdb.OsmAndPoiBoxData.Y_FIELD_NUMBER:
					y = codedIS.readUInt32();
					break;
				case OsmandOdb.OsmAndPoiBoxData.ZOOM_FIELD_NUMBER:
					zoom = codedIS.readUInt32();
					break;
				case OsmandOdb.OsmAndPoiBoxData.POIDATA_FIELD_NUMBER: {
					int len = codedIS.readRawVarint32();
					long oldLimit = codedIS.pushLimitLong(len);
					RawPoiObject objectRecord = readRawPoiObject(codedIS, x, y, zoom, poiRegion, tagGroups);
					if (objectRecord != null) {
						PoiAddress value = findRawValue(objectRecord, poiPattern, normalizedPoiPattern);
						if (value != null) {
							results.add(value);
						}
					}
					codedIS.popLimit(oldLimit);
					break;
				}
				default:
					codedIS.skipField(tagValue);
					break;
			}
		}
	}

	default RawPoiObject readRawPoiObject(CodedInputStream codedIS,
			int parentX,
			int parentY,
			int parentZoom,
			BinaryMapPoiReaderAdapter.PoiRegion poiRegion) throws IOException {
		return readRawPoiObject(codedIS, parentX, parentY, parentZoom, poiRegion, Collections.emptyMap());
	}

	default RawPoiObject readRawPoiObject(CodedInputStream codedIS,
			int parentX,
			int parentY,
			int parentZoom,
			BinaryMapPoiReaderAdapter.PoiRegion poiRegion,
			Map<Integer, List<TagValuePair>> tagGroupsById) throws IOException {
		RawPoiObject record = new RawPoiObject();
		List<String> textTags = new ArrayList<>();
		MapPoiTypes poiTypes = MapPoiTypes.getDefault();
		int x = 0;
		int y = 0;
		int precisionXY = 0;
		boolean hasLocation = false;
		PoiCategory amenityType = null;
		while (true) {
			int tagValue = codedIS.readTag();
			int tag = WireFormat.getTagFieldNumber(tagValue);
			if (amenityType == null && (tag > OsmandOdb.OsmAndPoiBoxDataAtom.CATEGORIES_FIELD_NUMBER || tag == 0)) {
				InspectorService.consumeRemainingInLimit(codedIS);
				return null;
			}
			switch (tag) {
				case 0:
					if (!hasLocation) {
						return null;
					}
					if (precisionXY != 0) {
						int[] xy = MapUtils.calculateFinalXYFromBaseAndPrecisionXY(BASE_POI_ZOOM, FINAL_POI_ZOOM,
								precisionXY, x >> BASE_POI_SHIFT, y >> BASE_POI_SHIFT, true);
						int x31 = xy[0] << FINAL_POI_SHIFT;
						int y31 = xy[1] << FINAL_POI_SHIFT;
						record.lat = MapUtils.get31LatitudeY(y31);
						record.lon = MapUtils.get31LongitudeX(x31);
					} else {
						record.lat = MapUtils.get31LatitudeY(y);
						record.lon = MapUtils.get31LongitudeX(x);
					}
					return record;
				case OsmandOdb.OsmAndPoiBoxDataAtom.DX_FIELD_NUMBER:
					x = (codedIS.readSInt32() + (parentX << (BASE_POI_ZOOM - parentZoom))) << BASE_POI_SHIFT;
					hasLocation = true;
					break;
				case OsmandOdb.OsmAndPoiBoxDataAtom.DY_FIELD_NUMBER:
					y = (codedIS.readSInt32() + (parentY << (BASE_POI_ZOOM - parentZoom))) << BASE_POI_SHIFT;
					hasLocation = true;
					break;
				case OsmandOdb.OsmAndPoiBoxDataAtom.CATEGORIES_FIELD_NUMBER: {
					int categoryValue = codedIS.readUInt32();
					int subcategoryId = categoryValue >> BinaryMapPoiReaderAdapter.SHIFT_BITS_CATEGORY;
					int categoryId = categoryValue & CATEGORY_MASK;
					PoiCategory categoryType = poiTypes.getOtherPoiCategory();

					String subtype = "";
					if (categoryId < poiRegion.getSubcategories().size() && categoryId < poiRegion.getCategories().size()) {
						String categoryName = poiRegion.getCategories().get(categoryId);
						List<String> subcategories = poiRegion.getSubcategories().get(categoryId);
						if (categoryName != null) {
							categoryType = poiTypes.getPoiCategoryByName(categoryName.toLowerCase(Locale.ROOT), true);
						}
						if (subcategoryId < subcategories.size()) {
							subtype = subcategories.get(subcategoryId);
						}
					}
					subtype = poiTypes.replaceDeprecatedSubtype(categoryType, subtype);
					if (!poiTypes.isTypeForbidden(subtype)) {
						String categoryKey = categoryType == null ? "" : safeString(categoryType.getKeyName());
						if (!Algorithms.isEmpty(categoryKey) || !Algorithms.isEmpty(subtype)) {
							record.decodedCategories.add(new PoiCategoryMeta(categoryKey, safeString(subtype)));
						}
						if (amenityType == null) {
							amenityType = categoryType;
							record.type = categoryKey;
							record.subType = safeString(subtype);
						} else if (amenityType == categoryType && !Algorithms.isEmpty(subtype)) {
							record.subType = Algorithms.isEmpty(record.subType) ? subtype : record.subType + ";" + subtype;
						}
					}
					break;
				}
				case OsmandOdb.OsmAndPoiBoxDataAtom.SUBCATEGORIES_FIELD_NUMBER: {
					StringBuilder valueBuilder = new StringBuilder();
					BinaryMapPoiReaderAdapter.PoiSubType poiSubtype = poiRegion.getSubtypeFromId(codedIS.readUInt32(), valueBuilder);
					if (poiSubtype != null && !poiRegion.getTopIndexSubTypes().contains(poiSubtype)) {
						record.addDecodedSubcategory(poiSubtype.name, decodePoiString(valueBuilder.toString()));
					}
					break;
				}

				case OsmandOdb.OsmAndPoiBoxDataAtom.NAME_FIELD_NUMBER:
					record.name = decodePoiString(codedIS.readString());
					record.addDecodedTextTag(Amenity.NAME, record.name);
					break;
				case OsmandOdb.OsmAndPoiBoxDataAtom.NAMEEN_FIELD_NUMBER:
					record.nameEn = decodePoiString(codedIS.readString());
					record.addDecodedTextTag("name:en", record.nameEn);
					break;
				case OsmandOdb.OsmAndPoiBoxDataAtom.ID_FIELD_NUMBER:
					record.id = codedIS.readUInt64();
					break;
				case OsmandOdb.OsmAndPoiBoxDataAtom.OPENINGHOURS_FIELD_NUMBER:
					record.openingHours = decodePoiString(codedIS.readString());
					break;
				case OsmandOdb.OsmAndPoiBoxDataAtom.SITE_FIELD_NUMBER:
					record.site = decodePoiString(codedIS.readString());
					break;
				case OsmandOdb.OsmAndPoiBoxDataAtom.PHONE_FIELD_NUMBER:
					record.phone = decodePoiString(codedIS.readString());
					break;
				case OsmandOdb.OsmAndPoiBoxDataAtom.NOTE_FIELD_NUMBER:
					record.description = decodePoiString(codedIS.readString());
					break;
				case OsmandOdb.OsmAndPoiBoxDataAtom.TEXTCATEGORIES_FIELD_NUMBER: {
					String tagName = getPoiSubtypeTagName(codedIS.readUInt32(), poiRegion);
					if (!tagName.isEmpty()) {
						textTags.add(tagName);
					}
					break;
				}
				case OsmandOdb.OsmAndPoiBoxDataAtom.TEXTVALUES_FIELD_NUMBER: {
					String textValue = decodePoiString(codedIS.readString());
					String textTag = textTags.isEmpty() ? "" : textTags.remove(0);
					record.addDecodedTextTag(textTag, textValue);
					break;
				}
				case OsmandOdb.OsmAndPoiBoxDataAtom.PRECISIONXY_FIELD_NUMBER:
					if (hasLocation) {
						precisionXY = codedIS.readInt32();
					} else {
						codedIS.readInt32();
					}
					break;
				case OsmandOdb.OsmAndPoiBoxDataAtom.TAGGROUPS_FIELD_NUMBER: {
					int length = codedIS.readRawVarint32();
					long oldLimit = codedIS.pushLimitLong(length);
					while (codedIS.getBytesUntilLimit() > 0) {
						int tagGroupId = codedIS.readUInt32();
						List<TagValuePair> tagValues = tagGroupsById == null ? null : tagGroupsById.get(tagGroupId);
						if (tagValues != null && !tagValues.isEmpty()) {
							record.tagGroups.put(tagGroupId, tagValues);
						}
					}
					codedIS.popLimit(oldLimit);
					break;
				}
				default:
					codedIS.skipField(tagValue);
					break;
			}
		}
	}

	default Map<Integer, List<TagValuePair>> preloadPoiTagGroups(CodedInputStream codedIS,
			BinaryMapPoiReaderAdapter.PoiRegion poiRegion) throws IOException {
		Map<Integer, List<TagValuePair>> tagGroups = new LinkedHashMap<>();
		codedIS.seek(poiRegion.getFilePointer());
		long oldLimit = codedIS.pushLimitLong(poiRegion.getLength());
		try {
			while (true) {
				int tagValue = codedIS.readTag();
				int tag = WireFormat.getTagFieldNumber(tagValue);
				switch (tag) {
					case 0:
						return tagGroups;
					case OsmandOdb.OsmAndPoiIndex.BOXES_FIELD_NUMBER: {
						long length = InspectorService.readFixed32Length(codedIS);
						long boxOldLimit = codedIS.pushLimitLong(length);
						readPoiBoxTagGroups(codedIS, tagGroups);
						codedIS.popLimit(boxOldLimit);
						break;
					}
					default:
						InspectorService.skipUnknownField(codedIS, tagValue);
						break;
				}
			}
		} finally {
			codedIS.popLimit(oldLimit);
		}
	}

	default void readPoiBoxTagGroups(CodedInputStream codedIS,
			Map<Integer, List<TagValuePair>> tagGroups) throws IOException {
		while (true) {
			int tagValue = codedIS.readTag();
			int tag = WireFormat.getTagFieldNumber(tagValue);
			switch (tag) {
				case 0:
					return;
				case OsmandOdb.OsmAndPoiBox.TAGGROUPS_FIELD_NUMBER: {
					int length = codedIS.readRawVarint32();
					long oldLimit = codedIS.pushLimitLong(length);
					readPoiTagGroups(codedIS, tagGroups);
					codedIS.popLimit(oldLimit);
					break;
				}
				case OsmandOdb.OsmAndPoiBox.SUBBOXES_FIELD_NUMBER: {
					long length = InspectorService.readFixed32Length(codedIS);
					long oldLimit = codedIS.pushLimitLong(length);
					readPoiBoxTagGroups(codedIS, tagGroups);
					codedIS.popLimit(oldLimit);
					break;
				}
				default:
					InspectorService.skipUnknownField(codedIS, tagValue);
					break;
			}
		}
	}

	default void readPoiTagGroups(CodedInputStream codedIS,
			Map<Integer, List<TagValuePair>> tagGroups) throws IOException {
		while (true) {
			int tagValue = codedIS.readTag();
			int tag = WireFormat.getTagFieldNumber(tagValue);
			switch (tag) {
				case 0:
					return;
				case OsmandOdb.OsmAndPoiTagGroups.GROUPS_FIELD_NUMBER: {
					int length = codedIS.readRawVarint32();
					long oldLimit = codedIS.pushLimitLong(length);
					readPoiTagGroup(codedIS, tagGroups);
					codedIS.popLimit(oldLimit);
					break;
				}
				default:
					InspectorService.skipUnknownField(codedIS, tagValue);
					break;
			}
		}
	}

	default void readPoiTagGroup(CodedInputStream codedIS,
			Map<Integer, List<TagValuePair>> tagGroups) throws IOException {
		int id = -1;
		List<String> tagValues = new ArrayList<>();
		while (true) {
			int tagValue = codedIS.readTag();
			int tag = WireFormat.getTagFieldNumber(tagValue);
			switch (tag) {
				case 0:
					if (id > 0 && tagValues.size() > 1 && tagValues.size() % 2 == 0) {
						List<TagValuePair> pairs = new ArrayList<>();
						for (int i = 0; i < tagValues.size(); i += 2) {
							pairs.add(new TagValuePair(tagValues.get(i), tagValues.get(i + 1), -1));
						}
						tagGroups.put(id, pairs);
					}
					return;
				case OsmandOdb.OsmAndPoiTagGroup.ID_FIELD_NUMBER:
					id = codedIS.readUInt32();
					break;
				case OsmandOdb.OsmAndPoiTagGroup.TAGVALUES_FIELD_NUMBER:
					tagValues.add(decodePoiString(codedIS.readString()).intern());
					break;
				default:
					InspectorService.skipUnknownField(codedIS, tagValue);
					break;
			}
		}
	}

	default String getPoiSubtypeTagName(int subtypeId, BinaryMapPoiReaderAdapter.PoiRegion poiRegion) {
		StringBuilder valueBuilder = new StringBuilder();
		BinaryMapPoiReaderAdapter.PoiSubType poiSubtype = poiRegion.getSubtypeFromId(subtypeId, valueBuilder);
		if (poiSubtype != null && poiSubtype.text && poiSubtype.name != null) {
			return poiSubtype.name;
		}
		return "";
	}

	default String decodePoiString(String value) {
		return MapObject.unzipContent(safeString(value));
	}

	default String safeString(String value) {
		return value == null ? "" : value;
	}

	default Pattern compileNormalizedPattern(String regex) {
		if (regex == null) {
			return null;
		}
		String normalizedRegex = normalizeCaseInsensitiveText(regex);
		return Pattern.compile(normalizedRegex, Pattern.CASE_INSENSITIVE);
	}

	default boolean matchesPattern(String value, Pattern pattern, Pattern normalizedPattern) {
		if (value == null || pattern == null) {
			return false;
		}
		if (pattern.matcher(value).find()) {
			return true;
		}
		return normalizedPattern != null && normalizedPattern.matcher(normalizeCaseInsensitiveText(value)).find();
	}

	default String normalizeCaseInsensitiveText(String value) {
		if (value == null) {
			return "";
		}
		String normalizedValue = Normalizer.normalize(value, Normalizer.Form.NFKC).toLowerCase(Locale.ROOT);
		return normalizedValue.replace("\u0307", "");
	}

	static Map<String, Map<Integer, InspectorService.ObfFieldSpec>> buildObfMessageSchema() {
		Map<String, Map<Integer, InspectorService.ObfFieldSpec>> schema = new HashMap<>();
		addObfSpec(schema, "OsmAndStructure", 7, "addressIndex", "OsmAndAddressIndex", InspectorService.ObfLengthType.FIXED32, false, true);
		addObfSpec(schema, "OsmAndStructure", 4, "transportIndex", "OsmAndTransportIndex", InspectorService.ObfLengthType.FIXED32, false, true);
		addObfSpec(schema, "OsmAndStructure", 8, "poiIndex", "OsmAndPoiIndex", InspectorService.ObfLengthType.FIXED32, false, true);
		addObfSpec(schema, "OsmAndStructure", 6, "mapIndex", "OsmAndMapIndex", InspectorService.ObfLengthType.FIXED32, false, true);
		addObfSpec(schema, "OsmAndStructure", 9, "routingIndex", "OsmAndRoutingIndex", InspectorService.ObfLengthType.FIXED32, false, true);
		addObfSpec(schema, "OsmAndStructure", 10, "hhRoutingIndex", "OsmAndHHRoutingIndex", InspectorService.ObfLengthType.FIXED32, false, true);

		addObfSpec(schema, "OsmAndTileBox", 1, "left", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndTileBox", 2, "right", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndTileBox", 3, "top", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndTileBox", 4, "bottom", null, InspectorService.ObfLengthType.VAR_INT);

		addObfSpec(schema, "OsmAndPoiIndex", 1, "name", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiIndex", 2, "boundaries", "OsmAndTileBox", InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiIndex", 3, "categoriesTable", "OsmAndCategoryTable", InspectorService.ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "OsmAndPoiIndex", 4, "nameIndex", "OsmAndPoiNameIndex", InspectorService.ObfLengthType.FIXED32);
		addObfSpec(schema, "OsmAndPoiIndex", 5, "subtypesTable", "OsmAndSubtypesTable", InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiIndex", 6, "boxes", "OsmAndPoiBox", InspectorService.ObfLengthType.FIXED32, false, true);
		addObfSpec(schema, "OsmAndPoiIndex", 9, "poiData", "OsmAndPoiBoxData", InspectorService.ObfLengthType.FIXED32, false, true);
		addObfSpec(schema, "OsmAndSubtypesTable", 4, "subtypes", "OsmAndPoiSubtype", InspectorService.ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "OsmAndCategoryTable", 1, "category", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndCategoryTable", 3, "subcategories", null, InspectorService.ObfLengthType.VAR_INT);

		addObfSpec(schema, "OsmAndPoiNameIndex", 3, "table", "IndexedStringTable", InspectorService.ObfLengthType.FIXED32);
		addObfSpec(schema, "OsmAndPoiNameIndex", 5, "data", "OsmAndPoiNameIndexData", InspectorService.ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "OsmAndPoiNameIndexData", 2, "suffixesDictionary", null, InspectorService.ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "OsmAndPoiNameIndexData", 3, "atoms", "OsmAndPoiNameIndexDataAtom", InspectorService.ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "OsmAndPoiNameIndexDataAtom", 2, "zoom", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiNameIndexDataAtom", 3, "x", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiNameIndexDataAtom", 4, "y", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiNameIndexDataAtom", 5, "suffixesBitsetIndex", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiNameIndexDataAtom", 6, "suffixesBitset", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiNameIndexDataAtom", 7, "extraSuffix", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiNameIndexDataAtom", 9, "poiIndInBlock", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiNameIndexDataAtom", 14, "shiftTo", null, InspectorService.ObfLengthType.FIXED32);

		addObfSpec(schema, "IndexedStringTable", 1, "prefix", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "IndexedStringTable", 3, "key", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "IndexedStringTable", 4, "val", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "IndexedStringTable", 5, "subtables", "IndexedStringTable", InspectorService.ObfLengthType.VAR_INT, false, true);

		addObfSpec(schema, "OsmAndPoiBox", 1, "zoom", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiBox", 2, "left", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiBox", 3, "top", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiBox", 4, "categories", "OsmAndPoiCategories", InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiBox", 8, "tagGroups", "OsmAndPoiTagGroups", InspectorService.ObfLengthType.FIXED32);
		addObfSpec(schema, "OsmAndPoiBox", 10, "subBoxes", "OsmAndPoiBox", InspectorService.ObfLengthType.FIXED32, false, true);
		addObfSpec(schema, "OsmAndPoiBox", 14, "shiftToData", null, InspectorService.ObfLengthType.FIXED32);
		addObfPackedVarIntSpec(schema, "OsmAndPoiCategories", 3, "categories");
		addObfPackedVarIntSpec(schema, "OsmAndPoiCategories", 5, "subcategories");
		addObfPackedVarIntSpec(schema, "OsmAndPoiTagGroups", 2, "ids");
		addObfSpec(schema, "OsmAndPoiTagGroups", 5, "groups", "OsmAndPoiTagGroup", InspectorService.ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "OsmAndPoiBoxData", 5, "poiData", "OsmAndPoiBoxDataAtom", InspectorService.ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "OsmAndPoiTagGroup", 1, "id", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiTagGroup", 5, "tagValues", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiSubtype", 1, "name", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiSubtype", 2, "tagname", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiSubtype", 3, "isText", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiSubtype", 5, "frequency", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiSubtype", 6, "subtypeValuesSize", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiSubtype", 8, "subtypeValue", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiBoxData", 1, "zoom", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiBoxData", 2, "x", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiBoxData", 3, "y", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiBoxDataAtom", 2, "dx", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiBoxDataAtom", 3, "dy", null, InspectorService.ObfLengthType.VAR_INT);
		addObfPackedVarIntSpec(schema, "OsmAndPoiBoxDataAtom", 4, "categories");
		addObfPackedVarIntSpec(schema, "OsmAndPoiBoxDataAtom", 5, "subcategories");
		addObfSpec(schema, "OsmAndPoiBoxDataAtom", 6, "name", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiBoxDataAtom", 7, "nameEn", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiBoxDataAtom", 8, "id", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiBoxDataAtom", 10, "openingHours", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiBoxDataAtom", 11, "site", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiBoxDataAtom", 12, "phone", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiBoxDataAtom", 13, "note", null, InspectorService.ObfLengthType.VAR_INT);
		addObfPackedVarIntSpec(schema, "OsmAndPoiBoxDataAtom", 14, "textCategories");
		addObfSpec(schema, "OsmAndPoiBoxDataAtom", 15, "textValues", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiBoxDataAtom", 16, "precisionXY", null, InspectorService.ObfLengthType.VAR_INT);
		addObfPackedVarIntSpec(schema, "OsmAndPoiBoxDataAtom", 17, "tagGroups");

		addObfSpec(schema, "OsmAndAddressIndex", 3, "boundaries", "OsmAndTileBox", InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndAddressIndex", 4, "attributeTagsTable", "StringTable", InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndAddressIndex", 6, "cities", "CitiesIndex", InspectorService.ObfLengthType.FIXED32, false, true);
		addObfSpec(schema, "OsmAndAddressIndex", 7, "nameIndex", "OsmAndAddressNameIndexData", InspectorService.ObfLengthType.FIXED32);

		addObfSpec(schema, "CitiesIndex", 5, "cities", "CityIndex", InspectorService.ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "CitiesIndex", 7, "blocks", "CityBlockIndex", InspectorService.ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "CityBlockIndex", 10, "buildings", "BuildingIndex", InspectorService.ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "CityBlockIndex", 12, "streets", "StreetIndex", InspectorService.ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "StreetIndex", 5, "intersections", "StreetIntersection", InspectorService.ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "StreetIndex", 12, "buildings", "BuildingIndex", InspectorService.ObfLengthType.VAR_INT, false, true);

		addObfSpec(schema, "OsmAndAddressNameIndexData", 4, "table", "IndexedStringTable", InspectorService.ObfLengthType.FIXED32);
		addObfSpec(schema, "OsmAndAddressNameIndexData", 7, "atom", "AddressNameIndexData", InspectorService.ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "AddressNameIndexData", 2, "suffixesDictionary", null, InspectorService.ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "AddressNameIndexData", 4, "atom", "AddressNameIndexDataAtom", InspectorService.ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "AddressNameIndexDataAtom", 4, "suffixesBitset", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "AddressNameIndexDataAtom", 8, "suffixesBitsetIndex", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "AddressNameIndexDataAtom", 9, "extraSuffix", null, InspectorService.ObfLengthType.VAR_INT);

		addObfSpec(schema, "OsmAndMapIndex", 4, "rules", "OsmAndMapIndex.MapEncodingRule", InspectorService.ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "OsmAndMapIndex", 5, "levels", "OsmAndMapIndex.MapRootLevel", InspectorService.ObfLengthType.FIXED32, false, true);
		addObfSpec(schema, "OsmAndMapIndex.MapRootLevel", 7, "boxes", "OsmAndMapIndex.MapDataBox", InspectorService.ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "OsmAndMapIndex.MapRootLevel", 15, "blocks", "MapDataBlock", InspectorService.ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "OsmAndMapIndex.MapDataBox", 7, "boxes", "OsmAndMapIndex.MapDataBox", InspectorService.ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "MapDataBlock", 12, "dataObjects", "MapData", InspectorService.ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "MapDataBlock", 15, "stringTable", "StringTable", InspectorService.ObfLengthType.VAR_INT);

		return schema;
	}

	static void addObfPackedVarIntSpec(Map<String, Map<Integer, InspectorService.ObfFieldSpec>> schema, String messageType,
	                                   int fieldNumber, String fieldName) {
		addObfSpec(schema, messageType, fieldNumber, fieldName, null, InspectorService.ObfLengthType.VAR_INT, true, false);
	}

	static void addObfSpec(Map<String, Map<Integer, InspectorService.ObfFieldSpec>> schema, String messageType,
	                       int fieldNumber, String fieldName, String childMessageType, InspectorService.ObfLengthType lengthType) {
		addObfSpec(schema, messageType, fieldNumber, fieldName, childMessageType, lengthType, false, false);
	}

	static void addObfSpec(Map<String, Map<Integer, InspectorService.ObfFieldSpec>> schema, String messageType,
	                       int fieldNumber, String fieldName, String childMessageType, InspectorService.ObfLengthType lengthType,
	                       boolean packedVarInt, boolean repeated) {
		Map<Integer, InspectorService.ObfFieldSpec> byField = schema.computeIfAbsent(messageType, k -> new HashMap<>());
		byField.put(fieldNumber, new InspectorService.ObfFieldSpec(fieldName, childMessageType, lengthType, packedVarInt, repeated));
	}
}
