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

	List<String> OBF_CONTINENTS = List.of(
			"asia",
			"europe",
			"africa",
			"antarctica",
			"australia-oceania",
			"northamerica",
			"centralamerica",
			"southamerica");

	record IndexToken(String name, AddressRef[] addressRefs, int[] poiRefs, int[] poiAtomRefs, int[] poiAtomSizes, boolean isCommon, boolean isFrequent) {
	}
	record ObfFileInfo(String path, String name, String continent, String country, String region, long size) {}
	record IndexTokenPage(List<IndexToken> content, int pageToShow, int pageSizeLimit, long totalElements, int totalPages, IndexTokenSummary summary) {}
	record IndexTokenSummary(int poiSum, int addressSum, int commonSum, int frequentSum, int poiMax, int addressMax) {}
	record IndexTokenBuilder(String name, int[] addressOffsets, int[] addressSuffixIndexes, int[] poiRefs, int[] poiAtomRefs, int[] poiAtomSizes) {}
	record AddressRef(int shiftToIndex, int shiftToCityIndex, int objectOffset, int cityOffset, int typeIndex, int atomSize) {}

	record ObjectAddress(int sequenceId, String name, LatLon point, Map<String, String> commonTags,
	                     Map<String, Object> extraTags, boolean isPoi, boolean isMatched,
	                     boolean isInvalidAtom, boolean isAlone, String type, Long osmId,
	                     String osmType, int payloadOffset, int payloadSize, int sourceOffset) {}
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
	record DbToken(long id, String name, long matched, long alone, boolean isCommon, boolean isFrequent) {}
	record DbTokenSummary(long matchedSum, long aloneSum, long commonSum, long frequentSum, long matchedMax, long aloneMax) {}
	record DbTokenPage(List<DbToken> content, int pageToShow, int pageSizeLimit, long totalElements, int totalPages, DbTokenSummary summary) {}
	record DbObject(int sequenceId, String name, LatLon point, Map<String, String> commonTags,
	                Map<String, Object> extraTags, String type, Long osmId, String osmType,
	                boolean isAlone, String obfName) {}
	record DbObjectPage(List<DbObject> content, int pageToShow, int pageSizeLimit, long totalElements, int totalPages) {}
	@FunctionalInterface
	interface GenerateDbProgressListener {
		void onProgress(GenerateDbProgress progress);
	}
	record CityAddress(String name, LatLon point, List<StreetAddress> streets, int streetsCount, String type) {}
	record PoiAddress(String name, LatLon point, String value) {}
	record HouseAddress(String name, LatLon point) {}
	record StreetAddress(String name, LatLon point, List<HouseAddress> houses, int houseCount) {}

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
		long size = new File(obf).length();
		return new ObfFileInfo(obf, name, continent, normalizeObfDisplayName(country), normalizeObfDisplayName(region), size);
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
}
