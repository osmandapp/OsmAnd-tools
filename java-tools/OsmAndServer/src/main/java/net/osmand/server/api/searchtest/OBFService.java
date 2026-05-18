package net.osmand.server.api.searchtest;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.amazonaws.util.StringInputStream;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import net.osmand.CollatorStringMatcher;
import net.osmand.binary.*;
import net.osmand.data.*;
import net.osmand.obf.OBFDataCreator;
import net.osmand.osm.MapPoiTypes;
import net.osmand.osm.PoiCategory;
import net.osmand.router.RoutingContext;
import net.osmand.search.SearchUICore;
import net.osmand.search.core.SearchExportSettings;
import net.osmand.search.core.SearchPhrase;
import net.osmand.search.core.SearchResult;
import net.osmand.search.core.SearchSettings;
import net.osmand.server.api.services.OsmAndMapsService;
import net.osmand.server.api.services.SearchService;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import net.osmand.util.SearchAlgorithms;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.text.Normalizer;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

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

	private void collectAddressObjects(BinaryMapIndexReaderExt index,
			BinaryMapAddressReaderAdapter.AddressRegion region,
			IndexToken token,
			int tokenOffset,
			boolean isFiltered,
			List<ObjectAddress> results,
			String lang) throws IOException {
		AddressTokenRefs refs = readAddressTokenRefs(index, token.name(), tokenOffset, isFiltered);
		for (Integer offset : refs.cityOffsets) {
			ObjectAddress objectAddress = loadCityObjectAddress(index, region, offset, lang);
			if (objectAddress != null) {
				results.add(objectAddress);
			}
		}
		Map<Integer, City> streetCities = new HashMap<>();
		for (Integer cityOffset : refs.streetCityOffsets) {
			if (cityOffset == null || streetCities.containsKey(cityOffset)) {
				continue;
			}
			City city = loadCity(index, region, cityOffset);
			if (city != null) {
				streetCities.put(cityOffset, city);
			}
		}
		for (int indexOffset = 0; indexOffset < refs.streetOffsets.size(); indexOffset++) {
			int streetOffset = refs.streetOffsets.get(indexOffset);
			int cityOffset = indexOffset < refs.streetCityOffsets.size() ? refs.streetCityOffsets.get(indexOffset) : 0;
			City city = streetCities.get(cityOffset);
			ObjectAddress objectAddress = loadStreetObjectAddress(index, region, streetOffset, city, lang);
			if (objectAddress != null) {
				results.add(objectAddress);
			}
		}
	}
	record CachedIndexTokens(String cacheKey, long fileLength, long lastModified, List<IndexToken> tokens) {}

	record IndexToken(String name, int[] addressOffsets, int[] poiOffsets, int[] poiTokenOffsets, int[] poiExactSuffixIndexes, int addressSize, int poiSize, int addressCount, int poiCount) {
		Boolean isPoi() {
			boolean hasAddressOffsets = addressOffsets != null && addressOffsets.length > 0;
			boolean hasPoiOffsets = poiOffsets != null && poiOffsets.length > 0;
			if (hasPoiOffsets && !hasAddressOffsets) {
				return true;
			}
			if (hasAddressOffsets && !hasPoiOffsets) {
				return false;
			}
			return null;
		}
	}

	record ObjectAddress(String name, LatLon point, Map<String, String> values, boolean isPoi, String type, Long osmId, String osmType, int payloadOffset, int payloadSize) {}
	record ObjectAddressPage(List<ObjectAddress> content, int page, int size, long totalElements, int totalPages, int totalObjectSize, int totalObjectCount, int allObjectSize, int allObjectCount) {}
	record CityAddress(String name, LatLon point, List<StreetAddress> streets, int streetsCount, String type) {}
	record PoiAddress(String name, LatLon point, String value) {}
	record HouseAddress(String name, LatLon point) {}
	record StreetAddress(String name, LatLon point, List<HouseAddress> houses, int houseCount) {}

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
	}

	enum ObfLengthType {
        VAR_INT,
		FIXED32
	}

	record ObfFieldSpec(String fieldName, String childMessageType, ObfLengthType lengthType, boolean packedVarInt, boolean repeated) {}

	static Map<String, Map<Integer, ObfFieldSpec>> buildObfMessageSchema() {
		Map<String, Map<Integer, ObfFieldSpec>> schema = new HashMap<>();
		addObfSpec(schema, "OsmAndStructure", 7, "addressIndex", "OsmAndAddressIndex", ObfLengthType.FIXED32, false, true);
		addObfSpec(schema, "OsmAndStructure", 4, "transportIndex", "OsmAndTransportIndex", ObfLengthType.FIXED32, false, true);
		addObfSpec(schema, "OsmAndStructure", 8, "poiIndex", "OsmAndPoiIndex", ObfLengthType.FIXED32, false, true);
		addObfSpec(schema, "OsmAndStructure", 6, "mapIndex", "OsmAndMapIndex", ObfLengthType.FIXED32, false, true);
		addObfSpec(schema, "OsmAndStructure", 9, "routingIndex", "OsmAndRoutingIndex", ObfLengthType.FIXED32, false, true);
		addObfSpec(schema, "OsmAndStructure", 10, "hhRoutingIndex", "OsmAndHHRoutingIndex", ObfLengthType.FIXED32, false, true);

		addObfSpec(schema, "OsmAndTileBox", 1, "left", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndTileBox", 2, "right", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndTileBox", 3, "top", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndTileBox", 4, "bottom", null, ObfLengthType.VAR_INT);

		addObfSpec(schema, "OsmAndPoiIndex", 1, "name", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiIndex", 2, "boundaries", "OsmAndTileBox", ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiIndex", 3, "categoriesTable", "OsmAndCategoryTable", ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "OsmAndPoiIndex", 4, "nameIndex", "OsmAndPoiNameIndex", ObfLengthType.FIXED32);
		addObfSpec(schema, "OsmAndPoiIndex", 5, "subtypesTable", "OsmAndSubtypesTable", ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiIndex", 6, "boxes", "OsmAndPoiBox", ObfLengthType.FIXED32, false, true);
		addObfSpec(schema, "OsmAndPoiIndex", 9, "poiData", "OsmAndPoiBoxData", ObfLengthType.FIXED32, false, true);
		addObfSpec(schema, "OsmAndSubtypesTable", 4, "subtypes", "OsmAndPoiSubtype", ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "OsmAndCategoryTable", 1, "category", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndCategoryTable", 3, "subcategories", null, ObfLengthType.VAR_INT);

		addObfSpec(schema, "OsmAndPoiNameIndex", 3, "table", "IndexedStringTable", ObfLengthType.FIXED32);
		addObfSpec(schema, "OsmAndPoiNameIndex", 5, "data", "OsmAndPoiNameIndexData", ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "OsmAndPoiNameIndexData", 3, "atoms", "OsmAndPoiNameIndexDataAtom", ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "OsmAndPoiNameIndexDataAtom", 2, "zoom", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiNameIndexDataAtom", 3, "x", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiNameIndexDataAtom", 4, "y", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiNameIndexDataAtom", 5, "bloomIndex", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiNameIndexDataAtom", 14, "shiftTo", null, ObfLengthType.FIXED32);

		addObfSpec(schema, "IndexedStringTable", 1, "prefix", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "IndexedStringTable", 3, "key", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "IndexedStringTable", 4, "val", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "IndexedStringTable", 5, "subtables", "IndexedStringTable", ObfLengthType.VAR_INT, false, true);

		addObfSpec(schema, "OsmAndPoiBox", 1, "zoom", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiBox", 2, "left", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiBox", 3, "top", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiBox", 4, "categories", "OsmAndPoiCategories", ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiBox", 8, "tagGroups", "OsmAndPoiTagGroups", ObfLengthType.FIXED32);
		addObfSpec(schema, "OsmAndPoiBox", 10, "subBoxes", "OsmAndPoiBox", ObfLengthType.FIXED32, false, true);
		addObfSpec(schema, "OsmAndPoiBox", 14, "shiftToData", null, ObfLengthType.FIXED32);
		addObfPackedVarIntSpec(schema, "OsmAndPoiCategories", 3, "categories");
		addObfPackedVarIntSpec(schema, "OsmAndPoiCategories", 5, "subcategories");
		addObfPackedVarIntSpec(schema, "OsmAndPoiTagGroups", 2, "ids");
		addObfSpec(schema, "OsmAndPoiTagGroups", 5, "groups", "OsmAndPoiTagGroup", ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "OsmAndPoiBoxData", 5, "poiData", "OsmAndPoiBoxDataAtom", ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "OsmAndPoiTagGroup", 1, "id", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiTagGroup", 5, "tagValues", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiSubtype", 1, "name", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiSubtype", 2, "tagname", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiSubtype", 3, "isText", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiSubtype", 5, "frequency", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiSubtype", 6, "subtypeValuesSize", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiSubtype", 8, "subtypeValue", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiBoxData", 1, "zoom", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiBoxData", 2, "x", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiBoxData", 3, "y", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiBoxDataAtom", 2, "dx", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiBoxDataAtom", 3, "dy", null, ObfLengthType.VAR_INT);
		addObfPackedVarIntSpec(schema, "OsmAndPoiBoxDataAtom", 4, "categories");
		addObfPackedVarIntSpec(schema, "OsmAndPoiBoxDataAtom", 5, "subcategories");
		addObfSpec(schema, "OsmAndPoiBoxDataAtom", 6, "name", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiBoxDataAtom", 7, "nameEn", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiBoxDataAtom", 8, "id", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiBoxDataAtom", 10, "openingHours", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiBoxDataAtom", 11, "site", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiBoxDataAtom", 12, "phone", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiBoxDataAtom", 13, "note", null, ObfLengthType.VAR_INT);
		addObfPackedVarIntSpec(schema, "OsmAndPoiBoxDataAtom", 14, "textCategories");
		addObfSpec(schema, "OsmAndPoiBoxDataAtom", 15, "textValues", null, ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiBoxDataAtom", 16, "precisionXY", null, ObfLengthType.VAR_INT);
		addObfPackedVarIntSpec(schema, "OsmAndPoiBoxDataAtom", 17, "tagGroups");

		addObfSpec(schema, "OsmAndAddressIndex", 3, "boundaries", "OsmAndTileBox", ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndAddressIndex", 4, "attributeTagsTable", "StringTable", ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndAddressIndex", 6, "cities", "CitiesIndex", ObfLengthType.FIXED32, false, true);
		addObfSpec(schema, "OsmAndAddressIndex", 7, "nameIndex", "OsmAndAddressNameIndexData", ObfLengthType.FIXED32);

		addObfSpec(schema, "CitiesIndex", 5, "cities", "CityIndex", ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "CitiesIndex", 7, "blocks", "CityBlockIndex", ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "CityBlockIndex", 10, "buildings", "BuildingIndex", ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "CityBlockIndex", 12, "streets", "StreetIndex", ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "StreetIndex", 5, "intersections", "StreetIntersection", ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "StreetIndex", 12, "buildings", "BuildingIndex", ObfLengthType.VAR_INT, false, true);

		addObfSpec(schema, "OsmAndAddressNameIndexData", 4, "table", "IndexedStringTable", ObfLengthType.FIXED32);
		addObfSpec(schema, "OsmAndAddressNameIndexData", 7, "atom", "AddressNameIndexData", ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "AddressNameIndexData", 4, "atom", "AddressNameIndexDataAtom", ObfLengthType.VAR_INT, false, true);

		addObfSpec(schema, "OsmAndMapIndex", 4, "rules", "OsmAndMapIndex.MapEncodingRule", ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "OsmAndMapIndex", 5, "levels", "OsmAndMapIndex.MapRootLevel", ObfLengthType.FIXED32, false, true);
		addObfSpec(schema, "OsmAndMapIndex.MapRootLevel", 7, "boxes", "OsmAndMapIndex.MapDataBox", ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "OsmAndMapIndex.MapRootLevel", 15, "blocks", "MapDataBlock", ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "OsmAndMapIndex.MapDataBox", 7, "boxes", "OsmAndMapIndex.MapDataBox", ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "MapDataBlock", 12, "dataObjects", "MapData", ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "MapDataBlock", 15, "stringTable", "StringTable", ObfLengthType.VAR_INT);

		return schema;
	}

	static void addObfPackedVarIntSpec(Map<String, Map<Integer, ObfFieldSpec>> schema, String messageType,
                                       int fieldNumber, String fieldName) {
		addObfSpec(schema, messageType, fieldNumber, fieldName, null, ObfLengthType.VAR_INT, true, false);
	}

	static void addObfSpec(Map<String, Map<Integer, ObfFieldSpec>> schema, String messageType,
	                       int fieldNumber, String fieldName, String childMessageType, ObfLengthType lengthType) {
		addObfSpec(schema, messageType, fieldNumber, fieldName, childMessageType, lengthType, false, false);
	}

	static void addObfSpec(Map<String, Map<Integer, ObfFieldSpec>> schema, String messageType,
	                       int fieldNumber, String fieldName, String childMessageType, ObfLengthType lengthType,
	                       boolean packedVarInt, boolean repeated) {
		Map<Integer, ObfFieldSpec> byField = schema.computeIfAbsent(messageType, k -> new HashMap<>());
		byField.put(fieldNumber, new ObfFieldSpec(fieldName, childMessageType, lengthType, packedVarInt, repeated));
	}

	Map<String, Map<Integer, ObfFieldSpec>> OBF_MESSAGE_SCHEMA = buildObfMessageSchema();

	Map<Integer, String> OBF_STRUCTURE_FIELD_NAMES = buildObfStructureFieldNames();

	static boolean isRepeatedMessageField(String messageType, int fieldNumber) {
		Map<Integer, ObfFieldSpec> specByFieldNumber = OBF_MESSAGE_SCHEMA.get(messageType);
		ObfFieldSpec fieldSpec = specByFieldNumber == null ? null : specByFieldNumber.get(fieldNumber);
		return fieldSpec != null && fieldSpec.repeated();
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

	static long[] getOrCreateSectionStats(Map<String, long[]> out, String key) {
		return out.computeIfAbsent(key, k -> new long[]{0L, 0L, 0L});
	}

	static boolean messageHasNestedElements(CodedInputStream codedIS, String messageType) throws IOException {
		Map<Integer, ObfFieldSpec> specByFieldNumber = OBF_MESSAGE_SCHEMA.get(messageType);
		try {
			while (true) {
				int tag = codedIS.readTag();
				int fieldNumber = WireFormat.getTagFieldNumber(tag);
				if (fieldNumber == 0) {
					return false;
				}
				ObfFieldSpec spec = specByFieldNumber == null ? null : specByFieldNumber.get(fieldNumber);
				if (spec == null) {
					skipUnknownField(codedIS, tag);
					continue;
				}
				if (spec.childMessageType() == null) {
					skipUnknownField(codedIS, tag);
					continue;
				}
				long payloadLength = readPayloadLength(codedIS, tag, spec);
				if (payloadLength < 0) {
					skipUnknownField(codedIS, tag);
					continue;
				}
				if (payloadLength > 0) {
					return true;
				}
			}
		} catch (com.google.protobuf.InvalidProtocolBufferException e) {
			return false;
		}
	}

	static void collectImmediateChildStats(CodedInputStream codedIS, String messageType, Map<String, long[]> sizes) throws IOException {
		Map<Integer, ObfFieldSpec> specByFieldNumber = OBF_MESSAGE_SCHEMA.get(messageType);
		if (specByFieldNumber != null) {
			for (ObfFieldSpec spec : specByFieldNumber.values()) {
				if (spec == null || spec.childMessageType() == null || spec.fieldName() == null) {
					continue;
				}
				getOrCreateSectionStats(sizes, spec.fieldName());
			}
		}
		try {
			while (true) {
				int tag = codedIS.readTag();
				int fieldNumber = WireFormat.getTagFieldNumber(tag);
				if (fieldNumber == 0) {
					return;
				}
				ObfFieldSpec spec = specByFieldNumber == null ? null : specByFieldNumber.get(fieldNumber);
				if (spec == null || spec.childMessageType() == null) {
					skipUnknownField(codedIS, tag);
					continue;
				}
				long payloadLength = readPayloadLength(codedIS, tag, spec);
				if (payloadLength < 0) {
					skipUnknownField(codedIS, tag);
					continue;
				}
				long[] stats = getOrCreateSectionStats(sizes, spec.fieldName());
				if (payloadLength > 0) {
					stats[0] += payloadLength;
				}
				if (spec.repeated()) {
					stats[2]++;
				}
				long oldLimit = codedIS.pushLimitLong(payloadLength);
				try {
					if (messageHasNestedElements(codedIS, spec.childMessageType())) {
						stats[1] = 1;
					}
				} catch (com.google.protobuf.InvalidProtocolBufferException e) {
					long remainingInLimit = codedIS.getBytesUntilLimit();
					if (remainingInLimit > 0) {
						skipRawBytesLong(codedIS, remainingInLimit);
					}
				} finally {
					consumeRemainingInLimit(codedIS);
					codedIS.popLimit(oldLimit);
				}
			}
		} catch (com.google.protobuf.InvalidProtocolBufferException ignored) {
        }
	}

	static boolean collectChildStatsAtPath(CodedInputStream codedIS, String messageType, String[] path,
	                                      int pathIndex, Map<String, long[]> sizes) throws IOException {
		Map<Integer, ObfFieldSpec> specByFieldNumber = OBF_MESSAGE_SCHEMA.get(messageType);
		String expectedFieldName = path[pathIndex];
		boolean found = false;
		try {
			while (true) {
				int tag = codedIS.readTag();
				int fieldNumber = WireFormat.getTagFieldNumber(tag);
				if (fieldNumber == 0) {
					return found;
				}
				ObfFieldSpec spec = specByFieldNumber == null ? null : specByFieldNumber.get(fieldNumber);
				if (spec == null || spec.childMessageType() == null || !expectedFieldName.equalsIgnoreCase(spec.fieldName())) {
					skipUnknownField(codedIS, tag);
					continue;
				}
				long payloadLength = readPayloadLength(codedIS, tag, spec);
				if (payloadLength < 0) {
					skipUnknownField(codedIS, tag);
					continue;
				}
				found = true;
				long oldLimit = codedIS.pushLimitLong(payloadLength);
				try {
					if (pathIndex == path.length - 1) {
						collectImmediateChildStats(codedIS, spec.childMessageType(), sizes);
					} else {
                        collectChildStatsAtPath(codedIS, spec.childMessageType(), path, pathIndex + 1, sizes);
                    }
				} finally {
					consumeRemainingInLimit(codedIS);
					codedIS.popLimit(oldLimit);
				}
			}
		} catch (com.google.protobuf.InvalidProtocolBufferException e) {
			return found;
		}
	}

	static JSONObject readIndexedStringTableToJson(CodedInputStream codedIS, int depth) throws IOException {
		JsonDumpState state = JSON_DUMP_STATE.get();
		if (state != null && depth > state.maxDepth) {
			codedIS.skipRawBytes(codedIS.getBytesUntilLimit());
			JSONObject out = new JSONObject();
			out.put("_truncated", true);
			out.put("_reason", "maxDepth");
			return out;
		}
		JSONObject out = new JSONObject();
		JSONArray entries = new JSONArray();
		JSONObject lastEntry = null;
		while (true) {
			if (state != null && state.isFieldLimitReached()) {
				codedIS.skipRawBytes(codedIS.getBytesUntilLimit());
				out.put("_truncated", true);
				out.put("_reason", "maxFields");
				break;
			}
			int t = codedIS.readTag();
			int fieldNumber = WireFormat.getTagFieldNumber(t);
			if (fieldNumber == 0) {
				break;
			}
			if (fieldNumber == OsmandOdb.IndexedStringTable.PREFIX_FIELD_NUMBER) {
				String prefix = codedIS.readString();
				out.put("prefix", prefix);
				if (state != null) {
					state.onFieldAdded();
				}
				continue;
			}
			if (fieldNumber == OsmandOdb.IndexedStringTable.KEY_FIELD_NUMBER) {
				String key = codedIS.readString();
				JSONObject entry = new JSONObject();
				entry.put("key", key);
				entries.put(entry);
				lastEntry = entry;
				if (state != null) {
					state.onFieldAdded();
				}
				continue;
			}
			if (fieldNumber == OsmandOdb.IndexedStringTable.VAL_FIELD_NUMBER) {
				Object v = readScalarValueByWireType(codedIS, t);
				if (lastEntry != null && !lastEntry.has("val")) {
					lastEntry.put("val", v);
				} else {
					JSONObject entry = new JSONObject();
					entry.put("val", v);
					entries.put(entry);
					lastEntry = entry;
				}
				if (state != null) {
					state.onFieldAdded();
				}
				continue;
			}
			if (fieldNumber == OsmandOdb.IndexedStringTable.SUBTABLES_FIELD_NUMBER) {
				long payloadLength = readPayloadLengthByWireType(codedIS, t);
				if (payloadLength < 0) {
					skipUnknownField(codedIS, t);
					continue;
				}
				long oldLimit = codedIS.pushLimitLong(payloadLength);
				try {
					JSONObject nested = readIndexedStringTableToJson(codedIS, depth + 1);
					if (lastEntry != null) {
						if (!lastEntry.has("subtables")) {
							JSONArray st = new JSONArray();
							st.put(nested);
							lastEntry.put("subtables", st);
						} else {
							Object existing = lastEntry.get("subtables");
							if (existing instanceof JSONArray arr) {
								if (state == null || arr.length() < state.maxArrayLength) {
									arr.put(nested);
								}
								lastEntry.put("subtables", arr);
							} else {
								JSONArray arr = new JSONArray();
								arr.put(existing);
								if (state == null || arr.length() < state.maxArrayLength) {
									arr.put(nested);
								}
								lastEntry.put("subtables", arr);
							}
						}
					} else {
						JSONObject entry = new JSONObject();
						JSONArray st = new JSONArray();
						st.put(nested);
						entry.put("subtables", st);
						entries.put(entry);
						lastEntry = entry;
					}
				} finally {
					codedIS.popLimit(oldLimit);
				}
				if (state != null) {
					state.onFieldAdded();
				}
				continue;
			}
			skipUnknownField(codedIS, t);
		}
		out.put("entries", entries);
		return out;
	}

	static long readFixed32Length(CodedInputStream codedIS) throws IOException {
		long l = readUnsignedByte(codedIS);
		boolean eightBytes = l > 0x7f;
		if (eightBytes) {
			l = l & 0x7f;
		}
		l = (l << 8) + readUnsignedByte(codedIS);
		l = (l << 8) + readUnsignedByte(codedIS);
		l = (l << 8) + readUnsignedByte(codedIS);
		if (eightBytes) {
			l = (l << 8) + readUnsignedByte(codedIS);
			l = (l << 8) + readUnsignedByte(codedIS);
			l = (l << 8) + readUnsignedByte(codedIS);
			l = (l << 8) + readUnsignedByte(codedIS);
		}
		return l;
	}

	static int readUnsignedByte(CodedInputStream codedIS) throws IOException {
		byte b = codedIS.readRawByte();
		return b < 0 ? b + 256 : b;
	}

	static void skipRawBytesLong(CodedInputStream codedIS, long length) throws IOException {
		if (length <= 0) {
			return;
		}
		while (length > 0) {
			int chunk = (int) Math.min(length, Integer.MAX_VALUE);
			codedIS.skipRawBytes(chunk);
			length -= chunk;
		}
	}

	static void consumeRemainingInLimit(CodedInputStream codedIS) throws IOException {
		long remainingInLimit = codedIS.getBytesUntilLimit();
		if (remainingInLimit > 0) {
			skipRawBytesLong(codedIS, remainingInLimit);
		}
	}

	record RootSpec(int rootFieldNumber, String rootMessageType) {}

	static RootSpec resolveRootSpec(String rootSegment) {
		if (rootSegment == null || rootSegment.trim().isEmpty()) {
			throw new IllegalArgumentException("fieldPath root is empty");
		}
		Map<Integer, ObfFieldSpec> rootSpecs = OBF_MESSAGE_SCHEMA.get("OsmAndStructure");
		if (rootSpecs == null) {
			throw new IllegalStateException("Missing schema for OsmAndStructure");
		}
		for (Map.Entry<Integer, ObfFieldSpec> e : rootSpecs.entrySet()) {
			ObfFieldSpec spec = e.getValue();
			if (spec == null || spec.childMessageType() == null || spec.fieldName() == null) {
				continue;
			}
			if (rootSegment.equalsIgnoreCase(spec.fieldName())) {
				return new RootSpec(e.getKey(), spec.childMessageType());
			}
		}
		throw new IllegalArgumentException("Unsupported fieldPath root: '" + rootSegment + "'");
	}

	static void skipUnknownField(CodedInputStream codedIS, int tag) throws IOException {
		try {
			int wireType = WireFormat.getTagWireType(tag);
			if (wireType == WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED) {
				long length = readFixed32Length(codedIS);
				long remainingInLimit = codedIS.getBytesUntilLimit();
				if (remainingInLimit >= 0 && length > remainingInLimit) {
					length = remainingInLimit;
				}
				skipRawBytesLong(codedIS, length);
			} else if (wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
				long length = codedIS.readRawVarint32();
				long remainingInLimit = codedIS.getBytesUntilLimit();
				if (remainingInLimit >= 0 && length > remainingInLimit) {
					length = remainingInLimit;
				}
				skipRawBytesLong(codedIS, length);
			} else {
				codedIS.skipField(tag);
			}
		} catch (com.google.protobuf.InvalidProtocolBufferException e) {
			long remainingInLimit = codedIS.getBytesUntilLimit();
			if (remainingInLimit > 0) {
				skipRawBytesLong(codedIS, remainingInLimit);
			}
		}
	}

	static long readPayloadLengthByWireType(CodedInputStream codedIS, int tag) throws IOException {
		int wireType = WireFormat.getTagWireType(tag);
		if (wireType == WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED) {
			return readFixed32Length(codedIS);
		} else if (wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
			return codedIS.readRawVarint32();
		}
		return -1;
	}

	static long readPayloadLength(CodedInputStream codedIS, int tag, ObfFieldSpec spec) throws IOException {
		if (spec != null && spec.lengthType() != null) {
			if (spec.lengthType() == ObfLengthType.FIXED32) {
				return readFixed32Length(codedIS);
			}
			if (spec.lengthType() == ObfLengthType.VAR_INT) {
				return codedIS.readRawVarint32();
			}
		}
		return readPayloadLengthByWireType(codedIS, tag);
	}

	// fieldPath is null or empty string for root, otherwise dot separated path like 'poiIndex.nameIndex' or 'addressIndex.cities'
	default String getSectionJson(String obf, String fieldPath) {
		String normalizedFieldPath = fieldPath == null ? "" : fieldPath.trim();
		String[] path = normalizedFieldPath.isEmpty() ? new String[0] : normalizedFieldPath.split("\\.");
		File file = new File(obf);
		try (RandomAccessFile r = new RandomAccessFile(file.getAbsolutePath(), "r")) {
			CodedInputStream codedIS = CodedInputStream.newInstance(r);
			codedIS.setSizeLimit(CodedInputStream.MAX_DEFAULT_SIZE_LIMIT);
			JsonDumpState state = JsonDumpState.fromEnv();
			JSON_DUMP_STATE.set(state);

			Object result;
			if (path.length == 0) {
				result = readMessageToJson(codedIS, "OsmAndStructure", 0);
			} else {
				RootSpec rootSpec = resolveRootSpec(path[0]);
				int rootFieldNumber = rootSpec.rootFieldNumber;
				String rootMessageType = rootSpec.rootMessageType;

				JSONArray matches = new JSONArray();
				while (true) {
					int t = codedIS.readTag();
					int fieldNumber = WireFormat.getTagFieldNumber(t);
					if (fieldNumber == 0) {
						break;
					}
					if (fieldNumber != rootFieldNumber) {
						skipUnknownField(codedIS, t);
						continue;
					}
					ObfFieldSpec rootFieldSpec = OBF_MESSAGE_SCHEMA.get("OsmAndStructure").get(rootFieldNumber);
					long payloadLength = readPayloadLength(codedIS, t, rootFieldSpec);
					if (payloadLength < 0) {
						skipUnknownField(codedIS, t);
						continue;
					}
					long oldLimit = codedIS.pushLimitLong(payloadLength);
					try {
						Object match = readMessageAtPathToJson(codedIS, rootMessageType, path, 1);
						if (match != null) {
							if (match instanceof JSONArray arr) {
								for (int i = 0; i < arr.length(); i++) {
									matches.put(arr.get(i));
								}
							} else {
								matches.put(match);
							}
						}
					} finally {
						codedIS.popLimit(oldLimit);
					}
				}
				if (matches.length() == 1) {
					result = matches.get(0);
				} else {
					result = matches;
				}
			}

			if (result instanceof JSONObject obj) {
				return obj.toString(2);
			}
			if (result instanceof JSONArray arr) {
				return arr.toString(2);
			}
			return String.valueOf(result);
		} catch (IOException e) {
			getLogger().error("Failed to read OBF file: {}", file, e);
			throw new RuntimeException("Failed to read OBF file: " + e.getMessage(), e);
		} finally {
			JSON_DUMP_STATE.remove();
		}
	}

	static Object readMessageAtPathToJson(CodedInputStream codedIS, String messageType, String[] path, int pathIndex) throws IOException {
		if (pathIndex >= path.length) {
			return readMessageToJson(codedIS, messageType, 0);
		}
		Map<Integer, ObfFieldSpec> specByFieldNumber = OBF_MESSAGE_SCHEMA.get(messageType);
		String next = path[pathIndex];
		JSONArray matches = new JSONArray();
		while (true) {
			int t = codedIS.readTag();
			int fieldNumber = WireFormat.getTagFieldNumber(t);
			if (fieldNumber == 0) {
				break;
			}
			ObfFieldSpec spec = specByFieldNumber == null ? null : specByFieldNumber.get(fieldNumber);
			if (spec == null || spec.childMessageType() == null || !next.equalsIgnoreCase(spec.fieldName())) {
				skipUnknownField(codedIS, t);
				continue;
			}
			long payloadLength = readPayloadLength(codedIS, t, spec);
			if (payloadLength < 0) {
				skipUnknownField(codedIS, t);
				continue;
			}
			long oldLimit = codedIS.pushLimitLong(payloadLength);
			try {
				Object match = readMessageAtPathToJson(codedIS, spec.childMessageType(), path, pathIndex + 1);
				if (match != null) {
					matches.put(match);
				}
			} finally {
				codedIS.popLimit(oldLimit);
			}
		}
		if (matches.isEmpty()) {
			return null;
		}
		if (matches.length() == 1) {
			return matches.get(0);
		}
		return matches;
	}

	static JSONObject readMessageToJson(CodedInputStream codedIS, String messageType, int depth) throws IOException {
		if ("IndexedStringTable".equals(messageType)) {
			return readIndexedStringTableToJson(codedIS, depth);
		}
		JsonDumpState state = JSON_DUMP_STATE.get();
		if (state != null && depth > state.maxDepth) {
			codedIS.skipRawBytes(codedIS.getBytesUntilLimit());
			JSONObject out = new JSONObject();
			out.put("_truncated", true);
			out.put("_reason", "maxDepth");
			return out;
		}
		Map<Integer, ObfFieldSpec> specByFieldNumber = OBF_MESSAGE_SCHEMA.get(messageType);
		JSONObject out = new JSONObject();
		while (true) {
			if (state != null && state.isFieldLimitReached()) {
				codedIS.skipRawBytes(codedIS.getBytesUntilLimit());
				out.put("_truncated", true);
				out.put("_reason", "maxFields");
				return out;
			}
			int t = codedIS.readTag();
			int fieldNumber = WireFormat.getTagFieldNumber(t);
			if (fieldNumber == 0) {
				return out;
			}
			ObfFieldSpec spec = specByFieldNumber == null ? null : specByFieldNumber.get(fieldNumber);
			String fieldName = (spec != null && spec.fieldName() != null) ? spec.fieldName() : ("#" + fieldNumber);

			if (spec != null && spec.childMessageType() != null) {
				long payloadLength = readPayloadLength(codedIS, t, spec);
				if (payloadLength < 0) {
					skipUnknownField(codedIS, t);
					continue;
				}
				long oldLimit = codedIS.pushLimitLong(payloadLength);
				try {
					JSONObject nested = readMessageToJson(codedIS, spec.childMessageType(), depth + 1);
					addJsonValue(out, fieldName, nested);
				} finally {
					codedIS.popLimit(oldLimit);
				}
				continue;
			}

			if (spec != null && spec.packedVarInt() && WireFormat.getTagWireType(t) == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
				JSONArray arr = readPackedVarintArray(codedIS, t);
				addJsonValue(out, fieldName, arr);
				continue;
			}
			Object scalar = readScalarValueByWireType(codedIS, t);
			addJsonValue(out, fieldName, scalar);
		}
	}

	ThreadLocal<JsonDumpState> JSON_DUMP_STATE = new ThreadLocal<>();

	final class JsonDumpState {
		final int maxDepth;
		final long maxFields;
		final int maxArrayLength;
		long fieldsSeen;

		private JsonDumpState(int maxDepth, long maxFields, int maxArrayLength) {
			this.maxDepth = maxDepth;
			this.maxFields = maxFields;
			this.maxArrayLength = maxArrayLength;
		}

		static JsonDumpState fromEnv() {
			int maxDepth = parseEnvInt("OBF_JSON_MAX_DEPTH", 50);
			long maxFields = parseEnvLong("OBF_JSON_MAX_FIELDS", 200_000L);
			int maxArrayLength = parseEnvInt("OBF_JSON_MAX_ARRAY", 10_000);
			return new JsonDumpState(maxDepth, maxFields, maxArrayLength);
		}

		boolean isFieldLimitReached() {
			return fieldsSeen >= maxFields;
		}

		void onFieldAdded() {
			fieldsSeen++;
		}
	}

	static int parseEnvInt(String key, int def) {
		String raw = System.getenv(key);
		if (raw == null || raw.trim().isEmpty()) {
			return def;
		}
		try {
			return Integer.parseInt(raw.trim());
		} catch (NumberFormatException e) {
			return def;
		}
	}

	static long parseEnvLong(String key, long def) {
		String raw = System.getenv(key);
		if (raw == null || raw.trim().isEmpty()) {
			return def;
		}
		try {
			return Long.parseLong(raw.trim());
		} catch (Exception e) {
			return def;
		}
	}

	static JSONArray readPackedVarintArray(CodedInputStream codedIS, int tag) throws IOException {
		long payloadLength = readPayloadLengthByWireType(codedIS, tag);
		if (payloadLength <= 0) {
			return new JSONArray();
		}
		long remainingInLimit = codedIS.getBytesUntilLimit();
		if (remainingInLimit >= 0 && payloadLength > remainingInLimit) {
			payloadLength = remainingInLimit;
		}
		int length = (int) Math.min(payloadLength, Integer.MAX_VALUE);
		byte[] data;
		try {
			data = codedIS.readRawBytes(length);
		} catch (com.google.protobuf.InvalidProtocolBufferException e) {
			long remaining = codedIS.getBytesUntilLimit();
			if (remaining <= 0) {
				return new JSONArray();
			}
			data = codedIS.readRawBytes((int) Math.min(remaining, length));
		}
		CodedInputStream cis = CodedInputStream.newInstance(data);
		JSONArray arr = new JSONArray();
		while (!cis.isAtEnd()) {
			arr.put(cis.readRawVarint64());
		}
		return arr;
	}

	static void addJsonValue(JSONObject out, String fieldName, Object value) {
		JsonDumpState state = JSON_DUMP_STATE.get();
		if (out.has(fieldName)) {
			Object existing = out.get(fieldName);
			JSONArray arr;
			if (existing instanceof JSONArray existingArr) {
				arr = existingArr;
			} else {
				arr = new JSONArray();
				arr.put(existing);
			}
			if (state == null || arr.length() < state.maxArrayLength) {
				arr.put(value);
			}
			out.put(fieldName, arr);
		} else {
			out.put(fieldName, value);
		}
		if (state != null) {
			state.onFieldAdded();
		}
	}

	static Object readScalarValueByWireType(CodedInputStream codedIS, int tag) throws IOException {
		int wireType = WireFormat.getTagWireType(tag);
		if (wireType == WireFormat.WIRETYPE_VARINT) {
			return codedIS.readRawVarint64();
		} else if (wireType == WireFormat.WIRETYPE_FIXED32) {
			return codedIS.readRawLittleEndian32();
		} else if (wireType == WireFormat.WIRETYPE_FIXED64) {
			return codedIS.readRawLittleEndian64();
		} else if (wireType == WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED) {
			return readFixed32Length(codedIS);
		} else if (wireType == WireFormat.WIRETYPE_LENGTH_DELIMITED) {
			long payloadLength = readPayloadLengthByWireType(codedIS, tag);
			if (payloadLength <= 0) {
				return "";
			}
			long remainingInLimit = codedIS.getBytesUntilLimit();
			if (remainingInLimit >= 0 && payloadLength > remainingInLimit) {
				payloadLength = remainingInLimit;
			}
			int length = (int) Math.min(payloadLength, Integer.MAX_VALUE);
			byte[] data;
			try {
				data = codedIS.readRawBytes(length);
			} catch (com.google.protobuf.InvalidProtocolBufferException e) {
				long remaining = codedIS.getBytesUntilLimit();
				if (remaining <= 0) {
					return "";
				}
				data = codedIS.readRawBytes((int) Math.min(remaining, length));
			}
			String utf8 = tryDecodeUtf8Printable(data);
			if (utf8 != null) {
				return utf8;
			}
			return Base64.getEncoder().encodeToString(data);
		} else {
			codedIS.skipField(tag);
			return JSONObject.NULL;
		}
	}

	static String tryDecodeUtf8Printable(byte[] data) {
		if (data == null || data.length == 0) {
			return "";
		}
		String s;
		try {
			s = new String(data, StandardCharsets.UTF_8);
		} catch (Exception e) {
			return null;
		}
		for (int i = 0; i < s.length(); i++) {
			char ch = s.charAt(i);
			if (ch == '\n' || ch == '\r' || ch == '\t') {
				continue;
			}
			if (Character.isISOControl(ch)) {
				return null;
			}
		}
		return s;
	}

	// fieldPath is null or empty string for root, otherwise dot separated path like 'poiIndex.nameIndex' or 'addressIndex.cities'
	default Map<String, long[]> getSectionSizes(String obf, String fieldPath) {
		String normalizedFieldPath = fieldPath == null ? "" : fieldPath.trim();
		String[] path = normalizedFieldPath.isEmpty() ? new String[0] : normalizedFieldPath.split("\\.");
		File file = new File(obf);
		try (RandomAccessFile indexFile = new RandomAccessFile(file.getAbsolutePath(), "r");
		     RandomAccessFile dataFile = new RandomAccessFile(file.getAbsolutePath(), "r")) {
			BinaryMapIndexReader indexReader = new BinaryMapIndexReader(indexFile, file);
			if (path.length == 0) {
				Map<String, long[]> sizes = new HashMap<>();
				Map<Integer, ObfFieldSpec> rootSpecs = OBF_MESSAGE_SCHEMA.get("OsmAndStructure");
				if (rootSpecs != null) {
					for (ObfFieldSpec rootSpec : rootSpecs.values()) {
						if (rootSpec == null || rootSpec.childMessageType() == null || rootSpec.fieldName() == null) {
							continue;
						}
						getOrCreateSectionStats(sizes, rootSpec.fieldName());
					}
				}
				for (BinaryIndexPart indexPart : indexReader.getIndexes()) {
					if (indexPart == null) {
						continue;
					}
					int fieldNumber = indexPart.getFieldNumber();
					ObfFieldSpec rootFieldSpec = rootSpecs == null ? null : rootSpecs.get(fieldNumber);
					if (rootFieldSpec == null || rootFieldSpec.childMessageType() == null) {
						continue;
					}
					String fieldName = rootFieldSpec.fieldName() != null ? rootFieldSpec.fieldName() : OBF_STRUCTURE_FIELD_NAMES.get(fieldNumber);
					if (fieldName == null) {
						fieldName = "field_" + fieldNumber;
					}
					long[] stats = getOrCreateSectionStats(sizes, fieldName);
					long payloadLength = indexPart.getLength();
					if (payloadLength > 0) {
						stats[0] += payloadLength;
					}
					stats[1] = 1;
					if (isRepeatedMessageField("OsmAndStructure", fieldNumber)) {
						stats[2]++;
					}
				}
				return sizes;
			}

			RootSpec rootSpec = resolveRootSpec(path[0]);
			int rootFieldNumber = rootSpec.rootFieldNumber;
			String rootMessageType = rootSpec.rootMessageType;
			Map<String, long[]> sizes = new HashMap<>();
			boolean foundPath = false;

			for (BinaryIndexPart indexPart : indexReader.getIndexes()) {
				if (indexPart == null || indexPart.getFieldNumber() != rootFieldNumber) {
					continue;
				}
				long payloadLength = indexPart.getLength();
				if (payloadLength <= 0) {
					continue;
				}
				CodedInputStream partInput = CodedInputStream.newInstance(dataFile);
				partInput.setSizeLimit(CodedInputStream.MAX_DEFAULT_SIZE_LIMIT);
				partInput.seek(indexPart.getFilePointer());
				long oldLimit = partInput.pushLimitLong(payloadLength);
				try {
					if (path.length == 1) {
						foundPath = true;
						collectImmediateChildStats(partInput, rootMessageType, sizes);
					} else {
						foundPath = collectChildStatsAtPath(partInput, rootMessageType, path, 1, sizes) || foundPath;
					}
				} finally {
					consumeRemainingInLimit(partInput);
					partInput.popLimit(oldLimit);
				}
			}
			if (!foundPath) {
				return null;
			}
			return sizes;
		} catch (IOException e) {
			getLogger().error("Failed to read OBF file: {}", file, e);
			throw new RuntimeException("Failed to read OBF file: " + e.getMessage(), e);
		}
	}

	private static PoiAddress findRawValue(RawPoiObject objectRecord, Pattern poiPattern, Pattern normalizedPoiPattern) {
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

	private static List<PoiAddress> findPoiAddressesRaw(RandomAccessFile randomAccessFile,
	                                                    BinaryMapIndexReader index,
	                                                    BinaryMapPoiReaderAdapter.PoiRegion poiRegion,
	                                                    Pattern poiPattern,
	                                                    Pattern normalizedPoiPattern) throws IOException {
		List<PoiAddress> results = new ArrayList<>();
		index.initCategories(poiRegion);
		CodedInputStream codedIS = CodedInputStream.newInstance(randomAccessFile);
		codedIS.setSizeLimit(CodedInputStream.MAX_DEFAULT_SIZE_LIMIT);
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
						long length = readFixed32Length(codedIS);
						long innerOldLimit = codedIS.pushLimitLong(length);
						findPoiAddressesInBoxData(codedIS, poiRegion, poiPattern, normalizedPoiPattern, results);
						codedIS.popLimit(innerOldLimit);
						break;
					}
					default:
						skipUnknownField(codedIS, tagValue);
						break;
				}
			}
		} finally {
			codedIS.popLimit(oldLimit);
		}
	}

	private static void findPoiAddressesInBoxData(CodedInputStream codedIS,
			BinaryMapPoiReaderAdapter.PoiRegion poiRegion,
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
					RawPoiObject objectRecord = readRawPoiObject(codedIS, x, y, zoom, poiRegion);
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

	private static RawPoiObject readRawPoiObject(CodedInputStream codedIS,
			int parentX,
			int parentY,
			int parentZoom,
			BinaryMapPoiReaderAdapter.PoiRegion poiRegion) throws IOException {
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
				consumeRemainingInLimit(codedIS);
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
					if (!poiTypes.isTypeForbidden(subtype) && amenityType == null) {
						amenityType = categoryType;
						record.type = categoryType == null ? "" : safeString(categoryType.getKeyName());
						record.subType = safeString(subtype);
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
				default:
					codedIS.skipField(tagValue);
					break;
			}
		}
	}

	private static String getPoiSubtypeTagName(int subtypeId, BinaryMapPoiReaderAdapter.PoiRegion poiRegion) {
		StringBuilder valueBuilder = new StringBuilder();
		BinaryMapPoiReaderAdapter.PoiSubType poiSubtype = poiRegion.getSubtypeFromId(subtypeId, valueBuilder);
		if (poiSubtype != null && poiSubtype.text && poiSubtype.name != null) {
			return poiSubtype.name;
		}
		return "";
	}

	private static String decodePoiString(String value) {
		return MapObject.unzipContent(safeString(value));
	}

	private static String safeString(String value) {
		return value == null ? "" : value;
	}

	private static Pattern compileNormalizedPattern(String regex) {
		if (regex == null) {
			return null;
		}
		String normalizedRegex = normalizeCaseInsensitiveText(regex);
		return Pattern.compile(normalizedRegex, Pattern.CASE_INSENSITIVE);
	}

	private static boolean matchesPattern(String value, Pattern pattern, Pattern normalizedPattern) {
		if (value == null || pattern == null) {
			return false;
		}
		if (pattern.matcher(value).find()) {
			return true;
		}
		return normalizedPattern != null && normalizedPattern.matcher(normalizeCaseInsensitiveText(value)).find();
	}

	private static String normalizeCaseInsensitiveText(String value) {
		if (value == null) {
			return "";
		}
		String normalizedValue = Normalizer.normalize(value, Normalizer.Form.NFKC).toLowerCase(Locale.ROOT);
		return normalizedValue.replace("\u0307", "");
	}

	default List<Record> getAddresses(String obf, String lang, boolean includesBoundaryPostcode, String cityRegExp, String streetRegExp, String houseRegExp, String poiRegExp) {
		List<Record> results = new ArrayList<>();
		boolean isCityEmpty = cityRegExp == null || cityRegExp.trim().isEmpty();
		boolean isStreetEmpty = streetRegExp == null || streetRegExp.trim().isEmpty();
		boolean isHouseEmpty = houseRegExp == null || houseRegExp.trim().isEmpty();
		boolean isPoiEmpty = poiRegExp == null || poiRegExp.trim().isEmpty();
		if (isCityEmpty && isStreetEmpty && isPoiEmpty)
			return results;

		File file = new File(obf);
		try (RandomAccessFile r = new RandomAccessFile(file.getAbsolutePath(), "r");
			 RandomAccessFile poiRawFile = new RandomAccessFile(file.getAbsolutePath(), "r")) {
			final Pattern cityPattern, streetPattern, housePattern, poiPattern, normalizedPoiPattern;
			try {
				cityPattern = isCityEmpty ? null : Pattern.compile(cityRegExp, Pattern.CASE_INSENSITIVE);
				streetPattern = isStreetEmpty ? null : Pattern.compile(streetRegExp, Pattern.CASE_INSENSITIVE);
				housePattern = isHouseEmpty ? null : Pattern.compile(houseRegExp, Pattern.CASE_INSENSITIVE);
				poiPattern = !(isCityEmpty || isStreetEmpty) || !isHouseEmpty || isPoiEmpty ? null : Pattern.compile(poiRegExp, Pattern.CASE_INSENSITIVE);
				normalizedPoiPattern = poiPattern == null ? null : compileNormalizedPattern(poiRegExp);
			} catch (PatternSyntaxException e) {
				throw new RuntimeException("Invalid regex provided: " + e.getDescription(), e);
			}

			BinaryMapIndexReader index = new BinaryMapIndexReader(r, file);
			try {
				for (BinaryIndexPart p : index.getIndexes()) {
					if (poiPattern == null && p instanceof BinaryMapAddressReaderAdapter.AddressRegion region) {
						for (BinaryMapAddressReaderAdapter.CityBlocks type : BinaryMapAddressReaderAdapter.CityBlocks.values()) {
							if (type == BinaryMapAddressReaderAdapter.CityBlocks.UNKNOWN_TYPE)
								continue;

							final List<City> cities = index.getCities(null, type, region, null);
							for (City c : cities) {
								final boolean isBoundaryOrPostcode = c.getType() == City.CityType.BOUNDARY || c.getType() == City.CityType.POSTCODE;
								if (isBoundaryOrPostcode && !includesBoundaryPostcode) {
									continue;
								}

								final String cityName = c.getName(lang);
								List<StreetAddress> streets = new ArrayList<>();
								if (cityName == null || (!isCityEmpty && !cityPattern.matcher(cityName).find()))
									continue;
								
								index.preloadStreets(c, null, null);
								if (isStreetEmpty && isHouseEmpty) {
									results.add(new CityAddress(cityName, c.getLocation(), streets, c.getStreets().size(), c.getType().name().toLowerCase()));
									continue;
								}

								for (Street s : new ArrayList<>(c.getStreets())) {
									List<HouseAddress> buildings = new ArrayList<>();
									final String streetName = s.getName(lang);
									if (streetName == null || !isStreetEmpty && !streetPattern.matcher(streetName).find())
										continue;

									index.preloadBuildings(s, null, null);
									if (isHouseEmpty) {
										streets.add(new StreetAddress(streetName, s.getLocation(), buildings, s.getBuildings().size()));
										continue;
									}

									final List<Building> bs = s.getBuildings();
									if (bs != null && !bs.isEmpty()) {
										for (Building b : bs) {
											final String houseName = b.getName(lang);
											if (houseName != null && housePattern.matcher(houseName).find())
												buildings.add(new HouseAddress(houseName, b.getLocation()));
										}
									}
									if (!buildings.isEmpty()) {
										StreetAddress street = new StreetAddress(streetName, s.getLocation(), buildings, s.getBuildings().size());
										streets.add(street);
									}
								}
								if (!streets.isEmpty())
									results.add(new CityAddress(cityName, c.getLocation(), streets, c.getStreets().size(), c.getType().name().toLowerCase()));
							}
						}
					} else if (poiPattern != null && p instanceof BinaryMapPoiReaderAdapter.PoiRegion poi) {
						results.addAll(findPoiAddressesRaw(poiRawFile, index, poi, poiPattern, normalizedPoiPattern));
					}
				}
			} finally {
				index.close();
			}
			// Sort results by name (case-insensitive) for CityAddress and Address records
			results.sort(Comparator.comparing(o -> {
				if (o instanceof CityAddress ca) {
					return ca.name();
				} else if (o instanceof PoiAddress a) {
					return a.name();
				}
				return "";
			}, String.CASE_INSENSITIVE_ORDER));
			return results;
		} catch (Exception e) {
			getLogger().error("Failed to read OBF {}", file, e);
			throw new RuntimeException("Failed to read OBF:BinaryMapIndexReader.buildSearchPoiRequest( " + e.getMessage(), e);
		}
	}

	SearchService getSearchService();

	default ResultMetric toMetric(SearchResult r) {
		return new ResultMetric(r.file == null ? "" : r.file.getFile().getName(), r.getDepth(), r.getFoundWordCount(),
				r.getUnknownPhraseMatchWeight(), r.getOtherWordsMatch(), r.location == null ? null :
				MapUtils.getDistance(r.requiredSearchPhrase.getSettings().getOriginalLocation(), r.location)/1000.0,
				r.getCompleteMatchRes().allWordsEqual, r.getCompleteMatchRes().allWordsInPhraseAreInResult);
	}

	record ResultsWithStats(List<AddressResult> results, Collection<BinaryMapIndexReaderStats.WordSearchStat> wordStats,
	                        Map<BinaryMapIndexReaderStats.BinaryMapIndexReaderApiName, BinaryMapIndexReaderStats.StatByAPI> statsByApi) {}
	record ResultMetric(String obf, int depth, double foundWordCount, double unknownPhraseMatchWeight,
	                    Collection<String> otherWordsMatch, Double distance, boolean isEqual, boolean inResult) {}
	record AddressResult(String name, String type, String address, AddressResult parent, ResultMetric metric) {}

	default ResultsWithStats getResults(SearchService.SearchContext ctx, SearchService.SearchOption options) throws IOException {
		SearchService.SearchResults result = getSearchService().getImmediateSearchResults(ctx, options, null);

		List<AddressResult> results = new ArrayList<>();
		for (SearchResult r : result.results()) {
			AddressResult rec = toResult(r, Collections.newSetFromMap(new IdentityHashMap<>()));
			results.add(rec);
		}

		return new ResultsWithStats(results, result.settings().getStat().getWordStats().values(), result.settings().getStat().getByApis());
	}

	private AddressResult toResult(SearchResult r, Set<SearchResult> seen) {
		if (r == null || r == r.parentSearchResult)
			return null;

		ResultMetric metric = toMetric(r);
		String type = r.objectType.name().toLowerCase();

		// If we've already visited this node, break the cycle by not traversing further
		if (!seen.add(r))
			return new AddressResult(r.toString(), type, r.addressName, null, metric);

		AddressResult parent = toResult(r.parentSearchResult, seen);
		return new AddressResult(r.toString(), type, r.addressName, parent, metric);
	}

	record UnitTestPayload(
			@JsonProperty("name") String name,
			@JsonProperty("queries") String[] queries,
			@JsonProperty("resultsLimit") Integer resultsLimit,
			@JsonProperty("geocodingLimit") Integer geocodingLimit) {}

	record UnitTestResultsData(List<List<String>> results, JSONArray routing) {}

	default void createUnitTest(UnitTestPayload unitTest, SearchService.SearchContext ctx, OutputStream out) throws IOException, SQLException {
		SearchExportSettings exportSettings = new SearchExportSettings(true, true, -1);
		SearchService.SearchResults result = getSearchService()
				.getImmediateSearchResults(ctx, new SearchService.SearchOption(true, exportSettings, 
						null, true, (net.osmand.search.core.ObjectType[]) null), null);

		Path rootTmp = Path.of(System.getProperty("java.io.tmpdir"));
		Path dirPath = Files.createTempDirectory(rootTmp, "unit-tests-");
		try {
			File jsonFile = dirPath.resolve(unitTest.name + ".json").toFile();
			String unitTestJson = result.unitTestJson();
			if (unitTestJson == null)
				return;
			JSONObject sourceJson = new JSONObject(unitTestJson);
			int limit = unitTest.resultsLimit();
			int geocodingLimit = unitTest.geocodingLimit();
			UnitTestResultsData unitTestData = buildUnitTestResults(unitTest.queries(), ctx, limit, geocodingLimit);
			if (!unitTestData.routing().isEmpty()) {
				sourceJson.put("routing", unitTestData.routing());
			}
			Files.writeString(jsonFile.toPath(), sourceJson.toString(), StandardCharsets.UTF_8);

			OBFDataCreator creator = new OBFDataCreator();
			File outFile = creator.create(dirPath.resolve(unitTest.name + ".obf").toAbsolutePath().toString(),
					new String[] {jsonFile.getAbsolutePath()});

			// Build ZIP with JSON metadata and gzipped data, streaming directly to the servlet output
			SearchSettings settings = result.settings().setOriginalLocation(new LatLon(ctx.lat(), ctx.lon()));
			JSONObject settingsJson = settings.toJSON();
			JSONArray formattedResultsJson = new JSONArray();
			for (List<String> phraseResults : unitTestData.results()) {
				formattedResultsJson.put(new JSONArray(phraseResults));
			}
			Map<String, Object> rootJson = new LinkedHashMap<>();
			rootJson.put("description", String.format(Locale.US,
					"Created with Search Tool - Detector - Unit-Test (%d results, %d geocoding, routing %b)",
					limit, geocodingLimit, geocodingLimit > 0));
			if (geocodingLimit > 0) {
				rootJson.put("note", "@ prefix means reverse geocoding test for that result");
			}
			rootJson.put("settings", settingsJson);
			rootJson.put("phrases", unitTest.queries());
			rootJson.put("results", formattedResultsJson);
			unitTestJson = new JSONObject(rootJson).toString(4) + "\n";
			try (ZipOutputStream zipOut = new ZipOutputStream(out)) {
				// JSON metadata entry
				if (jsonFile.exists()) {
					ZipEntry jsonEntry = new ZipEntry(jsonFile.getName());
					zipOut.putNextEntry(jsonEntry);
					try (InputStream jsonIn = new StringInputStream(unitTestJson)) {
						Algorithms.streamCopy(jsonIn, zipOut);
					}
					zipOut.closeEntry();
				}

				// Gzipped data archive entry
				if (outFile.exists()) {
					ZipEntry gzEntry = new ZipEntry(outFile.getName());
					zipOut.putNextEntry(gzEntry);
					try (InputStream gzIn = new FileInputStream(outFile)) {
						Algorithms.streamCopy(gzIn, zipOut);
					}
					zipOut.closeEntry();
				}

				zipOut.finish();
				out.flush();
			}
		} finally {
			if (dirPath != null && Files.exists(dirPath)) {
				Files.walk(dirPath)
						.sorted(Comparator.reverseOrder())
						.forEach(p -> {
							File f = p.toFile();
							if (!f.delete()) {
								f.deleteOnExit();
							}
						});
			}
		}
	}

	private List<List<String>> emptyUnitTestResults(String[] phrases) {
		List<List<String>> results = new ArrayList<>();
		String[] phraseArray = phrases == null ? new String[0] : phrases;
		for (int i = 0; i < phraseArray.length; i++) {
			results.add(new ArrayList<>());
		}
		return results;
	}

	private UnitTestResultsData buildUnitTestResults(String[] phrases, SearchService.SearchContext baseCtx, int limit, int geocodingLimit) throws IOException {
		List<List<String>> results = emptyUnitTestResults(phrases);
		JSONArray routing = new JSONArray();
		Map<String, RoutingContext> geocodingContexts = new HashMap<>();
		Map<String, Long> exportedRoutes = new LinkedHashMap<>();
		long nextRouteId = 1;
		GeocodingUtilities geoUtils = new GeocodingUtilities();
		String[] phraseArray = phrases == null ? new String[0] : phrases;
		for (int phraseIndex = 0; phraseIndex < phraseArray.length; phraseIndex++) {
			String query = phraseArray[phraseIndex];
			SearchService.SearchContext phraseCtx = new SearchService.SearchContext(
					baseCtx.lat(), baseCtx.lon(), query == null ? "" : query, baseCtx.locale(),
					baseCtx.baseSearch(), baseCtx.northWest(), baseCtx.southEast());
			SearchService.SearchResults searchResult = getSearchService().getImmediateSearchResults(
					phraseCtx,
					new SearchService.SearchOption(true, null, null, true, (net.osmand.search.core.ObjectType[]) null),
					null);
			SearchPhrase phrase = searchResult.phrase();
			List<SearchResult> searchResults = searchResult.results();
			if (phrase == null || searchResults == null) {
				continue;
			}

			List<String> phraseResults = results.get(phraseIndex);
			for (int i = 0; i < Math.min(limit, searchResults.size()); i++) {
				SearchResult searchResultItem = searchResults.get(i);
				boolean markGeocoding = i < geocodingLimit && isReverseGeocodingCandidate(searchResultItem);
				if (markGeocoding) {
					nextRouteId = exportReverseGeocodingRoutes(searchResultItem, geoUtils, geocodingContexts,
							exportedRoutes, routing, nextRouteId);
				}
				String formatted = SearchUICore.formatSearchResultForTest(false, searchResultItem, phrase);
				phraseResults.add(markGeocoding ? "@" + formatted : formatted);
			}
		}
		return new UnitTestResultsData(results, routing);
	}

	private boolean isReverseGeocodingCandidate(SearchResult searchResult) {
		return searchResult != null
				&& searchResult.file != null
				&& searchResult.location != null;
	}

	private long exportReverseGeocodingRoutes(SearchResult searchResult, GeocodingUtilities geoUtils,
			Map<String, RoutingContext> geocodingContexts, Map<String, Long> exportedRoutes,
			JSONArray routing, long nextRouteId) throws IOException {
		BinaryMapIndexReader reader = searchResult.file;
		File file = reader.getFile();
		if (file == null) {
			return nextRouteId;
		}
		String readerKey = file.getAbsolutePath();
		RoutingContext ctx = geocodingContexts.get(readerKey);
		if (ctx == null) {
			ctx = GeocodingUtilities.buildDefaultContextForPOI(reader);
			geocodingContexts.put(readerKey, ctx);
		}
		List<GeocodingUtilities.GeocodingResult> geoResults = geoUtils.reverseGeocodingSearch(
				ctx, searchResult.location.getLatitude(), searchResult.location.getLongitude(), false);
		for (GeocodingUtilities.GeocodingResult geoResult : geoResults) {
			if (geoResult.point == null || geoResult.point.getRoad() == null) {
				continue;
			}
			RouteDataObject road = geoResult.point.getRoad();
			String routeKey = road.region.getFilePointer() + ":" + road.region.getLength() + ":" + road.getId();
			if (exportedRoutes.containsKey(routeKey)) {
				continue;
			}
			long routeId = nextRouteId++;
			exportedRoutes.put(routeKey, routeId);
			routing.put(routeDataObjectToJson(road, routeId));
		}
		return nextRouteId;
	}

	private JSONObject routeDataObjectToJson(RouteDataObject road, long routeId) {
		JSONObject routeJson = new JSONObject();
		routeJson.put("id", routeId);
		routeJson.put("pointsX", toJsonArray(road.pointsX));
		routeJson.put("pointsY", toJsonArray(road.pointsY));
		JSONArray types = new JSONArray();
		if (road.types != null) {
			for (int type : road.types) {
				BinaryMapRouteReaderAdapter.RouteTypeRule rule = road.region.quickGetEncodingRule(type);
				if (rule != null) {
					JSONObject typeJson = new JSONObject();
					typeJson.put("tag", rule.getTag());
					typeJson.put("value", rule.getValue());
					types.put(typeJson);
				}
			}
		}
		routeJson.put("types", types);
		JSONArray names = new JSONArray();
		if (road.nameIds != null && road.names != null) {
			for (int nameId : road.nameIds) {
				BinaryMapRouteReaderAdapter.RouteTypeRule rule = road.region.quickGetEncodingRule(nameId);
				if (rule == null) {
					continue;
				}
				JSONObject nameJson = new JSONObject();
				nameJson.put("tag", rule.getTag());
				nameJson.put("value", road.names.get(nameId));
				names.put(nameJson);
			}
		}
		routeJson.put("names", names);
		return routeJson;
	}

	private JSONArray toJsonArray(int[] values) {
		JSONArray arr = new JSONArray();
		if (values == null) {
			return arr;
		}
		for (int value : values) {
			arr.put(value);
		}
		return arr;
	}

	default List<IndexToken> getIndex(String obf, String prefix) {
		File file = new File(obf);
		Pattern prefixPattern = compileIndexPrefixPattern(prefix);
		try {
			List<IndexToken> allTokens = getCachedOrLoadIndexTokens(file);
			if (prefixPattern == null) {
				return new ArrayList<>(allTokens);
			}
			List<IndexToken> results = new ArrayList<>();
			for (IndexToken token : allTokens) {
				if (prefixPattern.matcher(token.name()).find()) {
					results.add(token);
				}
			}
			return results;
		} catch (Exception e) {
			getLogger().error("Failed to read OBF index {}", file, e);
			throw new RuntimeException("Failed to read OBF index: " + e.getMessage(), e);
		}
	}

	private List<IndexToken> getCachedOrLoadIndexTokens(File file) throws IOException {
		String cacheKey = getIndexCacheKey(file);
		long fileLength = file.length();
		long lastModified = file.lastModified();
		CachedIndexTokens cached;
		synchronized (INDEX_TOKENS_CACHE) {
			cached = INDEX_TOKENS_CACHE.get(cacheKey);
		}
		if (cached != null && cached.fileLength() == fileLength && cached.lastModified() == lastModified) {
			return cached.tokens();
		}
		List<IndexToken> loadedTokens = loadIndexTokens(file);
		List<IndexToken> cachedTokens = List.copyOf(loadedTokens);
		synchronized (INDEX_TOKENS_CACHE) {
			INDEX_TOKENS_CACHE.put(cacheKey, new CachedIndexTokens(cacheKey, fileLength, lastModified, cachedTokens));
		}
		return cachedTokens;
	}

	private List<IndexToken> loadIndexTokens(File file) throws IOException {
		try (RandomAccessFile randomAccessFile = new RandomAccessFile(file.getAbsolutePath(), "r")) {
			BinaryMapIndexReaderExt index = new BinaryMapIndexReaderExt(randomAccessFile, file);
			try {
				Map<String, IndexToken> tokens = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
				List<BinaryMapPoiReaderAdapter.PoiRegion> poiRegions = new ArrayList<>();
				for (BinaryIndexPart part : index.getIndexes()) {
					if (part instanceof BinaryMapAddressReaderAdapter.AddressRegion addressRegion) {
						collectAddressIndexTokens(index, addressRegion, tokens);
					} else if (part instanceof BinaryMapPoiReaderAdapter.PoiRegion poiRegion) {
						poiRegions.add(poiRegion);
						collectPoiIndexTokens(index, poiRegion, tokens);
					}
				}
				return buildIndexTokensWithSizes(index, tokens, poiRegions);
			} finally {
				index.close();
			}
		}
	}

	private String getIndexCacheKey(File file) throws IOException {
		return file.getCanonicalPath();
	}

	private Pattern compileIndexPrefixPattern(String prefix) {
		if (prefix == null || prefix.trim().isEmpty()) {
			return null;
		}
		try {
			return Pattern.compile(prefix, Pattern.CASE_INSENSITIVE);
		} catch (PatternSyntaxException e) {
			throw new RuntimeException("Invalid regex provided: " + e.getDescription(), e);
		}
	}

	private void collectAddressIndexTokens(BinaryMapIndexReaderExt index, BinaryMapAddressReaderAdapter.AddressRegion region,
			Map<String, IndexToken> tokens) throws IOException {
		long indexNameOffset = region.getIndexNameOffset();
		if (indexNameOffset < 0) {
			return;
		}
		index.getInputStream().seek(indexNameOffset);
		long length = index.readInt();
		long oldLimit = index.getInputStream().pushLimitLong(length);
		try {
			readAddressNameIndexTokens(index, tokens);
		} finally {
			index.getInputStream().popLimit(oldLimit);
		}
	}

	private long calculatePoiObjectSizeAtShift(BinaryMapIndexReaderExt index,
			BinaryMapPoiReaderAdapter.PoiRegion region,
			int relativeOffset) throws IOException {
		index.getInputStream().seek(region.getFilePointer() + relativeOffset);
		long length = readInt(index.getInputStream());
		long oldLimit = index.getInputStream().pushLimitLong(length);
		try {
			long totalSize = 0L;
			while (true) {
				int tagWithType = index.getInputStream().readTag();
				int tag = WireFormat.getTagFieldNumber(tagWithType);
				switch (tag) {
					case 0:
						return totalSize;
					case OsmandOdb.OsmAndPoiBoxData.POIDATA_FIELD_NUMBER:
						int poiLength = index.getInputStream().readRawVarint32();
						totalSize += poiLength + computeVarint32Size(poiLength);
						index.getInputStream().skipRawBytes(poiLength);
						break;
					default:
						skipUnknownField(index.getInputStream(), tagWithType);
						break;
				}
			}
		} finally {
			index.getInputStream().popLimit(oldLimit);
		}
	}

	private void collectPoiIndexTokens(BinaryMapIndexReaderExt index, BinaryMapPoiReaderAdapter.PoiRegion region,
			Map<String, IndexToken> tokens) throws IOException {
		index.getInputStream().seek(region.getFilePointer());
		long oldLimit = index.getInputStream().pushLimitLong(region.getLength());
		try {
			while (true) {
				int tagWithType = index.getInputStream().readTag();
				int tag = WireFormat.getTagFieldNumber(tagWithType);
				switch (tag) {
					case 0:
						return;
					case OsmandOdb.OsmAndPoiIndex.NAMEINDEX_FIELD_NUMBER:
						long length2 = index.readInt();
						long nameIndexOldLimit = index.getInputStream().pushLimitLong(length2);
						try {
							readPoiNameIndexTokens(index, tokens);
						} finally {
							index.getInputStream().popLimit(nameIndexOldLimit);
						}
						return;
					default:
						skipUnknownField(index.getInputStream(), tagWithType);
						break;
				}
			}
		} finally {
			index.getInputStream().popLimit(oldLimit);
		}
	}

	private void readAddressNameIndexTokens(BinaryMapIndexReaderExt index, Map<String, IndexToken> tokens) throws IOException {
		while (true) {
			int tagWithType = index.getInputStream().readTag();
			int tag = WireFormat.getTagFieldNumber(tagWithType);
			switch (tag) {
				case 0:
					return;
				case OsmandOdb.OsmAndAddressNameIndexData.TABLE_FIELD_NUMBER:
					readNameIndexTableTokens(index, tokens, false);
					return;
				default:
					skipUnknownField(index.getInputStream(), tagWithType);
					break;
			}
		}
	}

	private void readPoiNameIndexTokens(BinaryMapIndexReaderExt index, Map<String, IndexToken> tokens) throws IOException {
		while (true) {
			int tagWithType = index.getInputStream().readTag();
			int tag = WireFormat.getTagFieldNumber(tagWithType);
			switch (tag) {
				case 0:
					return;
				case OsmandOdb.OsmAndPoiNameIndex.TABLE_FIELD_NUMBER:
					readNameIndexTableTokens(index, tokens, true);
					return;
				default:
					skipUnknownField(index.getInputStream(), tagWithType);
					break;
			}
		}
	}

	private void readNameIndexTableTokens(BinaryMapIndexReaderExt index, Map<String, IndexToken> tokens, boolean poi) throws IOException {
		long tableLength = index.readInt();
		long tableContentStart = index.getInputStream().getTotalBytesRead();
		Map<String, Integer> prefixOffsets = new TreeMap<>();
		long oldLimit = index.getInputStream().pushLimitLong(tableLength);
		try {
			readIndexedStringTableOffsets(index, "", prefixOffsets);
		} finally {
			index.getInputStream().popLimit(oldLimit);
		}
		for (Map.Entry<String, Integer> entry : prefixOffsets.entrySet()) {
			long absoluteOffset = tableContentStart + entry.getValue();
			List<String> suffixDictionary = readSuffixDictionaryAtOffset(index, absoluteOffset, poi);
			for (int suffixIndex = 0; suffixIndex < suffixDictionary.size(); suffixIndex++) {
				String suffix = suffixDictionary.get(suffixIndex);
				addIndexToken(tokens, entry.getKey() + suffix, (int) absoluteOffset, poi, suffixIndex);
			}
		}
	}

	private void readIndexedStringTableOffsets(BinaryMapIndexReaderExt index, String prefix,
			Map<String, Integer> prefixOffsets) throws IOException {
		String currentKey = null;
		while (true) {
			int tagWithType = index.getInputStream().readTag();
			int tag = WireFormat.getTagFieldNumber(tagWithType);
			switch (tag) {
				case 0:
					return;
				case OsmandOdb.IndexedStringTable.KEY_FIELD_NUMBER:
					currentKey = prefix + index.getInputStream().readString();
					break;
				case OsmandOdb.IndexedStringTable.VAL_FIELD_NUMBER:
					int offset = (int) index.readInt();
					if (currentKey != null) {
						prefixOffsets.put(currentKey, offset);
					}
					break;
				case OsmandOdb.IndexedStringTable.SUBTABLES_FIELD_NUMBER:
					long length = index.getInputStream().readRawVarint32();
					long oldLimit = index.getInputStream().pushLimitLong(length);
					try {
						readIndexedStringTableOffsets(index, currentKey == null ? prefix : currentKey, prefixOffsets);
					} finally {
						index.getInputStream().popLimit(oldLimit);
					}
					break;
				default:
					skipUnknownField(index.getInputStream(), tagWithType);
					break;
			}
		}
	}

	private List<String> readSuffixDictionaryAtOffset(BinaryMapIndexReaderExt index, long absoluteOffset, boolean poi) throws IOException {
		index.getInputStream().seek(absoluteOffset);
		long length = index.getInputStream().readRawVarint32();
		long oldLimit = index.getInputStream().pushLimitLong(length);
		try {
			List<String> suffixDictionary = new ArrayList<>();
			while (true) {
				int tagWithType = index.getInputStream().readTag();
				int tag = WireFormat.getTagFieldNumber(tagWithType);
				if (tag == 0) {
					return suffixDictionary;
				}
				boolean isSuffixField = poi
						? tag == OsmandOdb.OsmAndPoiNameIndex.OsmAndPoiNameIndexData.SUFFIXESDICTIONARY_FIELD_NUMBER
						: tag == OsmandOdb.OsmAndAddressNameIndexData.AddressNameIndexData.SUFFIXESDICTIONARY_FIELD_NUMBER;
				boolean isAtomField = poi
						? tag == OsmandOdb.OsmAndPoiNameIndex.OsmAndPoiNameIndexData.ATOMS_FIELD_NUMBER
						: tag == OsmandOdb.OsmAndAddressNameIndexData.AddressNameIndexData.ATOM_FIELD_NUMBER;
				if (isSuffixField) {
					String encodedSuffix = index.getInputStream().readString();
					if (SearchAlgorithms.EMPTY_SUFFIX_DICTIONARY_SENTINEL.equals(encodedSuffix)) {
						continue;
					}
					String previousSuffix = suffixDictionary.isEmpty() ? null : suffixDictionary.get(suffixDictionary.size() - 1);
					String decodedSuffix = SearchAlgorithms.nameIndexDecodeDictionarySuffix(previousSuffix, encodedSuffix);
					suffixDictionary.add(decodedSuffix);
					continue;
				}
				if (isAtomField) {
					index.getInputStream().skipRawBytes(index.getInputStream().getBytesUntilLimit());
					return suffixDictionary;
				}
				skipUnknownField(index.getInputStream(), tagWithType);
			}
		} finally {
			index.getInputStream().popLimit(oldLimit);
		}
	}

	private void addIndexToken(Map<String, IndexToken> tokens, String name, int offset, boolean poi, int exactSuffixIndex) {
		IndexToken existing = tokens.get(name);
		if (existing == null) {
			tokens.put(name, poi
					? new IndexToken(name, new int[0], new int[] {offset}, new int[] {offset}, new int[] {exactSuffixIndex}, 0, 0, 0, 0)
					: new IndexToken(name, new int[] {offset}, new int[0], new int[0], new int[0], 0, 0, 0, 0));
			return;
		}
		int[] mergedAddressOffsets = poi ? existing.addressOffsets() : appendDistinctOffset(existing.addressOffsets(), offset);
		int[] mergedPoiOffsets = existing.poiOffsets();
		int[] mergedPoiTokenOffsets = existing.poiTokenOffsets();
		int[] mergedPoiExactSuffixIndexes = existing.poiExactSuffixIndexes();
		if (poi) {
			boolean alreadyExists = false;
			for (int i = 0; i < mergedPoiTokenOffsets.length; i++) {
				if (mergedPoiTokenOffsets[i] == offset && i < mergedPoiExactSuffixIndexes.length && mergedPoiExactSuffixIndexes[i] == exactSuffixIndex) {
					alreadyExists = true;
					break;
				}
			}
			if (!alreadyExists) {
				mergedPoiOffsets = appendDistinctOffset(mergedPoiOffsets, offset);
				mergedPoiTokenOffsets = appendOffset(mergedPoiTokenOffsets, offset);
				mergedPoiExactSuffixIndexes = Arrays.copyOf(mergedPoiExactSuffixIndexes, mergedPoiExactSuffixIndexes.length + 1);
				mergedPoiExactSuffixIndexes[mergedPoiExactSuffixIndexes.length - 1] = exactSuffixIndex;
			}
		}
		tokens.put(name, new IndexToken(name, mergedAddressOffsets, mergedPoiOffsets, mergedPoiTokenOffsets, mergedPoiExactSuffixIndexes,
				existing.addressSize(), existing.poiSize(), existing.addressCount(), existing.poiCount()));
	}

	private List<IndexToken> buildIndexTokensWithSizes(BinaryMapIndexReaderExt index,
			Map<String, IndexToken> tokens,
			List<BinaryMapPoiReaderAdapter.PoiRegion> poiRegions) throws IOException {
		List<IndexToken> sizedTokens = new ArrayList<>(tokens.size());
		for (IndexToken token : tokens.values()) {
			Set<Integer> allAddressOffsets = collectUniqueAddressObjectOffsets(index, token, false);
			Set<Integer> tokenAddressOffsets = collectUniqueAddressObjectOffsets(index, token, true);
			Map<BinaryMapPoiReaderAdapter.PoiRegion, Set<Integer>> tokenPoiOffsets = collectUniquePoiObjectOffsets(index, token, poiRegions, true, false);
			int addressCount = tokenAddressOffsets.size();
			int poiCount = countPoiObjectsByStoredOffsets(index, tokenPoiOffsets);
			sizedTokens.add(new IndexToken(token.name(), token.addressOffsets(), flattenPoiOffsets(tokenPoiOffsets, poiRegions), token.poiTokenOffsets(), token.poiExactSuffixIndexes(),
					0, 0, addressCount, poiCount));
		}
		return sizedTokens;
	}

	private Set<Integer> collectUniqueAddressObjectOffsets(BinaryMapIndexReaderExt index, IndexToken token, boolean isFiltered) throws IOException {
		Set<Integer> uniqueObjectOffsets = new LinkedHashSet<>();
		int[] addressOffsets = token.addressOffsets();
		if (addressOffsets == null) {
			return uniqueObjectOffsets;
		}
		for (int tokenOffset : addressOffsets) {
			AddressTokenRefs refs = readAddressTokenRefs(index, token.name(), tokenOffset, isFiltered);
			uniqueObjectOffsets.addAll(refs.cityOffsets);
			uniqueObjectOffsets.addAll(refs.streetOffsets);
		}
		return uniqueObjectOffsets;
	}

	private long calculateAddressTokenSize(BinaryMapIndexReaderExt index, Set<Integer> uniqueObjectOffsets) throws IOException {
		long totalSize = 0L;
		for (Integer objectOffset : uniqueObjectOffsets) {
			totalSize += getLengthDelimitedMessageSizeAtOffset(index, objectOffset);
		}
		return totalSize;
	}

	private long calculateObjectAddressesSize(BinaryMapIndexReaderExt index,
			List<ObjectAddress> objects,
			List<BinaryMapPoiReaderAdapter.PoiRegion> poiRegions) throws IOException {
		if (objects == null || objects.isEmpty()) {
			return 0L;
		}
		long totalSize = 0L;
		Set<Integer> visitedOffsets = new LinkedHashSet<>();
		for (ObjectAddress object : objects) {
			if (object == null) {
				continue;
			}
			int payloadOffset = object.payloadOffset();
			if (payloadOffset <= 0 || !visitedOffsets.add(payloadOffset)) {
				continue;
			}
			int payloadSize = object.payloadSize();
			if (payloadSize > 0) {
				totalSize += payloadSize;
				continue;
			}
			if (object.isPoi()) {
				totalSize += getPoiDataBoxSizeAtAbsoluteOffset(index, poiRegions, payloadOffset);
			} else {
				totalSize += getLengthDelimitedMessageSizeAtOffset(index, payloadOffset);
			}
		}
		return totalSize;
	}

	private long calculatePoiObjectSizeByStoredOffsets(BinaryMapIndexReaderExt index,
			Map<BinaryMapPoiReaderAdapter.PoiRegion, Set<Integer>> storedPoiOffsets) throws IOException {
		if (storedPoiOffsets.isEmpty()) {
			return 0L;
		}
		long totalSize = 0L;
		for (Map.Entry<BinaryMapPoiReaderAdapter.PoiRegion, Set<Integer>> entry : storedPoiOffsets.entrySet()) {
			for (Integer relativeOffset : entry.getValue()) {
				totalSize += calculatePoiObjectSizeAtShift(index, entry.getKey(), relativeOffset);
			}
		}
		return totalSize;
	}

	private Map<BinaryMapPoiReaderAdapter.PoiRegion, Set<Integer>> collectUniquePoiObjectOffsets(BinaryMapIndexReaderExt index,
			IndexToken token,
			List<BinaryMapPoiReaderAdapter.PoiRegion> poiRegions,
			boolean isFiltered,
			boolean flattenedPoiOffsets) throws IOException {
		Map<BinaryMapPoiReaderAdapter.PoiRegion, Set<Integer>> uniqueOffsetsByRegion = new LinkedHashMap<>();
		int[] poiOffsets = isFiltered ? token.poiTokenOffsets() : token.poiOffsets();
		int[] poiExactSuffixIndexes = token.poiExactSuffixIndexes();
		if (poiOffsets == null || poiRegions == null || poiRegions.isEmpty()) {
			return uniqueOffsetsByRegion;
		}
		for (BinaryMapPoiReaderAdapter.PoiRegion poiRegion : poiRegions) {
			Set<Integer> uniqueRelativeOffsets = new LinkedHashSet<>();
			for (int indexInToken = 0; indexInToken < poiOffsets.length; indexInToken++) {
				int tokenOffset = poiOffsets[indexInToken];
				if (!isOffsetWithinPart(tokenOffset, poiRegion)) {
					continue;
				}
				if (isFiltered) {
					int exactSuffixIndex = indexInToken < poiExactSuffixIndexes.length ? poiExactSuffixIndexes[indexInToken] : -1;
					uniqueRelativeOffsets.addAll(readPoiTokenOffsets(index, tokenOffset, exactSuffixIndex, true));
				} else if (flattenedPoiOffsets) {
					uniqueRelativeOffsets.add((int) (tokenOffset - poiRegion.getFilePointer()));
				} else {
					uniqueRelativeOffsets.addAll(readPoiTokenOffsets(index, tokenOffset, -1, false));
				}
			}
			if (!uniqueRelativeOffsets.isEmpty()) {
				uniqueOffsetsByRegion.put(poiRegion, uniqueRelativeOffsets);
			}
		}
		return uniqueOffsetsByRegion;
	}

	private int[] toIntArray(Collection<Integer> offsets) {
		if (offsets == null || offsets.isEmpty()) {
			return new int[0];
		}
		int[] values = new int[offsets.size()];
		int index = 0;
		for (Integer offset : offsets) {
			values[index++] = offset == null ? 0 : offset;
		}
		return values;
	}

	private int[] flattenPoiOffsets(Map<BinaryMapPoiReaderAdapter.PoiRegion, Set<Integer>> offsetsByRegion,
			List<BinaryMapPoiReaderAdapter.PoiRegion> poiRegions) {
		if (offsetsByRegion == null || offsetsByRegion.isEmpty() || poiRegions == null || poiRegions.isEmpty()) {
			return new int[0];
		}
		List<Integer> flattenedOffsets = new ArrayList<>();
		for (BinaryMapPoiReaderAdapter.PoiRegion poiRegion : poiRegions) {
			Set<Integer> relativeOffsets = offsetsByRegion.get(poiRegion);
			if (relativeOffsets == null || relativeOffsets.isEmpty()) {
				continue;
			}
			for (Integer relativeOffset : relativeOffsets) {
				if (relativeOffset != null) {
					flattenedOffsets.add((int) poiRegion.getFilePointer() + relativeOffset);
				}
			}
		}
		return toIntArray(flattenedOffsets);
	}

	private boolean isOffsetWithinPart(int offset, BinaryIndexPart part) {
		long partStart = part.getFilePointer();
		long partEnd = partStart + part.getLength();
		return offset >= partStart && offset < partEnd;
	}

	private int computeVarint32Size(long value) {
		int size = 1;
		long normalizedValue = value;
		while ((normalizedValue & ~0x7FL) != 0L) {
			size++;
			normalizedValue >>>= 7;
		}
		return size;
	}

	private int computeSmartLengthPrefixSize(long value) {
		return value > 0x7f ? 8 : 1;
	}

	private int[] appendDistinctOffset(int[] offsets, int offset) {
		if (offsets == null || offsets.length == 0) {
			return new int[] {offset};
		}
		for (int existingOffset : offsets) {
			if (existingOffset == offset) {
				return offsets;
			}
		}
		int[] mergedOffsets = Arrays.copyOf(offsets, offsets.length + 1);
		mergedOffsets[offsets.length] = offset;
		return mergedOffsets;
	}

	private int[] appendOffset(int[] offsets, int offset) {
		if (offsets == null || offsets.length == 0) {
			return new int[] {offset};
		}
		int[] mergedOffsets = Arrays.copyOf(offsets, offsets.length + 1);
		mergedOffsets[offsets.length] = offset;
		return mergedOffsets;
	}

	private int safeMetricInt(long value) {
		if (value <= 0L) {
			return 0;
		}
		return value >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) value;
	}

	private long getLengthDelimitedMessageSizeAtOffset(BinaryMapIndexReaderExt index, int absoluteOffset) throws IOException {
		if (absoluteOffset <= 0) {
			return 0L;
		}
		index.getInputStream().seek(absoluteOffset);
		long contentLength = index.getInputStream().readRawVarint32();
		return contentLength + computeVarint32Size(contentLength);
	}

	private long getPoiDataBoxSizeAtRelativeOffset(BinaryMapIndexReaderExt index,
			BinaryMapPoiReaderAdapter.PoiRegion poiRegion,
			int relativeOffset) throws IOException {
		if (relativeOffset < 0) {
			return 0L;
		}
		long absoluteOffset = poiRegion.getFilePointer() + relativeOffset;
		index.getInputStream().seek(absoluteOffset);
		long contentLength = readInt(index.getInputStream());
		return contentLength + computeSmartLengthPrefixSize(contentLength);
	}

	private long getPoiDataBoxSizeAtAbsoluteOffset(BinaryMapIndexReaderExt index,
			List<BinaryMapPoiReaderAdapter.PoiRegion> poiRegions,
			int absoluteOffset) throws IOException {
		if (absoluteOffset <= 0 || poiRegions == null || poiRegions.isEmpty()) {
			return 0L;
		}
		for (BinaryMapPoiReaderAdapter.PoiRegion poiRegion : poiRegions) {
			if (isOffsetWithinPart(absoluteOffset, poiRegion)) {
				return getPoiDataBoxSizeAtRelativeOffset(index, poiRegion, (int) (absoluteOffset - poiRegion.getFilePointer()));
			}
		}
		return 0L;
	}

	private int countPoiObjectsByStoredOffsets(BinaryMapIndexReaderExt index,
			Map<BinaryMapPoiReaderAdapter.PoiRegion, Set<Integer>> storedPoiOffsets) throws IOException {
		if (storedPoiOffsets.isEmpty()) {
			return 0;
		}
		int totalCount = 0;
		for (Map.Entry<BinaryMapPoiReaderAdapter.PoiRegion, Set<Integer>> entry : storedPoiOffsets.entrySet()) {
			for (Integer relativeOffset : entry.getValue()) {
				totalCount += countPoiObjectsAtShift(index, entry.getKey(), relativeOffset);
			}
		}
		return totalCount;
	}

	private int countPoiObjectsAtShift(BinaryMapIndexReaderExt index,
			BinaryMapPoiReaderAdapter.PoiRegion region,
			int relativeOffset) throws IOException {
		index.getInputStream().seek(region.getFilePointer() + relativeOffset);
		long length = readInt(index.getInputStream());
		long oldLimit = index.getInputStream().pushLimitLong(length);
		try {
			int count = 0;
			while (true) {
				int tagWithType = index.getInputStream().readTag();
				int tag = WireFormat.getTagFieldNumber(tagWithType);
				switch (tag) {
					case 0:
						return count;
					case OsmandOdb.OsmAndPoiBoxData.POIDATA_FIELD_NUMBER:
						int poiLength = index.getInputStream().readRawVarint32();
						index.getInputStream().skipRawBytes(poiLength);
						count++;
						break;
					default:
						skipUnknownField(index.getInputStream(), tagWithType);
						break;
				}
			}
		} finally {
			index.getInputStream().popLimit(oldLimit);
		}
	}

	private AddressTokenRefs readAddressTokenRefs(BinaryMapIndexReaderExt index,
			String tokenName,
			int tokenOffset,
			boolean isFiltered) throws IOException {
		AddressTokenRefs refs = new AddressTokenRefs();
		index.getInputStream().seek(tokenOffset);
		long length = index.getInputStream().readRawVarint32();
		long oldLimit = index.getInputStream().pushLimitLong(length);
		try {
			List<String> suffixDictionary = new ArrayList<>();
			List<Integer> matchedSuffixIndexes = isFiltered ? new ArrayList<>() : null;
			while (true) {
				int tagWithType = index.getInputStream().readTag();
				int tag = WireFormat.getTagFieldNumber(tagWithType);
				switch (tag) {
					case 0:
						return refs;
					case OsmandOdb.OsmAndAddressNameIndexData.AddressNameIndexData.SUFFIXESDICTIONARY_FIELD_NUMBER:
						String encodedSuffix = index.getInputStream().readString();
						if (SearchAlgorithms.EMPTY_SUFFIX_DICTIONARY_SENTINEL.equals(encodedSuffix)) {
							break;
						}
						String previousSuffix = suffixDictionary.isEmpty() ? null : suffixDictionary.get(suffixDictionary.size() - 1);
						String decodedSuffix = SearchAlgorithms.nameIndexDecodeDictionarySuffix(previousSuffix, encodedSuffix);
						if (isFiltered && (tokenName.equals(decodedSuffix) || tokenName.endsWith(decodedSuffix))) {
							matchedSuffixIndexes.add(suffixDictionary.size());
						}
						suffixDictionary.add(decodedSuffix);
						break;
					case OsmandOdb.OsmAndAddressNameIndexData.AddressNameIndexData.ATOM_FIELD_NUMBER:
						int atomLength = index.getInputStream().readRawVarint32();
						long atomOldLimit = index.getInputStream().pushLimitLong(atomLength);
						try {
							readAddressTokenAtom(index, tokenOffset, matchedSuffixIndexes, refs, isFiltered);
						} finally {
							index.getInputStream().popLimit(atomOldLimit);
						}
						break;
					default:
						skipUnknownField(index.getInputStream(), tagWithType);
						break;
				}
			}
		} finally {
			index.getInputStream().popLimit(oldLimit);
		}
	}

	private void readAddressTokenAtom(BinaryMapIndexReaderExt index,
			int tokenOffset,
			List<Integer> matchedSuffixIndexes,
			AddressTokenRefs refs,
			boolean isFiltered) throws IOException {
		int objectOffset = 0;
		int cityOffset = 0;
		int typeIndex = -1;
		boolean matched = !isFiltered;
		int maskIndex = 0;
		while (true) {
			int tagWithType = index.getInputStream().readTag();
			int tag = WireFormat.getTagFieldNumber(tagWithType);
			if (tag == 0 || tag == OsmandOdb.AddressNameIndexDataAtom.SHIFTTOINDEX_FIELD_NUMBER) {
				if (matched) {
					if (typeIndex >= 0 && typeIndex < BinaryMapAddressReaderAdapter.CityBlocks.STREET_TYPE.index && objectOffset != 0) {
						refs.cityOffsets.add(objectOffset);
					}
					if (typeIndex == BinaryMapAddressReaderAdapter.CityBlocks.STREET_TYPE.index && objectOffset != 0) {
						refs.streetOffsets.add(objectOffset);
						refs.streetCityOffsets.add(cityOffset);
					}
				}
			}
			if (tag == 0) {
				return;
			}
			switch (tag) {
				case OsmandOdb.AddressNameIndexDataAtom.NAMEEN_FIELD_NUMBER:
				case OsmandOdb.AddressNameIndexDataAtom.NAME_FIELD_NUMBER:
					index.getInputStream().readString();
					break;
				case OsmandOdb.AddressNameIndexDataAtom.SUFFIXESBITSET_FIELD_NUMBER:
					int mask = index.getInputStream().readUInt32();
					if (isFiltered && !matched && matchesSuffixMask(maskIndex, mask, matchedSuffixIndexes)) {
						matched = true;
					}
					maskIndex++;
					break;
				case OsmandOdb.AddressNameIndexDataAtom.SHIFTTOCITYINDEX_FIELD_NUMBER:
					cityOffset = tokenOffset - index.getInputStream().readInt32();
					break;
				case OsmandOdb.AddressNameIndexDataAtom.XY16_FIELD_NUMBER:
					index.getInputStream().readInt32();
					break;
				case OsmandOdb.AddressNameIndexDataAtom.SHIFTTOINDEX_FIELD_NUMBER:
					objectOffset = tokenOffset - index.getInputStream().readInt32();
					break;
				case OsmandOdb.AddressNameIndexDataAtom.TYPE_FIELD_NUMBER:
					typeIndex = index.getInputStream().readInt32();
					break;
				default:
					skipUnknownField(index.getInputStream(), tagWithType);
					break;
			}
		}
	}

	private City loadCity(BinaryMapIndexReaderExt index,
			BinaryMapAddressReaderAdapter.AddressRegion region,
			int offset) throws IOException {
		index.getInputStream().seek(offset);
		long length = index.getInputStream().readRawVarint32();
		long oldLimit = index.getInputStream().pushLimitLong(length);
		try {
			return readCityAtOffset(index.getInputStream(), offset, region.getAttributeTagsTable());
		} finally {
			index.getInputStream().popLimit(oldLimit);
		}
	}

	private ObjectAddress loadCityObjectAddress(BinaryMapIndexReaderExt index,
			BinaryMapAddressReaderAdapter.AddressRegion region,
			int offset,
			String lang) throws IOException {
		index.getInputStream().seek(offset);
		long length = index.getInputStream().readRawVarint32();
		long oldLimit = index.getInputStream().pushLimitLong(length);
		City city;
		try {
			city = readCityAtOffset(index.getInputStream(), offset, region.getAttributeTagsTable());
		} finally {
			index.getInputStream().popLimit(oldLimit);
		}
		if (city == null) {
			return null;
		}
		Map<String, String> values = buildMapObjectValues(city, lang);
		String type = city.getType() == null ? null : city.getType().name();
		int payloadSize = safeMetricInt(length + computeVarint32Size(length));
		return new ObjectAddress(city.getName(lang), city.getLocation(), values, false, type, city.getId(), null, offset, payloadSize);
	}

	private ObjectAddress loadStreetObjectAddress(BinaryMapIndexReaderExt index,
			BinaryMapAddressReaderAdapter.AddressRegion region,
			int offset,
			City city,
			String lang) throws IOException {
		if (city == null || city.getLocation() == null) {
			return null;
		}
		int cityX = MapUtils.get31TileNumberX(city.getLocation().getLongitude()) >> 7;
		int cityY = MapUtils.get31TileNumberY(city.getLocation().getLatitude()) >> 7;
		index.getInputStream().seek(offset);
		long length = index.getInputStream().readRawVarint32();
		long oldLimit = index.getInputStream().pushLimitLong(length);
		try {
			Street street = readStreetAtOffset(index.getInputStream(), city, cityX, cityY, region.getAttributeTagsTable());
			Map<String, String> values = buildMapObjectValues(street, lang);
			int payloadSize = safeMetricInt(length + computeVarint32Size(length));
			return new ObjectAddress(street.getName(lang), street.getLocation(), values, false, "Street", street.getId(), null, offset, payloadSize);
		} finally {
			index.getInputStream().popLimit(oldLimit);
		}
	}

	private City readCityAtOffset(CodedInputStream codedIS,
			long filePointer,
			List<String> additionalTagsTable) throws IOException {
		int x = 0;
		int y = 0;
		City city = null;
		LinkedList<String> additionalTags = null;
		while (true) {
			int tagWithType = codedIS.readTag();
			int tag = WireFormat.getTagFieldNumber(tagWithType);
			switch (tag) {
				case 0:
					if (city != null) {
						city.setLocation(MapUtils.get31LatitudeY(y), MapUtils.get31LongitudeX(x));
					}
					return city;
				case OsmandOdb.CityIndex.CITY_TYPE_FIELD_NUMBER:
					int type = codedIS.readUInt32();
					City.CityType[] values = City.CityType.values();
					if (type <= City.CityType.POSTCODE.ordinal()) {
						city = new City(values[type]);
					}
					break;
				case OsmandOdb.CityIndex.ID_FIELD_NUMBER:
					long id = codedIS.readUInt64();
					if (city != null) {
						city.setId(id);
					}
					break;
				case OsmandOdb.CityIndex.ATTRIBUTETAGIDS_FIELD_NUMBER:
					int tagId = codedIS.readUInt32();
					if (additionalTags == null) {
						additionalTags = new LinkedList<>();
					}
					if (additionalTagsTable != null && tagId < additionalTagsTable.size()) {
						additionalTags.add(additionalTagsTable.get(tagId));
					}
					break;
				case OsmandOdb.CityIndex.ATTRIBUTEVALUES_FIELD_NUMBER:
					String attributeValue = codedIS.readString();
					if (city != null && additionalTags != null && !additionalTags.isEmpty()) {
						String attributeTag = additionalTags.pollFirst();
						if (attributeTag.startsWith("name:")) {
							city.setName(attributeTag.substring("name:".length()), attributeValue);
						}
					}
					break;
				case OsmandOdb.CityIndex.NAME_EN_FIELD_NUMBER:
					if (city != null) {
						city.setEnName(codedIS.readString());
					} else {
						codedIS.readString();
					}
					break;
				case OsmandOdb.CityIndex.BOUNDARY_FIELD_NUMBER:
					int size = codedIS.readRawVarint32();
					codedIS.skipRawBytes(size);
					break;
				case OsmandOdb.CityIndex.NAME_FIELD_NUMBER:
					String name = codedIS.readString();
					if (city == null) {
						city = City.createPostcode(name);
					}
					city.setName(name);
					break;
				case OsmandOdb.CityIndex.X_FIELD_NUMBER:
					x = codedIS.readUInt32();
					break;
				case OsmandOdb.CityIndex.Y_FIELD_NUMBER:
					y = codedIS.readUInt32();
					break;
				case OsmandOdb.CityIndex.SHIFTTOCITYBLOCKINDEX_FIELD_NUMBER:
					long offset = readInt(codedIS) + filePointer;
					if (city != null) {
						city.setFileOffset(offset);
					}
					break;
				default:
					skipUnknownField(codedIS, tagWithType);
					break;
			}
		}
	}

	private Street readStreetAtOffset(CodedInputStream codedIS,
			City city,
			int city24X,
			int city24Y,
			List<String> additionalTagsTable) throws IOException {
		Street street = new Street(city);
		int x = 0;
		int y = 0;
		LinkedList<String> additionalTags = null;
		while (true) {
			int tagWithType = codedIS.readTag();
			int tag = WireFormat.getTagFieldNumber(tagWithType);
			switch (tag) {
				case 0:
					street.setLocation(MapUtils.getLatitudeFromTile(24, y), MapUtils.getLongitudeFromTile(24, x));
					return street;
				case OsmandOdb.StreetIndex.ID_FIELD_NUMBER:
					street.setId(codedIS.readUInt64());
					break;
				case OsmandOdb.StreetIndex.ATTRIBUTETAGIDS_FIELD_NUMBER:
					int tagId = codedIS.readUInt32();
					if (additionalTags == null) {
						additionalTags = new LinkedList<>();
					}
					if (additionalTagsTable != null && tagId < additionalTagsTable.size()) {
						additionalTags.add(additionalTagsTable.get(tagId));
					}
					break;
				case OsmandOdb.StreetIndex.ATTRIBUTEVALUES_FIELD_NUMBER:
					String attributeValue = codedIS.readString();
					if (additionalTags != null && !additionalTags.isEmpty()) {
						String attributeTag = additionalTags.pollFirst();
						if (attributeTag.startsWith("name:")) {
							street.setName(attributeTag.substring("name:".length()), attributeValue);
						}
					}
					break;
				case OsmandOdb.StreetIndex.NAME_EN_FIELD_NUMBER:
					street.setEnName(codedIS.readString());
					break;
				case OsmandOdb.StreetIndex.NAME_FIELD_NUMBER:
					street.setName(codedIS.readString());
					break;
				case OsmandOdb.StreetIndex.X_FIELD_NUMBER:
					x = codedIS.readSInt32() + city24X;
					break;
				case OsmandOdb.StreetIndex.Y_FIELD_NUMBER:
					y = codedIS.readSInt32() + city24Y;
					break;
				case OsmandOdb.StreetIndex.INTERSECTIONS_FIELD_NUMBER:
				case OsmandOdb.StreetIndex.BUILDINGS_FIELD_NUMBER:
					long length = codedIS.readRawVarint32();
					codedIS.skipRawBytes((int) length);
					break;
				default:
					skipUnknownField(codedIS, tagWithType);
					break;
			}
		}
	}

	private void collectPoiObjectsByStoredOffsets(BinaryMapIndexReaderExt index,
			Map<BinaryMapPoiReaderAdapter.PoiRegion, Set<Integer>> storedPoiOffsets,
			List<BinaryMapPoiReaderAdapter.PoiRegion> poiRegions,
			List<ObjectAddress> results,
			String lang) throws IOException {
		for (BinaryMapPoiReaderAdapter.PoiRegion poiRegion : poiRegions) {
			Set<Integer> relativeOffsets = storedPoiOffsets.get(poiRegion);
			if (relativeOffsets == null || relativeOffsets.isEmpty()) {
				continue;
			}
			index.initCategories(poiRegion);
			for (Integer relativeOffset : relativeOffsets) {
				readPoiObjectsAtShift(index, poiRegion, relativeOffset, results, lang);
			}
		}
	}

	private Map<BinaryMapPoiReaderAdapter.PoiRegion, Set<Integer>> mapStoredPoiOffsets(int[] absolutePoiOffsets,
			List<BinaryMapPoiReaderAdapter.PoiRegion> poiRegions) {
		Map<BinaryMapPoiReaderAdapter.PoiRegion, Set<Integer>> offsetsByRegion = new LinkedHashMap<>();
		if (absolutePoiOffsets == null || absolutePoiOffsets.length == 0 || poiRegions == null || poiRegions.isEmpty()) {
			return offsetsByRegion;
		}
		for (int absolutePoiOffset : absolutePoiOffsets) {
			for (BinaryMapPoiReaderAdapter.PoiRegion poiRegion : poiRegions) {
				if (!isOffsetWithinPart(absolutePoiOffset, poiRegion)) {
					continue;
				}
				offsetsByRegion.computeIfAbsent(poiRegion, ignored -> new LinkedHashSet<>())
						.add((int) (absolutePoiOffset - poiRegion.getFilePointer()));
				break;
			}
		}
		return offsetsByRegion;
	}

	private Set<Integer> readPoiTokenOffsets(BinaryMapIndexReaderExt index,
			int tokenOffset,
			int exactSuffixIndex,
			boolean isFiltered) throws IOException {
		Set<Integer> offsets = new LinkedHashSet<>();
		index.getInputStream().seek(tokenOffset);
		long length = index.getInputStream().readRawVarint32();
		long oldLimit = index.getInputStream().pushLimitLong(length);
		try {
			List<String> suffixDictionary = new ArrayList<>();
			List<Integer> matchedSuffixIndexes = isFiltered ? new ArrayList<>() : null;
			while (true) {
				int tagWithType = index.getInputStream().readTag();
				int tag = WireFormat.getTagFieldNumber(tagWithType);
				switch (tag) {
					case 0:
						return offsets;
					case OsmandOdb.OsmAndPoiNameIndex.OsmAndPoiNameIndexData.SUFFIXESDICTIONARY_FIELD_NUMBER:
						String encodedSuffix = index.getInputStream().readString();
						if (SearchAlgorithms.EMPTY_SUFFIX_DICTIONARY_SENTINEL.equals(encodedSuffix)) {
							break;
						}
						String previousSuffix = suffixDictionary.isEmpty() ? null : suffixDictionary.get(suffixDictionary.size() - 1);
						String decodedSuffix = SearchAlgorithms.nameIndexDecodeDictionarySuffix(previousSuffix, encodedSuffix);
						if (isFiltered && exactSuffixIndex == suffixDictionary.size()) {
							matchedSuffixIndexes.add(suffixDictionary.size());
						}
						suffixDictionary.add(decodedSuffix);
						break;
					case OsmandOdb.OsmAndPoiNameIndex.OsmAndPoiNameIndexData.ATOMS_FIELD_NUMBER:
						int atomLength = index.getInputStream().readRawVarint32();
						long atomOldLimit = index.getInputStream().pushLimitLong(atomLength);
						try {
							readPoiTokenAtom(index, matchedSuffixIndexes, offsets, isFiltered);
						} finally {
							index.getInputStream().popLimit(atomOldLimit);
						}
						break;
					default:
						skipUnknownField(index.getInputStream(), tagWithType);
						break;
				}
			}
		} finally {
			index.getInputStream().popLimit(oldLimit);
		}
	}

	private void readPoiTokenAtom(BinaryMapIndexReaderExt index,
			List<Integer> matchedSuffixIndexes,
			Set<Integer> offsets,
			boolean isFiltered) throws IOException {
		int shift = Integer.MIN_VALUE;
		boolean matched = !isFiltered;
		int maskIndex = 0;
		while (true) {
			int tagWithType = index.getInputStream().readTag();
			int tag = WireFormat.getTagFieldNumber(tagWithType);
			switch (tag) {
				case 0:
					if (matched && shift != Integer.MIN_VALUE) {
						offsets.add(shift);
					}
					return;
				case OsmandOdb.OsmAndPoiNameIndexDataAtom.X_FIELD_NUMBER,
                     OsmandOdb.OsmAndPoiNameIndexDataAtom.Y_FIELD_NUMBER,
                     OsmandOdb.OsmAndPoiNameIndexDataAtom.ZOOM_FIELD_NUMBER:
					index.getInputStream().readUInt32();
					break;
                case OsmandOdb.OsmAndPoiNameIndexDataAtom.SUFFIXESBITSET_FIELD_NUMBER:
					int mask = index.getInputStream().readUInt32();
					if (isFiltered && !matched && matchesSuffixMask(maskIndex, mask, matchedSuffixIndexes)) {
						matched = true;
					}
					maskIndex++;
					break;
				case OsmandOdb.OsmAndPoiNameIndexDataAtom.SHIFTTO_FIELD_NUMBER:
					long value = readInt(index.getInputStream());
					if (value > Integer.MAX_VALUE) {
						throw new IllegalStateException();
					}
					shift = (int) value;
					break;
				default:
					skipUnknownField(index.getInputStream(), tagWithType);
					break;
			}
		}
	}

	private void readPoiObjectsAtShift(BinaryMapIndexReaderExt index,
			BinaryMapPoiReaderAdapter.PoiRegion region,
			int relativeOffset,
			List<ObjectAddress> results,
			String lang) throws IOException {
		index.getInputStream().seek(region.getFilePointer() + relativeOffset);
		long length = readInt(index.getInputStream());
		long oldLimit = index.getInputStream().pushLimitLong(length);
		try {
			int x = 0;
			int y = 0;
			int zoom = 0;
			while (true) {
				int tagWithType = index.getInputStream().readTag();
				int tag = WireFormat.getTagFieldNumber(tagWithType);
				switch (tag) {
					case 0:
						return;
					case OsmandOdb.OsmAndPoiBoxData.X_FIELD_NUMBER:
						x = index.getInputStream().readUInt32();
						break;
					case OsmandOdb.OsmAndPoiBoxData.Y_FIELD_NUMBER:
						y = index.getInputStream().readUInt32();
						break;
					case OsmandOdb.OsmAndPoiBoxData.ZOOM_FIELD_NUMBER:
						zoom = index.getInputStream().readUInt32();
						break;
					case OsmandOdb.OsmAndPoiBoxData.POIDATA_FIELD_NUMBER:
						int poiLength = index.getInputStream().readRawVarint32();
						long poiOldLimit = index.getInputStream().pushLimitLong(poiLength);
						try {
							RawPoiObject rawPoiObject = readRawPoiObject(index.getInputStream(), x, y, zoom, region);
							if (rawPoiObject != null) {
								ObjectAddress objectAddress = toPoiObjectAddress(rawPoiObject, lang);
								if (objectAddress != null) {
									int payloadSize = poiLength + computeVarint32Size(poiLength);
									int payloadOffset = (int) (index.getInputStream().getTotalBytesRead() - poiLength);
									results.add(new ObjectAddress(objectAddress.name(), objectAddress.point(), objectAddress.values(), objectAddress.isPoi(), objectAddress.type(), objectAddress.osmId(), objectAddress.osmType(), payloadOffset, payloadSize));
								}
							}
						} finally {
							index.getInputStream().popLimit(poiOldLimit);
						}
						break;
					default:
						skipUnknownField(index.getInputStream(), tagWithType);
						break;
				}
			}
		} finally {
			index.getInputStream().popLimit(oldLimit);
		}
	}

	private ObjectAddress toPoiObjectAddress(RawPoiObject rawPoiObject,
			String lang) {
		LatLon location = new LatLon(rawPoiObject.lat, rawPoiObject.lon);
		Map<String, String> values = new LinkedHashMap<>();
		if (!Algorithms.isEmpty(rawPoiObject.name)) {
			values.put(Amenity.NAME, rawPoiObject.name);
		}
		if (!Algorithms.isEmpty(rawPoiObject.nameEn)) {
			values.put("name:en", rawPoiObject.nameEn);
		}
		if (!Algorithms.isEmpty(rawPoiObject.type)) {
			values.put(Amenity.TYPE, rawPoiObject.type);
		}
		if (!Algorithms.isEmpty(rawPoiObject.subType)) {
			values.put(Amenity.SUBTYPE, rawPoiObject.subType);
			if (!Algorithms.isEmpty(rawPoiObject.type)) {
				values.put(Amenity.OSMAND_POI_KEY, rawPoiObject.type + " " + rawPoiObject.subType);
			}
		}
		if (!Algorithms.isEmpty(rawPoiObject.openingHours)) {
			values.put(Amenity.OPENING_HOURS, rawPoiObject.openingHours);
		}
		if (!Algorithms.isEmpty(rawPoiObject.site)) {
			values.put(Amenity.WEBSITE, rawPoiObject.site);
		}
		if (!Algorithms.isEmpty(rawPoiObject.phone)) {
			values.put(Amenity.PHONE, rawPoiObject.phone);
		}
		if (!Algorithms.isEmpty(rawPoiObject.description)) {
			values.put(Amenity.DESCRIPTION, rawPoiObject.description);
		}
 		for (Map.Entry<String, List<String>> entry : rawPoiObject.decodedTextTags.entrySet()) {
 			if (!Algorithms.isEmpty(entry.getKey()) && entry.getValue() != null && !entry.getValue().isEmpty()) {
 				values.put(entry.getKey(), String.join("; ", entry.getValue()));
 			}
 		}
 		String displayName = selectPoiDisplayName(rawPoiObject, lang);
 		String type = buildPoiType(values);
		Long osmId = rawPoiObject.id > 0 ? ObfConstants.getOsmIdFromMapObjectId(rawPoiObject.id) : null;
		String osmType = decodePoiOsmType(rawPoiObject.id);
		return new ObjectAddress(displayName, location, values, true, type, osmId, osmType, 0, 0);
	}

 	private String decodePoiOsmType(long rawPoiObjectId) {
 		if (rawPoiObjectId <= 0) {
 			return null;
 		}
 		Amenity amenity = new Amenity();
 		amenity.setId(rawPoiObjectId);
		net.osmand.osm.edit.Entity.EntityType entityType = ObfConstants.getOsmEntityType(amenity);
 		return entityType == null ? null : entityType.name().toLowerCase(Locale.US);
 	}

	private String selectPoiDisplayName(RawPoiObject rawPoiObject, String lang) {
		if ("en".equalsIgnoreCase(lang) && !Algorithms.isEmpty(rawPoiObject.nameEn)) {
			return rawPoiObject.nameEn;
		}
		if (!Algorithms.isEmpty(rawPoiObject.name)) {
			return rawPoiObject.name;
		}
		if (!Algorithms.isEmpty(rawPoiObject.nameEn)) {
			return rawPoiObject.nameEn;
		}
		return "";
	}

	private String buildPoiType(Map<String, String> values) {
		String osmandPoiKey = values.get(Amenity.OSMAND_POI_KEY);
		if (!Algorithms.isEmpty(osmandPoiKey)) {
			return osmandPoiKey;
		}
		String subtype = values.get(Amenity.SUBTYPE);
		String type = values.get(Amenity.TYPE);
		if (!Algorithms.isEmpty(type) && !Algorithms.isEmpty(subtype)) {
			return type + ": " + subtype;
		}
		if (!Algorithms.isEmpty(type)) {
			return type;
		}
		return "";
	}

	private Map<String, String> buildMapObjectValues(MapObject mapObject, String lang) {
		Map<String, String> values = new LinkedHashMap<>();
		if (mapObject == null) {
			return values;
		}
		String localizedName = mapObject.getName(lang);
		if (!Algorithms.isEmpty(localizedName)) {
			values.put(Amenity.NAME, localizedName);
		}
		String englishName = mapObject.getEnName(true);
		if (!Algorithms.isEmpty(englishName)) {
			values.put("name:en", englishName);
		}
		Map<String, String> namesMap = mapObject.getNamesMap(true);
		if (namesMap != null && !namesMap.isEmpty()) {
			for (Map.Entry<String, String> entry : namesMap.entrySet()) {
				if (!Algorithms.isEmpty(entry.getKey()) && !Algorithms.isEmpty(entry.getValue())) {
					values.put(entry.getKey(), entry.getValue());
				}
			}
		}
		return values;
	}

	private long readInt(CodedInputStream codedIS) throws IOException {
		long value = readUnsignedByte(codedIS);
		boolean eightBytes = value > 0x7f;
		if (eightBytes) {
			value = value & 0x7f;
		}
		value = (value << 8) + readUnsignedByte(codedIS);
		value = (value << 8) + readUnsignedByte(codedIS);
		value = (value << 8) + readUnsignedByte(codedIS);
		if (eightBytes) {
			value = (value << 8) + readUnsignedByte(codedIS);
			value = (value << 8) + readUnsignedByte(codedIS);
			value = (value << 8) + readUnsignedByte(codedIS);
			value = (value << 8) + readUnsignedByte(codedIS);
		}
		return value;
	}

	private boolean matchesSuffixMask(int maskIndex, int mask, List<Integer> matchedSuffixIndexes) {
		for (Integer suffixIndex : matchedSuffixIndexes) {
			if (suffixIndex == null) {
				continue;
			}
			int wordIndex = suffixIndex >> 5;
			if (wordIndex != maskIndex) {
				continue;
			}
			int bitMask = 1 << (suffixIndex & 31);
			if ((mask & bitMask) != 0) {
				return true;
			}
		}
		return false;
	}

	private boolean matchesObjectAddressFilter(ObjectAddress objectAddress,
			Pattern pattern,
			Pattern normalizedPattern) {
		return matchesObjectAddressText(objectAddress, pattern, normalizedPattern);
	}

	private boolean matchesObjectAddressText(ObjectAddress objectAddress,
			Pattern pattern,
			Pattern normalizedPattern) {
		if (objectAddress == null) {
			return false;
		}
		if (matchesPattern(objectAddress.name(), pattern, normalizedPattern)) {
			return true;
		}
		Map<String, String> values = objectAddress.values();
		if (values == null || values.isEmpty()) {
			return false;
		}
		for (Map.Entry<String, String> entry : values.entrySet()) {
			if (matchesPattern(entry.getKey(), pattern, normalizedPattern)
					|| matchesPattern(entry.getValue(), pattern, normalizedPattern)) {
				return true;
			}
		}
		return false;
	}

	private void filterExactObjects(List<ObjectAddress> results, String tokenName) {
		if (results == null || results.isEmpty() || Algorithms.isEmpty(tokenName)) {
			return;
		}
		String normalizedIndexToken = normalizeIndexToken(tokenName);
		results.removeIf(objectAddress -> !matchesObjectAddressExactToken(objectAddress, normalizedIndexToken));
	}

	private boolean matchesObjectAddressExactToken(ObjectAddress objectAddress, String normalizedIndexToken) {
		if (objectAddress == null || Algorithms.isEmpty(normalizedIndexToken)) {
			return false;
		}
		if (containsExactIndexToken(objectAddress.name(), normalizedIndexToken)) {
			return true;
		}
		Map<String, String> values = objectAddress.values();
		if (values == null || values.isEmpty()) {
			return false;
		}
		for (Map.Entry<String, String> entry : values.entrySet()) {
			if (containsExactIndexToken(entry.getKey(), normalizedIndexToken)
					|| containsExactIndexToken(entry.getValue(), normalizedIndexToken)) {
				return true;
			}
		}
		return false;
	}

	private boolean containsExactIndexToken(String value, String normalizedIndexToken) {
		if (Algorithms.isEmpty(value) || Algorithms.isEmpty(normalizedIndexToken)) {
			return false;
		}
		for (String candidateToken : extractIndexedTokens(value)) {
			if (normalizedIndexToken.equals(candidateToken)) {
				return true;
			}
		}
		return false;
	}

	private List<String> extractIndexedTokens(String value) {
		if (Algorithms.isEmpty(value)) {
			return List.of();
		}
		String alignedValue = CollatorStringMatcher.alignChars(value).toLowerCase(Locale.ROOT);
		return SearchAlgorithms.splitByWordsLowercase(alignedValue);
	}

	private String normalizeIndexToken(String tokenName) {
		if (Algorithms.isEmpty(tokenName)) {
			return "";
		}
		String alignedToken = CollatorStringMatcher.alignChars(tokenName).toLowerCase(Locale.ROOT);
		List<String> tokens = SearchAlgorithms.splitByWordsLowercase(alignedToken);
		return tokens.isEmpty() ? alignedToken : tokens.get(0);
	}

	record AddressTokenRefs(List<Integer> cityOffsets, List<Integer> streetOffsets, List<Integer> streetCityOffsets) {
		AddressTokenRefs() {
			this(new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
		}
	}

	class BinaryMapIndexReaderExt extends BinaryMapIndexReader {
		BinaryMapIndexReaderExt(final RandomAccessFile raf, File file) throws IOException {
			super(raf, file);
		}

		CodedInputStream getInputStream() {
			return codedIS;
		}
	}
	
	default ObjectAddressPage getObjects(String obf,
			String lang,
			IndexToken token,
			String regExp,
			int page,
			int sizeLimit,
			boolean isFiltered) {
		List<ObjectAddress> results = new ArrayList<>();
		if (token == null) {
			return new ObjectAddressPage(List.of(), Math.max(page, 0), Math.max(sizeLimit, 1), 0, 0, 0, 0, 0, 0);
		}
		final Pattern objectPattern;
		final Pattern normalizedObjectPattern;
		final boolean hasAnyFilter;
		final int safePage = Math.max(page, 0);
		final int safeSize = Math.max(sizeLimit, 1);
		try {
			objectPattern = Algorithms.isEmpty(regExp) ? null : Pattern.compile(regExp, Pattern.CASE_INSENSITIVE);
			normalizedObjectPattern = objectPattern == null ? null : compileNormalizedPattern(regExp);
			hasAnyFilter = objectPattern != null;
		} catch (PatternSyntaxException e) {
			throw new RuntimeException("Invalid regex provided: " + e.getDescription(), e);
		}
		File file = new File(obf);
		try (RandomAccessFile randomAccessFile = new RandomAccessFile(file.getAbsolutePath(), "r")) {
			BinaryMapIndexReaderExt index = new BinaryMapIndexReaderExt(randomAccessFile, file);
			try {
				List<BinaryMapAddressReaderAdapter.AddressRegion> addressRegions = new ArrayList<>();
				List<BinaryMapPoiReaderAdapter.PoiRegion> poiRegions = new ArrayList<>();
				for (BinaryIndexPart part : index.getIndexes()) {
					if (part instanceof BinaryMapAddressReaderAdapter.AddressRegion addressRegion) {
						addressRegions.add(addressRegion);
					} else if (part instanceof BinaryMapPoiReaderAdapter.PoiRegion poiRegion) {
						poiRegions.add(poiRegion);
					}
				}
				int[] addressOffsets = token.addressOffsets();
				int[] poiOffsets = token.poiOffsets();
				boolean hasAddressOffsets = addressOffsets != null && addressOffsets.length > 0;
				boolean hasPoiOffsets = poiOffsets != null && poiOffsets.length > 0;
				if (!hasAddressOffsets && !hasPoiOffsets) {
					return new ObjectAddressPage(List.of(), safePage, safeSize, 0, 0, 0, 0, 0, 0);
				}
				Map<BinaryMapPoiReaderAdapter.PoiRegion, Set<Integer>> storedPoiOffsets = mapStoredPoiOffsets(poiOffsets, poiRegions);
				Set<Integer> allAddressObjectOffsets = collectUniqueAddressObjectOffsets(index, token, false);
				int allObjectSize = safeMetricInt(calculateAddressTokenSize(index, allAddressObjectOffsets)
						+ calculatePoiObjectSizeByStoredOffsets(index, storedPoiOffsets));
				int allObjectCount = safeMetricInt((long) allAddressObjectOffsets.size() + countPoiObjectsByStoredOffsets(index, storedPoiOffsets));
				if (hasAddressOffsets) {
					for (int tokenOffset : addressOffsets) {
						for (BinaryMapAddressReaderAdapter.AddressRegion addressRegion : addressRegions) {
							if (tokenOffset >= addressRegion.getFilePointer()
									&& tokenOffset < addressRegion.getFilePointer() + addressRegion.getLength()) {
								collectAddressObjects(index, addressRegion, token, tokenOffset, isFiltered, results, lang);
							}
						}
					}
				}
				if (hasPoiOffsets) {
					Map<BinaryMapPoiReaderAdapter.PoiRegion, Set<Integer>> effectivePoiOffsets = isFiltered
							? collectUniquePoiObjectOffsets(index, token, poiRegions, true, false)
							: storedPoiOffsets;
					collectPoiObjectsByStoredOffsets(index, effectivePoiOffsets, poiRegions, results, lang);
				}
				if (isFiltered) {
					filterExactObjects(results, token.name());
				}
				int totalObjectSize = safeMetricInt(calculateObjectAddressesSize(index, results, poiRegions));
				int totalObjectCount = results.size();
				if (hasAnyFilter) {
					results.removeIf(objectAddress -> !matchesObjectAddressFilter(objectAddress,
							objectPattern,
							normalizedObjectPattern));
				}
				results.sort(Comparator.comparing(ObjectAddress::name, String.CASE_INSENSITIVE_ORDER));
				long totalElements = results.size();
				int totalPages = totalElements == 0 ? 0 : (int) ((totalElements + safeSize - 1) / safeSize);
				int fromIndex = Math.min(safePage * safeSize, results.size());
				int toIndex = Math.min(fromIndex + safeSize, results.size());
				List<ObjectAddress> pageContent = fromIndex >= toIndex
						? List.of()
						: new ArrayList<>(results.subList(fromIndex, toIndex));
				return new ObjectAddressPage(pageContent, safePage, safeSize, totalElements, totalPages, totalObjectSize, totalObjectCount, allObjectSize, allObjectCount);
			} finally {
				index.close();
			}
		} catch (Exception e) {
			getLogger().error("Failed to read OBF objects {} for token {}", file, token, e);
			throw new RuntimeException("Failed to read OBF objects: " + e.getMessage(), e);
		}
	}
}
