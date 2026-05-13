package net.osmand.server.api.searchtest;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.amazonaws.util.StringInputStream;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import net.osmand.binary.*;
import net.osmand.data.*;
import net.osmand.obf.OBFDataCreator;
import net.osmand.osm.MapPoiTypes;
import net.osmand.osm.PoiCategory;
import net.osmand.search.SearchUICore;
import net.osmand.search.core.SearchExportSettings;
import net.osmand.search.core.SearchPhrase;
import net.osmand.search.core.SearchResult;
import net.osmand.search.core.SearchSettings;
import net.osmand.server.api.services.OsmAndMapsService;
import net.osmand.server.api.services.SearchService;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
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

	OsmAndMapsService getMapsService();

	default List<String> getOBFs(Double radius, Double lat, Double lon) throws IOException {
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

	record CityAddress(String name, List<StreetAddress> streets, int streetsCount, LatLon point, String type) {}
	record Address(String name, String value, LatLon point) {}
	record StreetAddress(String name, List<Address> houses, int houseCount, LatLon point) {}

	final class RawPoiObject {
		String name = "";
		String nameEn = "";
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
			if (spec == null || spec.childMessageType() == null) {
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

	// return JSON representation of section by using field path from OBF file
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
					ObfFieldSpec rootSpec = rootSpecs == null ? null : rootSpecs.get(fieldNumber);
					if (rootSpec == null || rootSpec.childMessageType() == null) {
						continue;
					}
					String fieldName = rootSpec.fieldName() != null ? rootSpec.fieldName() : OBF_STRUCTURE_FIELD_NAMES.get(fieldNumber);
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

	private static Address findRawValue(RawPoiObject objectRecord, Pattern poiPattern, Pattern normalizedPoiPattern) {
		if (objectRecord == null || poiPattern == null) {
			return null;
		}
		LatLon location = new LatLon(objectRecord.lat, objectRecord.lon);
		String displayName = Algorithms.isEmpty(objectRecord.name) ? objectRecord.nameEn : objectRecord.name;
		if (matchesPattern(objectRecord.name, poiPattern, normalizedPoiPattern)) {
			return new Address(objectRecord.name, "name-> " + objectRecord.name, location);
		}
		if (matchesPattern(objectRecord.nameEn, poiPattern, normalizedPoiPattern)) {
			return new Address(objectRecord.nameEn, "name:en-> " + objectRecord.nameEn, location);
		}
		for (Map.Entry<String, List<String>> entry : objectRecord.decodedTextTags.entrySet()) {
			for (String value : entry.getValue()) {
				if (matchesPattern(value, poiPattern, normalizedPoiPattern)) {
					return new Address(displayName, entry.getKey() + "-> " + value, location);
				}
			}
		}
		return null;
	}

	private static List<Address> findPoiAddressesRaw(RandomAccessFile randomAccessFile,
			BinaryMapIndexReader index,
			BinaryMapPoiReaderAdapter.PoiRegion poiRegion,
			Pattern poiPattern,
			Pattern normalizedPoiPattern) throws IOException {
		List<Address> results = new ArrayList<>();
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
			List<Address> results) throws IOException {
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
						Address value = findRawValue(objectRecord, poiPattern, normalizedPoiPattern);
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

	private static Address findValue(Amenity amenity, Pattern poiPattern, Pattern normalizedPoiPattern) {
        if (amenity == null || poiPattern == null) {
            return null;
        }
		
		String name = amenity.getName();
		if (matchesPattern(name, poiPattern, normalizedPoiPattern)) {
			return new Address(name, "name-> " + name, amenity.getLocation());
		}

		String enName = amenity.getEnName(false);
		if (matchesPattern(enName, poiPattern, normalizedPoiPattern)) {
			return new Address(enName, "name:en-> " + enName, amenity.getLocation());
		}
		
		name = name == null ? enName : name;
		for (Map.Entry<String, String> e : amenity.getNamesMap(true).entrySet()) {
			if (matchesPattern(e.getValue(), poiPattern, normalizedPoiPattern)) {
				return new Address(name, e.getKey() + "-> " + e.getValue(), amenity.getLocation());
			}
		}
		
        for (String key : amenity.getAdditionalInfoKeys()) {
			String info = amenity.getAdditionalInfo(key);
			if (matchesPattern(info, poiPattern, normalizedPoiPattern)) {
                return new Address(name, key + "-> " + info, amenity.getLocation());
            }
        }
        return null;
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
									results.add(new CityAddress(cityName, streets, c.getStreets().size(), c.getLocation(), c.getType().name().toLowerCase()));
									continue;
								}

								for (Street s : new ArrayList<>(c.getStreets())) {
									List<Address> buildings = new ArrayList<>();
									final String streetName = s.getName(lang);
									if (streetName == null || !isStreetEmpty && !streetPattern.matcher(streetName).find())
										continue;

									index.preloadBuildings(s, null, null);
									if (isHouseEmpty) {
										streets.add(new StreetAddress(streetName, buildings, s.getBuildings().size(), s.getLocation()));
										continue;
									}

									final List<Building> bs = s.getBuildings();
									if (bs != null && !bs.isEmpty()) {
										for (Building b : bs) {
											final String houseName = b.getName(lang);
											if (houseName != null && housePattern.matcher(houseName).find())
												buildings.add(new Address(houseName, null, b.getLocation()));
										}
									}
									if (!buildings.isEmpty()) {
										StreetAddress street = new StreetAddress(streetName, buildings, s.getBuildings().size(), s.getLocation());
										streets.add(street);
									}
								}
								if (!streets.isEmpty())
									results.add(new CityAddress(cityName, streets, c.getStreets().size(), c.getLocation(), c.getType().name().toLowerCase()));
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
				} else if (o instanceof Address a) {
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
		getLogger().info(result.settings().getStat().toDetailedString());

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
			@JsonProperty("resultsLimit") Integer resultsLimit) {}

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
			Files.writeString(jsonFile.toPath(), unitTestJson, StandardCharsets.UTF_8);

			OBFDataCreator creator = new OBFDataCreator();
			File outFile = creator.create(dirPath.resolve(unitTest.name + ".obf").toAbsolutePath().toString(),
					new String[] {jsonFile.getAbsolutePath()});

			// Build ZIP with JSON metadata and gzipped data, streaming directly to the servlet output
			SearchSettings settings = result.settings().setOriginalLocation(new LatLon(ctx.lat(), ctx.lon()));
			JSONObject settingsJson = settings.toJSON();
			int limit = unitTest.resultsLimit();
			List<List<String>> formattedResults = buildUnitTestResults(unitTest.queries(), ctx, limit);
			JSONArray formattedResultsJson = new JSONArray();
			for (List<String> phraseResults : formattedResults) {
				formattedResultsJson.put(new JSONArray(phraseResults));
			}
			Map<String, Object> rootJson = new LinkedHashMap<>();
			rootJson.put("settings", settingsJson);
			rootJson.put("phrases", unitTest.queries);
			rootJson.put("results", formattedResultsJson);

			unitTestJson = new JSONObject(rootJson).toString(4);
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

	private List<List<String>> buildUnitTestResults(String[] phrases, SearchService.SearchContext baseCtx, int limit) throws IOException {
		List<List<String>> results = emptyUnitTestResults(phrases);
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
				phraseResults.add(SearchUICore.formatSearchResultForTest(false, searchResults.get(i), phrase));
			}
		}
		return results;
	}
}
