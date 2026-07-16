package net.osmand.server.api.searchtest;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import net.osmand.ResultMatcher;
import net.osmand.binary.*;
import net.osmand.binary.BinaryMapIndexReader.TagValuePair;
import net.osmand.data.*;
import net.osmand.map.WorldRegion;
import net.osmand.osm.MapPoiTypes;
import net.osmand.osm.PoiCategory;
import net.osmand.server.api.services.OsmAndMapsService;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.Normalizer;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public interface OBFService extends BaseService {
	int BASE_POI_SHIFT = BinaryMapPoiReaderAdapter.SHIFT_BITS_CATEGORY;
	int FINAL_POI_SHIFT = BinaryMapIndexReader.SHIFT_COORDINATES;
	int BASE_POI_ZOOM = 31 - BASE_POI_SHIFT;
	int FINAL_POI_ZOOM = 31 - FINAL_POI_SHIFT;
	int CATEGORY_MASK = (1 << BinaryMapPoiReaderAdapter.SHIFT_BITS_CATEGORY) - 1;

	Map<String, Map<Integer, InspectorService.ObfFieldSpec>> OBF_MESSAGE_SCHEMA = buildObfMessageSchema();

	Map<Integer, String> OBF_STRUCTURE_FIELD_NAMES = buildObfStructureFieldNames();

	record ObfFieldSpec(String fieldName, String childMessageType, InspectorService.ObfLengthType lengthType, boolean packedVarInt, boolean repeated) {}
	record PoiCategoryMeta(String type, String subtype) {}
	
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

	default List<String> getOBFs(Double radius, Double lat, Double lon, String obfPath) throws IOException {
		radius = radius == null ? 1.5 : radius;
		File[] customObfs = null;
		if (!Algorithms.isEmpty(obfPath)) {
			customObfs = getCustomObfFiles(obfPath);
		}
		if (lat == null || lon == null) {
			if (customObfs != null) {
				List<String> obfList = new ArrayList<>();
				for (File file : customObfs) {
					obfList.add(file.getAbsolutePath());
				}
				return obfList;
			}
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
		if (Algorithms.isEmpty(obfPath)) {
			List<OsmAndMapsService.BinaryMapIndexReaderReference> list = getMapsService().getObfReaders(
					points, OsmAndMapsService.ObfReason.SEARCH_TEST.value());
			for (OsmAndMapsService.BinaryMapIndexReaderReference ref : list)
				obfList.add(ref.getFile().getAbsolutePath());
			return obfList;
		}
		return getMaps(points, customObfs);
	}

	private File[] getCustomObfFiles(String obfPath) {
		File mapsFolder = new File(obfPath);
		File[] files = Algorithms.getSortedFilesVersions(mapsFolder);
		if (files == null || files.length == 0) {
			return new File[0];
		}
		return Arrays.stream(files)
				.filter(file -> file != null && file.isFile() && file.getName().toLowerCase(Locale.ROOT).endsWith(".obf"))
				.toArray(File[]::new);
	}

	
	private List<String> getMaps(QuadRect quadRect, File[] candidates) {
		List<String> maps = new ArrayList<>();
		if (quadRect == null || quadRect.hasInitialState() || candidates == null) {
			return maps;
		}

		QuadRect queryLatLon = new QuadRect(
				MapUtils.get31LongitudeX((int) Math.min(quadRect.left, quadRect.right)),
				MapUtils.get31LatitudeY((int) Math.min(quadRect.top, quadRect.bottom)),
				MapUtils.get31LongitudeX((int) Math.max(quadRect.left, quadRect.right)),
				MapUtils.get31LatitudeY((int) Math.max(quadRect.top, quadRect.bottom)));

		for (File file : candidates) {
			String downloadName = getDownloadNameByFileName(file.getName());
			WorldRegion wr = getMapsService().getOsmandRegions().getRegionDataByDownloadName(downloadName);
			if (wr == null) {
				continue;
			}
			List<QuadRect> polyBoxes = wr.getAllPolygonsBounds();
			if (polyBoxes != null && !polyBoxes.isEmpty()
					&& polyBoxes.stream().anyMatch(pb -> QuadRect.intersects(pb, queryLatLon))) {
				maps.add(file.getAbsolutePath());
			}
		}
		return maps;
	}

	private String getDownloadNameByFileName(String fileName) {
		String dwName = fileName.substring(0, fileName.indexOf('.')).toLowerCase();
		if (dwName.endsWith("_2")) {
			dwName = dwName.substring(0, dwName.length() - 2);
		}
		return dwName;
	}

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
	
	record ObfFileInfo(String path, String name, String continent, String country, String region, long lastModified, long size) {}
	
	@FunctionalInterface
	interface GenerateDbProgressListener {
		void onProgress(GenDbService.GenerateDbProgress progress);
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
						long length = readFixed32Length(codedIS);
						long innerOldLimit = codedIS.pushLimitLong(length);
						findPoiAddressesInBoxData(codedIS, poiRegion, tagGroups, poiPattern, normalizedPoiPattern, results);
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
						long length = readFixed32Length(codedIS);
						long boxOldLimit = codedIS.pushLimitLong(length);
						readPoiBoxTagGroups(codedIS, tagGroups);
						codedIS.popLimit(boxOldLimit);
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
					long length = readFixed32Length(codedIS);
					long oldLimit = codedIS.pushLimitLong(length);
					readPoiBoxTagGroups(codedIS, tagGroups);
					codedIS.popLimit(oldLimit);
					break;
				}
				default:
					skipUnknownField(codedIS, tagValue);
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
					skipUnknownField(codedIS, tagValue);
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
					skipUnknownField(codedIS, tagValue);
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
		addObfSpec(schema, "OsmAndPoiNameIndex", 4, "commonStats", "CommonIndexedStats", InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiNameIndex", 5, "data", "OsmAndPoiNameIndexData", InspectorService.ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "OsmAndPoiNameIndexData", 1, "suffixesCommonDictionary", null, InspectorService.ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "OsmAndPoiNameIndexData", 3, "atoms", "OsmAndPoiNameIndexDataAtom", InspectorService.ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "OsmAndPoiNameIndexData", 7, "atomsLength", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiNameIndexDataAtom", 2, "zoom", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiNameIndexDataAtom", 3, "x", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiNameIndexDataAtom", 4, "y", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiNameIndexDataAtom", 5, "bloomIndex", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndPoiNameIndexDataAtom", 11, "poiCategories", null, InspectorService.ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "OsmAndPoiNameIndexDataAtom", 12, "eloRating", null, InspectorService.ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "OsmAndPoiNameIndexDataAtom", 14, "shiftTo", null, InspectorService.ObfLengthType.FIXED32);

		addObfSpec(schema, "IndexedStringTable", 1, "prefix", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "IndexedStringTable", 3, "key", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "IndexedStringTable", 4, "val", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "IndexedStringTable", 5, "subtables", "IndexedStringTable", InspectorService.ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "CommonIndexedStats", 4, "value", null, InspectorService.ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "CommonIndexedStats", 5, "matched", null, InspectorService.ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "CommonIndexedStats", 6, "nonindexed", null, InspectorService.ObfLengthType.VAR_INT, false, true);

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
		addObfSpec(schema, "OsmAndAddressNameIndexData", 6, "commonStats", "CommonIndexedStats", InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "OsmAndAddressNameIndexData", 7, "atom", "AddressNameIndexData", InspectorService.ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "AddressNameIndexData", 7, "atomsLength", null, InspectorService.ObfLengthType.VAR_INT);
		addObfSpec(schema, "AddressNameIndexData", 3, "suffixesCommonDictionary", null, InspectorService.ObfLengthType.VAR_INT, false, true);
		addObfSpec(schema, "AddressNameIndexData", 4, "atom", "AddressNameIndexDataAtom", InspectorService.ObfLengthType.VAR_INT, false, true);

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

	default File gzip(File sourceFile) throws IOException {
		File gzFile = new File(sourceFile.getParentFile(), sourceFile.getName() + ".gz");
		try (FileInputStream inputStream = new FileInputStream(sourceFile);
		     FileOutputStream fileOutputStream = new FileOutputStream(gzFile);
		     GZIPOutputStream gzipOutputStream = new GZIPOutputStream(fileOutputStream)) {
			Algorithms.streamCopy(inputStream, gzipOutputStream);
		}
		return gzFile;
	}

	default void unzip(File gzFile, File file) throws IOException {
		GZIPInputStream gzin = new GZIPInputStream(new FileInputStream(gzFile));
		FileOutputStream fous = new FileOutputStream(file);
		Algorithms.streamCopy(gzin, fous);
		fous.close();
		gzin.close();
	}

	enum ObfLengthType {
		VAR_INT,
		FIXED32
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
				payloadLength = clampPayloadLengthToLimit(codedIS, payloadLength);
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
				payloadLength = clampPayloadLengthToLimit(codedIS, payloadLength);
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

	static long clampPayloadLengthToLimit(CodedInputStream codedIS, long payloadLength) {
		long remainingInLimit = codedIS.getBytesUntilLimit();
		if (remainingInLimit >= 0 && payloadLength > remainingInLimit) {
			return remainingInLimit;
		}
		return payloadLength;
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

	static boolean isRepeatedMessageField(String messageType, int fieldNumber) {
		Map<Integer, InspectorService.ObfFieldSpec> specByFieldNumber = OBF_MESSAGE_SCHEMA.get(messageType);
		InspectorService.ObfFieldSpec fieldSpec = specByFieldNumber == null ? null : specByFieldNumber.get(fieldNumber);
		return fieldSpec != null && fieldSpec.repeated();
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

	default Map<String, long[]> getSectionSizes(List<String> obfs, String fieldPath) {
		Map<String, long[]> merged = new HashMap<>();
		if (obfs == null) {
			return merged;
		}
		for (String obf : obfs) {
			if (obf == null || obf.isBlank()) {
				continue;
			}
			Map<String, long[]> sizes = getSectionSizes(obf, fieldPath);
			if (sizes == null) {
				continue;
			}
			for (Map.Entry<String, long[]> entry : sizes.entrySet()) {
				long[] source = entry.getValue();
				long[] target = merged.computeIfAbsent(entry.getKey(), key -> new long[3]);
				if (source == null) {
					continue;
				}
				for (int i = 0; i < Math.min(target.length, source.length); i++) {
					target[i] += source[i];
				}
			}
		}
		return merged;
	}

	default void readIndexedStringTableOffsets(BinaryMapIndexReaderExt index, String prefix,
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

	default int computeVarint32Size(long value) {
		int size = 1;
		long normalizedValue = value;
		while ((normalizedValue & ~0x7FL) != 0L) {
			size++;
			normalizedValue >>>= 7;
		}
		return size;
	}
	
	default long readInt(CodedInputStream codedIS) throws IOException {
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
	
	default City readCityAtOffset(CodedInputStream codedIS,
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
					if (type >= 0 && type < values.length) {
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
						} else {
							city.setName(attributeTag, attributeValue);
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
	
	default Street readStreetAtOffset(CodedInputStream codedIS,
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
						} else {
							street.setName(attributeTag, attributeValue);
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

	default List<Record> getAddresses(List<String> obfs, String lang, boolean includesBoundaryPostcode, String cityRegExp, String streetRegExp, String houseRegExp, String poiRegExp) {
		List<Record> results = new ArrayList<>();
		if (obfs != null) {
			for (String obf : obfs) {
				if (obf == null || obf.isBlank()) {
					continue;
				}
				for (Record record : getAddresses(obf, lang, includesBoundaryPostcode, cityRegExp, streetRegExp, houseRegExp, poiRegExp)) {
					results.add(withObf(record, obf));
				}
			}
		}
		results.sort(Comparator.comparing(o -> {
			if (o instanceof CityAddress ca) {
				return ca.name();
			} else if (o instanceof PoiAddress a) {
				return a.name();
			}
			return "";
		}, String.CASE_INSENSITIVE_ORDER));
		return results;
	}

	private Record withObf(Record record, String obf) {
		if (record instanceof CityAddress city) {
			List<StreetAddress> streets = new ArrayList<>();
			if (city.streets() != null) {
				for (StreetAddress street : city.streets()) {
					streets.add((StreetAddress) withObf(street, obf));
				}
			}
			return new CityAddress(city.name(), city.point(), streets, city.streetsCount(), city.type(), obf);
		}
		if (record instanceof StreetAddress street) {
			List<HouseAddress> houses = new ArrayList<>();
			if (street.houses() != null) {
				for (HouseAddress house : street.houses()) {
					houses.add((HouseAddress) withObf(house, obf));
				}
			}
			return new StreetAddress(street.name(), street.point(), houses, street.houseCount(), obf);
		}
		if (record instanceof HouseAddress house) {
			return new HouseAddress(house.name(), house.point(), obf);
		}
		if (record instanceof PoiAddress poi) {
			return new PoiAddress(poi.name(), poi.point(), poi.value(), obf);
		}
		return record;
	}


	default List<Amenity> getAmenities(String obf, String lang) {
		List<Amenity> results = new ArrayList<>();
		File file = new File(obf);
		try (RandomAccessFile r = new RandomAccessFile(file.getAbsolutePath(), "r")) {
			BinaryMapIndexReader index = new BinaryMapIndexReader(r, file);
			try {
				for (BinaryMapPoiReaderAdapter.PoiRegion poiIndex : index.getPoiIndexes()) {
					BinaryMapIndexReader.SearchRequest<Amenity> request = BinaryMapIndexReader.buildSearchPoiRequest(
							0, Integer.MAX_VALUE, 0, Integer.MAX_VALUE,
							-1, BinaryMapIndexReader.ACCEPT_ALL_POI_TYPE_FILTER, null);
					results.addAll(index.searchPoi(request, poiIndex));
				}
				results.sort(Comparator.comparing(a -> {
					String name = a == null ? null : a.getName(lang);
					return name == null ? "" : name;
				}, String.CASE_INSENSITIVE_ORDER));
				return results;
			} finally {
				index.close();
			}
		} catch (Exception e) {
			getLogger().error("Failed to read OBF amenities {}", file, e);
			throw new RuntimeException("Failed to read OBF amenities: " + e.getMessage(), e);
		}
	}

	default List<City> getCities(String obf, String lang) {
		Map<String, City> mergedCities = new LinkedHashMap<>();
		File file = new File(obf);
		try (RandomAccessFile r = new RandomAccessFile(file.getAbsolutePath(), "r")) {
			BinaryMapIndexReader index = new BinaryMapIndexReader(r, file);
			try {
				for (BinaryMapAddressReaderAdapter.AddressRegion region : index.getAddressIndexes()) {
					for (BinaryMapAddressReaderAdapter.CityBlocks type : BinaryMapAddressReaderAdapter.CityBlocks.values()) {
						if (type == BinaryMapAddressReaderAdapter.CityBlocks.UNKNOWN_TYPE) {
							continue;
						}
						for (City city : index.getCities(null, type, region, null)) {
							index.preloadStreets(city, null, true, null);
							for (Street street : new ArrayList<>(city.getStreets())) {
								index.preloadBuildings(street, null, null);
							}
							mergeCity(mergedCities, city, lang);
						}
					}
				}
				List<City> results = new ArrayList<>(mergedCities.values());
				results.sort(Comparator.comparing(c -> {
					String name = c == null ? null : c.getName(lang);
					return name == null ? "" : name;
				}, String.CASE_INSENSITIVE_ORDER));
				return results;
			} finally {
				index.close();
			}
		} catch (Exception e) {
			getLogger().error("Failed to read OBF cities {}", file, e);
			throw new RuntimeException("Failed to read OBF cities: " + e.getMessage(), e);
		}
	}

	default List<RouteDataObject> getRoutes(String obf, String lang) {
		List<RouteDataObject> results = new ArrayList<>();
		File file = new File(obf);
		try (RandomAccessFile r = new RandomAccessFile(file.getAbsolutePath(), "r")) {
			BinaryMapIndexReader index = new BinaryMapIndexReader(r, file);
			try {
				for (BinaryMapRouteReaderAdapter.RouteRegion region : index.getRoutingIndexes()) {
					BinaryMapIndexReader.SearchRequest<RouteDataObject> request = BinaryMapIndexReader.buildSearchRouteRequest(
							0, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, null);
					List<BinaryMapRouteReaderAdapter.RouteSubregion> subregions =
							index.searchRouteIndexTree(request, region.getSubregions());
					index.loadRouteIndexData(subregions, new ResultMatcher<>() {
						@Override
						public boolean publish(RouteDataObject object) {
							results.add(object);
							return true;
						}

						@Override
						public boolean isCancelled() {
							return false;
						}
					});
				}
				results.sort(Comparator.comparingLong(rdo -> rdo == null ? Long.MAX_VALUE : rdo.id));
				return results;
			} finally {
				index.close();
			}
		} catch (Exception e) {
			getLogger().error("Failed to read OBF routes {}", file, e);
			throw new RuntimeException("Failed to read OBF routes: " + e.getMessage(), e);
		}
	}

	default void createJsonFile(File sourceJsonFile, List<Amenity> amenities, List<City> cities, List<RouteDataObject> routings) throws IOException {
		JSONObject sourceJson = new JSONObject();
		if (amenities != null && !amenities.isEmpty()) {
			JSONArray amenitiesJson = new JSONArray();
			for (Amenity amenity : amenities) {
				if (amenity != null) {
					amenitiesJson.put(amenity.toJSON());
				}
			}
			if (!amenitiesJson.isEmpty()) {
				sourceJson.put("amenities", amenitiesJson);
			}
		}
		List<City> mergedCities = mergeCities(cities, "");
		if (!mergedCities.isEmpty()) {
			JSONArray citiesJson = new JSONArray();
			for (City city : mergedCities) {
				citiesJson.put(city.toJSON(true));
			}
			sourceJson.put("cities", citiesJson);
		}
		if (routings != null && !routings.isEmpty()) {
			JSONArray routingJson = new JSONArray();
			long routeId = 1;
			for (RouteDataObject route : routings) {
				if (route != null) {
					routingJson.put(routeDataObjectToJson(route, routeId++));
				}
			}
			if (!routingJson.isEmpty()) {
				sourceJson.put("routing", routingJson);
			}
		}
		File parent = sourceJsonFile.getParentFile();
		if (parent != null && !parent.isDirectory() && !parent.mkdirs()) {
			throw new IOException("Cannot create directory " + parent);
		}
		Files.writeString(sourceJsonFile.toPath(), sourceJson.toString(4), StandardCharsets.UTF_8);
	}

	private List<City> mergeCities(List<City> cities, String lang) {
		Map<String, City> merged = new LinkedHashMap<>();
		if (cities != null) {
			for (City city : cities) {
				mergeCity(merged, city, lang);
			}
		}
		return new ArrayList<>(merged.values());
	}

	private void mergeCity(Map<String, City> mergedCities, City city, String lang) {
		if (city == null) {
			return;
		}
		String key = cityMergeKey(city, lang);
		City existing = mergedCities.get(key);
		if (existing == null) {
			mergedCities.put(key, city);
		} else {
			existing.mergeWith(city);
		}
	}

	private String cityMergeKey(City city, String lang) {
		Long id = city.getId();
		if (id != null) {
			return id.toString();
		}
		String name = city.getName(lang);
		LatLon location = city.getLocation();
		return city.getType() + "|" + (name == null ? "" : name.toLowerCase(Locale.ROOT)) + "|"
				+ (location == null ? "" : String.format(Locale.US, "%.6f,%.6f", location.getLatitude(), location.getLongitude()));
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
}
