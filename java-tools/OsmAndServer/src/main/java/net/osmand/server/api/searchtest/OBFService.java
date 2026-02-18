package net.osmand.server.api.searchtest;

import com.amazonaws.util.StringInputStream;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import net.osmand.binary.*;
import net.osmand.data.*;
import net.osmand.obf.OBFDataCreator;
import net.osmand.search.core.SearchExportSettings;
import net.osmand.search.core.SearchResult;
import net.osmand.search.core.SearchSettings;
import net.osmand.server.api.services.OsmAndMapsService;
import net.osmand.server.api.services.SearchService;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import org.json.JSONObject;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public interface OBFService extends BaseService {
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
		List<OsmAndMapsService.BinaryMapIndexReaderReference> list = getMapsService().getObfReaders(points, null,
				"search-test");
		for (OsmAndMapsService.BinaryMapIndexReaderReference ref : list)
			obfList.add(ref.getFile().getAbsolutePath());
		return obfList;
	}

	record CityAddress(String name, List<StreetAddress> streets, boolean boundary) {}
	record Address(String name, LatLon point) {}
	record StreetAddress(String name, List<Address> houses) {}

	enum ObfLengthType {
		VARINT,
		FIXED32
	}

	record ObfFieldSpec(String fieldName, String childMessageType, ObfLengthType lengthType) {}

	static Map<String, Map<Integer, ObfFieldSpec>> buildObfMessageSchema() {
		Map<String, Map<Integer, ObfFieldSpec>> schema = new HashMap<>();
		addObfSpec(schema, "OsmAndStructure", 7, "addressIndex", "OsmAndAddressIndex", ObfLengthType.FIXED32);
		addObfSpec(schema, "OsmAndStructure", 4, "transportIndex", "OsmAndTransportIndex", ObfLengthType.FIXED32);
		addObfSpec(schema, "OsmAndStructure", 8, "poiIndex", "OsmAndPoiIndex", ObfLengthType.FIXED32);
		addObfSpec(schema, "OsmAndStructure", 6, "mapIndex", "OsmAndMapIndex", ObfLengthType.FIXED32);
		addObfSpec(schema, "OsmAndStructure", 9, "routingIndex", "OsmAndRoutingIndex", ObfLengthType.FIXED32);
		addObfSpec(schema, "OsmAndStructure", 10, "hhRoutingIndex", "OsmAndHHRoutingIndex", ObfLengthType.FIXED32);

		addObfSpec(schema, "OsmAndPoiIndex", 2, "boundaries", "OsmAndTileBox", ObfLengthType.VARINT);
		addObfSpec(schema, "OsmAndPoiIndex", 3, "categoriesTable", "OsmAndCategoryTable", ObfLengthType.VARINT);
		addObfSpec(schema, "OsmAndPoiIndex", 4, "nameIndex", "OsmAndPoiNameIndex", ObfLengthType.FIXED32);
		addObfSpec(schema, "OsmAndPoiIndex", 5, "subtypesTable", "OsmAndSubtypesTable", ObfLengthType.VARINT);
		addObfSpec(schema, "OsmAndPoiIndex", 6, "boxes", "OsmAndPoiBox", ObfLengthType.FIXED32);
		addObfSpec(schema, "OsmAndPoiIndex", 9, "poiData", "OsmAndPoiBoxData", ObfLengthType.FIXED32);

		addObfSpec(schema, "OsmAndPoiNameIndex", 3, "table", "IndexedStringTable", ObfLengthType.FIXED32);
		addObfSpec(schema, "OsmAndPoiNameIndex", 5, "data", "OsmAndPoiNameIndexData", ObfLengthType.VARINT);
		addObfSpec(schema, "OsmAndPoiNameIndexData", 3, "atoms", "OsmAndPoiNameIndexDataAtom", ObfLengthType.VARINT);

		addObfSpec(schema, "IndexedStringTable", 5, "subtables", "IndexedStringTable", ObfLengthType.VARINT);

		addObfSpec(schema, "OsmAndPoiBox", 4, "categories", "OsmAndPoiCategories", ObfLengthType.VARINT);
		addObfSpec(schema, "OsmAndPoiBox", 8, "tagGroups", "OsmAndPoiTagGroups", ObfLengthType.FIXED32);
		addObfSpec(schema, "OsmAndPoiBox", 10, "subBoxes", "OsmAndPoiBox", ObfLengthType.FIXED32);
		addObfSpec(schema, "OsmAndPoiTagGroups", 5, "groups", "OsmAndPoiTagGroup", ObfLengthType.VARINT);
		addObfSpec(schema, "OsmAndPoiBoxData", 5, "poiData", "OsmAndPoiBoxDataAtom", ObfLengthType.VARINT);

		addObfSpec(schema, "OsmAndAddressIndex", 3, "boundaries", "OsmAndTileBox", ObfLengthType.VARINT);
		addObfSpec(schema, "OsmAndAddressIndex", 4, "attributeTagsTable", "StringTable", ObfLengthType.VARINT);
		addObfSpec(schema, "OsmAndAddressIndex", 6, "cities", "CitiesIndex", ObfLengthType.FIXED32);
		addObfSpec(schema, "OsmAndAddressIndex", 7, "nameIndex", "OsmAndAddressNameIndexData", ObfLengthType.FIXED32);

		addObfSpec(schema, "CitiesIndex", 5, "cities", "CityIndex", ObfLengthType.VARINT);
		addObfSpec(schema, "CitiesIndex", 7, "blocks", "CityBlockIndex", ObfLengthType.VARINT);
		addObfSpec(schema, "CityBlockIndex", 10, "buildings", "BuildingIndex", ObfLengthType.VARINT);
		addObfSpec(schema, "CityBlockIndex", 12, "streets", "StreetIndex", ObfLengthType.VARINT);
		addObfSpec(schema, "StreetIndex", 5, "intersections", "StreetIntersection", ObfLengthType.VARINT);
		addObfSpec(schema, "StreetIndex", 12, "buildings", "BuildingIndex", ObfLengthType.VARINT);

		addObfSpec(schema, "OsmAndAddressNameIndexData", 4, "table", "IndexedStringTable", ObfLengthType.FIXED32);
		addObfSpec(schema, "OsmAndAddressNameIndexData", 7, "atom", "AddressNameIndexData", ObfLengthType.VARINT);
		addObfSpec(schema, "AddressNameIndexData", 4, "atom", "AddressNameIndexDataAtom", ObfLengthType.VARINT);

		addObfSpec(schema, "OsmAndMapIndex", 4, "rules", "OsmAndMapIndex.MapEncodingRule", ObfLengthType.VARINT);
		addObfSpec(schema, "OsmAndMapIndex", 5, "levels", "OsmAndMapIndex.MapRootLevel", ObfLengthType.FIXED32);
		addObfSpec(schema, "OsmAndMapIndex.MapRootLevel", 7, "boxes", "OsmAndMapIndex.MapDataBox", ObfLengthType.VARINT);
		addObfSpec(schema, "OsmAndMapIndex.MapRootLevel", 15, "blocks", "MapDataBlock", ObfLengthType.VARINT);
		addObfSpec(schema, "OsmAndMapIndex.MapDataBox", 7, "boxes", "OsmAndMapIndex.MapDataBox", ObfLengthType.VARINT);
		addObfSpec(schema, "MapDataBlock", 12, "dataObjects", "MapData", ObfLengthType.VARINT);
		addObfSpec(schema, "MapDataBlock", 15, "stringTable", "StringTable", ObfLengthType.VARINT);

		return schema;
	}

	Map<String, Map<Integer, ObfFieldSpec>> OBF_MESSAGE_SCHEMA = buildObfMessageSchema();

	Map<Integer, String> OBF_STRUCTURE_FIELD_NAMES = buildObfStructureFieldNames();

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

	static void addObfSpec(Map<String, Map<Integer, ObfFieldSpec>> schema, String messageType, int fieldNumber,
	                       String fieldName, String childMessageType, ObfLengthType lengthType) {
		schema.computeIfAbsent(messageType, k -> new HashMap<>())
				.put(fieldNumber, new ObfFieldSpec(fieldName, childMessageType, lengthType));
	}

	static long[] getOrCreateSectionStats(Map<String, long[]> out, String key) {
		return out.computeIfAbsent(key, k -> new long[]{0L, 0L});
	}

	static long countExpandableChildren(String messageType) {
		if (messageType == null) {
			return 0;
		}
		Map<Integer, ObfFieldSpec> specByFieldNumber = OBF_MESSAGE_SCHEMA.get(messageType);
		if (specByFieldNumber == null || specByFieldNumber.isEmpty()) {
			return 0;
		}
		long count = 0;
		for (ObfFieldSpec spec : specByFieldNumber.values()) {
			if (spec == null) {
				continue;
			}
			if (spec.childMessageType != null) {
				count++;
			}
		}
		return count;
	}

	static void addSectionSize(Map<String, long[]> out, String key, long value, long expandableChildrenCount) {
		if (key == null || key.isEmpty()) {
			return;
		}
		long[] stats = getOrCreateSectionStats(out, key);
		if (value > 0) {
			stats[0] += value;
		}
		if (expandableChildrenCount > stats[1]) {
			stats[1] = expandableChildrenCount;
		}
	}

	static void normalizeExpandableCounts(Map<String, long[]> out) {
		for (long[] stats : out.values()) {
			if (stats == null) {
				continue;
			}
			if (stats[0] <= 0) {
				stats[1] = 0;
			}
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
			if (spec == null || spec.childMessageType == null) {
				continue;
			}
			if (rootSegment.equalsIgnoreCase(spec.fieldName)) {
				return new RootSpec(e.getKey(), spec.childMessageType);
			}
		}
		throw new IllegalArgumentException("Unsupported fieldPath root: '" + rootSegment + "'");
	}

	static void skipUnknownField(CodedInputStream codedIS, int tag) throws IOException {
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

	static void readMessageAndCollectImmediateChildSizes(CodedInputStream codedIS, String messageType,
	                                                     Map<String, long[]> outSizes) throws IOException {
		Map<Integer, ObfFieldSpec> specByFieldNumber = OBF_MESSAGE_SCHEMA.get(messageType);
		while (true) {
			int t = codedIS.readTag();
			int fieldNumber = WireFormat.getTagFieldNumber(t);
			if (fieldNumber == 0)
				return;

			ObfFieldSpec spec = specByFieldNumber == null ? null : specByFieldNumber.get(fieldNumber);
			if (spec == null) {
				skipUnknownField(codedIS, t);
				continue;
			}
			if (spec.childMessageType == null) {
				skipUnknownField(codedIS, t);
				continue;
			}
			long payloadLength = readPayloadLengthByWireType(codedIS, t);
			if (payloadLength < 0) {
				skipUnknownField(codedIS, t);
				continue;
			}
			addSectionSize(outSizes, spec.fieldName, payloadLength, countExpandableChildren(spec.childMessageType));
			skipRawBytesLong(codedIS, payloadLength);
		}
	}

	static void readMessageAndCollectAtPath(CodedInputStream codedIS, String messageType, String[] path,
	                                        int pathIndex, Map<String, long[]> outSizes) throws IOException {
		if (pathIndex >= path.length) {
			readMessageAndCollectImmediateChildSizes(codedIS, messageType, outSizes);
			return;
		}
		Map<Integer, ObfFieldSpec> specByFieldNumber = OBF_MESSAGE_SCHEMA.get(messageType);
		String next = path[pathIndex];
		while (true) {
			int t = codedIS.readTag();
			int fieldNumber = WireFormat.getTagFieldNumber(t);
			if (fieldNumber == 0)
				return;

			ObfFieldSpec spec = specByFieldNumber == null ? null : specByFieldNumber.get(fieldNumber);
			if (spec == null) {
				skipUnknownField(codedIS, t);
				continue;
			}
			if (!next.equalsIgnoreCase(spec.fieldName) || spec.childMessageType == null) {
				skipUnknownField(codedIS, t);
				continue;
			}
			long payloadLength = readPayloadLengthByWireType(codedIS, t);
			if (payloadLength < 0) {
				skipUnknownField(codedIS, t);
				continue;
			}
			long oldLimit = codedIS.pushLimitLong(payloadLength);
			try {
				readMessageAndCollectAtPath(codedIS, spec.childMessageType, path, pathIndex + 1, outSizes);
			} finally {
				codedIS.popLimit(oldLimit);
			}
		}
	}

	// fieldPath is null or empty string for root, otherwise dot separated path like 'poiIndex.nameIndex' or 'addressIndex.cities'
	default Map<String, long[]> getSectionSizes(String obf, String fieldPath) {
		Map<String, long[]> sizes = new HashMap<>();
		String normalizedFieldPath = fieldPath == null ? "" : fieldPath.trim();
		String[] path = normalizedFieldPath.isEmpty() ? new String[0] : normalizedFieldPath.split("\\.");
		File file = new File(obf);
		try (RandomAccessFile r = new RandomAccessFile(file.getAbsolutePath(), "r")) {
			CodedInputStream codedIS = CodedInputStream.newInstance(r);
			codedIS.setSizeLimit(CodedInputStream.MAX_DEFAULT_SIZE_LIMIT);
			if (path.length == 0) {
				Map<Integer, ObfFieldSpec> rootSpecs = OBF_MESSAGE_SCHEMA.get("OsmAndStructure");
				if (rootSpecs != null) {
					for (ObfFieldSpec s : rootSpecs.values()) {
						if (s == null || s.childMessageType == null || s.fieldName == null) {
							continue;
						}
						long[] stats = getOrCreateSectionStats(sizes, s.fieldName);
						stats[1] = Math.max(stats[1], countExpandableChildren(s.childMessageType));
					}
				}
				while (true) {
					int t = codedIS.readTag();
					int fieldNumber = WireFormat.getTagFieldNumber(t);
					if (fieldNumber == 0) {
						normalizeExpandableCounts(sizes);
						return sizes;
					}

					long payloadLength = readPayloadLengthByWireType(codedIS, t);
					if (payloadLength < 0) {
						skipUnknownField(codedIS, t);
						continue;
					}
					String fieldName = null;
					ObfFieldSpec rootSpec = rootSpecs == null ? null : rootSpecs.get(fieldNumber);
					if (rootSpec != null && rootSpec.fieldName != null) {
						fieldName = rootSpec.fieldName;
					} else {
						fieldName = OBF_STRUCTURE_FIELD_NAMES.get(fieldNumber);
					}
					if (fieldName == null) {
						fieldName = "field_" + fieldNumber;
					}
					long expandableChildren = 0;
					if (rootSpec != null) {
						expandableChildren = countExpandableChildren(rootSpec.childMessageType);
					}
					addSectionSize(sizes, fieldName, payloadLength, expandableChildren);
					skipRawBytesLong(codedIS, payloadLength);
				}
			}

			RootSpec rootSpec = resolveRootSpec(path[0]);
			int rootFieldNumber = rootSpec.rootFieldNumber;
			String rootMessageType = rootSpec.rootMessageType;

			while (true) {
				int t = codedIS.readTag();
				int fieldNumber = WireFormat.getTagFieldNumber(t);
				if (fieldNumber == 0) {
					normalizeExpandableCounts(sizes);
					return sizes;
				}

				if (fieldNumber != rootFieldNumber) {
					skipUnknownField(codedIS, t);
					continue;
				}
				long payloadLength = readPayloadLengthByWireType(codedIS, t);
				if (payloadLength < 0) {
					skipUnknownField(codedIS, t);
					continue;
				}
				long oldLimit = codedIS.pushLimitLong(payloadLength);
				try {
					readMessageAndCollectAtPath(codedIS, rootMessageType, path, 1, sizes);
				} finally {
					codedIS.popLimit(oldLimit);
				}
			}
		} catch (IOException e) {
			getLogger().error("Failed to read OBF file: {}", file, e);
			throw new RuntimeException("Failed to read OBF file: " + e.getMessage(), e);
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
		try (RandomAccessFile r = new RandomAccessFile(file.getAbsolutePath(), "r")) {
			final Pattern cityPattern, streetPattern, housePattern, poiPattern;
			try {
				cityPattern = isCityEmpty ? null : Pattern.compile(cityRegExp, Pattern.CASE_INSENSITIVE);
				streetPattern = isStreetEmpty ? null : Pattern.compile(streetRegExp, Pattern.CASE_INSENSITIVE);
				housePattern = isHouseEmpty ? null : Pattern.compile(houseRegExp, Pattern.CASE_INSENSITIVE);
				poiPattern = !(isCityEmpty || isStreetEmpty) || !isHouseEmpty || isPoiEmpty ? null : Pattern.compile(poiRegExp, Pattern.CASE_INSENSITIVE);
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

								if (isStreetEmpty && isHouseEmpty) {
									results.add(new CityAddress(cityName, streets, c.getType() == City.CityType.BOUNDARY));
									continue;
								}

								index.preloadStreets(c, null, null);
								for (Street s : new ArrayList<>(c.getStreets())) {
									List<Address> buildings = new ArrayList<>();
									final String streetName = s.getName(lang);
									if (streetName == null || !isStreetEmpty && !streetPattern.matcher(streetName).find())
										continue;

									if (isHouseEmpty) {
										streets.add(new StreetAddress(streetName, buildings));
										continue;
									}

									index.preloadBuildings(s, null, null);
									final List<Building> bs = s.getBuildings();
									if (bs != null && !bs.isEmpty()) {
										for (Building b : bs) {
											final String houseName = b.getName(lang);
											if (houseName != null && housePattern.matcher(houseName).find())
												buildings.add(new Address(houseName, b.getLocation()));
										}
									}
									if (!buildings.isEmpty()) {
										StreetAddress street = new StreetAddress(streetName, buildings);
										streets.add(street);
									}
								}
								if (!streets.isEmpty())
									results.add(new CityAddress(cityName, streets, c.getType() == City.CityType.BOUNDARY));
							}
						}
					} else if (poiPattern != null && p instanceof BinaryMapPoiReaderAdapter.PoiRegion poi) {
						BinaryMapIndexReader.SearchRequest<Amenity> req = BinaryMapIndexReader.buildSearchPoiRequest(
								poi.getLeft31(), poi.getRight31(), poi.getTop31(), poi.getBottom31(), 15,
								null, null);
						for (Amenity amenity : index.searchPoi(req, poi)) {
							final String poiName = amenity.getName(lang);
							if (poiName == null || !poiPattern.matcher(poiName).find())
								continue;
							results.add(new Address(poiName, amenity.getLocation()));
						}
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

	default ResultsWithStats getResults(SearchService.SearchContext ctx, SearchService.SearchOption option) throws IOException {
		SearchService.SearchResultWrapper result = getSearchService().searchResults(ctx, option, null);

		List<AddressResult> results = new ArrayList<>();
		for (SearchResult r : result.results()) {
			AddressResult rec = toResult(r, Collections.newSetFromMap(new IdentityHashMap<>()));
			results.add(rec);
		}
		return new ResultsWithStats(results, result.stat().getWordStats().values(), result.stat().getByApis());
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

	record UnitTestPayload(String name, String[] queries) {}

	default void createUnitTest(UnitTestPayload unitTest, SearchService.SearchContext ctx, OutputStream out) throws IOException, SQLException {
		SearchExportSettings exportSettings = new SearchExportSettings(true, true, -1);
		SearchService.SearchResultWrapper result = getSearchService()
				.searchResults(ctx, new SearchService.SearchOption(true, exportSettings), null);

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
			Map<String, Object> rootJson = new LinkedHashMap<>();
			rootJson.put("settings", settingsJson);
			rootJson.put("phrases", unitTest.queries);
			rootJson.put("results", Arrays.stream(unitTest.queries()).map(x -> "").toArray());

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
}
