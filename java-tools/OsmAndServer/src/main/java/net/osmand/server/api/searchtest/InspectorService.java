package net.osmand.server.api.searchtest;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import net.osmand.CollatorStringMatcher;
import net.osmand.binary.*;
import net.osmand.binary.BinaryMapIndexReader.TagValuePair;
import net.osmand.data.*;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import net.osmand.util.SearchAlgorithms;
import net.osmand.util.TransliterationHelper;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static net.osmand.binary.ObfConstants.*;

public interface InspectorService extends OBFService {
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

    record AddressTokenRefs(List<Integer> cityOffsets, List<Integer> streetOffsets, List<Integer> streetCityOffsets,
                            List<AddressRef> addressRefs) {
        AddressTokenRefs() {
            this(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
        }
    }


    record AddressTokenRef(IndexToken token, AddressRef ref) {
    }


    record GenerateDbRawPoiObject(RawPoiObject rawPoiObject, int payloadOffset, int payloadSize, int sourceOffset) {
    }



    default void collectAddressObjects(BinaryMapIndexReaderExt index,
                                       BinaryMapAddressReaderAdapter.AddressRegion region,
                                       AddressRef[] addressRefs,
                                       List<ObjectAddress> results,
                                       String lang,
                                       CollatorStringMatcher matcher) throws IOException {
        List<AddressRef> refs = addressRefs == null ? List.of() : Arrays.asList(addressRefs);
        for (AddressRef ref : refs) {
            if (ref == null || ref.typeIndex() >= BinaryMapAddressReaderAdapter.CityBlocks.STREET_TYPE.index
                    || !isOffsetWithinPart(ref.objectOffset(), region)) {
                continue;
            }
            int offset = ref.objectOffset();
            ObjectAddress objectAddress = loadCityObjectAddress(index, region, offset, lang, matcher);
            if (objectAddress != null) {
                results.add(objectAddress);
            }
        }
        Map<Integer, City> streetCities = new HashMap<>();
        for (AddressRef ref : refs) {
            if (ref == null || ref.typeIndex() != BinaryMapAddressReaderAdapter.CityBlocks.STREET_TYPE.index) {
                continue;
            }
            Integer cityOffset = ref.cityOffset();
            if (streetCities.containsKey(cityOffset)) {
                continue;
            }
            City city = loadCity(index, region, cityOffset);
            if (city != null) {
                streetCities.put(cityOffset, city);
            }
        }
        for (AddressRef ref : refs) {
            if (ref == null || ref.typeIndex() != BinaryMapAddressReaderAdapter.CityBlocks.STREET_TYPE.index
                    || !isOffsetWithinPart(ref.objectOffset(), region)) {
                continue;
            }
            int streetOffset = ref.objectOffset();
            int cityOffset = ref.cityOffset();
            City city = streetCities.get(cityOffset);
            ObjectAddress objectAddress = loadStreetObjectAddress(index, region, streetOffset, city, lang, matcher);
            if (objectAddress != null) {
                results.add(objectAddress);
            }
        }
    }

    default IndexTokenPage getIndex(String obf, String prefix, int pageToShow, int pageSizeLimit, String sortBy, String sortOrder) {
        File file = new File(obf);
        Pattern prefixPattern = compileIndexPrefixPattern(prefix);
        final int safePage = Math.max(pageToShow, 0);
        final int safeSize = Math.max(1, Math.min(pageSizeLimit, 100));
        try {
            List<IndexToken> allTokens = getCachedOrLoadIndexTokens(file);
            List<IndexToken> results = new ArrayList<>();
            if (prefixPattern == null) {
                results.addAll(allTokens);
            } else {
                for (IndexToken token : allTokens) {
                    if (prefixPattern.matcher(token.name()).find()) {
                        results.add(token);
                    }
                }
            }
            IndexTokenSummary summary = buildIndexTokenSummary(results);
            results.sort(buildIndexTokenComparator(sortBy, sortOrder));
            long totalElements = results.size();
            int totalPages = totalElements == 0 ? 0 : (int) ((totalElements + safeSize - 1) / safeSize);
            int fromIndex = Math.min(safePage * safeSize, results.size());
            int toIndex = Math.min(fromIndex + safeSize, results.size());
            List<IndexToken> pageContent = fromIndex >= toIndex
                    ? List.of()
                    : new ArrayList<>(results.subList(fromIndex, toIndex));
            return new IndexTokenPage(pageContent, safePage, safeSize, totalElements, totalPages, summary);
        } catch (Exception e) {
            getLogger().error("Failed to read OBF index {}", file, e);
            throw new RuntimeException("Failed to read OBF index: " + e.getMessage(), e);
        }
    }

    default IndexTokenPage getIndex(List<String> obfs, String prefix, int pageToShow, int pageSizeLimit, String sortBy, String sortOrder) {
        Pattern prefixPattern = compileIndexPrefixPattern(prefix);
        final int safePage = Math.max(pageToShow, 0);
        final int safeSize = Math.max(1, Math.min(pageSizeLimit, 100));
        try {
            Map<String, IndexToken> mergedByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            if (obfs != null) {
                for (String obf : obfs) {
                    if (Algorithms.isEmpty(obf)) {
                        continue;
                    }
                    List<IndexToken> allTokens = getCachedOrLoadIndexTokens(new File(obf));
                    for (IndexToken token : allTokens) {
                        if (token == null || token.name() == null) {
                            continue;
                        }
                        if (prefixPattern == null || prefixPattern.matcher(token.name()).find()) {
                            mergedByName.merge(token.name(), token, this::mergeIndexTokens);
                        }
                    }
                }
            }
            List<IndexToken> results = new ArrayList<>(mergedByName.values());
            IndexTokenSummary summary = buildIndexTokenSummary(results);
            results.sort(buildIndexTokenComparator(sortBy, sortOrder));
            long totalElements = results.size();
            int totalPages = totalElements == 0 ? 0 : (int) ((totalElements + safeSize - 1) / safeSize);
            int fromIndex = Math.min(safePage * safeSize, results.size());
            int toIndex = Math.min(fromIndex + safeSize, results.size());
            List<IndexToken> pageContent = fromIndex >= toIndex
                    ? List.of()
                    : new ArrayList<>(results.subList(fromIndex, toIndex));
            return new IndexTokenPage(pageContent, safePage, safeSize, totalElements, totalPages, summary);
        } catch (Exception e) {
            getLogger().error("Failed to read OBF indexes {}", obfs, e);
            throw new RuntimeException("Failed to read OBF indexes: " + e.getMessage(), e);
        }
    }

    default IndexToken mergeIndexTokens(IndexToken left, IndexToken right) {
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        return new IndexToken(left.name() != null ? left.name() : right.name(),
                concatAddressRefs(left.addressRefs(), right.addressRefs()),
                concatIntArrays(left.poiRefs(), right.poiRefs()),
                concatIntArrays(left.poiAtomRefs(), right.poiAtomRefs()),
                concatIntArrays(left.poiAtomSizes(), right.poiAtomSizes()),
                left.isCommon() || right.isCommon(),
                left.isFrequent() || right.isFrequent(),
                null);
    }

    default AddressRef[] concatAddressRefs(AddressRef[] left, AddressRef[] right) {
        if (left == null || left.length == 0) {
            return right == null ? new AddressRef[0] : Arrays.copyOf(right, right.length);
        }
        if (right == null || right.length == 0) {
            return Arrays.copyOf(left, left.length);
        }
        AddressRef[] merged = Arrays.copyOf(left, left.length + right.length);
        System.arraycopy(right, 0, merged, left.length, right.length);
        return merged;
    }

    default int[] concatIntArrays(int[] left, int[] right) {
        if (left == null || left.length == 0) {
            return right == null ? new int[0] : Arrays.copyOf(right, right.length);
        }
        if (right == null || right.length == 0) {
            return Arrays.copyOf(left, left.length);
        }
        int[] merged = Arrays.copyOf(left, left.length + right.length);
        System.arraycopy(right, 0, merged, left.length, right.length);
        return merged;
    }

    default Comparator<IndexToken> buildIndexTokenComparator(String sortBy, String sortOrder) {
        String normalizedSortBy = Algorithms.isEmpty(sortBy) ? "name" : sortBy.trim().toLowerCase(Locale.ROOT);
        Comparator<IndexToken> comparator = switch (normalizedSortBy) {
            case "poi" -> Comparator.comparingInt(this::getIndexTokenPoiCount);
            case "address" -> Comparator.comparingInt(this::getIndexTokenAddressCount);
            case "common" -> Comparator.comparingInt(token -> token != null && token.isCommon() ? 1 : 0);
            case "frequent" -> Comparator.comparingInt(token -> token != null && token.isFrequent() ? 1 : 0);
            case "count" ->
                    Comparator.comparingInt(token -> getIndexTokenPoiCount(token) + getIndexTokenAddressCount(token));
            default ->
                    Comparator.comparing(token -> token == null || token.name() == null ? "" : token.name(), String.CASE_INSENSITIVE_ORDER);
        };
        comparator = comparator.thenComparing(token -> token == null || token.name() == null ? "" : token.name(), String.CASE_INSENSITIVE_ORDER);
        return "desc".equalsIgnoreCase(sortOrder) ? comparator.reversed() : comparator;
    }

    default IndexTokenSummary buildIndexTokenSummary(List<IndexToken> tokens) {
        int poiSum = 0;
        int addressSum = 0;
        int commonSum = 0;
        int frequentSum = 0;
        int poiMax = 0;
        int addressMax = 0;
        for (IndexToken token : tokens) {
            int poiCount = getIndexTokenPoiCount(token);
            int addressCount = getIndexTokenAddressCount(token);
            poiSum += poiCount;
            addressSum += addressCount;
            commonSum += token != null && token.isCommon() ? 1 : 0;
            frequentSum += token != null && token.isFrequent() ? 1 : 0;
            poiMax = Math.max(poiMax, poiCount);
            addressMax = Math.max(addressMax, addressCount);
        }
        return new IndexTokenSummary(poiSum, addressSum, commonSum, frequentSum, poiMax, addressMax);
    }

    default int getIndexTokenPoiCount(IndexToken token) {
        return token == null || token.poiRefs() == null ? 0 : token.poiRefs().length;
    }

    default int getIndexTokenAddressCount(IndexToken token) {
        return token == null || token.addressRefs() == null ? 0 : token.addressRefs().length;
    }

    default List<IndexToken> getCachedOrLoadIndexTokens(File file) throws IOException {
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

    default List<IndexToken> loadIndexTokens(File file) throws IOException {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file.getAbsolutePath(), "r")) {
            BinaryMapIndexReaderExt index = new BinaryMapIndexReaderExt(randomAccessFile, file);
            try {
                Map<String, IndexTokenBuilder> tokens = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                for (BinaryIndexPart part : index.getIndexes()) {
                    if (part instanceof BinaryMapAddressReaderAdapter.AddressRegion addressRegion) {
                        collectAddressIndexTokens(index, addressRegion, tokens);
                    } else if (part instanceof BinaryMapPoiReaderAdapter.PoiRegion poiRegion) {
                        collectPoiIndexTokens(index, poiRegion, tokens);
                    }
                }
                return buildIndexTokensWithRefs(index, tokens);
            } finally {
                index.close();
            }
        }
    }

    default String getIndexCacheKey(File file) throws IOException {
        return file.getCanonicalPath();
    }

    default Pattern compileIndexPrefixPattern(String prefix) {
        if (prefix == null || prefix.trim().isEmpty()) {
            return null;
        }
        try {
            return Pattern.compile(prefix, Pattern.CASE_INSENSITIVE);
        } catch (PatternSyntaxException e) {
            throw new RuntimeException("Invalid regex provided: " + e.getDescription(), e);
        }
    }

    default void collectAddressIndexTokens(BinaryMapIndexReaderExt index, BinaryMapAddressReaderAdapter.AddressRegion region,
                                           Map<String, IndexTokenBuilder> tokens) throws IOException {
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

    default void collectPoiIndexTokens(BinaryMapIndexReaderExt index, BinaryMapPoiReaderAdapter.PoiRegion region,
                                       Map<String, IndexTokenBuilder> tokens) throws IOException {
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
                        InspectorService.skipUnknownField(index.getInputStream(), tagWithType);
                        break;
                }
            }
        } finally {
            index.getInputStream().popLimit(oldLimit);
        }
    }

    default void readAddressNameIndexTokens(BinaryMapIndexReaderExt index, Map<String, IndexTokenBuilder> tokens) throws IOException {
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
                    InspectorService.skipUnknownField(index.getInputStream(), tagWithType);
                    break;
            }
        }
    }

    default void readPoiNameIndexTokens(BinaryMapIndexReaderExt index, Map<String, IndexTokenBuilder> tokens) throws IOException {
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
                    InspectorService.skipUnknownField(index.getInputStream(), tagWithType);
                    break;
            }
        }
    }

    default void readNameIndexTableTokens(BinaryMapIndexReaderExt index, Map<String, IndexTokenBuilder> tokens, boolean poi) throws IOException {
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
                PoiTokenRefs poiTokenRefs = poi ? readPoiTokenRefs(index, (int) absoluteOffset, suffixIndex, true) : new PoiTokenRefs(new LinkedHashSet<>(), new ArrayList<>());
                addIndexToken(tokens, entry.getKey() + suffix, (int) absoluteOffset, suffixIndex, poi, toIntArray(poiTokenRefs.offsets()), toIntArray(poiTokenRefs.atomSizes()));
            }
        }
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
                    InspectorService.skipUnknownField(index.getInputStream(), tagWithType);
                    break;
            }
        }
    }

    default List<String> readSuffixDictionaryAtOffset(BinaryMapIndexReaderExt index, long absoluteOffset, boolean poi) throws IOException {
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
                InspectorService.skipUnknownField(index.getInputStream(), tagWithType);
            }
        } finally {
            index.getInputStream().popLimit(oldLimit);
        }
    }

    default void addIndexToken(Map<String, IndexTokenBuilder> tokens, String name, int offset, int suffixIndex, boolean poi, int[] poiRefs, int[] poiAtomSizes) {
        IndexTokenBuilder existing = tokens.get(name);
        if (existing == null) {
            tokens.put(name, poi
                    ? new IndexTokenBuilder(name, new int[0], new int[0], poiRefs == null ? new int[0] : distinctOffsets(poiRefs), poiRefs == null ? new int[0] : poiRefs, poiAtomSizes == null ? new int[0] : poiAtomSizes)
                    : new IndexTokenBuilder(name, new int[]{offset}, new int[]{suffixIndex}, new int[0], new int[0], new int[0]));
            return;
        }
        int[] mergedAddressOffsets = poi ? existing.addressOffsets() : appendDistinctOffset(existing.addressOffsets(), offset);
        int[] mergedAddressSuffixIndexes = poi ? existing.addressSuffixIndexes() : appendAddressSuffixIndex(existing.addressOffsets(), existing.addressSuffixIndexes(), offset, suffixIndex);
        int[] mergedPoiRefs = poi ? appendDistinctOffsets(existing.poiRefs(), poiRefs) : existing.poiRefs();
        int[] mergedPoiAtomRefs = poi ? appendOffsets(existing.poiAtomRefs(), poiRefs) : existing.poiAtomRefs();
        int[] mergedPoiAtomSizes = poi ? appendOffsets(existing.poiAtomSizes(), poiAtomSizes) : existing.poiAtomSizes();
        tokens.put(name, new IndexTokenBuilder(name, mergedAddressOffsets, mergedAddressSuffixIndexes, mergedPoiRefs, mergedPoiAtomRefs, mergedPoiAtomSizes));
    }

    default int[] appendAddressSuffixIndex(int[] addressOffsets, int[] suffixIndexes, int offset, int suffixIndex) {
        if (addressOffsets != null) {
            for (int existingOffset : addressOffsets) {
                if (existingOffset == offset) {
                    return suffixIndexes == null ? new int[0] : suffixIndexes;
                }
            }
        }
        return appendOffset(suffixIndexes, suffixIndex);
    }

    default List<IndexToken> buildIndexTokensWithRefs(BinaryMapIndexReaderExt index,
                                                      Map<String, IndexTokenBuilder> tokens) throws IOException {
        List<IndexToken> tokensWithRefs = new ArrayList<>(tokens.size());
        for (IndexTokenBuilder token : tokens.values()) {
            AddressRef[] tokenAddressRefs = collectAddressRefs(index, token);
            tokensWithRefs.add(new IndexToken(token.name(), tokenAddressRefs, token.poiRefs(), token.poiAtomRefs(),
                    token.poiAtomSizes(), CommonWords.getCommon(token.name()) != -1,
                    CommonWords.getFrequentlyUsed(token.name()) != -1));
        }
        return tokensWithRefs;
    }

    default AddressRef[] collectAddressRefs(BinaryMapIndexReaderExt index, IndexTokenBuilder token) throws IOException {
        List<AddressRef> refs = new ArrayList<>();
        Set<String> uniqueRefs = new LinkedHashSet<>();
        int[] addressOffsets = token.addressOffsets();
        if (addressOffsets == null) {
            return new AddressRef[0];
        }
        int[] addressSuffixIndexes = token.addressSuffixIndexes();
        for (int i = 0; i < addressOffsets.length; i++) {
            int tokenOffset = addressOffsets[i];
            int suffixIndex = addressSuffixIndexes != null && i < addressSuffixIndexes.length ? addressSuffixIndexes[i] : -1;
            AddressTokenRefs tokenRefs = readAddressTokenRefs(index, tokenOffset, suffixIndex, true);
            for (AddressRef ref : tokenRefs.addressRefs) {
                if (ref != null && uniqueRefs.add(ref.shiftToIndex() + ":" + ref.shiftToCityIndex() + ":"
                        + ref.objectOffset() + ":" + ref.cityOffset() + ":" + ref.typeIndex())) {
                    refs.add(ref);
                }
            }
        }
        return refs.toArray(new AddressRef[0]);
    }

    default long calculateObjectAddressesSize(BinaryMapIndexReaderExt index,
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

    default ObjectAddressStats calculateObjectAddressStats(BinaryMapIndexReaderExt index,
                                                           List<ObjectAddress> objects,
                                                           List<BinaryMapPoiReaderAdapter.PoiRegion> poiRegions,
                                                           Boolean isPoi) throws IOException {
        if (objects == null || objects.isEmpty()) {
            return new ObjectAddressStats(0, 0);
        }
        List<ObjectAddress> filteredObjects = new ArrayList<>();
        for (ObjectAddress object : objects) {
            if (object != null && (isPoi == null || object.isPoi() == isPoi)) {
                filteredObjects.add(object);
            }
        }
        return new ObjectAddressStats(safeMetricInt(calculateObjectAddressesSize(index, filteredObjects, poiRegions)), filteredObjects.size());
    }

    default ObjectAddressStats calculateAloneTokenStats(BinaryMapIndexReaderExt index,
                                                        List<ObjectAddress> objects,
                                                        List<BinaryMapPoiReaderAdapter.PoiRegion> poiRegions,
                                                        IndexToken token,
                                                        Boolean isPoi) throws IOException {
        if (token == null || Algorithms.isEmpty(token.name()) || objects == null || objects.isEmpty()) {
            return new ObjectAddressStats(0, 0);
        }
        List<ObjectAddress> aloneObjects = new ArrayList<>();
        for (ObjectAddress object : objects) {
            if (object != null && (isPoi == null || object.isPoi() == isPoi) && isAloneTokenObject(object, token.name())) {
                aloneObjects.add(object);
            }
        }
        return new ObjectAddressStats(safeMetricInt(calculateObjectAddressesSize(index, aloneObjects, poiRegions)), aloneObjects.size());
    }

    default boolean isAloneTokenObject(ObjectAddress object, String tokenName) {
        if (Algorithms.isEmpty(tokenName)) {
            return false;
        }
        Set<String> candidateNames = new LinkedHashSet<>();
        if (!Algorithms.isEmpty(object.name())) {
            candidateNames.add(object.name());
        }
        Map<String, String> values = object.commonTags();
        if (values != null) {
            for (Map.Entry<String, String> entry : values.entrySet()) {
                String key = entry.getKey();
                if (key != null && (Amenity.NAME.equals(key) || key.startsWith(Amenity.NAME + ":") || isTagIndexedForSearchAsName(key) || isTagIndexedForSearchAsId(key))) {
                    if (!Algorithms.isEmpty(entry.getValue())) {
                        candidateNames.add(entry.getValue());
                    }
                }
            }
        }
        for (String candidateName : candidateNames) {
            List<String> tokens = SearchAlgorithms.splitAndNormalize(candidateName, true);
            SearchAlgorithms.removeCommonWords(tokens);
            if (tokens.size() == 1 && tokens.contains(tokenName)) {
                return true;
            }
        }
        return false;
    }

    default int sumAddressAtomSizes(AddressRef[] addressRefs) {
        if (addressRefs == null || addressRefs.length == 0) {
            return 0;
        }
        long totalSize = 0L;
        for (AddressRef addressRef : addressRefs) {
            if (addressRef != null && addressRef.atomSize() > 0) {
                totalSize += addressRef.atomSize();
            }
        }
        return safeMetricInt(totalSize);
    }

    default int sumExactAddressAtomSizes(AddressRef[] addressRefs, List<ObjectAddress> exactResults) {
        if (addressRefs == null || addressRefs.length == 0 || exactResults == null || exactResults.isEmpty()) {
            return 0;
        }
        Set<Integer> exactSourceOffsets = new LinkedHashSet<>();
        for (ObjectAddress object : exactResults) {
            if (object != null && !object.isPoi()) {
                exactSourceOffsets.add(object.sourceOffset());
            }
        }
        long totalSize = 0L;
        for (AddressRef addressRef : addressRefs) {
            if (addressRef != null && addressRef.atomSize() > 0 && exactSourceOffsets.contains(addressRef.objectOffset())) {
                totalSize += addressRef.atomSize();
            }
        }
        return safeMetricInt(totalSize);
    }

    default int sumExactPoiAtomSizes(IndexToken token,
                                     List<BinaryMapPoiReaderAdapter.PoiRegion> poiRegions,
                                     List<ObjectAddress> exactResults) {
        if (token == null || token.poiRefs() == null || token.poiAtomSizes() == null
                || token.poiRefs().length == 0 || exactResults == null || exactResults.isEmpty()) {
            return 0;
        }
        Set<Integer> exactSourceOffsets = new LinkedHashSet<>();
        for (ObjectAddress object : exactResults) {
            if (object != null && object.isPoi()) {
                exactSourceOffsets.add(object.sourceOffset());
            }
        }
        long totalSize = 0L;
        int[] poiAtomRefs = token.poiAtomRefs() == null ? token.poiRefs() : token.poiAtomRefs();
        for (int i = 0; i < poiAtomRefs.length; i++) {
            int atomSize = i < token.poiAtomSizes().length ? token.poiAtomSizes()[i] : 0;
            if (atomSize <= 0) {
                continue;
            }
            int poiRef = poiAtomRefs[i];
            for (BinaryMapPoiReaderAdapter.PoiRegion poiRegion : poiRegions) {
                if (exactSourceOffsets.contains((int) (poiRegion.getFilePointer() + poiRef))) {
                    totalSize += atomSize;
                    break;
                }
            }
        }
        return safeMetricInt(totalSize);
    }

    default int sumIntValues(int[] values) {
        if (values == null || values.length == 0) {
            return 0;
        }
        long total = 0L;
        for (int value : values) {
            if (value > 0) {
                total += value;
            }
        }
        return safeMetricInt(total);
    }

    default int[] toIntArray(Collection<Integer> offsets) {
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

    default boolean isOffsetWithinPart(int offset, BinaryIndexPart part) {
        long partStart = part.getFilePointer();
        long partEnd = partStart + part.getLength();
        return offset >= partStart && offset < partEnd;
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

    default int computeSmartLengthPrefixSize(long value) {
        return value > 0x7f ? 8 : 1;
    }

    default int[] appendDistinctOffset(int[] offsets, int offset) {
        if (offsets == null || offsets.length == 0) {
            return new int[]{offset};
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

    default int[] appendOffset(int[] offsets, int offset) {
        if (offsets == null || offsets.length == 0) {
            return new int[]{offset};
        }
        int[] mergedOffsets = Arrays.copyOf(offsets, offsets.length + 1);
        mergedOffsets[offsets.length] = offset;
        return mergedOffsets;
    }

    default int[] appendDistinctOffsets(int[] offsets, int[] values) {
        if (values == null || values.length == 0) {
            return offsets == null ? new int[0] : offsets;
        }
        int[] mergedOffsets = offsets == null ? new int[0] : offsets;
        for (int value : values) {
            mergedOffsets = appendDistinctOffset(mergedOffsets, value);
        }
        return mergedOffsets;
    }

    default int[] distinctOffsets(int[] values) {
        return appendDistinctOffsets(new int[0], values);
    }

    default int[] appendOffsets(int[] offsets, int[] values) {
        if (values == null || values.length == 0) {
            return offsets == null ? new int[0] : offsets;
        }
        int[] existingOffsets = offsets == null ? new int[0] : offsets;
        int[] mergedOffsets = Arrays.copyOf(existingOffsets, existingOffsets.length + values.length);
        System.arraycopy(values, 0, mergedOffsets, existingOffsets.length, values.length);
        return mergedOffsets;
    }

    default int safeMetricInt(long value) {
        if (value <= 0L) {
            return 0;
        }
        return value >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) value;
    }

    default long getLengthDelimitedMessageSizeAtOffset(BinaryMapIndexReaderExt index, int absoluteOffset) throws IOException {
        if (absoluteOffset <= 0) {
            return 0L;
        }
        index.getInputStream().seek(absoluteOffset);
        long contentLength = index.getInputStream().readRawVarint32();
        return contentLength + computeVarint32Size(contentLength);
    }

    default long getPoiDataBoxSizeAtRelativeOffset(BinaryMapIndexReaderExt index,
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

    default long getPoiDataBoxSizeAtAbsoluteOffset(BinaryMapIndexReaderExt index,
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

    default AddressTokenRefs readAddressTokenRefs(BinaryMapIndexReaderExt index,
                                                  int tokenOffset,
                                                  int exactSuffixIndex,
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
                        if (isFiltered && exactSuffixIndex == suffixDictionary.size()) {
                            matchedSuffixIndexes.add(suffixDictionary.size());
                        }
                        suffixDictionary.add(decodedSuffix);
                        break;
                    case OsmandOdb.OsmAndAddressNameIndexData.AddressNameIndexData.ATOM_FIELD_NUMBER:
                        int atomLength = index.getInputStream().readRawVarint32();
                        long atomOldLimit = index.getInputStream().pushLimitLong(atomLength);
                        try {
                            readAddressTokenAtom(index, tokenOffset, atomLength + computeVarint32Size(atomLength), matchedSuffixIndexes, refs, isFiltered);
                        } finally {
                            index.getInputStream().popLimit(atomOldLimit);
                        }
                        break;
                    default:
                        InspectorService.skipUnknownField(index.getInputStream(), tagWithType);
                        break;
                }
            }
        } finally {
            index.getInputStream().popLimit(oldLimit);
        }
    }

    default void readAddressTokenAtom(BinaryMapIndexReaderExt index,
                                      int tokenOffset,
                                      int atomSize,
                                      List<Integer> matchedSuffixIndexes,
                                      AddressTokenRefs refs,
                                      boolean isFiltered) throws IOException {
        int objectOffset = 0;
        int shiftToIndex = 0;
        int cityOffset = 0;
        int shiftToCityIndex = 0;
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
                    if (shiftToIndex != 0) {
                        refs.addressRefs.add(new AddressRef(shiftToIndex, shiftToCityIndex, objectOffset, cityOffset, typeIndex, atomSize));
                    }
                }
            }
            if (tag == 0) {
                return;
            }
            switch (tag) {
                case OsmandOdb.AddressNameIndexDataAtom.SHIFTTOCITYINDEX_FIELD_NUMBER:
                    shiftToCityIndex = index.getInputStream().readInt32();
                    cityOffset = tokenOffset - shiftToCityIndex;
                    break;
                case OsmandOdb.AddressNameIndexDataAtom.XY16_FIELD_NUMBER:
                    index.getInputStream().readInt32();
                    break;
                case OsmandOdb.AddressNameIndexDataAtom.SHIFTTOINDEX_FIELD_NUMBER:
                    shiftToIndex = index.getInputStream().readInt32();
                    objectOffset = tokenOffset - shiftToIndex;
                    break;
                case OsmandOdb.AddressNameIndexDataAtom.TYPE_FIELD_NUMBER:
                    typeIndex = index.getInputStream().readInt32();
                    break;
                default:
                    InspectorService.skipUnknownField(index.getInputStream(), tagWithType);
                    break;
            }
        }
    }

    default City loadCity(BinaryMapIndexReaderExt index,
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

    default ObjectAddress loadCityObjectAddress(BinaryMapIndexReaderExt index,
                                                BinaryMapAddressReaderAdapter.AddressRegion region,
                                                int offset,
                                                String lang,
                                                CollatorStringMatcher matcher) throws IOException {
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
        String name = city.getName(lang);
        boolean isMatched = matchesLegacyCity(city, matcher);
        return new ObjectAddress(0, name, city.getLocation(), arrangeObjectAddressValues(values),
                false, isMatched, false, false, type, city.getId(), null, offset, payloadSize, offset);
    }

    default ObjectAddress loadStreetObjectAddress(BinaryMapIndexReaderExt index,
                                                  BinaryMapAddressReaderAdapter.AddressRegion region,
                                                  int offset,
                                                  City city,
                                                  String lang,
                                                  CollatorStringMatcher matcher) throws IOException {
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
            String name = street.getName(lang);
            boolean isMatched = matchesLegacyStreet(street, matcher);
            return new ObjectAddress(0, name, street.getLocation(), arrangeObjectAddressValues(values),
                    false, isMatched, false, false, "Street", street.getId(), null, offset, payloadSize, offset);
        } finally {
            index.getInputStream().popLimit(oldLimit);
        }
    }

    default ObjectAddress loadCityGenerateDbObjectAddress(BinaryMapIndexReaderExt index,
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
        String name = city.getName(lang);
        return new ObjectAddress(0, name, city.getLocation(), arrangeObjectAddressValues(values),
                false, true, false, false, type, city.getId(), null, offset, payloadSize, offset);
    }

    default ObjectAddress loadStreetGenerateDbObjectAddress(BinaryMapIndexReaderExt index,
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
            String name = street.getName(lang);
            return new ObjectAddress(0, name, street.getLocation(), arrangeObjectAddressValues(values),
                    false, true, false, false, "Street", street.getId(), null, offset, payloadSize, offset);
        } finally {
            index.getInputStream().popLimit(oldLimit);
        }
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
                    InspectorService.skipUnknownField(codedIS, tagWithType);
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
                    InspectorService.skipUnknownField(codedIS, tagWithType);
                    break;
            }
        }
    }

    default boolean matchesLegacyCity(City city, CollatorStringMatcher matcher) {
        if (city == null || matcher == null) {
            return false;
        }
        boolean matches = matcher.matches(city.getName());
        if (!matches) {
            for (String name : city.getOtherNames()) {
                matches = matcher.matches(name);
                if (matches) {
                    break;
                }
            }
        }
        return matches || matchesAddressAttributeValues(city, matcher);
    }

    default boolean matchesAddressAttributeValues(MapObject object, CollatorStringMatcher matcher) {
        if (object == null || matcher == null) {
            return false;
        }
        Map<String, String> values = object.getNamesMap(true);
        if (values == null || values.isEmpty()) {
            return false;
        }
        for (Map.Entry<String, String> entry : values.entrySet()) {
            String key = entry.getKey();
            if (Amenity.NAME.equals(key) || "en".equals(key) || (key != null && key.startsWith("name:"))) {
                continue;
            }
            if ("place".equals(key) || isTagIndexedForSearchAsName(key) || isTagIndexedForSearchAsId(key)) {
                if (matcher.matches(entry.getValue())) {
                    return true;
                }
            }
        }
        return false;
    }

    default boolean matchesLegacyStreet(Street street, CollatorStringMatcher matcher) {
        if (street == null || matcher == null) {
            return false;
        }
        boolean matches = matcher.matches(street.getName());
        if (!matches) {
            for (String name : street.getOtherNames()) {
                matches = matcher.matches(name);
                if (matches) {
                    break;
                }
            }
        }
        return matches || matchesAddressAttributeValues(street, matcher);
    }

    default void collectPoiObjectsByStoredOffsets(BinaryMapIndexReaderExt index,
                                                  Map<BinaryMapPoiReaderAdapter.PoiRegion, Set<Integer>> storedPoiOffsets,
                                                  List<BinaryMapPoiReaderAdapter.PoiRegion> poiRegions,
                                                  List<ObjectAddress> results,
                                                  String lang,
                                                  CollatorStringMatcher matcher) throws IOException {
        for (BinaryMapPoiReaderAdapter.PoiRegion poiRegion : poiRegions) {
            Set<Integer> relativeOffsets = storedPoiOffsets.get(poiRegion);
            if (relativeOffsets == null || relativeOffsets.isEmpty()) {
                continue;
            }
            index.initCategories(poiRegion);
            Map<Integer, List<TagValuePair>> tagGroups = preloadPoiTagGroups(index.getInputStream(), poiRegion);
            List<Integer> sortedOffsets = new ArrayList<>(relativeOffsets);
            Collections.sort(sortedOffsets);
            for (Integer relativeOffset : sortedOffsets) {
                readPoiObjectsAtShift(index, poiRegion, tagGroups, relativeOffset, results, lang, matcher);
            }
        }
    }

    default Map<BinaryMapPoiReaderAdapter.PoiRegion, Set<Integer>> mapPoiRefs(int[] poiRefs,
                                                                              List<BinaryMapPoiReaderAdapter.PoiRegion> poiRegions) {
        Map<BinaryMapPoiReaderAdapter.PoiRegion, Set<Integer>> offsetsByRegion = new LinkedHashMap<>();
        if (poiRefs == null || poiRefs.length == 0 || poiRegions == null || poiRegions.isEmpty()) {
            return offsetsByRegion;
        }
        for (int poiRef : poiRefs) {
            for (BinaryMapPoiReaderAdapter.PoiRegion poiRegion : poiRegions) {
                if (poiRef < 0 || poiRef >= poiRegion.getLength()) {
                    continue;
                }
                offsetsByRegion.computeIfAbsent(poiRegion, ignored -> new LinkedHashSet<>())
                        .add(poiRef);
            }
        }
        return offsetsByRegion;
    }

    default PoiTokenRefs readPoiTokenRefs(BinaryMapIndexReaderExt index,
                                          int tokenOffset,
                                          int exactSuffixIndex,
                                          boolean isFiltered) throws IOException {
        Set<Integer> offsets = new LinkedHashSet<>();
        List<Integer> atomSizes = new ArrayList<>();
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
                        return new PoiTokenRefs(offsets, atomSizes);
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
                            readPoiTokenAtom(index, matchedSuffixIndexes, offsets, atomSizes, atomLength + computeVarint32Size(atomLength), isFiltered);
                        } finally {
                            index.getInputStream().popLimit(atomOldLimit);
                        }
                        break;
                    default:
                        InspectorService.skipUnknownField(index.getInputStream(), tagWithType);
                        break;
                }
            }
        } finally {
            index.getInputStream().popLimit(oldLimit);
        }
    }

    default void readPoiTokenAtom(BinaryMapIndexReaderExt index,
                                  List<Integer> matchedSuffixIndexes,
                                  Set<Integer> offsets,
                                  List<Integer> atomSizes,
                                  int atomSize,
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
                        atomSizes.add(atomSize);
                    }
                    return;
                case OsmandOdb.OsmAndPoiNameIndexDataAtom.X_FIELD_NUMBER,
                     OsmandOdb.OsmAndPoiNameIndexDataAtom.Y_FIELD_NUMBER,
                     OsmandOdb.OsmAndPoiNameIndexDataAtom.ZOOM_FIELD_NUMBER:
                    index.getInputStream().readUInt32();
                    break;
                case OsmandOdb.OsmAndPoiNameIndexDataAtom.SHIFTTO_FIELD_NUMBER:
                    long value = readInt(index.getInputStream());
                    if (value > Integer.MAX_VALUE) {
                        throw new IllegalStateException();
                    }
                    shift = (int) value;
                    break;
                default:
                    InspectorService.skipUnknownField(index.getInputStream(), tagWithType);
                    break;
            }
        }
    }

    default void readPoiObjectsAtShift(BinaryMapIndexReaderExt index,
                                       BinaryMapPoiReaderAdapter.PoiRegion region,
                                       Map<Integer, List<TagValuePair>> tagGroups,
                                       int relativeOffset,
                                       List<ObjectAddress> results,
                                       String lang,
                                       CollatorStringMatcher matcher) throws IOException {
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
                            RawPoiObject rawPoiObject = readRawPoiObject(index.getInputStream(), x, y, zoom, region, tagGroups);
                            if (rawPoiObject != null) {
                                ObjectAddress objectAddress = toPoiObjectAddress(rawPoiObject, lang);
                                int payloadSize = poiLength + computeVarint32Size(poiLength);
                                int payloadOffset = (int) (index.getInputStream().getTotalBytesRead() - poiLength);
                                boolean isMatched = matchesLegacyPoi(rawPoiObject, matcher);
                                results.add(new ObjectAddress(0, objectAddress.name(), objectAddress.point(), objectAddress.commonTags(), objectAddress.isPoi(), isMatched, false, false, objectAddress.type(), objectAddress.osmId(), objectAddress.osmType(), payloadOffset, payloadSize, (int) (region.getFilePointer() + relativeOffset)));
                            }
                        } finally {
                            index.getInputStream().popLimit(poiOldLimit);
                        }
                        break;
                    default:
                        InspectorService.skipUnknownField(index.getInputStream(), tagWithType);
                        break;
                }
            }
        } finally {
            index.getInputStream().popLimit(oldLimit);
        }
    }

    default List<GenerateDbRawPoiObject> readGenerateDbRawPoiObjectsAtShift(BinaryMapIndexReaderExt index,
                                                                            BinaryMapPoiReaderAdapter.PoiRegion region,
                                                                            Map<Integer, List<TagValuePair>> tagGroups,
                                                                            int relativeOffset) throws IOException {
        List<GenerateDbRawPoiObject> objects = new ArrayList<>();
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
                        return objects;
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
                            RawPoiObject rawPoiObject = readRawPoiObject(index.getInputStream(), x, y, zoom, region, tagGroups);
                            if (rawPoiObject != null) {
                                int payloadSize = poiLength + computeVarint32Size(poiLength);
                                int payloadOffset = (int) (index.getInputStream().getTotalBytesRead() - poiLength);
                                objects.add(new GenerateDbRawPoiObject(rawPoiObject, payloadOffset, payloadSize,
                                        (int) (region.getFilePointer() + relativeOffset)));
                            }
                        } finally {
                            index.getInputStream().popLimit(poiOldLimit);
                        }
                        break;
                    default:
                        InspectorService.skipUnknownField(index.getInputStream(), tagWithType);
                        break;
                }
            }
        } finally {
            index.getInputStream().popLimit(oldLimit);
        }
    }

    default ObjectAddress toGenerateDbPoiObjectAddress(GenerateDbRawPoiObject rawObject, String lang) {
        ObjectAddress objectAddress = toPoiObjectAddress(rawObject.rawPoiObject(), lang);
        return new ObjectAddress(0, objectAddress.name(), objectAddress.point(), objectAddress.commonTags(), objectAddress.isPoi(), true, false, false, objectAddress.type(),
                objectAddress.osmId(), objectAddress.osmType(), rawObject.payloadOffset(), rawObject.payloadSize(),
                rawObject.sourceOffset());
    }

    default ObjectAddress toPoiObjectAddress(RawPoiObject rawPoiObject,
                                             String lang) {
        LatLon location = new LatLon(rawPoiObject.lat, rawPoiObject.lon);
        Map<String, String> values = new LinkedHashMap<>();
        if (!Algorithms.isEmpty(rawPoiObject.name)) {
            values.put(Amenity.NAME, rawPoiObject.name);
        }
        if (!Algorithms.isEmpty(rawPoiObject.nameEn)) {
            values.put("name:en", rawPoiObject.nameEn);
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
            if (!Algorithms.isEmpty(entry.getKey()) && entry.getValue() != null && !entry.getValue().isEmpty()
                    && !rawPoiObject.decodedSubcategories.containsKey(entry.getKey())) {
                values.put(entry.getKey(), String.join("; ", entry.getValue()));
            }
        }
        String displayName = selectPoiDisplayName(rawPoiObject, lang);
        Long osmId = rawPoiObject.id > 0 ? ObfConstants.getOsmIdFromMapObjectId(rawPoiObject.id) : null;
        String osmType = decodePoiOsmType(rawPoiObject.id);
        return new ObjectAddress(0, displayName, location, arrangeObjectAddressValues(values),
                true, false, false, false, "POI", osmId, osmType, 0, 0, 0);
    }

    default boolean matchesLegacyPoi(RawPoiObject rawPoiObject, CollatorStringMatcher matcher) {
        if (rawPoiObject == null || matcher == null) {
            return false;
        }
        if (matcher.matches(safeLowerCase(rawPoiObject.name))
                || matcher.matches(safeLowerCase(getLegacyPoiEnName(rawPoiObject, true)))) {
            return true;
        }
        if (matchesLegacyPoiOtherNames(rawPoiObject, matcher)) {
            return true;
        }
        for (Map.Entry<String, List<String>> entry : rawPoiObject.decodedTextTags.entrySet()) {
            String key = entry.getKey();
            if (isTagIndexedForSearchAsName(key) || isTagIndexedForSearchAsId(key)
                    || isTagIndexedAsSearchRelated(key)) {
                if (matchesAnyLegacyPoiValue(entry.getValue(), matcher, false)) {
                    return true;
                }
            }
        }
        return false;
    }

    default boolean matchesLegacyPoiOtherNames(RawPoiObject rawPoiObject, CollatorStringMatcher matcher) {
        for (Map.Entry<String, List<String>> entry : rawPoiObject.decodedTextTags.entrySet()) {
            String key = entry.getKey();
            if (key == null || (!key.startsWith("name:") && !key.startsWith("name_"))) {
                continue;
            }
            if (matchesAnyLegacyPoiValue(entry.getValue(), matcher, true)) {
                return true;
            }
        }
        return false;
    }

    default boolean matchesAnyLegacyPoiValue(List<String> values, CollatorStringMatcher matcher, boolean lowerCase) {
        if (values == null || values.isEmpty()) {
            return false;
        }
        for (String value : values) {
            String candidate = lowerCase ? safeLowerCase(value) : value;
            if (matcher.matches(candidate)) {
                return true;
            }
        }
        return false;
    }

    default String getLegacyPoiEnName(RawPoiObject rawPoiObject, boolean transliterate) {
        if (!Algorithms.isEmpty(rawPoiObject.nameEn)) {
            return rawPoiObject.nameEn;
        } else if (!Algorithms.isEmpty(rawPoiObject.name) && transliterate) {
            return TransliterationHelper.transliterate(rawPoiObject.name);
        }
        return "";
    }

    default String safeLowerCase(String value) {
        return value == null ? "" : value.toLowerCase(Locale.ROOT);
    }

    default Map<String, String> arrangeObjectAddressValues(Map<String, String> values) {
        if (values == null || values.isEmpty()) {
            return new LinkedHashMap<>();
        }
        Map<String, String> sortedValues = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, String> entry : values.entrySet()) {
            if (entry == null || Algorithms.isEmpty(entry.getKey()) || Algorithms.isEmpty(entry.getValue())) {
                continue;
            }
            String key = entry.getKey().trim();
            if (Amenity.NAME.equals(key) || "name".equalsIgnoreCase(key)) {
                continue;
            }
            sortedValues.put(key, entry.getValue());
        }
        return new LinkedHashMap<>(sortedValues);
    }

    default String decodePoiOsmType(long rawPoiObjectId) {
        if (rawPoiObjectId <= 0) {
            return null;
        }
        Amenity amenity = new Amenity();
        amenity.setId(rawPoiObjectId);
        net.osmand.osm.edit.Entity.EntityType entityType = ObfConstants.getOsmEntityType(amenity);
        return entityType == null ? null : entityType.name().toLowerCase(Locale.US);
    }

    default String selectPoiDisplayName(RawPoiObject rawPoiObject, String lang) {
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

    default Map<String, String> buildMapObjectValues(MapObject mapObject, String lang) {
        Map<String, String> values = new LinkedHashMap<>();
        if (mapObject == null) {
            return values;
        }
        if (mapObject instanceof City city && city.getType() != null) {
            values.put("place", City.CityType.valueToString(city.getType()));
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

    default long readInt(CodedInputStream codedIS) throws IOException {
        long value = InspectorService.readUnsignedByte(codedIS);
        boolean eightBytes = value > 0x7f;
        if (eightBytes) {
            value = value & 0x7f;
        }
        value = (value << 8) + InspectorService.readUnsignedByte(codedIS);
        value = (value << 8) + InspectorService.readUnsignedByte(codedIS);
        value = (value << 8) + InspectorService.readUnsignedByte(codedIS);
        if (eightBytes) {
            value = (value << 8) + InspectorService.readUnsignedByte(codedIS);
            value = (value << 8) + InspectorService.readUnsignedByte(codedIS);
            value = (value << 8) + InspectorService.readUnsignedByte(codedIS);
            value = (value << 8) + InspectorService.readUnsignedByte(codedIS);
        }
        return value;
    }

    default boolean matchesSuffixMask(int maskIndex, int mask, List<Integer> matchedSuffixIndexes) {
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

    default boolean matchesObjectAddressFilter(ObjectAddress objectAddress,
                                               Pattern pattern,
                                               Pattern normalizedPattern) {
        return matchesObjectAddressText(objectAddress, pattern, normalizedPattern);
    }

    default boolean matchesObjectAddressText(ObjectAddress objectAddress,
                                             Pattern pattern,
                                             Pattern normalizedPattern) {
        if (objectAddress == null) {
            return false;
        }
        if (matchesPattern(objectAddress.name(), pattern, normalizedPattern)) {
            return true;
        }
        Map<String, String> values = objectAddress.commonTags();
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

    default List<ObjectAddress> filterMatchedObjects(List<ObjectAddress> results) {
        if (results == null || results.isEmpty()) {
            return new ArrayList<>();
        }
        List<ObjectAddress> matchedResults = new ArrayList<>();
        for (ObjectAddress objectAddress : results) {
            if (objectAddress != null && objectAddress.isMatched()) {
                matchedResults.add(objectAddress);
            }
        }
        return matchedResults;
    }

    default List<ObjectAddress> markInvalidPoiAtoms(List<ObjectAddress> results) {
        if (results == null || results.isEmpty()) {
            return new ArrayList<>();
        }
        Map<Integer, int[]> poiAtomStats = new HashMap<>();
        for (ObjectAddress objectAddress : results) {
            if (objectAddress != null && objectAddress.isPoi()) {
                int[] stats = poiAtomStats.computeIfAbsent(objectAddress.sourceOffset(), ignored -> new int[2]);
                stats[0]++;
                if (objectAddress.isMatched()) {
                    stats[1]++;
                }
            }
        }
        Set<Integer> invalidPoiAtomOffsets = new HashSet<>();
        for (Map.Entry<Integer, int[]> entry : poiAtomStats.entrySet()) {
            int[] stats = entry.getValue();
            if (stats[0] > 0 && stats[1] == 0) {
                invalidPoiAtomOffsets.add(entry.getKey());
            }
        }
        List<ObjectAddress> markedResults = new ArrayList<>(results.size());
        for (ObjectAddress objectAddress : results) {
            if (objectAddress == null) {
                continue;
            }
            boolean isInvalidAtom = objectAddress.isPoi() && invalidPoiAtomOffsets.contains(objectAddress.sourceOffset());
            markedResults.add(new ObjectAddress(objectAddress.sequenceId(), objectAddress.name(), objectAddress.point(), objectAddress.commonTags(), objectAddress.isPoi(), objectAddress.isMatched(), isInvalidAtom, objectAddress.isAlone(), objectAddress.type(),
                    objectAddress.osmId(), objectAddress.osmType(), objectAddress.payloadOffset(),
                    objectAddress.payloadSize(), objectAddress.sourceOffset()));
        }
        return markedResults;
    }

    default List<ObjectAddress> markAloneObjects(List<ObjectAddress> results, IndexToken token) {
        if (results == null || results.isEmpty()) {
            return new ArrayList<>();
        }
        List<ObjectAddress> markedResults = new ArrayList<>(results.size());
        for (ObjectAddress objectAddress : results) {
            if (objectAddress == null) {
                continue;
            }
            boolean isAlone = isAloneTokenObject(objectAddress, token == null ? null : token.name());
            markedResults.add(new ObjectAddress(objectAddress.sequenceId(), objectAddress.name(), objectAddress.point(), objectAddress.commonTags(), objectAddress.isPoi(), objectAddress.isMatched(), objectAddress.isInvalidAtom(), isAlone, objectAddress.type(),
                    objectAddress.osmId(), objectAddress.osmType(), objectAddress.payloadOffset(),
                    objectAddress.payloadSize(), objectAddress.sourceOffset()));
        }
        return markedResults;
    }

    default List<ObjectAddress> assignObjectSequenceIds(List<ObjectAddress> results) {
        if (results == null || results.isEmpty()) {
            return List.of();
        }
        List<ObjectAddress> orderedResults = new ArrayList<>(results);
        orderedResults.sort(Comparator
                .comparingInt((ObjectAddress object) -> object == null ? Integer.MAX_VALUE : object.sourceOffset())
                .thenComparingInt(object -> object == null ? Integer.MAX_VALUE : object.payloadOffset()));
        List<ObjectAddress> numberedResults = new ArrayList<>(orderedResults.size());
        int sequenceId = 1;
        for (ObjectAddress objectAddress : orderedResults) {
            if (objectAddress == null) {
                continue;
            }
            numberedResults.add(new ObjectAddress(sequenceId++, objectAddress.name(), objectAddress.point(), objectAddress.commonTags(), objectAddress.isPoi(), objectAddress.isMatched(), objectAddress.isInvalidAtom(), objectAddress.isAlone(), objectAddress.type(),
                    objectAddress.osmId(), objectAddress.osmType(), objectAddress.payloadOffset(),
                    objectAddress.payloadSize(), objectAddress.sourceOffset()));
        }
        return numberedResults;
    }

    default int countInvalidPoiAtoms(List<ObjectAddress> results) {
        if (results == null || results.isEmpty()) {
            return 0;
        }
        Set<Integer> invalidPoiAtomOffsets = new HashSet<>();
        for (ObjectAddress objectAddress : results) {
            if (objectAddress != null && objectAddress.isInvalidAtom()) {
                invalidPoiAtomOffsets.add(objectAddress.sourceOffset());
            }
        }
        return invalidPoiAtomOffsets.size();
    }

    default ObjectAddressPage getObjects(String obf,
                                         String lang,
                                         IndexToken token,
                                         String regExp,
                                         int pageToShow,
                                         int pageSizeLimit,
                                         String sortBy,
                                         String sortOrder,
                                         boolean isFiltered,
                                         boolean invalidOnly,
                                         String objectType) {
        List<ObjectAddress> results = new ArrayList<>();
        if (token == null) {
            return new ObjectAddressPage(List.of(), Math.max(pageToShow, 0), Math.max(pageSizeLimit, 1), 0, 0, new int[7], new int[12], 0, 0);
        }
        final Pattern objectPattern;
        final Pattern normalizedObjectPattern;
        final boolean hasAnyFilter;
        final int safePage = Math.max(pageToShow, 0);
        final int safeSize = Math.max(pageSizeLimit, 1);
        try {
            objectPattern = Algorithms.isEmpty(regExp) ? null : Pattern.compile(regExp, Pattern.CASE_INSENSITIVE);
            normalizedObjectPattern = objectPattern == null ? null : compileNormalizedPattern(regExp);
            hasAnyFilter = objectPattern != null;
        } catch (PatternSyntaxException e) {
            return new ObjectAddressPage(List.of(), Math.max(pageToShow, 0), Math.max(pageSizeLimit, 1), 0, 0, new int[7], new int[12], 0, 0);
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
                AddressRef[] addressRefs = token.addressRefs();
                int[] poiRefs = token.poiRefs();
                boolean hasAddressRefs = addressRefs != null && addressRefs.length > 0;
                boolean hasPoiRefs = poiRefs != null && poiRefs.length > 0;
                if (!hasAddressRefs && !hasPoiRefs) {
                    return new ObjectAddressPage(List.of(), safePage, safeSize, 0, 0, new int[7], new int[12], 0, 0);
                }
                int allPoiAtomsSize = sumIntValues(token.poiAtomSizes());
                int allAddressAtomsSize = sumAddressAtomSizes(addressRefs);
                int allAtomsSize = safeMetricInt((long) allPoiAtomsSize + allAddressAtomsSize);
                Map<BinaryMapPoiReaderAdapter.PoiRegion, Set<Integer>> storedPoiOffsets = mapPoiRefs(poiRefs, poiRegions);
                CollatorStringMatcher matcher = new CollatorStringMatcher(token.name(),
                        CollatorStringMatcher.StringMatcherMode.CHECK_EQUALS_FROM_SPACE);
                if (hasAddressRefs) {
                    for (BinaryMapAddressReaderAdapter.AddressRegion addressRegion : addressRegions) {
                        collectAddressObjects(index, addressRegion, addressRefs, results, lang, matcher);
                    }
                }
                if (hasPoiRefs) {
                    collectPoiObjectsByStoredOffsets(index, storedPoiOffsets, poiRegions, results, lang, matcher);
                }
                results = assignObjectSequenceIds(results);
                results = markInvalidPoiAtoms(results);
                results = markAloneObjects(results, token);
                ObjectAddressStats allStats = calculateObjectAddressStats(index, results, poiRegions, null);
                ObjectAddressStats allPoiStats = calculateObjectAddressStats(index, results, poiRegions, true);
                ObjectAddressStats allAddressStats = calculateObjectAddressStats(index, results, poiRegions, false);
                ObjectAddressStats aloneStats = calculateAloneTokenStats(index, results, poiRegions, token, null);
                ObjectAddressStats alonePoiStats = calculateAloneTokenStats(index, results, poiRegions, token, true);
                ObjectAddressStats aloneAddressStats = calculateAloneTokenStats(index, results, poiRegions, token, false);
                List<ObjectAddress> matchedResults = filterMatchedObjects(results);
                int exactPoiAtomsSize = sumExactPoiAtomSizes(token, poiRegions, matchedResults);
                int exactAddressAtomsSize = sumExactAddressAtomSizes(addressRefs, matchedResults);
                int exactAtomsSize = safeMetricInt((long) exactPoiAtomsSize + exactAddressAtomsSize);
                ObjectAddressStats exactStats = calculateObjectAddressStats(index, matchedResults, poiRegions, null);
                ObjectAddressStats exactPoiStats = calculateObjectAddressStats(index, matchedResults, poiRegions, true);
                ObjectAddressStats exactAddressStats = calculateObjectAddressStats(index, matchedResults, poiRegions, false);
                int invalidPoiAtomsCount = countInvalidPoiAtoms(results);
                List<ObjectAddress> displayResults = invalidOnly ? new ArrayList<>(results) : isFiltered ? matchedResults : new ArrayList<>(results);
                if (invalidOnly) {
                    displayResults.removeIf(objectAddress -> objectAddress == null || !objectAddress.isInvalidAtom());
                }
                Boolean objectTypeFilter = parseObjectTypeFilter(objectType);
                if (objectTypeFilter != null) {
                    displayResults.removeIf(objectAddress -> objectAddress == null || objectAddress.isPoi() != objectTypeFilter);
                }
                if (hasAnyFilter) {
                    displayResults.removeIf(objectAddress -> !matchesObjectAddressFilter(objectAddress,
                            objectPattern,
                            normalizedObjectPattern));
                }
                displayResults.sort(buildObjectAddressComparator(sortBy, sortOrder));
                long totalElements = displayResults.size();
                int totalPages = totalElements == 0 ? 0 : (int) ((totalElements + safeSize - 1) / safeSize);
                int fromIndex = Math.min(safePage * safeSize, displayResults.size());
                int toIndex = Math.min(fromIndex + safeSize, displayResults.size());
                List<ObjectAddress> pageContent = fromIndex >= toIndex
                        ? List.of()
                        : new ArrayList<>(displayResults.subList(fromIndex, toIndex));
                int[] countMetrics = new int[]{
                        allStats.count(),
                        exactStats.count(),
                        allPoiStats.count(),
                        exactPoiStats.count(),
                        allAddressStats.count(),
                        exactAddressStats.count(),
                        invalidPoiAtomsCount,
                        aloneStats.count(),
                        alonePoiStats.count(),
                        aloneAddressStats.count()
                };
                int[] sizeMetrics = new int[]{
                        allAtomsSize,
                        allPoiAtomsSize,
                        allAddressAtomsSize,
                        allStats.size(),
                        exactStats.size(),
                        allPoiStats.size(),
                        exactPoiStats.size(),
                        allAddressStats.size(),
                        exactAddressStats.size(),
                        exactAtomsSize,
                        exactPoiAtomsSize,
                        exactAddressAtomsSize,
                        aloneStats.size(),
                        alonePoiStats.size(),
                        aloneAddressStats.size()
                };
                ObjectAddressStats displayedAloneStats = objectTypeFilter == null ? aloneStats : objectTypeFilter ? alonePoiStats : aloneAddressStats;
                return new ObjectAddressPage(pageContent,
                        safePage,
                        safeSize,
                        totalElements,
                        totalPages,
                        countMetrics,
                        sizeMetrics,
                        displayedAloneStats.count(),
                        displayedAloneStats.size());
            } finally {
                index.close();
            }
        } catch (Exception e) {
            getLogger().error("Failed to read OBF objects {} for token {}", file, token, e);
            throw new RuntimeException("Failed to read OBF objects: " + e.getMessage(), e);
        }
    }

    default ObjectAddressPage getObjects(List<String> obfs,
                                         String lang,
                                         IndexToken token,
                                         String regExp,
                                         int pageToShow,
                                         int pageSizeLimit,
                                         String sortBy,
                                         String sortOrder,
                                         boolean isFiltered,
                                         boolean invalidOnly,
                                         String objectType) {
        final int safePage = Math.max(pageToShow, 0);
        final int safeSize = Math.max(pageSizeLimit, 1);
        if (token == null) {
            return new ObjectAddressPage(List.of(), safePage, safeSize, 0, 0, new int[10], new int[15], 0, 0);
        }
        List<ObjectAddress> content = new ArrayList<>();
        int[] countMetrics = new int[10];
        int[] sizeMetrics = new int[15];
        int aloneCount = 0;
        int aloneSize = 0;
        List<String> targetObfs = token.obf() == null || token.obf().isBlank() ? obfs : List.of(token.obf());
        if (targetObfs != null) {
            for (String obf : targetObfs) {
                if (Algorithms.isEmpty(obf)) {
                    continue;
                }
                IndexToken obfToken = findIndexTokenByName(obf, token.name());
                if (obfToken == null) {
                    continue;
                }
                ObjectAddressPage page = getObjects(obf, lang, obfToken, regExp, 0, Integer.MAX_VALUE, sortBy, sortOrder, isFiltered, invalidOnly, objectType);
                if (page == null) {
                    continue;
                }
                if (page.content() != null) {
                    for (ObjectAddress objectAddress : page.content()) {
                        content.add(withObf(objectAddress, obf));
                    }
                }
                addMetrics(countMetrics, page.countMetrics());
                addMetrics(sizeMetrics, page.sizeMetrics());
                aloneCount += page.aloneCount();
                aloneSize += page.aloneSize();
            }
        }
        content.sort(buildObjectAddressComparator(sortBy, sortOrder));
        long totalElements = content.size();
        int totalPages = totalElements == 0 ? 0 : (int) ((totalElements + safeSize - 1) / safeSize);
        int fromIndex = Math.min(safePage * safeSize, content.size());
        int toIndex = Math.min(fromIndex + safeSize, content.size());
        List<ObjectAddress> pageContent = fromIndex >= toIndex
                ? List.of()
                : new ArrayList<>(content.subList(fromIndex, toIndex));
        return new ObjectAddressPage(pageContent, safePage, safeSize, totalElements, totalPages, countMetrics, sizeMetrics, aloneCount, aloneSize);
    }

    default IndexToken findIndexTokenByName(String obf, String tokenName) {
        if (Algorithms.isEmpty(obf) || tokenName == null) {
            return null;
        }
        File file = new File(obf);
        try {
            for (IndexToken token : getCachedOrLoadIndexTokens(file)) {
                if (token != null && tokenName.equalsIgnoreCase(token.name())) {
                    return token;
                }
            }
            return null;
        } catch (IOException e) {
            getLogger().error("Failed to read OBF index {}", file, e);
            throw new RuntimeException("Failed to read OBF index: " + e.getMessage(), e);
        }
    }

    default void addMetrics(int[] target, int[] source) {
        if (target == null || source == null) {
            return;
        }
        for (int i = 0; i < Math.min(target.length, source.length); i++) {
            target[i] = safeMetricInt((long) target[i] + source[i]);
        }
    }

    default ObjectAddress withObf(ObjectAddress objectAddress, String obf) {
        if (objectAddress == null) {
            return null;
        }
        return new ObjectAddress(objectAddress.sequenceId(), objectAddress.name(), objectAddress.point(),
                objectAddress.commonTags(), objectAddress.isPoi(), objectAddress.isMatched(),
                objectAddress.isInvalidAtom(), objectAddress.isAlone(), objectAddress.type(), objectAddress.osmId(),
                objectAddress.osmType(), objectAddress.payloadOffset(), objectAddress.payloadSize(),
                objectAddress.sourceOffset(), obf);
    }

    default Boolean parseObjectTypeFilter(String objectType) {
        if (Algorithms.isEmpty(objectType)) {
            return null;
        }
        return switch (objectType.trim().toLowerCase(Locale.ROOT)) {
            case "poi" -> Boolean.TRUE;
            case "address", "addr" -> Boolean.FALSE;
            default -> null;
        };
    }

    default Comparator<ObjectAddress> buildObjectAddressComparator(String sortBy, String sortOrder) {
        String normalizedSortBy = Algorithms.isEmpty(sortBy) ? "sequenceid" : sortBy.trim().toLowerCase(Locale.ROOT);
        Comparator<ObjectAddress> comparator = switch (normalizedSortBy) {
            case "#", "sequence", "sequenceid" ->
                    Comparator.comparingInt(object -> object == null ? Integer.MAX_VALUE : object.sequenceId());
            case "type" ->
                    Comparator.comparing(object -> object == null || object.type() == null ? "" : object.type(), String.CASE_INSENSITIVE_ORDER);
            case "osmid" ->
                    Comparator.comparingLong(object -> object == null || object.osmId() == null ? Long.MAX_VALUE : object.osmId());
            case "matched" -> Comparator.comparingInt(object -> object != null && object.isMatched() ? 1 : 0);
            case "alone", "isalone" -> Comparator.comparingInt(object -> object != null && object.isAlone() ? 1 : 0);
            default ->
                    Comparator.comparing(object -> object == null || object.name() == null ? "" : object.name(), String.CASE_INSENSITIVE_ORDER);
        };
        comparator = comparator.thenComparingInt(object -> object == null ? Integer.MAX_VALUE : object.sequenceId())
                .thenComparing(object -> object == null || object.name() == null ? "" : object.name(), String.CASE_INSENSITIVE_ORDER);
        return "desc".equalsIgnoreCase(sortOrder) ? comparator.reversed() : comparator;
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

}
