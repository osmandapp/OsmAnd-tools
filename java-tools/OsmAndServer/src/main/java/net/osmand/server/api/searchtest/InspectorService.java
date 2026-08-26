package net.osmand.server.api.searchtest;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import net.osmand.ResultMatcher;
import net.osmand.binary.*;
import net.osmand.binary.BinaryMapIndexReader.TagValuePair;
import net.osmand.data.*;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import net.osmand.util.SearchAlgorithms;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
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

    record AddressTokenRefs(List<Integer> cityOffsets, List<Integer> streetOffsets, List<Integer> streetCityOffsets,
                            List<AddressRef> addressRefs, Set<Integer> metricSuffixIndexes,
                            Set<String> metricIntegerSuffixes, Set<String> metricExtraSuffixes) {
        AddressTokenRefs() {
            this(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>(),
                    new LinkedHashSet<>(), new LinkedHashSet<>(), new LinkedHashSet<>());
        }
    }


    record AddressTokenRef(IndexToken token, AddressRef ref) {
    }


    record GenerateDbRawPoiObject(RawPoiObject rawPoiObject, int payloadOffset, int payloadSize, int sourceOffset) {
    }

    record CommonSuffix(String value, int matched, int nonindexed) {
    }

    record NameIndexTableInfo(long tableContentStart, Map<String, Integer> prefixOffsets) {
    }

    record NameIndexSuffixInfo(List<String> suffixDictionary, List<CommonSuffix> commonSuffixes,
                               Set<String> integerSuffixes, Set<String> extraSuffixes, int otherWordsCount) {
    }



    default void collectAddressObjects(BinaryMapIndexReaderExt index,
                                       BinaryMapAddressReaderAdapter.AddressRegion region,
                                       AddressRef[] addressRefs,
                                       List<ObjectAddress> results,
                                       String lang) throws IOException {
        List<AddressRef> refs = addressRefs == null ? List.of() : Arrays.asList(addressRefs);
        for (AddressRef ref : refs) {
            if (ref == null || ref.typeIndex() >= BinaryMapAddressReaderAdapter.CityBlocks.STREET_TYPE.index
                    || !isOffsetWithinPart(ref.objectOffset(), region)) {
                continue;
            }
            int offset = ref.objectOffset();
            ObjectAddress objectAddress = loadCityObjectAddress(index, region, offset, lang);
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
            ObjectAddress objectAddress = loadStreetObjectAddress(index, region, streetOffset, city, lang);
            if (objectAddress != null) {
                results.add(objectAddress);
            }
        }
    }

    default IndexTokenPage getIndex(String obf, String prefix, int pageToShow, int pageSizeLimit, String sortBy, String sortOrder) {
        return getIndex(obf, prefix, pageToShow, pageSizeLimit, sortBy, sortOrder, null);
    }

    default IndexTokenPage getIndex(String obf, String prefix, int pageToShow, int pageSizeLimit, String sortBy, String sortOrder,
                                    String objectType) {
        File file = new File(obf);
        Pattern prefixPattern = compileIndexPrefixPattern(prefix);
        final int safePage = Math.max(pageToShow, 0);
        final int safeSize = Math.max(1, Math.min(pageSizeLimit, 100));
        Boolean objectTypeFilter = parseObjectTypeFilter(objectType);
        long startedNs = System.nanoTime();
        try {
            long loadStartedNs = System.nanoTime();
            List<IndexToken> allTokens = getCachedOrLoadIndexTokens(file);
            long loadNs = System.nanoTime() - loadStartedNs;
            long filterStartedNs = System.nanoTime();
            List<IndexToken> results = new ArrayList<>();
            if (prefixPattern == null) {
                for (IndexToken token : allTokens) {
                    if (matchesIndexTokenObjectType(token, objectTypeFilter)) {
                        results.add(token);
                    }
                }
            } else {
                for (IndexToken token : allTokens) {
                    if (prefixPattern.matcher(token.name()).find() && matchesIndexTokenObjectType(token, objectTypeFilter)) {
                        results.add(token);
                    }
                }
            }
            long filterNs = System.nanoTime() - filterStartedNs;
            long summaryStartedNs = System.nanoTime();
            IndexTokenSummary summary = buildIndexTokenSummary(results);
            long summaryNs = System.nanoTime() - summaryStartedNs;
            long sortStartedNs = System.nanoTime();
            results.sort(buildIndexTokenComparator(sortBy, sortOrder));
            long sortNs = System.nanoTime() - sortStartedNs;
            long pageStartedNs = System.nanoTime();
            long totalElements = results.size();
            int totalPages = totalElements == 0 ? 0 : (int) ((totalElements + safeSize - 1) / safeSize);
            int fromIndex = Math.min(safePage * safeSize, results.size());
            int toIndex = Math.min(fromIndex + safeSize, results.size());
            List<IndexToken> pageContent = fromIndex >= toIndex
                    ? List.of()
                    : compactIndexTokens(results.subList(fromIndex, toIndex));
            long pageNs = System.nanoTime() - pageStartedNs;
            getLogger().info("getIndex obf={} prefix={} objectType={} page={}/{} size={} tokens={} filtered={} content={} timingsMs load={} filter={} summary={} sort={} page={} total={}",
                    file.getName(), prefix, objectType, safePage, totalPages, safeSize, allTokens.size(), totalElements, pageContent.size(),
                    elapsedMs(loadNs), elapsedMs(filterNs), elapsedMs(summaryNs), elapsedMs(sortNs), elapsedMs(pageNs),
                    elapsedMs(System.nanoTime() - startedNs));
            return new IndexTokenPage(pageContent, safePage, safeSize, totalElements, totalPages, summary);
        } catch (Exception e) {
            getLogger().error("Failed to read OBF index {}", file, e);
            throw new RuntimeException("Failed to read OBF index: " + e.getMessage(), e);
        }
    }

    default IndexTokenPage getIndex(List<String> obfs, String prefix, int pageToShow, int pageSizeLimit, String sortBy, String sortOrder) {
        return getIndex(obfs, prefix, pageToShow, pageSizeLimit, sortBy, sortOrder, null);
    }

    default IndexTokenPage getIndex(List<String> obfs, String prefix, int pageToShow, int pageSizeLimit, String sortBy, String sortOrder,
                                    String objectType) {
        Pattern prefixPattern = compileIndexPrefixPattern(prefix);
        final int safePage = Math.max(pageToShow, 0);
        final int safeSize = Math.max(1, Math.min(pageSizeLimit, 100));
        Boolean objectTypeFilter = parseObjectTypeFilter(objectType);
        long startedNs = System.nanoTime();
        try {
            long mergeStartedNs = System.nanoTime();
            Map<String, IndexToken> mergedByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            int sourceTokens = 0;
            int loadedObfs = 0;
            if (obfs != null) {
                for (String obf : obfs) {
                    if (Algorithms.isEmpty(obf)) {
                        continue;
                    }
                    long obfStartedNs = System.nanoTime();
                    List<IndexToken> allTokens = getCachedOrLoadIndexTokens(new File(obf));
                    sourceTokens += allTokens.size();
                    loadedObfs++;
                    for (IndexToken token : allTokens) {
                        if (token == null || token.name() == null) {
                            continue;
                        }
                        if ((prefixPattern == null || prefixPattern.matcher(token.name()).find())
                                && matchesIndexTokenObjectType(token, objectTypeFilter)) {
                            mergedByName.merge(token.name(), token, this::mergeIndexTokens);
                        }
                    }
                    getLogger().info("getIndex obfPart={} prefix={} objectType={} sourceTokens={} mergedSoFar={} elapsedMs={}",
                            new File(obf).getName(), prefix, objectType, allTokens.size(), mergedByName.size(),
                            elapsedMs(System.nanoTime() - obfStartedNs));
                }
            }
            long mergeNs = System.nanoTime() - mergeStartedNs;
            long listStartedNs = System.nanoTime();
            List<IndexToken> results = new ArrayList<>(mergedByName.values());
            long listNs = System.nanoTime() - listStartedNs;
            long summaryStartedNs = System.nanoTime();
            IndexTokenSummary summary = buildIndexTokenSummary(results);
            long summaryNs = System.nanoTime() - summaryStartedNs;
            long sortStartedNs = System.nanoTime();
            results.sort(buildIndexTokenComparator(sortBy, sortOrder));
            long sortNs = System.nanoTime() - sortStartedNs;
            long pageStartedNs = System.nanoTime();
            long totalElements = results.size();
            int totalPages = totalElements == 0 ? 0 : (int) ((totalElements + safeSize - 1) / safeSize);
            int fromIndex = Math.min(safePage * safeSize, results.size());
            int toIndex = Math.min(fromIndex + safeSize, results.size());
            List<IndexToken> pageContent = fromIndex >= toIndex
                    ? List.of()
                    : compactIndexTokens(results.subList(fromIndex, toIndex));
            long pageNs = System.nanoTime() - pageStartedNs;
            getLogger().info("getIndex obfs={} prefix={} objectType={} page={}/{} size={} sourceTokens={} merged={} content={} timingsMs merge={} list={} summary={} sort={} page={} total={}",
                    loadedObfs, prefix, objectType, safePage, totalPages, safeSize, sourceTokens, totalElements, pageContent.size(),
                    elapsedMs(mergeNs), elapsedMs(listNs), elapsedMs(summaryNs), elapsedMs(sortNs), elapsedMs(pageNs),
                    elapsedMs(System.nanoTime() - startedNs));
            return new IndexTokenPage(pageContent, safePage, safeSize, totalElements, totalPages, summary);
        } catch (Exception e) {
            getLogger().error("Failed to read OBF indexes {}", obfs, e);
            throw new RuntimeException("Failed to read OBF indexes: " + e.getMessage(), e);
        }
    }

    default IndexTokenPage getIndexSuffixSort(String obf, String prefix, int pageToShow, int pageSizeLimit, String sortBy, String sortOrder,
                                              String objectType) {
        return getIndexSuffixSort(List.of(obf), prefix, pageToShow, pageSizeLimit, sortBy, sortOrder, objectType);
    }

    default IndexTokenPage getIndexSuffixSort(List<String> obfs, String prefix, int pageToShow, int pageSizeLimit, String sortBy, String sortOrder,
                                              String objectType) {
        String suffixKey = normalizeIndexSuffixSortKey(sortBy);
        if (suffixKey == null) {
            return getIndex(obfs, prefix, pageToShow, pageSizeLimit, sortBy, sortOrder, objectType);
        }
        Pattern prefixPattern = compileIndexPrefixPattern(prefix);
        final int safePage = Math.max(pageToShow, 0);
        final int safeSize = Math.max(1, Math.min(pageSizeLimit, 100));
        Boolean objectTypeFilter = parseObjectTypeFilter(objectType);
        long startedNs = System.nanoTime();
        try {
            Map<String, IndexToken> mergedByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            int sourceTokens = 0;
            if (obfs != null) {
                for (String obf : obfs) {
                    if (Algorithms.isEmpty(obf)) {
                        continue;
                    }
                    List<IndexToken> allTokens = getCachedOrLoadIndexTokens(new File(obf));
                    sourceTokens += allTokens.size();
                    for (IndexToken token : allTokens) {
                        if (token == null || token.name() == null) {
                            continue;
                        }
                        if ((prefixPattern == null || prefixPattern.matcher(token.name()).find())
                                && matchesIndexTokenObjectType(token, objectTypeFilter)) {
                            mergedByName.merge(token.name(), token, this::mergeIndexTokens);
                        }
                    }
                }
            }
            List<IndexToken> results = new ArrayList<>(mergedByName.values());
            IndexTokenSummary summary = buildIndexTokenSummary(results);
            Comparator<IndexToken> comparator = Comparator
                    .comparingInt((IndexToken token) -> getIndexTokenSuffixMetricCount(token, suffixKey, objectTypeFilter))
                    .thenComparing(token -> token == null || token.name() == null ? "" : token.name(), String.CASE_INSENSITIVE_ORDER);
            if ("desc".equalsIgnoreCase(sortOrder)) {
                comparator = comparator.reversed();
            }
            results.sort(comparator);
            long totalElements = results.size();
            int totalPages = totalElements == 0 ? 0 : (int) ((totalElements + safeSize - 1) / safeSize);
            int fromIndex = Math.min(safePage * safeSize, results.size());
            int toIndex = Math.min(fromIndex + safeSize, results.size());
            List<IndexToken> pageContent = fromIndex >= toIndex
                    ? List.of()
                    : compactIndexTokens(results.subList(fromIndex, toIndex));
            getLogger().info("getIndexSuffixSort obfs={} prefix={} objectType={} sortBy={} sortOrder={} page={}/{} size={} sourceTokens={} filtered={} content={} totalMs={}",
                    obfs == null ? 0 : obfs.size(), prefix, objectType, suffixKey, sortOrder, safePage, totalPages, safeSize,
                    sourceTokens, totalElements, pageContent.size(), elapsedMs(System.nanoTime() - startedNs));
            return new IndexTokenPage(pageContent, safePage, safeSize, totalElements, totalPages, summary);
        } catch (Exception e) {
            getLogger().error("Failed to sort OBF indexes by suffix {}", obfs, e);
            throw new RuntimeException("Failed to sort OBF indexes by suffix: " + e.getMessage(), e);
        }
    }

    default String normalizeIndexSuffixSortKey(String sortBy) {
        if (Algorithms.isEmpty(sortBy)) {
            return null;
        }
        String key = sortBy.trim().toLowerCase(Locale.ROOT);
        return switch (key) {
            case "dict", "suffixcommon", "suffixpart", "integer", "extra", "other" -> key;
            default -> null;
        };
    }

    default IndexSuffixResponse getIndexSuffix(String obf, IndexSuffixRequest request) {
        return getIndexSuffix(List.of(obf), request);
    }

    default IndexSuffixResponse getIndexSuffix(List<String> obfs, IndexSuffixRequest request) {
        long startedNs = System.nanoTime();
        if (request == null || request.tokens() == null || request.tokens().isEmpty()) {
            return new IndexSuffixResponse(List.of(), List.of());
        }
        Boolean objectTypeFilter = parseObjectTypeFilter(request.objectType());
        String key = Algorithms.isEmpty(request.key()) ? null : request.key().trim();
        boolean loadValues = key != null && !key.isEmpty();
        long lookupStartedNs = System.nanoTime();
        Map<String, IndexToken> tokensByName = new LinkedHashMap<>();
        Set<String> requestedNames = new LinkedHashSet<>();
        for (String tokenName : request.tokens()) {
            if (!Algorithms.isEmpty(tokenName)) {
                requestedNames.add(tokenName);
            }
        }
        if (requestedNames.isEmpty()) {
            return new IndexSuffixResponse(List.of(), List.of());
        }
        if (obfs != null) {
            for (String obf : obfs) {
                if (Algorithms.isEmpty(obf)) {
                    continue;
                }
                for (String tokenName : requestedNames) {
                    IndexToken token = findIndexTokenByName(obf, tokenName);
                    if (token != null && matchesIndexTokenObjectType(token, objectTypeFilter)) {
                        tokensByName.merge(tokenName, token, this::mergeIndexTokens);
                    }
                }
            }
        }
        long lookupNs = System.nanoTime() - lookupStartedNs;
        if (loadValues) {
            long resolveStartedNs = System.nanoTime();
            List<String> values = new ArrayList<>();
            for (IndexToken token : tokensByName.values()) {
                values.addAll(getIndexTokenSuffixValues(obfs, token, key, objectTypeFilter));
            }
            List<String> resolvedValues = uniqueSortedSuffixValues(values);
            long resolveNs = System.nanoTime() - resolveStartedNs;
            getLogger().info("getIndexSuffix values obfs={} objectType={} key={} requested={} tokens={} values={} timingsMs lookup={} resolve={} total={}",
                    obfs == null ? 0 : obfs.size(), request.objectType(), key, requestedNames.size(), tokensByName.size(), resolvedValues.size(),
                    elapsedMs(lookupNs), elapsedMs(resolveNs), elapsedMs(System.nanoTime() - startedNs));
            return new IndexSuffixResponse(List.of(), resolvedValues);
        }
        long resolveStartedNs = System.nanoTime();
        List<IndexToken> tokens = new ArrayList<>(tokensByName.values());
        List<IndexSuffixMetric> metrics = new ArrayList<>(tokens.size());
        for (IndexToken token : tokens) {
            IndexSuffixCounts counts = getIndexTokenSuffixCounts(token, objectTypeFilter);
            metrics.add(new IndexSuffixMetric(token.name(), token.obf(),
                    counts.dict(),
                    counts.common(),
                    counts.part(),
                    counts.integer(),
                    counts.extra(),
                    counts.other()));
        }
        long resolveNs = System.nanoTime() - resolveStartedNs;
        getLogger().info("getIndexSuffix metrics obfs={} objectType={} requested={} tokens={} metrics={} timingsMs lookup={} resolve={} total={}",
                obfs == null ? 0 : obfs.size(), request.objectType(), requestedNames.size(), tokens.size(), metrics.size(),
                elapsedMs(lookupNs), elapsedMs(resolveNs), elapsedMs(System.nanoTime() - startedNs));
        return new IndexSuffixResponse(metrics, List.of());
    }

    default List<String> uniqueSortedSuffixValues(List<String> values) {
        if (values == null || values.isEmpty()) {
            return List.of();
        }
        Map<String, String> unique = new LinkedHashMap<>();
        for (String value : values) {
            if (!Algorithms.isEmpty(value)) {
                String key = suffixMetricValueKey(value);
                String existing = unique.get(key);
                if (existing == null || (!isEnabledSuffixMetricValue(existing) && isEnabledSuffixMetricValue(value))) {
                    unique.put(key, value);
                }
            }
        }
        List<String> sorted = new ArrayList<>(unique.values());
        sorted.sort(Comparator.comparing(this::suffixMetricValueKey, String.CASE_INSENSITIVE_ORDER));
        return sorted;
    }

    default String suffixMetricValueKey(String value) {
        if (value == null) {
            return "";
        }
        return isEnabledSuffixMetricValue(value) ? value.substring("enabled\t".length()) : value;
    }

    default boolean isEnabledSuffixMetricValue(String value) {
        return value != null && value.startsWith("enabled\t");
    }

    default long elapsedMs(long elapsedNs) {
        return TimeUnit.NANOSECONDS.toMillis(elapsedNs);
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
                mergePoiIndexes(left.poiIndexes(), right.poiIndexes()),
                concatSuffixRefs(left.poiSuffixRefs(), right.poiSuffixRefs()),
                concatSuffixRefs(left.addressSuffixRefs(), right.addressSuffixRefs()),
                mergeIndexSuffixCounts(left.poiSuffixCounts(), right.poiSuffixCounts()),
                mergeIndexSuffixCounts(left.addressSuffixCounts(), right.addressSuffixCounts()),
                left.isCommon() || right.isCommon(),
                left.isFrequent() || right.isFrequent(),
                null,
                getIndexTokenPoiCount(left) + getIndexTokenPoiCount(right),
                getIndexTokenAddressCount(left) + getIndexTokenAddressCount(right));
    }

    default List<IndexToken> compactIndexTokens(List<IndexToken> tokens) {
        if (tokens == null || tokens.isEmpty()) {
            return List.of();
        }
        List<IndexToken> compact = new ArrayList<>(tokens.size());
        for (IndexToken token : tokens) {
            compact.add(compactIndexToken(token));
        }
        return compact;
    }

    default IndexToken compactIndexToken(IndexToken token) {
        if (token == null) {
            return null;
        }
        return new IndexToken(token.name(), new AddressRef[0], new int[0], new int[0], new int[0],
                Map.of(), List.of(), List.of(),
                token.poiSuffixCounts(), token.addressSuffixCounts(),
                token.isCommon(), token.isFrequent(), token.obf(),
                getIndexTokenPoiCount(token), getIndexTokenAddressCount(token));
    }

    default IndexSuffixCounts mergeIndexSuffixCounts(IndexSuffixCounts left, IndexSuffixCounts right) {
        if (left == null) {
            return right == null ? emptyIndexSuffixCounts() : right;
        }
        if (right == null) {
            return left;
        }
        return new IndexSuffixCounts(
                safeMetricInt((long) left.dict() + right.dict()),
                safeMetricInt((long) left.common() + right.common()),
                safeMetricInt((long) left.part() + right.part()),
                safeMetricInt((long) left.integer() + right.integer()),
                safeMetricInt((long) left.extra() + right.extra()),
                safeMetricInt((long) left.other() + right.other()));
    }

    default IndexSuffixCounts emptyIndexSuffixCounts() {
        return new IndexSuffixCounts(0, 0, 0, 0, 0, 0);
    }

    default List<IndexSuffixRef> concatSuffixRefs(List<IndexSuffixRef> left, List<IndexSuffixRef> right) {
        if ((left == null || left.isEmpty()) && (right == null || right.isEmpty())) {
            return List.of();
        }
        List<IndexSuffixRef> out = new ArrayList<>();
        if (left != null) {
            out.addAll(left);
        }
        if (right != null) {
            out.addAll(right);
        }
        return List.copyOf(out);
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

    default Map<Integer, int[]> mergePoiIndexes(Map<Integer, int[]> left, Map<Integer, int[]> right) {
        Map<Integer, Set<Integer>> merged = new LinkedHashMap<>();
        mergePoiIndexesInto(merged, left);
        mergePoiIndexesInto(merged, right);
        if (merged.isEmpty()) {
            return Map.of();
        }
        Map<Integer, int[]> out = new LinkedHashMap<>();
        for (Map.Entry<Integer, Set<Integer>> entry : merged.entrySet()) {
            out.put(entry.getKey(), toIntArray(entry.getValue()));
        }
        return Map.copyOf(out);
    }

    default void mergePoiIndexesInto(Map<Integer, Set<Integer>> target, Map<Integer, int[]> source) {
        if (target == null || source == null || source.isEmpty()) {
            return;
        }
        for (Map.Entry<Integer, int[]> entry : source.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            Set<Integer> indexes = target.computeIfAbsent(entry.getKey(), ignored -> new LinkedHashSet<>());
            for (int value : entry.getValue()) {
                indexes.add(value);
            }
        }
    }

    default Comparator<IndexToken> buildIndexTokenComparator(String sortBy, String sortOrder) {
        String normalizedSortBy = Algorithms.isEmpty(sortBy) ? "name" : sortBy.trim().toLowerCase(Locale.ROOT);
        Comparator<IndexToken> comparator = switch (normalizedSortBy) {
            case "poi" -> Comparator.comparingInt(this::getIndexTokenPoiCount);
            case "address" -> Comparator.comparingInt(this::getIndexTokenAddressCount);
            case "common" -> Comparator.comparingInt(token -> token != null && token.isCommon() ? 1 : 0);
            case "frequent" -> Comparator.comparingInt(token -> token != null && token.isFrequent() ? 1 : 0);
            case "dict", "suffixcommon", "suffixpart", "integer", "extra", "other" ->
                    Comparator.comparingInt(token -> getIndexTokenSuffixMetricCount(token, normalizedSortBy, null));
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
        int dictSuffixSum = 0;
        int commonSuffixSum = 0;
        int partSuffixSum = 0;
        int integerSuffixSum = 0;
        int extraSuffixSum = 0;
        int otherSuffixSum = 0;
        int poiMax = 0;
        int addressMax = 0;
        int dictSuffixMax = 0;
        int commonSuffixMax = 0;
        int partSuffixMax = 0;
        int integerSuffixMax = 0;
        int extraSuffixMax = 0;
        int otherSuffixMax = 0;
        for (IndexToken token : tokens) {
            int poiCount = getIndexTokenPoiCount(token);
            int addressCount = getIndexTokenAddressCount(token);
            IndexSuffixCounts suffixCounts = getIndexTokenSuffixCounts(token, null);
            poiSum += poiCount;
            addressSum += addressCount;
            commonSum += token != null && token.isCommon() ? 1 : 0;
            frequentSum += token != null && token.isFrequent() ? 1 : 0;
            dictSuffixSum += suffixCounts.dict();
            commonSuffixSum += suffixCounts.common();
            partSuffixSum += suffixCounts.part();
            integerSuffixSum += suffixCounts.integer();
            extraSuffixSum += suffixCounts.extra();
            otherSuffixSum += suffixCounts.other();
            poiMax = Math.max(poiMax, poiCount);
            addressMax = Math.max(addressMax, addressCount);
            dictSuffixMax = Math.max(dictSuffixMax, suffixCounts.dict());
            commonSuffixMax = Math.max(commonSuffixMax, suffixCounts.common());
            partSuffixMax = Math.max(partSuffixMax, suffixCounts.part());
            integerSuffixMax = Math.max(integerSuffixMax, suffixCounts.integer());
            extraSuffixMax = Math.max(extraSuffixMax, suffixCounts.extra());
            otherSuffixMax = Math.max(otherSuffixMax, suffixCounts.other());
        }
        return new IndexTokenSummary(poiSum, addressSum, commonSum, frequentSum,
                dictSuffixSum, commonSuffixSum, integerSuffixSum, partSuffixSum, extraSuffixSum, otherSuffixSum,
                poiMax, addressMax, dictSuffixMax, commonSuffixMax, integerSuffixMax, partSuffixMax, extraSuffixMax, otherSuffixMax);
    }

    default int getIndexTokenPoiCount(IndexToken token) {
        if (token == null) {
            return 0;
        }
        if (token.poiCount() > 0) {
            return token.poiCount();
        }
        int indexedCount = countPoiIndexes(token.poiIndexes());
        return indexedCount > 0 ? indexedCount : token.poiRefs() == null ? 0 : token.poiRefs().length;
    }

    default int getIndexTokenAddressCount(IndexToken token) {
        if (token == null) {
            return 0;
        }
        if (token.addressCount() > 0) {
            return token.addressCount();
        }
        return token.addressRefs() == null ? 0 : token.addressRefs().length;
    }

    default boolean matchesIndexTokenObjectType(IndexToken token, Boolean objectTypeFilter) {
        if (objectTypeFilter == null) {
            return true;
        }
        return objectTypeFilter ? getIndexTokenPoiCount(token) > 0 : getIndexTokenAddressCount(token) > 0;
    }

    default int countPoiIndexes(Map<Integer, int[]> poiIndexes) {
        if (poiIndexes == null || poiIndexes.isEmpty()) {
            return 0;
        }
        long total = 0L;
        for (int[] indexes : poiIndexes.values()) {
            if (indexes != null) {
                total += indexes.length;
            }
        }
        return safeMetricInt(total);
    }

    default int getIndexTokenSuffixMetricCount(IndexToken token, String key, Boolean objectTypeFilter) {
        if (token == null || key == null) {
            return 0;
        }
        IndexSuffixCounts counts = getIndexTokenSuffixCounts(token, objectTypeFilter);
        return switch (key) {
            case "dict" -> counts.dict();
            case "suffixcommon" -> counts.common();
            case "suffixpart" -> counts.part();
            case "integer" -> counts.integer();
            case "extra" -> counts.extra();
            case "other" -> counts.other();
            default -> 0;
        };
    }

    default IndexSuffixCounts getIndexTokenSuffixCounts(IndexToken token, Boolean objectTypeFilter) {
        if (token == null) {
            return emptyIndexSuffixCounts();
        }
        if (objectTypeFilter == null) {
            return mergeIndexSuffixCounts(token.poiSuffixCounts(), token.addressSuffixCounts());
        }
        return objectTypeFilter
                ? token.poiSuffixCounts() == null ? emptyIndexSuffixCounts() : token.poiSuffixCounts()
                : token.addressSuffixCounts() == null ? emptyIndexSuffixCounts() : token.addressSuffixCounts();
    }

    default List<String> getIndexTokenSuffixValues(List<String> obfs, IndexToken token, String key, Boolean objectTypeFilter) {
        if (token == null || key == null) {
            return List.of();
        }
        List<IndexSuffixRef> refs = getIndexTokenSuffixRefs(token, objectTypeFilter);
        if (refs.isEmpty()) {
            return List.of();
        }
        List<String> values = new ArrayList<>();
        for (String obf : obfs == null ? List.<String>of() : obfs) {
            if (!Algorithms.isEmpty(obf)) {
                values.addAll(resolveIndexSuffixValues(new File(obf), refs, key));
            }
        }
        return values;
    }

    default List<IndexSuffixRef> getIndexTokenSuffixRefs(IndexToken token, Boolean objectTypeFilter) {
        if (token == null) {
            return List.of();
        }
        if (objectTypeFilter == null) {
            return concatSuffixRefs(token.poiSuffixRefs(), token.addressSuffixRefs());
        }
        return objectTypeFilter
                ? token.poiSuffixRefs() == null ? List.of() : token.poiSuffixRefs()
                : token.addressSuffixRefs() == null ? List.of() : token.addressSuffixRefs();
    }

    default List<String> resolveIndexSuffixValues(File file, List<IndexSuffixRef> refs, String key) {
        if (file == null || refs == null || refs.isEmpty() || Algorithms.isEmpty(key)) {
            return List.of();
        }
        List<String> values = new ArrayList<>();
        Map<String, NameIndexSuffixInfo> cache = new HashMap<>();
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file.getAbsolutePath(), "r")) {
            BinaryMapIndexReaderExt index = new BinaryMapIndexReaderExt(randomAccessFile, file);
            try {
                for (IndexSuffixRef ref : refs) {
                    if (ref == null) {
                        continue;
                    }
                    String cacheKey = ref.poi() + ":" + ref.offset();
                    NameIndexSuffixInfo suffixInfo = cache.get(cacheKey);
                    if (suffixInfo == null) {
                        suffixInfo = readSuffixInfoAtOffset(index, ref.offset(), ref.poi(),
                                getCachedCommonSuffixStats(ref.obf(), ref.poi(), ref.offset()));
                        cache.put(cacheKey, suffixInfo);
                    }
                    addResolvedSuffixMetrics(values, key, suffixInfo, ref);
                }
            } finally {
                index.close();
            }
        } catch (Exception e) {
            getLogger().warn("Failed to resolve suffix values from {} key={}", file, key, e);
        }
        return values;
    }

    default void addResolvedSuffixMetrics(List<String> values, String key, NameIndexSuffixInfo suffixInfo, IndexSuffixRef ref) {
        if ("integer".equals(key)) {
            if (suffixInfo != null && suffixInfo.integerSuffixes() != null) {
                Set<String> enabled = toStringSet(ref == null ? null : ref.metricIntegerSuffixes());
                for (String value : suffixInfo.integerSuffixes()) {
                    if (!Algorithms.isEmpty(value)) {
                        values.add(formatEnabledSuffixMetricValue(value, enabled.contains(value)));
                    }
                }
            }
            return;
        }
        if ("extra".equals(key)) {
            if (suffixInfo != null && suffixInfo.extraSuffixes() != null) {
                Set<String> enabled = toStringSet(ref == null ? null : ref.metricExtraSuffixes());
                for (String value : suffixInfo.extraSuffixes()) {
                    if (!Algorithms.isEmpty(value)) {
                        values.add(formatEnabledSuffixMetricValue(value, enabled.contains(value)));
                    }
                }
            }
            return;
        }
        if (ref == null || suffixInfo == null || suffixInfo.suffixDictionary() == null) {
            return;
        }
        Set<Integer> enabledIndexes = toIntegerSet(getMetricSuffixIndexes(ref, suffixInfo));
        for (int suffixIndex = 0; suffixIndex < suffixInfo.suffixDictionary().size(); suffixIndex++) {
            String value = getResolvedSuffixMetric(key, suffixInfo, suffixIndex);
            if (!Algorithms.isEmpty(value)) {
                values.add(formatEnabledSuffixMetricValue(value, enabledIndexes.contains(suffixIndex)));
            }
        }
    }

    default String formatEnabledSuffixMetricValue(String value, boolean enabled) {
        return enabled ? "enabled\t" + value : value;
    }

    default Set<Integer> toIntegerSet(int[] values) {
        if (values == null || values.length == 0) {
            return Set.of();
        }
        Set<Integer> out = new LinkedHashSet<>();
        for (int value : values) {
            out.add(value);
        }
        return out;
    }

    default Set<String> toStringSet(String[] values) {
        if (values == null || values.length == 0) {
            return Set.of();
        }
        Set<String> out = new LinkedHashSet<>();
        for (String value : values) {
            if (!Algorithms.isEmpty(value)) {
                out.add(value);
            }
        }
        return out;
    }

    default int[] getMetricSuffixIndexes(IndexSuffixRef ref, NameIndexSuffixInfo suffixInfo) {
        if (ref == null) {
            return new int[0];
        }
        int[] indexes = ref.metricSuffixIndexes();
        return indexes == null || indexes.length == 0 ? new int[]{ref.suffixIndex()} : expandMetricSuffixIndexes(indexes, suffixInfo);
    }

    default String getResolvedSuffixMetric(String key, NameIndexSuffixInfo suffixInfo, int suffixIndex) {
        if (key == null || suffixInfo == null || suffixInfo.suffixDictionary() == null
                || suffixIndex < 0 || suffixIndex >= suffixInfo.suffixDictionary().size()) {
            return null;
        }
        String suffix = suffixInfo.suffixDictionary().get(suffixIndex);
        CommonSuffix commonStat = getCommonSuffixStat(suffixInfo, suffixIndex);
        if ("dict".equals(key)) {
            return commonStat == null ? formatSuffixMetricText(suffix) : formatCommonSuffixMetricText(commonStat);
        }
        return null;
    }

    default List<IndexToken> getCachedOrLoadIndexTokens(File file) throws IOException {
        String cacheKey = getIndexCacheKey(file);
        long fileLength = file.length();
        long lastModified = file.lastModified();
        CachedIndexTokens cached;
        synchronized (INDEX_TOKENS_CACHE) {
            cached = INDEX_TOKENS_CACHE.get(cacheKey);
        }
        if (cached != null && cached.fileLength() == fileLength && cached.lastModified() == lastModified
                && hasPoiIndexData(cached.tokens())) {
            return cached.tokens();
        }
        List<IndexToken> loadedTokens = loadIndexTokens(file);
        List<IndexToken> cachedTokens = List.copyOf(loadedTokens);
        synchronized (INDEX_TOKENS_CACHE) {
            INDEX_TOKENS_CACHE.put(cacheKey, new CachedIndexTokens(cacheKey, fileLength, lastModified, cachedTokens));
        }
        return cachedTokens;
    }

    default boolean hasPoiIndexData(List<IndexToken> tokens) {
        if (tokens == null || tokens.isEmpty()) {
            return true;
        }
        for (IndexToken token : tokens) {
            if (token != null && token.poiRefs() != null && token.poiRefs().length > 0
                    && (token.poiIndexes() == null || token.poiIndexes().isEmpty())) {
                return false;
            }
        }
        return true;
    }

    default List<IndexToken> loadIndexTokens(File file) throws IOException {
        String obfKey = getIndexCacheKey(file);
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file.getAbsolutePath(), "r")) {
            BinaryMapIndexReaderExt index = new BinaryMapIndexReaderExt(randomAccessFile, file);
            try {
                Map<String, IndexTokenBuilder> tokens = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                for (BinaryIndexPart part : index.getIndexes()) {
                    if (part instanceof BinaryMapAddressReaderAdapter.AddressRegion addressRegion) {
                        collectAddressIndexTokens(index, addressRegion, tokens, obfKey);
                    } else if (part instanceof BinaryMapPoiReaderAdapter.PoiRegion poiRegion) {
                        collectPoiIndexTokens(index, poiRegion, tokens, obfKey);
                    }
                }
                return buildIndexTokensWithRefs(tokens, obfKey);
            } finally {
                index.close();
            }
        }
    }

    default String getIndexCacheKey(File file) {
        return file.getName();
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
                                           Map<String, IndexTokenBuilder> tokens, String obfKey) throws IOException {
        long indexNameOffset = region.getIndexNameOffset();
        if (indexNameOffset < 0) {
            return;
        }
        index.getInputStream().seek(indexNameOffset);
        long length = index.readInt();
        long oldLimit = index.getInputStream().pushLimitLong(length);
        try {
            readAddressNameIndexTokens(index, tokens, obfKey);
        } finally {
            index.getInputStream().popLimit(oldLimit);
        }
    }

    default void collectPoiIndexTokens(BinaryMapIndexReaderExt index, BinaryMapPoiReaderAdapter.PoiRegion region,
                                       Map<String, IndexTokenBuilder> tokens, String obfKey) throws IOException {
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
                            readPoiNameIndexTokens(index, tokens, obfKey);
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

    default void readAddressNameIndexTokens(BinaryMapIndexReaderExt index, Map<String, IndexTokenBuilder> tokens, String obfKey) throws IOException {
        NameIndexTableInfo tableInfo = null;
        List<CommonSuffix> commonStats = List.of();
        while (true) {
            int tagWithType = index.getInputStream().readTag();
            int tag = WireFormat.getTagFieldNumber(tagWithType);
            switch (tag) {
                case 0:
                    if (tableInfo != null) {
                        readNameIndexTableTokens(index, tokens, false, tableInfo, commonStats, obfKey);
                    }
                    return;
                case OsmandOdb.OsmAndAddressNameIndexData.TABLE_FIELD_NUMBER:
                    tableInfo = readNameIndexTableInfo(index);
                    break;
                case OsmandOdb.OsmAndAddressNameIndexData.COMMONSTATS_FIELD_NUMBER:
                    commonStats = readCommonSuffixStats(index);
                    break;
                default:
                    InspectorService.skipUnknownField(index.getInputStream(), tagWithType);
                    break;
            }
        }
    }

    default void readPoiNameIndexTokens(BinaryMapIndexReaderExt index, Map<String, IndexTokenBuilder> tokens, String obfKey) throws IOException {
        NameIndexTableInfo tableInfo = null;
        List<CommonSuffix> commonStats = List.of();
        while (true) {
            int tagWithType = index.getInputStream().readTag();
            int tag = WireFormat.getTagFieldNumber(tagWithType);
            switch (tag) {
                case 0:
                    if (tableInfo != null) {
                        readNameIndexTableTokens(index, tokens, true, tableInfo, commonStats, obfKey);
                    }
                    return;
                case OsmandOdb.OsmAndPoiNameIndex.TABLE_FIELD_NUMBER:
                    tableInfo = readNameIndexTableInfo(index);
                    break;
                case OsmandOdb.OsmAndPoiNameIndex.COMMONSTATS_FIELD_NUMBER:
                    commonStats = readCommonSuffixStats(index);
                    break;
                default:
                    InspectorService.skipUnknownField(index.getInputStream(), tagWithType);
                    break;
            }
        }
    }

    default NameIndexTableInfo readNameIndexTableInfo(BinaryMapIndexReaderExt index) throws IOException {
        long tableLength = index.readInt();
        long tableContentStart = index.getInputStream().getTotalBytesRead();
        Map<String, Integer> prefixOffsets = new TreeMap<>();
        long oldLimit = index.getInputStream().pushLimitLong(tableLength);
        try {
            readIndexedStringTableOffsets(index, "", prefixOffsets);
        } finally {
            index.getInputStream().popLimit(oldLimit);
        }
        return new NameIndexTableInfo(tableContentStart, prefixOffsets);
    }

    default void readNameIndexTableTokens(BinaryMapIndexReaderExt index, Map<String, IndexTokenBuilder> tokens, boolean poi,
                                          NameIndexTableInfo tableInfo, List<CommonSuffix> commonStats,
                                          String obfKey) throws IOException {
        if (tableInfo == null || tableInfo.prefixOffsets() == null) {
            return;
        }
        for (Map.Entry<String, Integer> entry : tableInfo.prefixOffsets().entrySet()) {
            long absoluteOffset = tableInfo.tableContentStart() + entry.getValue();
            NameIndexSuffixInfo suffixInfo = readSuffixInfoAtOffset(index, absoluteOffset, poi, commonStats);
            cacheCommonSuffix(obfKey, poi, (int) absoluteOffset, commonStats);
            List<String> suffixDictionary = suffixInfo.suffixDictionary();
            Map<Integer, PoiTokenRefs> poiRefsBySuffix = poi ? readPoiTokenRefsBySuffix(index, (int) absoluteOffset) : Map.of();
            Map<Integer, AddressTokenRefs> addressRefsBySuffix = poi ? Map.of() : readAddressTokenRefsBySuffix(index, (int) absoluteOffset);
            for (int suffixIndex = 0; suffixIndex < suffixDictionary.size(); suffixIndex++) {
                String suffix = suffixDictionary.get(suffixIndex);
                PoiTokenRefs poiTokenRefs = poi ? poiRefsBySuffix.getOrDefault(suffixIndex, emptyPoiTokenRefs())
                        : new PoiTokenRefs(new LinkedHashSet<>(), new ArrayList<>(), new ArrayList<>(), Map.of(),
                        Set.of(), Set.of(), Set.of());
                AddressTokenRefs addressTokenRefs = poi ? new AddressTokenRefs()
                        : addressRefsBySuffix.getOrDefault(suffixIndex, new AddressTokenRefs());
                if (!hasIndexTokenRefs(poi, addressTokenRefs, poiTokenRefs)) {
                    continue;
                }
                int[] enabledSuffixIndexes = poi
                        ? toIntArray(poiTokenRefs.metricSuffixIndexes())
                        : toIntArray(addressTokenRefs.metricSuffixIndexes());
                addIndexToken(tokens, entry.getKey() + suffix, (int) absoluteOffset, suffixIndex, poi,
                        addressTokenRefs.addressRefs().toArray(new AddressRef[0]),
                        toIntArray(poiTokenRefs.offsets()), toIntArray(poiTokenRefs.atomRefs()),
                        toIntArray(poiTokenRefs.atomSizes()), poiTokenRefs.poiIndexes(),
                        enabledSuffixIndexes,
                        poi ? toStringArray(poiTokenRefs.metricIntegerSuffixes()) : toStringArray(addressTokenRefs.metricIntegerSuffixes()),
                        poi ? toStringArray(poiTokenRefs.metricExtraSuffixes()) : toStringArray(addressTokenRefs.metricExtraSuffixes()),
                        countMetricSuffixes(suffixInfo, allMetricSuffixIndexes(), enabledSuffixIndexes));
            }
        }
    }

    default IndexSuffixCounts countMetricSuffixes(NameIndexSuffixInfo suffixInfo, int[] suffixIndexes, int[] enabledSuffixIndexes) {
        if (suffixInfo == null || suffixIndexes == null || suffixIndexes.length == 0) {
            return emptyIndexSuffixCounts();
        }
        Set<String> dict = new LinkedHashSet<>();
        for (int suffixIndex : expandMetricSuffixIndexes(suffixIndexes, suffixInfo)) {
            String dictValue = getResolvedSuffixMetric("dict", suffixInfo, suffixIndex);
            if (!Algorithms.isEmpty(dictValue)) {
                dict.add(dictValue);
            }
        }
        Set<Integer> enabledIndexes = toIntegerSet(expandMetricSuffixIndexes(enabledSuffixIndexes, suffixInfo));
        int commonCount = 0;
        int partCount = 0;
        if (suffixInfo.suffixDictionary() != null && !enabledIndexes.isEmpty()) {
            for (int suffixIndex : enabledIndexes) {
                if (suffixIndex < 0 || suffixIndex >= suffixInfo.suffixDictionary().size()) {
                    continue;
                }
                CommonSuffix commonStat = getCommonSuffixStat(suffixInfo, suffixIndex);
                if (commonStat != null) {
                    commonCount++;
                    continue;
                }
                String suffix = suffixInfo.suffixDictionary().get(suffixIndex);
                if (suffix != null && !suffix.startsWith(" ")) {
                    partCount++;
                }
            }
        }
        return new IndexSuffixCounts(dict.size(),
                commonCount,
                partCount,
                suffixInfo.integerSuffixes() == null ? 0 : suffixInfo.integerSuffixes().size(),
                suffixInfo.extraSuffixes() == null ? 0 : suffixInfo.extraSuffixes().size(),
                suffixInfo.otherWordsCount());
    }

    default int[] allMetricSuffixIndexes() {
        return new int[]{-1};
    }

    default int[] expandMetricSuffixIndexes(int[] suffixIndexes, NameIndexSuffixInfo suffixInfo) {
        if (suffixIndexes == null || suffixIndexes.length == 0) {
            return new int[0];
        }
        if (suffixIndexes.length == 1 && suffixIndexes[0] == -1) {
            int size = suffixInfo == null || suffixInfo.suffixDictionary() == null ? 0 : suffixInfo.suffixDictionary().size();
            int[] all = new int[size];
            for (int i = 0; i < size; i++) {
                all[i] = i;
            }
            return all;
        }
        return suffixIndexes;
    }

    default CommonSuffix getCommonSuffixStat(NameIndexSuffixInfo suffixInfo, int suffixIndex) {
        if (suffixInfo == null || suffixInfo.commonSuffixes() == null || suffixInfo.suffixDictionary() == null) {
            return null;
        }
        int commonIndex = suffixIndex - suffixInfo.suffixDictionary().size() + suffixInfo.commonSuffixes().size();
        return commonIndex >= 0 && commonIndex < suffixInfo.commonSuffixes().size()
                ? suffixInfo.commonSuffixes().get(commonIndex)
                : null;
    }

    default boolean hasIndexTokenRefs(boolean poi, AddressTokenRefs addressTokenRefs, PoiTokenRefs poiTokenRefs) {
        if (poi) {
            if (poiTokenRefs == null) {
                return false;
            }
            if (poiTokenRefs.poiIndexes() != null && countPoiIndexes(poiTokenRefs.poiIndexes()) > 0) {
                return true;
            }
            return poiTokenRefs.offsets() != null && !poiTokenRefs.offsets().isEmpty();
        }
        return addressTokenRefs != null && addressTokenRefs.addressRefs() != null && !addressTokenRefs.addressRefs().isEmpty();
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

    default List<CommonSuffix> readCommonSuffixStats(BinaryMapIndexReaderExt index) throws IOException {
        int length = index.getInputStream().readRawVarint32();
        byte[] data = index.getInputStream().readRawBytes(length);
        OsmandOdb.CommonIndexedStats stats = OsmandOdb.CommonIndexedStats.parseFrom(data);
        List<CommonSuffix> decoded = new ArrayList<>(stats.getValueCount());
        String value = null;
        for (int i = 0; i < stats.getValueCount(); i++) {
            value = SearchAlgorithms.nameIndexDecodeDictionarySuffix(value, stats.getValue(i));
            int matched = i < stats.getMatchedCount() ? stats.getMatched(i) : 0;
            int nonindexed = i < stats.getNonindexedCount() ? stats.getNonindexed(i) : 0;
            decoded.add(new CommonSuffix(value, matched, nonindexed));
        }
        return List.copyOf(decoded);
    }

    default NameIndexSuffixInfo readSuffixInfoAtOffset(BinaryMapIndexReaderExt index, long absoluteOffset, boolean poi,
                                                       List<CommonSuffix> commonStats) throws IOException {
        index.getInputStream().seek(absoluteOffset);
        long length = index.getInputStream().readRawVarint32();
        long oldLimit = index.getInputStream().pushLimitLong(length);
        try {
            List<String> suffixDictionary = new ArrayList<>();
            List<CommonSuffix> commonSuffixes = new ArrayList<>();
            Set<String> integerSuffixes = new LinkedHashSet<>();
            Set<String> extraSuffixes = new LinkedHashSet<>();
            int[] otherWordsCount = new int[]{0};
            while (true) {
                int tagWithType = index.getInputStream().readTag();
                int tag = WireFormat.getTagFieldNumber(tagWithType);
                if (tag == 0) {
                    appendCommonSuffixes(suffixDictionary, commonSuffixes);
                    return new NameIndexSuffixInfo(List.copyOf(suffixDictionary), List.copyOf(commonSuffixes),
                            Set.copyOf(integerSuffixes), Set.copyOf(extraSuffixes), otherWordsCount[0]);
                }
                boolean isSuffixField = poi
                        ? tag == OsmandOdb.OsmAndPoiNameIndex.OsmAndPoiNameIndexData.SUFFIXESDICTIONARY_FIELD_NUMBER
                        : tag == OsmandOdb.OsmAndAddressNameIndexData.AddressNameIndexData.SUFFIXESDICTIONARY_FIELD_NUMBER;
                boolean isAtomField = poi
                        ? tag == OsmandOdb.OsmAndPoiNameIndex.OsmAndPoiNameIndexData.ATOMS_FIELD_NUMBER
                        : tag == OsmandOdb.OsmAndAddressNameIndexData.AddressNameIndexData.ATOM_FIELD_NUMBER;
                if (isSuffixField) {
                    addDecodedNameIndexSuffix(suffixDictionary, index.getInputStream().readString());
                    continue;
                }
                boolean isCommonSuffixField = poi
                        ? tag == OsmandOdb.OsmAndPoiNameIndex.OsmAndPoiNameIndexData.SUFFIXESCOMMONDICTIONARY_FIELD_NUMBER
                        : tag == OsmandOdb.OsmAndAddressNameIndexData.AddressNameIndexData.SUFFIXESCOMMONDICTIONARY_FIELD_NUMBER;
                if (isCommonSuffixField) {
                    int commonSuffixIndex = index.getInputStream().readUInt32();
                    commonSuffixes.add(resolveCommonSuffixStat(commonStats, commonSuffixIndex));
                    continue;
                }
                if (isAtomField) {
                    int atomLength = index.getInputStream().readRawVarint32();
                    long atomOldLimit = index.getInputStream().pushLimitLong(atomLength);
                    try {
                        readNameIndexBlockAtomMetrics(index, poi, integerSuffixes, extraSuffixes, otherWordsCount);
                    } finally {
                        index.getInputStream().popLimit(atomOldLimit);
                    }
                    continue;
                }
                InspectorService.skipUnknownField(index.getInputStream(), tagWithType);
            }
        } finally {
            index.getInputStream().popLimit(oldLimit);
        }
    }

    default CommonSuffix resolveCommonSuffixStat(List<CommonSuffix> commonStats, int index) {
        if (commonStats != null && index >= 0 && index < commonStats.size()) {
            return commonStats.get(index);
        }
        return new CommonSuffix("#" + index, 0, 0);
    }

    default void readNameIndexBlockAtomMetrics(BinaryMapIndexReaderExt index, boolean poi,
                                               Set<String> integerSuffixes,
                                               Set<String> extraSuffixes,
                                               int[] otherWordsCount) throws IOException {
        while (true) {
            int tagWithType = index.getInputStream().readTag();
            int tag = WireFormat.getTagFieldNumber(tagWithType);
            if (tag == 0) {
                return;
            }
            int suffixBitsetField = poi
                    ? OsmandOdb.OsmAndPoiNameIndexDataAtom.SUFFIXESBITSETINDEX_FIELD_NUMBER
                    : OsmandOdb.AddressNameIndexDataAtom.SUFFIXESBITSETINDEX_FIELD_NUMBER;
            int extraSuffixField = poi
                    ? OsmandOdb.OsmAndPoiNameIndexDataAtom.EXTRASUFFIX_FIELD_NUMBER
                    : OsmandOdb.AddressNameIndexDataAtom.EXTRASUFFIX_FIELD_NUMBER;
            int otherWordsCountField = poi
                    ? OsmandOdb.OsmAndPoiNameIndexDataAtom.OTHERWORDSCOUNT_FIELD_NUMBER
                    : OsmandOdb.AddressNameIndexDataAtom.OTHERWORDSCOUNT_FIELD_NUMBER;
            if (tag == suffixBitsetField) {
                String value = formatOddSuffixBitsetValue(index.getInputStream().readUInt32());
                if (!Algorithms.isEmpty(value)) {
                    integerSuffixes.add(value);
                }
                continue;
            }
            if (tag == extraSuffixField) {
                String value = index.getInputStream().readString();
                if (!Algorithms.isEmpty(value)) {
                    extraSuffixes.add(value);
                }
                continue;
            }
            if (tag == otherWordsCountField) {
                long total = (long) otherWordsCount[0] + index.getInputStream().readUInt32();
                otherWordsCount[0] = safeMetricInt(total);
                continue;
            }
            InspectorService.skipUnknownField(index.getInputStream(), tagWithType);
        }
    }

    default String formatOddSuffixBitsetValue(int suffixBitsetIndex) {
        if (suffixBitsetIndex == 0 || suffixBitsetIndex % 2 == 0) {
            return null;
        }
        String prefix = suffixBitsetIndex % 4 == 1 ? " " : "";
        return prefix + (suffixBitsetIndex >> 2);
    }

    default void appendCommonSuffixes(List<String> suffixDictionary, List<CommonSuffix> commonSuffixes) {
        if (suffixDictionary == null || commonSuffixes == null || commonSuffixes.isEmpty()) {
            return;
        }
        for (CommonSuffix stat : commonSuffixes) {
            String suffix = stat == null ? null : stat.value();
            suffixDictionary.add(suffix == null ? "" : suffix);
        }
    }

    default String formatSuffixMetricText(String suffix) {
        return suffix == null || suffix.isEmpty() ? "<empty>" : suffix;
    }

    default String formatCommonSuffixMetricText(CommonSuffix stat) {
        if (stat == null) {
            return "common\t<empty>\t0\t0";
        }
        return "common\t" + formatSuffixMetricText(stat.value()) + "\t" + stat.matched() + "\t" + stat.nonindexed();
    }

    default void addDecodedNameIndexSuffix(List<String> suffixDictionary, String encodedSuffix) {
        if (SearchAlgorithms.OLD_EMPTY_SUFFIX_DICTIONARY_SENTINEL.equals(encodedSuffix)) {
            return;
        }
        String previousSuffix = suffixDictionary.isEmpty() ? null : suffixDictionary.get(suffixDictionary.size() - 1);
        suffixDictionary.add(SearchAlgorithms.nameIndexDecodeDictionarySuffix(previousSuffix, encodedSuffix));
    }

    default void addIndexToken(Map<String, IndexTokenBuilder> tokens, String name, int offset, int suffixIndex, boolean poi,
                               AddressRef[] addressRefs,
                               int[] poiRefs, int[] poiAtomRefs, int[] poiAtomSizes, Map<Integer, int[]> poiIndexes,
                               int[] metricSuffixIndexes, String[] metricIntegerSuffixes, String[] metricExtraSuffixes,
                               IndexSuffixCounts suffixCounts) {
        IndexSuffixRef suffixRef = new IndexSuffixRef(null, offset, suffixIndex, poi,
                metricSuffixIndexes == null || metricSuffixIndexes.length == 0 ? new int[]{suffixIndex} : metricSuffixIndexes,
                metricIntegerSuffixes == null ? new String[0] : metricIntegerSuffixes,
                metricExtraSuffixes == null ? new String[0] : metricExtraSuffixes);
        IndexSuffixCounts safeSuffixCounts = suffixCounts == null ? emptyIndexSuffixCounts() : suffixCounts;
        IndexTokenBuilder existing = tokens.get(name);
        if (existing == null) {
            tokens.put(name, poi
                    ? new IndexTokenBuilder(name, new int[0], new int[0], new AddressRef[0], poiRefs == null ? new int[0] : distinctOffsets(poiRefs),
                            poiAtomRefs == null ? new int[0] : poiAtomRefs, poiAtomSizes == null ? new int[0] : poiAtomSizes,
                            poiIndexes == null ? Map.of() : Map.copyOf(poiIndexes),
                            List.of(suffixRef), List.of(), safeSuffixCounts, emptyIndexSuffixCounts())
                    : new IndexTokenBuilder(name, new int[]{offset}, new int[]{suffixIndex},
                            addressRefs == null ? new AddressRef[0] : addressRefs, new int[0], new int[0], new int[0],
                            Map.of(), List.of(), List.of(suffixRef), emptyIndexSuffixCounts(), safeSuffixCounts));
            return;
        }
        int[] mergedAddressOffsets = poi ? existing.addressOffsets() : appendDistinctOffset(existing.addressOffsets(), offset);
        int[] mergedAddressSuffixIndexes = poi ? existing.addressSuffixIndexes() : appendAddressSuffixIndex(existing.addressOffsets(), existing.addressSuffixIndexes(), offset, suffixIndex);
        AddressRef[] mergedAddressRefs = poi ? existing.addressRefs() : concatAddressRefs(existing.addressRefs(), addressRefs);
        int[] mergedPoiRefs = poi ? appendDistinctOffsets(existing.poiRefs(), poiRefs) : existing.poiRefs();
        int[] mergedPoiAtomRefs = poi ? appendOffsets(existing.poiAtomRefs(), poiAtomRefs) : existing.poiAtomRefs();
        int[] mergedPoiAtomSizes = poi ? appendOffsets(existing.poiAtomSizes(), poiAtomSizes) : existing.poiAtomSizes();
        Map<Integer, int[]> mergedPoiIndexes = poi ? mergePoiIndexes(existing.poiIndexes(), poiIndexes) : existing.poiIndexes();
        List<IndexSuffixRef> mergedPoiSuffixRefs = poi
                ? concatSuffixRefs(existing.poiSuffixRefs(), List.of(suffixRef))
                : existing.poiSuffixRefs();
        List<IndexSuffixRef> mergedAddressSuffixRefs = poi
                ? existing.addressSuffixRefs()
                : concatSuffixRefs(existing.addressSuffixRefs(), List.of(suffixRef));
        IndexSuffixCounts mergedPoiSuffixCounts = poi
                ? mergeIndexSuffixCounts(existing.poiSuffixCounts(), safeSuffixCounts)
                : existing.poiSuffixCounts();
        IndexSuffixCounts mergedAddressSuffixCounts = poi
                ? existing.addressSuffixCounts()
                : mergeIndexSuffixCounts(existing.addressSuffixCounts(), safeSuffixCounts);
        tokens.put(name, new IndexTokenBuilder(name, mergedAddressOffsets, mergedAddressSuffixIndexes, mergedAddressRefs,
                mergedPoiRefs, mergedPoiAtomRefs, mergedPoiAtomSizes, mergedPoiIndexes, mergedPoiSuffixRefs, mergedAddressSuffixRefs,
                mergedPoiSuffixCounts, mergedAddressSuffixCounts));
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

    default List<IndexToken> buildIndexTokensWithRefs(Map<String, IndexTokenBuilder> tokens, String obf) {
        List<IndexToken> tokensWithRefs = new ArrayList<>(tokens.size());
        CommonWords cw = CommonWords.getInstance();
        for (IndexTokenBuilder token : tokens.values()) {
            AddressRef[] addressRefs = distinctAddressRefs(token.addressRefs());
            int poiCount = countPoiIndexes(token.poiIndexes());
            if (poiCount == 0 && token.poiRefs() != null) {
                poiCount = token.poiRefs().length;
            }
            tokensWithRefs.add(new IndexToken(token.name(), addressRefs, token.poiRefs(), token.poiAtomRefs(),
                    token.poiAtomSizes(), token.poiIndexes(), withObf(token.poiSuffixRefs(), obf), withObf(token.addressSuffixRefs(), obf),
                    token.poiSuffixCounts(), token.addressSuffixCounts(),
                    cw.getCommon(token.name()) != -1,
                    cw.getFrequentlyUsed(token.name()) != -1, null, poiCount, addressRefs.length));
        }
        return tokensWithRefs;
    }

    default List<IndexSuffixRef> withObf(List<IndexSuffixRef> refs, String obf) {
        if (refs == null || refs.isEmpty()) {
            return List.of();
        }
        List<IndexSuffixRef> out = new ArrayList<>(refs.size());
        for (IndexSuffixRef ref : refs) {
            if (ref != null) {
                out.add(new IndexSuffixRef(obf, ref.offset(), ref.suffixIndex(), ref.poi(), ref.metricSuffixIndexes(),
                        ref.metricIntegerSuffixes(), ref.metricExtraSuffixes()));
            }
        }
        return List.copyOf(out);
    }

    default void cacheCommonSuffix(String obfKey, boolean poi, int offset, List<CommonSuffix> commonStats) {
        if (Algorithms.isEmpty(obfKey) || commonStats == null || commonStats.isEmpty()) {
            return;
        }
        synchronized (INDEX_COMMON_SUFFIX_CACHE) {
            INDEX_COMMON_SUFFIX_CACHE.put(commonSuffixStatsCacheKey(obfKey, poi, offset), List.copyOf(commonStats));
        }
    }

    default List<CommonSuffix> getCachedCommonSuffixStats(String obfKey, boolean poi, int offset) {
        if (Algorithms.isEmpty(obfKey)) {
            return List.of();
        }
        synchronized (INDEX_COMMON_SUFFIX_CACHE) {
            List<CommonSuffix> stats = INDEX_COMMON_SUFFIX_CACHE.get(commonSuffixStatsCacheKey(obfKey, poi, offset));
            return stats == null ? List.of() : stats;
        }
    }

    default String commonSuffixStatsCacheKey(String obfKey, boolean poi, int offset) {
        return obfKey + "|" + (poi ? "poi" : "address") + "|" + offset;
    }

    default AddressRef[] distinctAddressRefs(AddressRef[] refs) {
        if (refs == null || refs.length == 0) {
            return new AddressRef[0];
        }
        List<AddressRef> out = new ArrayList<>();
        Set<String> uniqueRefs = new LinkedHashSet<>();
        for (AddressRef ref : refs) {
            if (ref != null && uniqueRefs.add(ref.shiftToIndex() + ":" + ref.shiftToCityIndex() + ":"
                    + ref.objectOffset() + ":" + ref.cityOffset() + ":" + ref.typeIndex())) {
                out.add(ref);
            }
        }
        return out.toArray(new AddressRef[0]);
    }

    default PoiTokenRefs emptyPoiTokenRefs() {
        return new PoiTokenRefs(new LinkedHashSet<>(), new ArrayList<>(), new ArrayList<>(), new LinkedHashMap<>(),
                new LinkedHashSet<>(), new LinkedHashSet<>(), new LinkedHashSet<>());
    }

    default Map<Integer, PoiTokenRefs> readPoiTokenRefsBySuffix(BinaryMapIndexReaderExt index, int tokenOffset) throws IOException {
        Map<Integer, PoiTokenRefs> refsBySuffix = new LinkedHashMap<>();
        index.getInputStream().seek(tokenOffset);
        long length = index.getInputStream().readRawVarint32();
        long oldLimit = index.getInputStream().pushLimitLong(length);
        try {
            int suffixIndex = 0;
            while (true) {
                int tagWithType = index.getInputStream().readTag();
                int tag = WireFormat.getTagFieldNumber(tagWithType);
                switch (tag) {
                    case 0:
                        return immutablePoiRefsBySuffix(refsBySuffix);
                    case OsmandOdb.OsmAndPoiNameIndex.OsmAndPoiNameIndexData.SUFFIXESDICTIONARY_FIELD_NUMBER:
                        String encodedSuffix = index.getInputStream().readString();
                        if (!SearchAlgorithms.OLD_EMPTY_SUFFIX_DICTIONARY_SENTINEL.equals(encodedSuffix)) {
                            suffixIndex++;
                        }
                        break;
                    case OsmandOdb.OsmAndPoiNameIndex.OsmAndPoiNameIndexData.SUFFIXESCOMMONDICTIONARY_FIELD_NUMBER:
                        index.getInputStream().readUInt32();
                        suffixIndex++;
                        break;
                    case OsmandOdb.OsmAndPoiNameIndex.OsmAndPoiNameIndexData.ATOMS_FIELD_NUMBER:
                        int atomLength = index.getInputStream().readRawVarint32();
                        long atomOldLimit = index.getInputStream().pushLimitLong(atomLength);
                        try {
                            readPoiTokenAtomBySuffix(index, refsBySuffix, atomLength + computeVarint32Size(atomLength));
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

    default Map<Integer, AddressTokenRefs> readAddressTokenRefsBySuffix(BinaryMapIndexReaderExt index, int tokenOffset) throws IOException {
        Map<Integer, AddressTokenRefs> refsBySuffix = new LinkedHashMap<>();
        index.getInputStream().seek(tokenOffset);
        long length = index.getInputStream().readRawVarint32();
        long oldLimit = index.getInputStream().pushLimitLong(length);
        try {
            int suffixIndex = 0;
            while (true) {
                int tagWithType = index.getInputStream().readTag();
                int tag = WireFormat.getTagFieldNumber(tagWithType);
                switch (tag) {
                    case 0:
                        return refsBySuffix;
                    case OsmandOdb.OsmAndAddressNameIndexData.AddressNameIndexData.SUFFIXESDICTIONARY_FIELD_NUMBER:
                        String encodedSuffix = index.getInputStream().readString();
                        if (!SearchAlgorithms.OLD_EMPTY_SUFFIX_DICTIONARY_SENTINEL.equals(encodedSuffix)) {
                            suffixIndex++;
                        }
                        break;
                    case OsmandOdb.OsmAndAddressNameIndexData.AddressNameIndexData.SUFFIXESCOMMONDICTIONARY_FIELD_NUMBER:
                        index.getInputStream().readUInt32();
                        suffixIndex++;
                        break;
                    case OsmandOdb.OsmAndAddressNameIndexData.AddressNameIndexData.ATOM_FIELD_NUMBER:
                        int atomLength = index.getInputStream().readRawVarint32();
                        long atomOldLimit = index.getInputStream().pushLimitLong(atomLength);
                        try {
                            readAddressTokenAtomBySuffix(index, tokenOffset, atomLength + computeVarint32Size(atomLength), refsBySuffix);
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

    default Map<Integer, PoiTokenRefs> immutablePoiRefsBySuffix(Map<Integer, PoiTokenRefs> refsBySuffix) {
        if (refsBySuffix == null || refsBySuffix.isEmpty()) {
            return Map.of();
        }
        Map<Integer, PoiTokenRefs> out = new LinkedHashMap<>();
        for (Map.Entry<Integer, PoiTokenRefs> entry : refsBySuffix.entrySet()) {
            PoiTokenRefs refs = entry.getValue();
            if (refs == null) {
                continue;
            }
            Map<Integer, int[]> poiIndexes = new LinkedHashMap<>();
            if (refs.poiIndexes() != null) {
                for (Map.Entry<Integer, int[]> poiEntry : refs.poiIndexes().entrySet()) {
                    if (poiEntry.getKey() != null && poiEntry.getValue() != null) {
                        poiIndexes.put(poiEntry.getKey(), poiEntry.getValue());
                    }
                }
            }
            out.put(entry.getKey(), new PoiTokenRefs(refs.offsets(), refs.atomRefs(), refs.atomSizes(), Map.copyOf(poiIndexes),
                    refs.metricSuffixIndexes() == null ? Set.of() : Set.copyOf(refs.metricSuffixIndexes()),
                    refs.metricIntegerSuffixes() == null ? Set.of() : Set.copyOf(refs.metricIntegerSuffixes()),
                    refs.metricExtraSuffixes() == null ? Set.of() : Set.copyOf(refs.metricExtraSuffixes())));
        }
        return Map.copyOf(out);
    }

    default void readPoiTokenAtomBySuffix(BinaryMapIndexReaderExt index,
                                          Map<Integer, PoiTokenRefs> refsBySuffix,
                                          int atomSize) throws IOException {
        int shift = Integer.MIN_VALUE;
        List<Integer> poiIndexes = new ArrayList<>();
        List<Integer> matchedSuffixIndexes = new ArrayList<>();
        Set<Integer> enabledSuffixIndexes = new LinkedHashSet<>();
        Set<String> integerSuffixes = new LinkedHashSet<>();
        Set<String> extraSuffixes = new LinkedHashSet<>();
        int maskIndex = 0;
        int[] previousMask = new int[]{0};
        while (true) {
            int tagWithType = index.getInputStream().readTag();
            int tag = WireFormat.getTagFieldNumber(tagWithType);
            switch (tag) {
                case 0:
                    if (shift != Integer.MIN_VALUE && !matchedSuffixIndexes.isEmpty()) {
                        for (int suffixIndex : matchedSuffixIndexes) {
                            PoiTokenRefs refs = refsBySuffix.computeIfAbsent(suffixIndex, ignored -> emptyPoiTokenRefs());
                            refs.offsets().add(shift);
                            refs.atomRefs().add(shift);
                            refs.atomSizes().add(atomSize);
                            refs.metricSuffixIndexes().addAll(enabledSuffixIndexes);
                            refs.metricIntegerSuffixes().addAll(integerSuffixes);
                            refs.metricExtraSuffixes().addAll(extraSuffixes);
                            if (!poiIndexes.isEmpty()) {
                                Set<Integer> merged = new LinkedHashSet<>();
                                int[] existing = refs.poiIndexes().get(shift);
                                if (existing != null) {
                                    for (int existingIndex : existing) {
                                        merged.add(existingIndex);
                                    }
                                }
                                merged.addAll(poiIndexes);
                                refs.poiIndexes().put(shift, toIntArray(merged));
                            }
                        }
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
                case OsmandOdb.OsmAndPoiNameIndexDataAtom.POIINDINBLOCK_FIELD_NUMBER:
                    poiIndexes.add(index.getInputStream().readUInt32());
                    break;
                case OsmandOdb.OsmAndPoiNameIndexDataAtom.SUFFIXESBITSETINDEX_FIELD_NUMBER:
                    collectMatchedSuffixIndex(matchedSuffixIndexes, enabledSuffixIndexes, integerSuffixes, maskIndex, previousMask,
                            index.getInputStream().readUInt32());
                    maskIndex++;
                    break;
                case OsmandOdb.OsmAndPoiNameIndexDataAtom.OTHERWORDSCOUNT_FIELD_NUMBER:
                    index.getInputStream().readUInt32();
                    break;
                case OsmandOdb.OsmAndPoiNameIndexDataAtom.EXTRASUFFIX_FIELD_NUMBER:
                    String extraSuffix = index.getInputStream().readString();
                    if (!Algorithms.isEmpty(extraSuffix)) {
                        extraSuffixes.add(extraSuffix);
                    }
                    break;
                default:
                    InspectorService.skipUnknownField(index.getInputStream(), tagWithType);
                    break;
            }
        }
    }

    default void readAddressTokenAtomBySuffix(BinaryMapIndexReaderExt index,
                                              int tokenOffset,
                                              int atomSize,
                                              Map<Integer, AddressTokenRefs> refsBySuffix) throws IOException {
        int objectOffset = 0;
        int shiftToIndex = 0;
        int cityOffset = 0;
        int shiftToCityIndex = 0;
        int typeIndex = -1;
        List<Integer> matchedSuffixIndexes = new ArrayList<>();
        Set<Integer> enabledSuffixIndexes = new LinkedHashSet<>();
        Set<String> integerSuffixes = new LinkedHashSet<>();
        Set<String> extraSuffixes = new LinkedHashSet<>();
        int maskIndex = 0;
        int[] previousMask = new int[]{0};
        while (true) {
            int tagWithType = index.getInputStream().readTag();
            int tag = WireFormat.getTagFieldNumber(tagWithType);
            if (tag == 0 || tag == OsmandOdb.AddressNameIndexDataAtom.SHIFTTOINDEX_FIELD_NUMBER) {
                addAddressAtomRefsBySuffix(refsBySuffix, matchedSuffixIndexes, enabledSuffixIndexes, shiftToIndex, shiftToCityIndex,
                        objectOffset, cityOffset, typeIndex, atomSize, integerSuffixes, extraSuffixes);
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
                case OsmandOdb.AddressNameIndexDataAtom.SUFFIXESBITSETINDEX_FIELD_NUMBER:
                    collectMatchedSuffixIndex(matchedSuffixIndexes, enabledSuffixIndexes, integerSuffixes, maskIndex, previousMask,
                            index.getInputStream().readUInt32());
                    maskIndex++;
                    break;
                case OsmandOdb.AddressNameIndexDataAtom.SHIFTTOINDEX_FIELD_NUMBER:
                    shiftToIndex = index.getInputStream().readInt32();
                    objectOffset = tokenOffset - shiftToIndex;
                    break;
                case OsmandOdb.AddressNameIndexDataAtom.TYPE_FIELD_NUMBER:
                    typeIndex = index.getInputStream().readInt32();
                    break;
                case OsmandOdb.AddressNameIndexDataAtom.EXTRASUFFIX_FIELD_NUMBER:
                    String extraSuffix = index.getInputStream().readString();
                    if (!Algorithms.isEmpty(extraSuffix)) {
                        extraSuffixes.add(extraSuffix);
                    }
                    break;
                case OsmandOdb.AddressNameIndexDataAtom.OTHERWORDSCOUNT_FIELD_NUMBER:
                    index.getInputStream().readUInt32();
                    break;
                default:
                    InspectorService.skipUnknownField(index.getInputStream(), tagWithType);
                    break;
            }
        }
    }

    default void addAddressAtomRefsBySuffix(Map<Integer, AddressTokenRefs> refsBySuffix,
                                            List<Integer> matchedSuffixIndexes,
                                            Set<Integer> enabledSuffixIndexes,
                                            int shiftToIndex,
                                            int shiftToCityIndex,
                                            int objectOffset,
                                            int cityOffset,
                                            int typeIndex,
                                            int atomSize,
                                            Set<String> integerSuffixes,
                                            Set<String> extraSuffixes) {
        if (matchedSuffixIndexes == null || matchedSuffixIndexes.isEmpty()) {
            return;
        }
        for (int suffixIndex : matchedSuffixIndexes) {
            AddressTokenRefs refs = refsBySuffix.computeIfAbsent(suffixIndex, ignored -> new AddressTokenRefs());
            refs.metricSuffixIndexes().addAll(enabledSuffixIndexes == null ? matchedSuffixIndexes : enabledSuffixIndexes);
            if (integerSuffixes != null) {
                refs.metricIntegerSuffixes().addAll(integerSuffixes);
            }
            if (extraSuffixes != null) {
                refs.metricExtraSuffixes().addAll(extraSuffixes);
            }
            if (typeIndex >= 0 && typeIndex < BinaryMapAddressReaderAdapter.CityBlocks.STREET_TYPE.index && objectOffset != 0) {
                refs.cityOffsets().add(objectOffset);
            }
            if (typeIndex == BinaryMapAddressReaderAdapter.CityBlocks.STREET_TYPE.index && objectOffset != 0) {
                refs.streetOffsets().add(objectOffset);
                refs.streetCityOffsets().add(cityOffset);
            }
            if (shiftToIndex != 0) {
                refs.addressRefs().add(new AddressRef(shiftToIndex, shiftToCityIndex, objectOffset, cityOffset, typeIndex, atomSize));
            }
        }
    }

    default void collectMatchedSuffixIndex(List<Integer> matchedSuffixIndexes, Set<Integer> enabledSuffixIndexes,
                                           Set<String> integerSuffixes,
                                           int maskIndex, int[] previousMask, int suffixBitsetIndex) {
        if (suffixBitsetIndex % 2 == 0 && suffixBitsetIndex != 0 && enabledSuffixIndexes != null) {
            enabledSuffixIndexes.add(suffixBitsetIndex / 2 - 1);
        }
        if (maskIndex == 0 && previousMask != null && previousMask.length > 0) {
            previousMask[0] = 0;
        }
        if (previousMask != null && previousMask.length > 0
                && previousMask[0] == 0
                && suffixBitsetIndex % 2 == 0
                && suffixBitsetIndex != 0) {
            matchedSuffixIndexes.add(suffixBitsetIndex / 2 - 1);
        }
        String integerSuffix = formatOddSuffixBitsetValue(suffixBitsetIndex);
        if (!Algorithms.isEmpty(integerSuffix) && integerSuffixes != null) {
            integerSuffixes.add(integerSuffix);
        }
        if (previousMask != null && previousMask.length > 0) {
            previousMask[0] = suffixBitsetIndex;
        }
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
                if (key != null && (
                		Amenity.NAME.equals(key) || key.startsWith(Amenity.NAME + ":")  
                		|| isTagNonIndexedForSearchAsName (key) || isTagIndexedForSearchAsName(key) 
                		|| isTagIndexedForSearchAsId(key))) {
                    if (!Algorithms.isEmpty(entry.getValue())) {
                        candidateNames.add(entry.getValue());
                    }
                }
            }
        }
        CommonWords defaultInstance = CommonWords.getInstance();
        for (String candidateName : candidateNames) {
            List<String> tokens = SearchAlgorithms.splitAndNormalize(candidateName, true);
            SearchAlgorithms.removeCommonWords(defaultInstance, tokens);
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

    default String[] toStringArray(Collection<String> values) {
        if (values == null || values.isEmpty()) {
            return new String[0];
        }
        List<String> out = new ArrayList<>();
        for (String value : values) {
            if (!Algorithms.isEmpty(value)) {
                out.add(value);
            }
        }
        return out.toArray(new String[0]);
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

    default ObjectAddress loadStreetObjectAddress(BinaryMapIndexReaderExt index,
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

    default void collectPoiObjectsByStoredOffsets(BinaryMapIndexReaderExt index,
                                                  Map<BinaryMapPoiReaderAdapter.PoiRegion, Set<Integer>> storedPoiOffsets,
                                                  Map<BinaryMapPoiReaderAdapter.PoiRegion, Map<Integer, Set<Integer>>> storedPoiIndexes,
                                                  List<BinaryMapPoiReaderAdapter.PoiRegion> poiRegions,
                                                  List<ObjectAddress> results,
                                                  String lang) throws IOException {
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
                Map<Integer, Set<Integer>> indexesByOffset = storedPoiIndexes == null ? null : storedPoiIndexes.get(poiRegion);
                Set<Integer> objectIndexes = indexesByOffset == null ? null : indexesByOffset.get(relativeOffset);
                readPoiObjectsAtShift(index, poiRegion, tagGroups, relativeOffset, objectIndexes, results, lang);
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

    default Map<BinaryMapPoiReaderAdapter.PoiRegion, Map<Integer, Set<Integer>>> mapPoiIndexes(Map<Integer, int[]> poiIndexes,
                                                                                               List<BinaryMapPoiReaderAdapter.PoiRegion> poiRegions) {
        Map<BinaryMapPoiReaderAdapter.PoiRegion, Map<Integer, Set<Integer>>> indexesByRegion = new LinkedHashMap<>();
        if (poiIndexes == null || poiIndexes.isEmpty() || poiRegions == null || poiRegions.isEmpty()) {
            return indexesByRegion;
        }
        for (Map.Entry<Integer, int[]> entry : poiIndexes.entrySet()) {
            Integer poiRef = entry.getKey();
            int[] indexes = entry.getValue();
            if (poiRef == null || indexes == null || indexes.length == 0) {
                continue;
            }
            for (BinaryMapPoiReaderAdapter.PoiRegion poiRegion : poiRegions) {
                if (poiRef < 0 || poiRef >= poiRegion.getLength()) {
                    continue;
                }
                Set<Integer> objectIndexes = indexesByRegion
                        .computeIfAbsent(poiRegion, ignored -> new LinkedHashMap<>())
                        .computeIfAbsent(poiRef, ignored -> new LinkedHashSet<>());
                for (int index : indexes) {
                    objectIndexes.add(index);
                }
            }
        }
        return indexesByRegion;
    }

    default void readPoiObjectsAtShift(BinaryMapIndexReaderExt index,
                                       BinaryMapPoiReaderAdapter.PoiRegion region,
                                       Map<Integer, List<TagValuePair>> tagGroups,
                                       int relativeOffset,
                                       Set<Integer> objectIndexes,
                                       List<ObjectAddress> results,
                                       String lang) throws IOException {
        if (objectIndexes == null || objectIndexes.isEmpty()) {
            return;
        }
        index.getInputStream().seek(region.getFilePointer() + relativeOffset);
        long length = readInt(index.getInputStream());
        long oldLimit = index.getInputStream().pushLimitLong(length);
        try {
            int x = 0;
            int y = 0;
            int zoom = 0;
            int poiIndex = 0;
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
                            boolean shouldRead = objectIndexes.contains(poiIndex);
                            RawPoiObject rawPoiObject = shouldRead
                                    ? readRawPoiObject(index.getInputStream(), x, y, zoom, region, tagGroups)
                                    : null;
                            if (shouldRead && rawPoiObject != null) {
                                ObjectAddress objectAddress = toPoiObjectAddress(rawPoiObject, lang);
                                int payloadSize = poiLength + computeVarint32Size(poiLength);
                                int payloadOffset = (int) (index.getInputStream().getTotalBytesRead() - poiLength);
                                results.add(new ObjectAddress(0, objectAddress.name(), objectAddress.point(), objectAddress.commonTags(), objectAddress.isPoi(), true, false, false, objectAddress.type(), objectAddress.osmId(), objectAddress.osmType(), payloadOffset, payloadSize, (int) (region.getFilePointer() + relativeOffset)));
                            }
                        } finally {
                            consumeRemainingInLimit(index.getInputStream());
                            poiIndex++;
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
            String key = entry.getKey();
            if (!Algorithms.isEmpty(key) && entry.getValue() != null && !entry.getValue().isEmpty()
                    && (!rawPoiObject.decodedSubcategories.containsKey(key) || isPoiSearchIndexedTextTag(key))) {
                values.put(key, String.join("; ", entry.getValue()));
            }
        }
        String displayName = selectPoiDisplayName(rawPoiObject, lang);
        Long osmId = rawPoiObject.id > 0 ? ObfConstants.getOsmIdFromMapObjectId(rawPoiObject.id) : null;
        String osmType = decodePoiOsmType(rawPoiObject.id);
        return new ObjectAddress(0, displayName, location, arrangeObjectAddressValues(values),
                true, false, false, false, "POI", osmId, osmType, 0, 0, 0);
    }

    default boolean isPoiSearchIndexedTextTag(String key) {
        return !Algorithms.isEmpty(key) && 
        		(isTagIndexedForSearchAsName(key)
                || isTagIndexedForSearchAsId(key) 
                || isTagIndexedAsSearchRelated(key)
                || isTagNonIndexedForSearchAsName(key)
                || Amenity.ROUTE_MEMBERS_IDS.equals(key));
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
                IndexToken loadedToken = findIndexTokenByName(obf, token.name());
                if (loadedToken != null) {
                    token = loadedToken;
                }
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
                Map<BinaryMapPoiReaderAdapter.PoiRegion, Map<Integer, Set<Integer>>> storedPoiIndexes = mapPoiIndexes(token.poiIndexes(), poiRegions);
                if (hasAddressRefs) {
                    for (BinaryMapAddressReaderAdapter.AddressRegion addressRegion : addressRegions) {
                        collectAddressObjects(index, addressRegion, addressRefs, results, lang);
                    }
                }
                if (hasPoiRefs) {
                    collectPoiObjectsByStoredOffsets(index, storedPoiOffsets, storedPoiIndexes, poiRegions, results, lang);
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
		Set<String> visitedObfs = new HashSet<>();
		for (String obf : obfs) {
			if (obf == null || obf.isBlank()) {
				continue;
			}
			if (!visitedObfs.add(obf)) {
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
				if (source.length > 0) {
					target[0] += source[0];
				}
				if (source.length > 1 && source[1] > 0) {
					target[1] = 1;
				}
				if (source.length > 2) {
					target[2] += source[2];
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
