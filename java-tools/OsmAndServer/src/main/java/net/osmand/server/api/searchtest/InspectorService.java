package net.osmand.server.api.searchtest;

import net.osmand.ResultMatcher;
import net.osmand.binary.*;
import net.osmand.data.*;
import net.osmand.util.Algorithms;
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
	
    default IndexTokenPage getIndex(String obf, String prefix, int pageToShow, int pageSizeLimit, String sortBy, String sortOrder, boolean isPOI) {
        File file = new File(obf);
        Pattern prefixPattern = compileIndexPrefixPattern(prefix);
        final int safePage = Math.max(pageToShow, 0);
        final int safeSize = Math.max(1, Math.min(pageSizeLimit, 100));
        boolean objectTypeFilter = isPOI;
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
            return null;
        } catch (Exception e) {
            getLogger().error("Failed to read OBF index {}", file, e);
            throw new RuntimeException("Failed to read OBF index: " + e.getMessage(), e);
        }
    }

    default IndexTokenPage getIndex(List<String> obfs, String prefix, int pageToShow, int pageSizeLimit, String sortBy, String sortOrder,
                                    boolean isPoi) {
        Pattern prefixPattern = compileIndexPrefixPattern(prefix);
        final int safePage = Math.max(pageToShow, 0);
        final int safeSize = Math.max(1, Math.min(pageSizeLimit, 100));
        Boolean objectTypeFilter = isPoi;
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

                        }
                    }
                    getLogger().info("getIndex obfPart={} prefix={} objectType={} sourceTokens={} mergedSoFar={} elapsedMs={}",
                            new File(obf).getName(), prefix, isPoi, allTokens.size(), mergedByName.size(),
                            elapsedMs(System.nanoTime() - obfStartedNs));
                }
            }
            long mergeNs = System.nanoTime() - mergeStartedNs;
            long listStartedNs = System.nanoTime();
            List<IndexToken> results = new ArrayList<>(mergedByName.values());
            long listNs = System.nanoTime() - listStartedNs;
            long summaryStartedNs = System.nanoTime();
            long summaryNs = System.nanoTime() - summaryStartedNs;
            long sortStartedNs = System.nanoTime();
            long sortNs = System.nanoTime() - sortStartedNs;
            long pageStartedNs = System.nanoTime();
            long totalElements = results.size();
            int totalPages = totalElements == 0 ? 0 : (int) ((totalElements + safeSize - 1) / safeSize);
            int fromIndex = Math.min(safePage * safeSize, results.size());
            int toIndex = Math.min(fromIndex + safeSize, results.size());

            long pageNs = System.nanoTime() - pageStartedNs;
            getLogger().info("getIndex obfs={} prefix={} objectType={} page={}/{} size={} sourceTokens={} merged={} content={} timingsMs merge={} list={} summary={} sort={} page={} total={}",
                    loadedObfs, prefix, isPoi, safePage, totalPages, safeSize, sourceTokens, totalElements, 0,
                    elapsedMs(mergeNs), elapsedMs(listNs), elapsedMs(summaryNs), elapsedMs(sortNs), elapsedMs(pageNs),
                    elapsedMs(System.nanoTime() - startedNs));
            return null;
        } catch (Exception e) {
            getLogger().error("Failed to read OBF indexes {}", obfs, e);
            throw new RuntimeException("Failed to read OBF indexes: " + e.getMessage(), e);
        }
    }

	default long elapsedMs(long elapsedNs) {
		return TimeUnit.NANOSECONDS.toMillis(elapsedNs);
	}
	
	default boolean matchesIndexTokenObjectType(IndexToken token, Boolean objectTypeFilter) {
		if (objectTypeFilter == null) {
			return true;
		}
		return objectTypeFilter;
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
        List<IndexToken> loadedTokens = null;
        List<IndexToken> cachedTokens = List.copyOf(loadedTokens);
        synchronized (INDEX_TOKENS_CACHE) {
            INDEX_TOKENS_CACHE.put(cacheKey, new CachedIndexTokens(cacheKey, fileLength, lastModified, cachedTokens));
        }
        return cachedTokens;
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

    default int safeMetricInt(long value) {
        if (value <= 0L) {
            return 0;
        }
        return value >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) value;
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
                true, false, "POI", osmId, osmType, 0, 0, 0, "");
    }

    default boolean isPoiSearchIndexedTextTag(String key) {
        return !Algorithms.isEmpty(key) && (isTagIndexedForSearchAsName(key)
                || isTagIndexedForSearchAsId(key) || isTagIndexedAsSearchRelated(key)
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
            markedResults.add(new ObjectAddress(objectAddress.sequenceId(), objectAddress.name(), objectAddress.point(), objectAddress.commonTags(), objectAddress.isPoi(), isAlone, objectAddress.type(),
                    objectAddress.osmId(), objectAddress.osmType(), objectAddress.payloadOffset(),
                    objectAddress.payloadSize(), objectAddress.sourceOffset(), objectAddress.obf()));
        }
        return markedResults;
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
                                         boolean isPOI) {
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
                return null;
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
                                         boolean isPOI) {
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
                ObjectAddressPage page = getObjects(obf, lang, obfToken, regExp, 0, Integer.MAX_VALUE, sortBy, sortOrder, isFiltered, isPOI);
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
                objectAddress.commonTags(), objectAddress.isPoi(), objectAddress.isAlone(), objectAddress.type(), objectAddress.osmId(),
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
            case "matched" -> Comparator.comparingInt(object -> object != null ? 1 : 0);
            case "alone", "isalone" -> Comparator.comparingInt(object -> object != null && object.isAlone() ? 1 : 0);
            default ->
                    Comparator.comparing(object -> object == null || object.name() == null ? "" : object.name(), String.CASE_INSENSITIVE_ORDER);
        };
        comparator = comparator.thenComparingInt(object -> object == null ? Integer.MAX_VALUE : object.sequenceId())
                .thenComparing(object -> object == null || object.name() == null ? "" : object.name(), String.CASE_INSENSITIVE_ORDER);
        return "desc".equalsIgnoreCase(sortOrder) ? comparator.reversed() : comparator;
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
