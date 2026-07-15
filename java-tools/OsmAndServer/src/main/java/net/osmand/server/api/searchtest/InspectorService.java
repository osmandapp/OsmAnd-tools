package net.osmand.server.api.searchtest;

import net.osmand.binary.*;
import net.osmand.data.*;
import net.osmand.util.Algorithms;
import net.osmand.util.SearchAlgorithms;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static net.osmand.binary.ObfConstants.*;

public interface InspectorService extends OBFService {
    int INDEX_TOKEN_CACHE_LIMIT = 32;
    Map<String, CachedIndexTokens> INDEX_TOKENS_CACHE = Collections.synchronizedMap(
            new LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, CachedIndexTokens> eldest) {
                    return size() > INDEX_TOKEN_CACHE_LIMIT;
                }
            });
    Map<String, List<CommonSuffix>> INDEX_COMMON_SUFFIX_CACHE = Collections.synchronizedMap(
            new LinkedHashMap<>(1024, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, List<CommonSuffix>> eldest) {
                    return size() > 65536;
                }
            });

    /**
     * Reader-side posting model. One atom is one name-index posting connected to one or more
     * token suffixes by suffixesBitsetIndex. Current OBF writers emit one object per atom, but
     * repeated proto fields are preserved here so Inspector can expose malformed or legacy data.
     */
    abstract class Atom {
        private final String obf;
        private final int atomOrder, nameIndexDataOffset, suffixIndex, atomSize;
        private final int[] suffixesBitsetIndex, otherWordsCount;
        private final String[] extraSuffix;
        private final byte[] bbox;
        
        protected Atom(String obf, int atomOrder, int nameIndexDataOffset, int suffixIndex, int atomSize,
                       int[] suffixesBitsetIndex, int[] otherWordsCount, String[] extraSuffix, byte[] bbox) {
            this.obf = obf;
            this.atomOrder = atomOrder;
            this.nameIndexDataOffset = nameIndexDataOffset;
            this.suffixIndex = suffixIndex;
            this.atomSize = atomSize;
            this.suffixesBitsetIndex = suffixesBitsetIndex == null ? new int[0] : suffixesBitsetIndex;
            this.otherWordsCount = otherWordsCount == null ? new int[0] : otherWordsCount;
            this.extraSuffix = extraSuffix == null ? new String[0] : extraSuffix;
            this.bbox = bbox == null ? new byte[0] : bbox;
        }

        public String obf() {
            return obf;
        }

        public int atomOrder() {
            return atomOrder;
        }

        public int nameIndexDataOffset() {
            return nameIndexDataOffset;
        }

        public int suffixIndex() {
            return suffixIndex;
        }

        public int atomSize() {
            return atomSize;
        }

        public int[] suffixesBitsetIndex() {
            return suffixesBitsetIndex;
        }

        public int[] otherWordsCount() {
            return otherWordsCount;
        }

        public String[] extraSuffix() {
            return extraSuffix;
        }
        
        public byte[] bbox() {
            return bbox;
        }
        
        public abstract boolean isPoi();

        public abstract int objectRefCount();
    }

    // Complies with OsmAndPoiNameIndexDataAtom in OBF.proto.
    class POIAtom extends Atom {
        private final int zoom;
        private final int x;
        private final int y;
        private final int[] poiIndInBlock;
        private final int shiftTo;

        public POIAtom(String obf, int atomOrder, int nameIndexDataOffset, int suffixIndex, int atomSize,
                       int[] suffixesBitsetIndex, int[] otherWordsCount, String[] extraSuffix,
                       int zoom, int x, int y, int[] poiIndInBlock, byte[] bbox, int shiftTo) {
            super(obf, atomOrder, nameIndexDataOffset, suffixIndex, atomSize, suffixesBitsetIndex, otherWordsCount, extraSuffix, bbox);
            this.zoom = zoom;
            this.x = x;
            this.y = y;
            this.poiIndInBlock = poiIndInBlock == null ? new int[0] : poiIndInBlock;
            this.shiftTo = shiftTo;
        }

        public int zoom() {
            return zoom;
        }

        public int x() {
            return x;
        }

        public int y() {
            return y;
        }

        public int[] poiIndInBlock() {
            return poiIndInBlock;
        }

        public int shiftTo() {
            return shiftTo;
        }

        @Override
        public boolean isPoi() {
            return true;
        }

        @Override
        public int objectRefCount() {
            return poiIndInBlock.length > 0 ? poiIndInBlock.length : shiftTo == 0 ? 0 : 1;
        }
    }

    // Complies with AddressNameIndexDataAtom in OBF.proto.
    class AddressAtom extends Atom {
        private final int type;
        private final int enclosingObjects;
        private final int[] shiftToIndex;
        private final int[] shiftToCityIndex;
        private final int[] xy16;

        public AddressAtom(String obf, int atomOrder, int nameIndexDataOffset, int suffixIndex, int atomSize,
                           int[] suffixesBitsetIndex, int[] otherWordsCount, String[] extraSuffix,
                           int type, byte[] bbox, int enclosingObjects,
                           int[] shiftToIndex, int[] shiftToCityIndex, int[] xy16) {
            super(obf, atomOrder, nameIndexDataOffset, suffixIndex, atomSize, suffixesBitsetIndex, otherWordsCount, extraSuffix, bbox);
            this.type = type;
            this.enclosingObjects = enclosingObjects;
            this.shiftToIndex = shiftToIndex == null ? new int[0] : shiftToIndex;
            this.shiftToCityIndex = shiftToCityIndex == null ? new int[0] : shiftToCityIndex;
            this.xy16 = xy16 == null ? new int[0] : xy16;
        }

        public int type() {
            return type;
        }

        public int enclosingObjects() {
            return enclosingObjects;
        }

        public int[] shiftToIndex() {
            return shiftToIndex;
        }

        public int[] shiftToCityIndex() {
            return shiftToCityIndex;
        }

        public int[] xy16() {
            return xy16;
        }

        @Override
        public boolean isPoi() {
            return false;
        }

        @Override
        public int objectRefCount() {
            return shiftToIndex.length;
        }
    }

    record IndexToken(String name, boolean isPoi, Atom[] atoms, boolean isCommon, boolean isFrequent, String obf, int count) {
        public IndexToken {
            atoms = filterAtomsByType(atoms, isPoi);
            count = count > 0 ? count : objectRefCount(atoms);
        }

        public int atomCount() {
            return atoms.length;
        }

        public int objectRefCount() {
            return objectRefCount(atoms);
        }

        public int poiAtomCount() {
            return isPoi ? atoms.length : 0;
        }

        public int addressAtomCount() {
            return isPoi ? 0 : atoms.length;
        }

        public POIAtom[] poiAtoms() {
            List<POIAtom> result = new ArrayList<>();
            for (Atom atom : atoms) {
                if (atom instanceof POIAtom poiAtom) {
                    result.add(poiAtom);
                }
            }
            return result.toArray(new POIAtom[0]);
        }

        public AddressAtom[] addressAtoms() {
            List<AddressAtom> result = new ArrayList<>();
            for (Atom atom : atoms) {
                if (atom instanceof AddressAtom addressAtom) {
                    result.add(addressAtom);
                }
            }
            return result.toArray(new AddressAtom[0]);
        }

        private static int objectRefCount(Atom[] atoms) {
            if (atoms == null || atoms.length == 0) {
                return 0;
            }
            long count = 0;
            for (Atom atom : atoms) {
                if (atom != null) {
                    count += atom.objectRefCount();
                }
            }
            return count >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) count;
        }

        private static Atom[] filterAtomsByType(Atom[] atoms, boolean isPoi) {
            if (atoms == null || atoms.length == 0) {
                return new Atom[0];
            }
            List<Atom> filtered = new ArrayList<>(atoms.length);
            for (Atom atom : atoms) {
                if (atom != null && atom.isPoi() == isPoi) {
                    filtered.add(atom);
                }
            }
            return filtered.toArray(new Atom[0]);
        }
    }
    record IndexTokenPage(List<IndexToken> content, int pageToShow, int pageSizeLimit, long totalElements, int totalPages) {}

    record CommonSuffix(String value, int matched, int nonindexed) {}

    record ObjectAddress(int sequenceId, String name, LatLon point, Map<String, String> commonTags,
                         boolean isPoi, boolean isAlone, String type, Long osmId,
                         String osmType, int payloadOffset, int payloadSize, int sourceOffset, String obf) {
    }

    record ObjectAddressPage(List<ObjectAddress> content, int pageToShow, int pageSizeLimit, long totalElements, int totalPages, int[] countMetrics, int[] sizeMetrics, int aloneCount, int aloneSize) {}
    record CachedIndexTokens(String cacheKey, long fileLength, long lastModified, List<IndexToken> tokens) {}

    default IndexTokenPage getIndex(String obf, String prefix, int pageToShow, int pageSizeLimit, String sortBy, String sortOrder, boolean isPOI) {
        File file = new File(obf);
        Pattern prefixPattern = compileIndexPrefixPattern(prefix);
        final int safePage = Math.max(pageToShow, 0);
        final int safeSize = Math.max(1, Math.min(pageSizeLimit, 100));
        long startedNs = System.nanoTime();
        try {
            long loadStartedNs = System.nanoTime();
            List<IndexToken> allTokens = getCachedOrLoadIndexTokens(file, isPOI);
            long loadNs = System.nanoTime() - loadStartedNs;
            long filterStartedNs = System.nanoTime();
            List<IndexToken> results = new ArrayList<>();
            if (prefixPattern == null) {
                for (IndexToken token : allTokens) {
                    if (matchesIndexTokenObjectType(token, isPOI)) {
                        results.add(token);
                    }
                }
            } else {
                for (IndexToken token : allTokens) {
                    if (prefixPattern.matcher(token.name()).find()) {
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
                    List<IndexToken> allTokens = getCachedOrLoadIndexTokens(new File(obf), isPoi);
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
		if (token == null) {
			return false;
		}
		return token.isPoi() == objectTypeFilter;
	}
	
    default List<IndexToken> getCachedOrLoadIndexTokens(File file) throws IOException {
        return getCachedOrLoadIndexTokens(file, true);
    }

    default List<IndexToken> getCachedOrLoadIndexTokens(File file, boolean isPoi) throws IOException {
        String cacheKey = getIndexCacheKey(file, isPoi);
        long fileLength = file.length();
        long lastModified = file.lastModified();
        CachedIndexTokens cached;
        synchronized (INDEX_TOKENS_CACHE) {
            cached = INDEX_TOKENS_CACHE.get(cacheKey);
        }
        if (cached != null && cached.fileLength() == fileLength && cached.lastModified() == lastModified) {
            return cached.tokens();
        }
        List<IndexToken> loadedTokens = loadIndexTokens(file, isPoi);
        if (loadedTokens == null) {
            loadedTokens = List.of();
        }
        List<IndexToken> cachedTokens = List.copyOf(loadedTokens);
        synchronized (INDEX_TOKENS_CACHE) {
            INDEX_TOKENS_CACHE.put(cacheKey, new CachedIndexTokens(cacheKey, fileLength, lastModified, cachedTokens));
        }
        return cachedTokens;
    }

    default List<IndexToken> loadIndexTokens(File file, boolean isPoi) throws IOException {
        return List.of();
    }

    default String getIndexCacheKey(File file) {
        return getIndexCacheKey(file, true);
    }

    default String getIndexCacheKey(File file, boolean isPoi) {
        return file.getName() + "|" + (isPoi ? "poi" : "address");
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

    default int safeMetricInt(long value) {
        if (value <= 0L) {
            return 0;
        }
        return value >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) value;
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
                                         boolean isPoi) {
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
                IndexToken obfToken = findIndexTokenByName(obf, token.name(), isPOI);
                if (obfToken == null) {
                    continue;
                }
                ObjectAddressPage page = getObjects(obf, lang, obfToken, regExp, 0, Integer.MAX_VALUE, sortBy, sortOrder, isPOI);
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
        return findIndexTokenByName(obf, tokenName, true);
    }

    default IndexToken findIndexTokenByName(String obf, String tokenName, boolean isPoi) {
        if (Algorithms.isEmpty(obf) || tokenName == null) {
            return null;
        }
        File file = new File(obf);
        try {
            for (IndexToken token : getCachedOrLoadIndexTokens(file, isPoi)) {
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
}
