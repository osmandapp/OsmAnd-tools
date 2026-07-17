package net.osmand.server.api.searchtest;

import com.google.protobuf.CodedInputStream;
import net.osmand.binary.*;
import net.osmand.binary.BinaryMapIndexReader.TagValuePair;
import net.osmand.data.*;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
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
        protected final String obf;
        protected final int atomOrder, nameIndexDataOffset, suffixIndex, atomSize, commonSuffixOffset;
        protected final int[] suffixesBitsetIndex, otherWordsCount;
        protected final String[] suffixesDictionary, extraSuffixes;
        protected final byte[] bbox;
        
        protected Atom(String obf, int atomOrder, int nameIndexDataOffset, int suffixIndex, int atomSize,
                       int[] suffixesBitsetIndex, int[] otherWordsCount, String[] extraSuffixes, byte[] bbox) {
            this(obf, atomOrder, nameIndexDataOffset, suffixIndex, atomSize,
                    suffixesBitsetIndex, otherWordsCount, new String[0], extraSuffixes, bbox, nameIndexDataOffset);
        }

        protected Atom(String obf, int atomOrder, int nameIndexDataOffset, int suffixIndex, int atomSize,
                       int[] suffixesBitsetIndex, int[] otherWordsCount, String[] suffixesDictionary,
                       String[] extraSuffixes, byte[] bbox, int commonSuffixOffset) {
            this.obf = obf;
            this.atomOrder = atomOrder;
            this.nameIndexDataOffset = nameIndexDataOffset;
            this.suffixIndex = suffixIndex;
            this.atomSize = atomSize;
            this.suffixesBitsetIndex = suffixesBitsetIndex == null ? new int[0] : suffixesBitsetIndex;
            this.otherWordsCount = otherWordsCount == null ? new int[0] : otherWordsCount;
            this.suffixesDictionary = suffixesDictionary == null ? new String[0] : suffixesDictionary;
            this.extraSuffixes = extraSuffixes == null ? new String[0] : extraSuffixes;
            this.bbox = bbox == null ? new byte[0] : bbox;
            this.commonSuffixOffset = commonSuffixOffset;
        }

        public String[] getCommonSuffixes() {
            List<CommonSuffix> suffixes = getSuffixDictionary();
            if (suffixes.isEmpty() || suffixesBitsetIndex.length == 0) {
                return new String[0];
            }
            List<String> resolved = new ArrayList<>();
            for (int value : suffixesBitsetIndex) {
                if (value != 0 && value % 2 == 0) {
                    int index = value / 2 - 1;
                    int commonIndex = index - suffixesDictionary.length;
                    if (commonIndex >= 0 && commonIndex < suffixes.size()) {
                        String suffix = suffixes.get(commonIndex).value();
                        if (!Algorithms.isEmpty(suffix)) {
                            resolved.add(suffix);
                        }
                    }
                }
            }
            return resolved.toArray(new String[0]);
        }
        
        public String[] getPartialSuffixes() {
            if (suffixesDictionary.length == 0 || suffixesBitsetIndex.length == 0) {
                return new String[0];
            }
            List<String> resolved = new ArrayList<>();
            for (int value : suffixesBitsetIndex) {
                if (value != 0 && value % 2 == 0) {
                    int index = value / 2 - 1;
                    if (index >= 0 && index < suffixesDictionary.length) {
                        String suffix = suffixesDictionary[index];
                        if (suffix != null && !suffix.startsWith(" ")) {
                            resolved.add(suffix);
                        }
                    }
                }
            }
            return resolved.toArray(new String[0]);
        }
        
        public String[] getIntSuffixes() {
            if (suffixesBitsetIndex.length == 0) {
                return new String[0];
            }
            List<String> resolved = new ArrayList<>();
            for (int value : suffixesBitsetIndex) {
                if (value % 2 == 1) {
                    resolved.add((value % 4 == 1 ? " " : "") + (value >> 2));
                }
            }
            return resolved.toArray(new String[0]);
        }
        
        public String[] getExtraSuffixes() {
            return extraSuffixes;
        }
        
        public int[] getOtherWordsCount() {
            return otherWordsCount;
        }
        
        public List<CommonSuffix> getSuffixDictionary() {
            List<CommonSuffix> stats = INDEX_COMMON_SUFFIX_CACHE.get(obf + "|" + (isPoi() ? "poi" : "address") + "|" + commonSuffixOffset);
            return stats == null ? List.of() : stats;
        }
        
        public abstract boolean isPoi();

        public abstract int getObjectRefsCount();
    }

    // Complies with OsmAndPoiNameIndexDataAtom in OBF.proto.
    class POIAtom extends Atom {
        protected final int zoom, x, y, shiftTo;
        protected final int[] poiIndInBlock;

        public POIAtom(String obf, int atomOrder, int nameIndexDataOffset, int suffixIndex, int atomSize,
                       int[] suffixesBitsetIndex, int[] otherWordsCount, String[] extraSuffix,
                       int zoom, int x, int y, int[] poiIndInBlock, byte[] bbox, int shiftTo) {
            this(obf, atomOrder, nameIndexDataOffset, suffixIndex, atomSize,
                    suffixesBitsetIndex, otherWordsCount, new String[0], extraSuffix, zoom, x, y, poiIndInBlock, bbox, shiftTo,
                    nameIndexDataOffset);
        }

        public POIAtom(String obf, int atomOrder, int nameIndexDataOffset, int suffixIndex, int atomSize,
                       int[] suffixesBitsetIndex, int[] otherWordsCount, String[] suffixesDictionary, String[] extraSuffix,
                       int zoom, int x, int y, int[] poiIndInBlock, byte[] bbox, int shiftTo, int commonSuffixOffset) {
            super(obf, atomOrder, nameIndexDataOffset, suffixIndex, atomSize,
                    suffixesBitsetIndex, otherWordsCount, suffixesDictionary, extraSuffix, bbox, commonSuffixOffset);
            this.zoom = zoom;
            this.x = x;
            this.y = y;
            this.poiIndInBlock = poiIndInBlock == null ? new int[0] : poiIndInBlock;
            this.shiftTo = shiftTo;
        }

        @Override
        public boolean isPoi() {
            return true;
        }

        @Override
        public int getObjectRefsCount() {
            return poiIndInBlock.length > 0 ? poiIndInBlock.length : (shiftTo == 0 ? 0 : 1);
        }
    }

    // Complies with AddressNameIndexDataAtom in OBF.proto.
    class AddressAtom extends Atom {
        private final int type, enclosingObjects;
        private final int[] shiftToIndex, shiftToCityIndex, xy16;

        public AddressAtom(String obf, int atomOrder, int nameIndexDataOffset, int suffixIndex, int atomSize,
                           int[] suffixesBitsetIndex, int[] otherWordsCount, String[] extraSuffix,
                           int type, byte[] bbox, int enclosingObjects,
                           int[] shiftToIndex, int[] shiftToCityIndex, int[] xy16) {
            this(obf, atomOrder, nameIndexDataOffset, suffixIndex, atomSize,
                    suffixesBitsetIndex, otherWordsCount, new String[0], extraSuffix, type, bbox, enclosingObjects,
                    shiftToIndex, shiftToCityIndex, xy16, nameIndexDataOffset);
        }

        public AddressAtom(String obf, int atomOrder, int nameIndexDataOffset, int suffixIndex, int atomSize,
                           int[] suffixesBitsetIndex, int[] otherWordsCount, String[] suffixesDictionary, String[] extraSuffix,
                           int type, byte[] bbox, int enclosingObjects,
                           int[] shiftToIndex, int[] shiftToCityIndex, int[] xy16, int commonSuffixOffset) {
            super(obf, atomOrder, nameIndexDataOffset, suffixIndex, atomSize,
                    suffixesBitsetIndex, otherWordsCount, suffixesDictionary, extraSuffix, bbox, commonSuffixOffset);
            this.type = type;
            this.enclosingObjects = enclosingObjects;
            this.shiftToIndex = shiftToIndex == null ? new int[0] : shiftToIndex;
            this.shiftToCityIndex = shiftToCityIndex == null ? new int[0] : shiftToCityIndex;
            this.xy16 = xy16 == null ? new int[0] : xy16;
        }

        public int type() {
            return type;
        }

        public int getEnclosingObjects() {
            return enclosingObjects;
        }

        @Override
        public boolean isPoi() {
            return false;
        }

        @Override
        public int getObjectRefsCount() {
            return shiftToIndex.length;
        }
    }

    record IndexToken(String name, boolean isPoi, Atom[] atoms, boolean isCommon, boolean isFrequent, int count, int size) {
        public IndexToken {
            count = count > 0 ? count : getObjectRefsCount();
            size = size > 0 ? size : getAtomsSize();
        }

        private int getObjectRefsCount() {
            if (atoms == null || atoms.length == 0) {
                return 0;
            }
            long count = 0;
            for (Atom atom : atoms) {
                if (atom != null) {
                    count += atom.getObjectRefsCount();
                }
            }
            return count >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) count;
        }

        private int getAtomsSize() {
            if (atoms == null || atoms.length == 0) {
                return 0;
            }
            long size = 0;
            for (Atom atom : atoms) {
                if (atom != null && atom.atomSize > 0) {
                    size += atom.atomSize;
                }
            }
            return size >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size;
        }
    }
    record IndexTokenRequest(String name, boolean isPoi) {}
    record IndexTokenPage(List<IndexToken> content, int pageToShow, int pageSizeLimit, long totalElements, int totalPages) {}

    record CommonSuffix(String value, int matched, int nonindexed) {}

    record ObjectAddress(int sequenceId, String name, LatLon point, Map<String, String> commonTags,
                         boolean isPoi, boolean isAlone, String type, Long osmId,
                         String osmType, int payloadOffset, int payloadSize, int sourceOffset, String obf) {
    }

    record ObjectAddressPage(List<ObjectAddress> content, int pageToShow, int pageSizeLimit, long totalElements, int totalPages, int[] countMetrics, int[] sizeMetrics, int aloneCount, int aloneSize) {}
    record CachedIndexTokens(String cacheKey, long fileLength, long lastModified, List<IndexToken> tokens) {}

    default IndexTokenPage getIndex(List<String> obfs, String prefix, int pageToShow, int pageSizeLimit, String sortBy, String sortOrder,
                                    boolean isPoi) {
        Pattern prefixPattern = compileIndexPrefixPattern(prefix);
        final int safePage = Math.max(pageToShow, 0);
        final int safeSize = Math.max(1, Math.min(pageSizeLimit, 100));
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
                        if ((prefixPattern == null || prefixPattern.matcher(token.name()).find())) {
                            mergedByName.merge(token.name(), token, InspectorService::mergeIndexTokens);
                        }
                    }
                    getLogger().info("getIndex obfPart={} prefix={} poi={} sourceTokens={} mergedSoFar={} elapsedMs={}",
                            new File(obf).getName(), prefix, isPoi, allTokens.size(), mergedByName.size(),
                            elapsedMs(System.nanoTime() - obfStartedNs));
                }
            }
            long mergeNs = System.nanoTime() - mergeStartedNs;
            long listStartedNs = System.nanoTime();
            List<IndexToken> results = new ArrayList<>(mergedByName.values());
            long listNs = System.nanoTime() - listStartedNs;
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
                    : new ArrayList<>(results.subList(fromIndex, toIndex));

            long pageNs = System.nanoTime() - pageStartedNs;
            getLogger().info("getIndex: obfs={} prefix={} poi={} page={}/{} size={} sourceTokens={} merged={} content={}, timings (ms): merge={} list={} sort={} page={} total={}",
                    loadedObfs, prefix, isPoi, safePage, totalPages, safeSize, sourceTokens, totalElements, pageContent.size(),
                    elapsedMs(mergeNs), elapsedMs(listNs), elapsedMs(sortNs), elapsedMs(pageNs),
                    elapsedMs(System.nanoTime() - startedNs));
            return new IndexTokenPage(pageContent, safePage, safeSize, totalElements, totalPages);
        } catch (Exception e) {
            getLogger().error("Failed to read OBF indexes {}", obfs, e);
            throw new RuntimeException("Failed to read OBF indexes: " + e.getMessage(), e);
        }
    }

    private static IndexToken mergeIndexTokens(IndexToken left, IndexToken right) {
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        List<Atom> atoms = new ArrayList<>();
        if (left.atoms() != null) {
            atoms.addAll(Arrays.asList(left.atoms()));
        }
        if (right.atoms() != null) {
            atoms.addAll(Arrays.asList(right.atoms()));
        }
        return new IndexToken(left.name(), left.isPoi(), atoms.toArray(new Atom[0]),
                left.isCommon() || right.isCommon(),
                left.isFrequent() || right.isFrequent(), 0, 0);
    }

    private static long elapsedMs(long elapsedNs) {
		return TimeUnit.NANOSECONDS.toMillis(elapsedNs);
	}

    private static Comparator<IndexToken> buildIndexTokenComparator(String sortBy, String sortOrder) {
        String[] sortKeys = Algorithms.isEmpty(sortBy) ? new String[]{"name"} : sortBy.split(",");
        String[] sortOrders = Algorithms.isEmpty(sortOrder) ? new String[0] : sortOrder.split(",");
        Comparator<IndexToken> comparator = null;
        for (int i = 0; i < sortKeys.length; i++) {
            String normalizedSortBy = sortKeys[i] == null ? "" : sortKeys[i].trim().toLowerCase(Locale.ROOT);
            if (Algorithms.isEmpty(normalizedSortBy)) {
                continue;
            }
            Comparator<IndexToken> next = buildIndexTokenSingleComparator(normalizedSortBy);
            String direction = i < sortOrders.length ? sortOrders[i].trim() : "";
            if ("desc".equalsIgnoreCase(direction)) {
                next = next.reversed();
            }
            comparator = comparator == null ? next : comparator.thenComparing(next);
        }
        if (comparator == null) {
            comparator = buildIndexTokenSingleComparator("name");
        }
        return comparator.thenComparing(token -> token == null || token.name() == null ? "" : token.name(), String.CASE_INSENSITIVE_ORDER);
    }

    private static Comparator<IndexToken> buildIndexTokenSingleComparator(String normalizedSortBy) {
        Comparator<IndexToken> comparator = switch (normalizedSortBy) {
            case "count" -> Comparator.comparingInt(token -> token == null ? 0 : token.count());
            case "size" -> Comparator.comparingInt(token -> token == null ? 0 : token.size());
            default -> Comparator.comparing(token -> token == null || token.name() == null ? "" : token.name(), String.CASE_INSENSITIVE_ORDER);
        };
        return comparator;
    }
	
    private List<IndexToken> getCachedOrLoadIndexTokens(File file, boolean isPoi) throws IOException {
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
        List<IndexToken> cachedTokens = List.copyOf(loadedTokens);
        synchronized (INDEX_TOKENS_CACHE) {
            INDEX_TOKENS_CACHE.put(cacheKey, new CachedIndexTokens(cacheKey, fileLength, lastModified, cachedTokens));
        }
        return cachedTokens;
    }

	private List<IndexToken> loadIndexTokens(File file, boolean isPoi) throws IOException {
        long startedNs = System.nanoTime();
        Map<String, List<Atom>> atomsByToken = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        CommonWords commonWords = isPoi ? CommonWords.getPoiInstance() : CommonWords.getAddrInstance();
        int prefixBlocks = 0;
        int atomCount = 0;
        String obfKey = file.getAbsolutePath();
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file.getAbsolutePath(), "r")) {
            BinaryMapIndexReader index = new BinaryMapIndexReader(randomAccessFile, file);
            try {
                for (BinaryIndexPart part : index.getIndexes()) {
                    if (isPoi && part instanceof BinaryMapPoiReaderAdapter.PoiRegion poiRegion) {
                        NameIndexReader nameIndexReader = new NameIndexReader(poiRegion);
                        List<NameIndexReader.PrefixNameValue> prefixes = index.readFullNameIndex(nameIndexReader);
                        if (prefixes == null) {
                            prefixes = nameIndexReader.getLoadedPrefixes();
                        }
                        PrefixLoadStats stats = collectPoiIndexTokens(obfKey, prefixes, nameIndexReader, atomsByToken);
                        prefixBlocks += stats.prefixBlocks();
                        atomCount += stats.atoms();
                    } else if (!isPoi && part instanceof BinaryMapAddressReaderAdapter.AddressRegion addressRegion) {
                        NameIndexReader nameIndexReader = new NameIndexReader(addressRegion);
                        List<NameIndexReader.PrefixNameValue> prefixes = index.readFullNameIndex(nameIndexReader);
                        if (prefixes == null) {
                            prefixes = nameIndexReader.getLoadedPrefixes();
                        }
                        PrefixLoadStats stats = collectAddressIndexTokens(obfKey, prefixes, nameIndexReader, atomsByToken);
                        prefixBlocks += stats.prefixBlocks();
                        atomCount += stats.atoms();
                    }
                }
            } finally {
                index.close();
            }
        }
        List<IndexToken> tokens = new ArrayList<>(atomsByToken.size());
        for (Map.Entry<String, List<Atom>> entry : atomsByToken.entrySet()) {
            String tokenName = entry.getKey();
            tokens.add(new IndexToken(tokenName, isPoi, entry.getValue().toArray(new Atom[0]),
                    commonWords.getCommon(tokenName) != -1,
                    commonWords.getFrequentlyUsed(tokenName) != -1,
                    0, 0));
        }
        getLogger().info("loadIndexTokens obf={} poi={} prefixes={} atoms={} tokens={} elapsedMs={}",
                file.getName(), isPoi ? "poi" : "address", prefixBlocks, atomCount, tokens.size(),
                elapsedMs(System.nanoTime() - startedNs));
        return tokens;
    }

    record PrefixLoadStats(int prefixBlocks, int atoms) {}

    private static PrefixLoadStats collectPoiIndexTokens(String obfKey,
                                                  List<NameIndexReader.PrefixNameValue> prefixes,
                                                  NameIndexReader reader,
                                                  Map<String, List<Atom>> atomsByToken) {
        if (prefixes == null || prefixes.isEmpty()) {
            return new PrefixLoadStats(0, 0);
        }
        int blocks = 0;
        int atoms = 0;
        for (NameIndexReader.PrefixNameValue prefix : prefixes) {
            if (prefix == null || prefix.poi == null) {
                continue;
            }
            blocks++;
            String[] suffixDictionary = decodeSuffixDictionary(prefix.poi.getSuffixesDictionaryList());
            cacheCommonSuffix(obfKey, true, safeMetricInt(prefix.shift),
                    decodePrefixCommonSuffixes(reader.getCommonStats(), prefix.poi.getSuffixesCommonDictionaryList()));
            int atomOrder = 0;
            for (OsmandOdb.OsmAndPoiNameIndexDataAtom atom : prefix.poi.getAtomsList()) {
                atoms++;
                addPoiAtomTokens(obfKey, prefix.key, safeMetricInt(prefix.shift), suffixDictionary, atomOrder++, atom, atomsByToken);
            }
        }
        return new PrefixLoadStats(blocks, atoms);
    }

    private static PrefixLoadStats collectAddressIndexTokens(String obfKey,
                                                      List<NameIndexReader.PrefixNameValue> prefixes,
                                                      NameIndexReader reader,
                                                      Map<String, List<Atom>> atomsByToken) {
        if (prefixes == null || prefixes.isEmpty()) {
            return new PrefixLoadStats(0, 0);
        }
        int blocks = 0;
        int atoms = 0;
        for (NameIndexReader.PrefixNameValue prefix : prefixes) {
            if (prefix == null || prefix.addr == null) {
                continue;
            }
            blocks++;
            String[] suffixDictionary = decodeSuffixDictionary(prefix.addr.getSuffixesDictionaryList());
            cacheCommonSuffix(obfKey, false, safeMetricInt(prefix.shift),
                    decodePrefixCommonSuffixes(reader.getCommonStats(), prefix.addr.getSuffixesCommonDictionaryList()));
            int atomOrder = 0;
            for (OsmandOdb.AddressNameIndexDataAtom atom : prefix.addr.getAtomList()) {
                atoms++;
                addAddressAtomTokens(obfKey, prefix.key, safeMetricInt(prefix.shift), suffixDictionary, atomOrder++, atom, atomsByToken);
            }
        }
        return new PrefixLoadStats(blocks, atoms);
    }

    private static void addPoiAtomTokens(String obfKey, String prefix, int nameIndexDataOffset, String[] suffixDictionary,
                                  int atomOrder, OsmandOdb.OsmAndPoiNameIndexDataAtom atom,
                                  Map<String, List<Atom>> atomsByToken) {
        for (Integer suffixIndex : getMatchedPartialSuffixIndexes(atom.getSuffixesBitsetIndexList(), suffixDictionary)) {
            String tokenName = prefix + suffixDictionary[suffixIndex];
            POIAtom poiAtom = new POIAtom(obfKey, atomOrder, nameIndexDataOffset, suffixIndex,
                    atom.getSerializedSize(), toIntArray(atom.getSuffixesBitsetIndexList()),
                    toIntArray(atom.getOtherWordsCountList()), suffixDictionary, toStringArray(atom.getExtraSuffixList()),
                    atom.getZoom(), atom.getX(), atom.getY(), toIntArray(atom.getPoiIndInBlockList()),
                    atom.hasBbox() ? atom.getBbox().toByteArray() : new byte[0],
                    atom.hasShiftTo() ? atom.getShiftTo() : 0, nameIndexDataOffset);
            atomsByToken.computeIfAbsent(tokenName, ignored -> new ArrayList<>()).add(poiAtom);
        }
    }

    private static void addAddressAtomTokens(String obfKey, String prefix, int nameIndexDataOffset, String[] suffixDictionary,
                                      int atomOrder, OsmandOdb.AddressNameIndexDataAtom atom,
                                      Map<String, List<Atom>> atomsByToken) {
        for (Integer suffixIndex : getMatchedPartialSuffixIndexes(atom.getSuffixesBitsetIndexList(), suffixDictionary)) {
            String tokenName = prefix + suffixDictionary[suffixIndex];
            AddressAtom addressAtom = new AddressAtom(obfKey, atomOrder, nameIndexDataOffset, suffixIndex,
                    atom.getSerializedSize(), toIntArray(atom.getSuffixesBitsetIndexList()),
                    toIntArray(atom.getOtherWordsCountList()), suffixDictionary, toStringArray(atom.getExtraSuffixList()),
                    atom.getType(), atom.hasBbox() ? atom.getBbox().toByteArray() : new byte[0],
                    atom.hasEnclosingObjects() ? atom.getEnclosingObjects() : 0,
                    toIntArray(atom.getShiftToIndexList()), toIntArray(atom.getShiftToCityIndexList()),
                    toIntArray(atom.getXy16List()), nameIndexDataOffset);
            atomsByToken.computeIfAbsent(tokenName, ignored -> new ArrayList<>()).add(addressAtom);
        }
    }

    private static Set<Integer> getMatchedPartialSuffixIndexes(List<Integer> suffixesBitsetIndex, String[] suffixDictionary) {
        if (suffixesBitsetIndex == null || suffixesBitsetIndex.isEmpty() || suffixDictionary == null || suffixDictionary.length == 0) {
            return Set.of();
        }
        Set<Integer> indexes = new LinkedHashSet<>();
        for (int value : suffixesBitsetIndex) {
            if (value != 0 && value % 2 == 0) {
                int index = value / 2 - 1;
                if (index >= 0 && index < suffixDictionary.length) {
                    String suffix = suffixDictionary[index];
                    if (suffix != null && !suffix.startsWith(" ")) {
                        indexes.add(index);
                    }
                }
            }
        }
        return indexes;
    }

    private static String[] decodeSuffixDictionary(List<String> encodedSuffixes) {
        if (encodedSuffixes == null || encodedSuffixes.isEmpty()) {
            return new String[0];
        }
        List<String> decoded = new ArrayList<>(encodedSuffixes.size());
        String previous = null;
        for (String encoded : encodedSuffixes) {
            if (SearchAlgorithms.OLD_EMPTY_SUFFIX_DICTIONARY_SENTINEL.equals(encoded)) {
                continue;
            }
            String suffix = SearchAlgorithms.nameIndexDecodeDictionarySuffix(previous, encoded);
            decoded.add(suffix);
            previous = suffix;
        }
        return decoded.toArray(new String[0]);
    }

    private static List<CommonSuffix> decodePrefixCommonSuffixes(OsmandOdb.CommonIndexedStats commonStats, List<Integer> commonRefs) {
        if (commonStats == null || commonRefs == null || commonRefs.isEmpty()) {
            return List.of();
        }
        List<CommonSuffix> all = decodeCommonSuffixes(commonStats);
        List<CommonSuffix> resolved = new ArrayList<>(commonRefs.size());
        for (Integer ref : commonRefs) {
            int index = ref == null ? -1 : ref;
            if (index >= 0 && index < all.size()) {
                resolved.add(all.get(index));
            }
        }
        return resolved;
    }

    private static List<CommonSuffix> decodeCommonSuffixes(OsmandOdb.CommonIndexedStats commonStats) {
        if (commonStats == null || commonStats.getValueCount() == 0) {
            return List.of();
        }
        List<CommonSuffix> decoded = new ArrayList<>(commonStats.getValueCount());
        String previous = null;
        for (int i = 0; i < commonStats.getValueCount(); i++) {
            String value = SearchAlgorithms.nameIndexDecodeDictionarySuffix(previous, commonStats.getValue(i));
            int matched = i < commonStats.getMatchedCount() ? commonStats.getMatched(i) : 0;
            int nonindexed = i < commonStats.getNonindexedCount() ? commonStats.getNonindexed(i) : 0;
            decoded.add(new CommonSuffix(value, matched, nonindexed));
            previous = value;
        }
        return decoded;
    }

    private static String getIndexCacheKey(File file, boolean isPoi) {
        return file.getName() + "|" + (isPoi ? "poi" : "address");
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

    private static void cacheCommonSuffix(String obfKey, boolean poi, int offset, List<CommonSuffix> commonStats) {
        if (Algorithms.isEmpty(obfKey) || commonStats == null || commonStats.isEmpty()) {
            return;
        }
        synchronized (INDEX_COMMON_SUFFIX_CACHE) {
            INDEX_COMMON_SUFFIX_CACHE.put(commonSuffixStatsCacheKey(obfKey, poi, offset), List.copyOf(commonStats));
        }
    }

    private static List<CommonSuffix> getCachedCommonSuffixStats(String obfKey, boolean poi, int offset) {
        if (Algorithms.isEmpty(obfKey)) {
            return List.of();
        }
        synchronized (INDEX_COMMON_SUFFIX_CACHE) {
            List<CommonSuffix> stats = INDEX_COMMON_SUFFIX_CACHE.get(commonSuffixStatsCacheKey(obfKey, poi, offset));
            return stats == null ? List.of() : stats;
        }
    }

    private static String commonSuffixStatsCacheKey(String obfKey, boolean poi, int offset) {
        return obfKey + "|" + (poi ? "poi" : "address") + "|" + offset;
    }

    private static boolean isAloneTokenObject(ObjectAddress object, String tokenName) {
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

    private static int[] toIntArray(Collection<Integer> offsets) {
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

    private static String[] toStringArray(Collection<String> values) {
        if (values == null || values.isEmpty()) {
            return new String[0];
        }
        return values.toArray(new String[0]);
    }

    private static int safeMetricInt(long value) {
        if (value <= 0L) {
            return 0;
        }
        return value >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) value;
    }

    private static ObjectAddress toPoiObjectAddress(RawPoiObject rawPoiObject,
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

    private static boolean isPoiSearchIndexedTextTag(String key) {
        return !Algorithms.isEmpty(key) && (isTagIndexedForSearchAsName(key)
                || isTagIndexedForSearchAsId(key) || isTagIndexedAsSearchRelated(key)
                || Amenity.ROUTE_MEMBERS_IDS.equals(key));
    }

    private static Map<String, String> arrangeObjectAddressValues(Map<String, String> values) {
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

    private static String decodePoiOsmType(long rawPoiObjectId) {
        if (rawPoiObjectId <= 0) {
            return null;
        }
        Amenity amenity = new Amenity();
        amenity.setId(rawPoiObjectId);
        net.osmand.osm.edit.Entity.EntityType entityType = ObfConstants.getOsmEntityType(amenity);
        return entityType == null ? null : entityType.name().toLowerCase(Locale.US);
    }

    private static String selectPoiDisplayName(RawPoiObject rawPoiObject, String lang) {
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

    private static List<ObjectAddress> markAloneObjects(List<ObjectAddress> results, IndexToken token) {
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

    private static List<ObjectAddress> assignObjectSequenceIds(List<ObjectAddress> results) {
        if (results == null || results.isEmpty()) {
            return new ArrayList<>();
        }
        List<ObjectAddress> assigned = new ArrayList<>(results.size());
        int sequenceId = 1;
        for (ObjectAddress objectAddress : results) {
            if (objectAddress == null) {
                continue;
            }
            assigned.add(new ObjectAddress(sequenceId++, objectAddress.name(), objectAddress.point(),
                    objectAddress.commonTags(), objectAddress.isPoi(), objectAddress.isAlone(), objectAddress.type(),
                    objectAddress.osmId(), objectAddress.osmType(), objectAddress.payloadOffset(),
                    objectAddress.payloadSize(), objectAddress.sourceOffset(), objectAddress.obf()));
        }
        return assigned;
    }

    private boolean matchesObjectAddressFilter(ObjectAddress objectAddress,
                                               Pattern pattern,
                                               Pattern normalizedPattern) {
        if (objectAddress == null) {
            return false;
        }
        if (pattern == null) {
            return true;
        }
        if (matchesPattern(objectAddress.name(), pattern, normalizedPattern)) {
            return true;
        }
        Map<String, String> values = objectAddress.commonTags();
        if (values != null) {
            for (Map.Entry<String, String> entry : values.entrySet()) {
                if (matchesPattern(entry.getKey(), pattern, normalizedPattern)
                        || matchesPattern(entry.getValue(), pattern, normalizedPattern)) {
                    return true;
                }
            }
        }
        if (objectAddress.osmId() != null && pattern.matcher(objectAddress.osmId().toString()).find()) {
            return true;
        }
        return objectAddress.type() != null && pattern.matcher(objectAddress.type()).find();
    }

    private ObjectAddressPage getObjects(String obf, IndexToken token, String lang, String regExp, int pageToShow,
                                         int pageSizeLimit, String sortBy, String sortOrder, boolean isPoi) {
        long startedNs = System.nanoTime();
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
                long collectStartedNs = System.nanoTime();
                if (isPoi) {
                    collectPoiObjects(index, token, results, lang);
                } else {
                    collectAddressObjects(index, token, results, lang);
                }
                long collectNs = System.nanoTime() - collectStartedNs;
                int collectedCount = results.size();
                long filterStartedNs = System.nanoTime();
                if (hasAnyFilter) {
                    results.removeIf(objectAddress -> !matchesObjectAddressFilter(objectAddress, objectPattern, normalizedObjectPattern));
                }
                long filterNs = System.nanoTime() - filterStartedNs;
                long markStartedNs = System.nanoTime();
                results = markAloneObjects(assignObjectSequenceIds(results), token);
                long markNs = System.nanoTime() - markStartedNs;
                long sortStartedNs = System.nanoTime();
                results.sort(buildObjectAddressComparator(sortBy, sortOrder));
                long sortNs = System.nanoTime() - sortStartedNs;
                long pageStartedNs = System.nanoTime();
                long totalElements = results.size();
                int totalPages = totalElements == 0 ? 0 : (int) ((totalElements + safeSize - 1) / safeSize);
                int fromIndex = Math.min(safePage * safeSize, results.size());
                int toIndex = Math.min(fromIndex + safeSize, results.size());
                List<ObjectAddress> pageContent = fromIndex >= toIndex
                        ? List.of()
                        : new ArrayList<>(results.subList(fromIndex, toIndex));
                int totalSize = 0;
                for (ObjectAddress objectAddress : results) {
                    totalSize = safeMetricInt((long) totalSize + (objectAddress == null ? 0 : Math.max(0, objectAddress.payloadSize())));
                }
                long pageNs = System.nanoTime() - pageStartedNs;
                getLogger().info("getObjects: obf={} token={} poi={} page={}/{} size={} collected={} filtered={} content={}, timings (ms): collect={} filter={} mark={} sort={} page={} total={}",
                        file.getName(), token.name(), isPoi ? "poi" : "address", safePage, totalPages, safeSize,
                        collectedCount, totalElements, pageContent.size(), elapsedMs(collectNs), elapsedMs(filterNs),
                        elapsedMs(markNs), elapsedMs(sortNs), elapsedMs(pageNs), elapsedMs(System.nanoTime() - startedNs));
                return new ObjectAddressPage(pageContent, safePage, safeSize, totalElements, totalPages,
                        new int[]{results.size()}, new int[]{totalSize}, 0, 0);
            } finally {
                index.close();
            }
        } catch (Exception e) {
            getLogger().error("Failed to read OBF objects {} for token {}", file, token, e);
            throw new RuntimeException("Failed to read OBF objects: " + e.getMessage(), e);
        }
    }

    private void collectAddressObjects(BinaryMapIndexReaderExt index, IndexToken token, List<ObjectAddress> results, String lang) throws IOException {
        if (token == null || token.atoms() == null) {
            return;
        }
        Map<Integer, BinaryMapAddressReaderAdapter.AddressRegion> regionsByOffset = new LinkedHashMap<>();
        for (BinaryIndexPart part : index.getIndexes()) {
            if (part instanceof BinaryMapAddressReaderAdapter.AddressRegion region) {
                regionsByOffset.put((int) region.getFilePointer(), region);
            }
        }
        Set<String> visited = new LinkedHashSet<>();
        for (Atom atom : token.atoms()) {
            if (!(atom instanceof AddressAtom addressAtom)) {
                continue;
            }
            for (int i = 0; i < addressAtom.shiftToIndex.length; i++) {
                int objectOffset = addressAtom.nameIndexDataOffset - addressAtom.shiftToIndex[i];
                int cityOffset = i < addressAtom.shiftToCityIndex.length && addressAtom.shiftToCityIndex[i] != 0
                        ? addressAtom.nameIndexDataOffset - addressAtom.shiftToCityIndex[i] : 0;
                if (!visited.add(objectOffset + ":" + cityOffset + ":" + addressAtom.type)) {
                    continue;
                }
                BinaryMapAddressReaderAdapter.AddressRegion region = findAddressRegion(regionsByOffset.values(), objectOffset);
                if (region == null) {
                    continue;
                }
                ObjectAddress objectAddress = loadAddressObject(index, region, addressAtom, objectOffset, cityOffset, lang);
                if (objectAddress != null) {
                    results.add(objectAddress);
                }
            }
        }
    }

    private BinaryMapAddressReaderAdapter.AddressRegion findAddressRegion(Collection<BinaryMapAddressReaderAdapter.AddressRegion> regions, int offset) {
        for (BinaryMapAddressReaderAdapter.AddressRegion region : regions) {
            long start = region.getFilePointer();
            long end = start + region.getLength();
            if (offset >= start && offset < end) {
                return region;
            }
        }
        return null;
    }

    private ObjectAddress loadAddressObject(BinaryMapIndexReaderExt index,
                                            BinaryMapAddressReaderAdapter.AddressRegion region,
                                            AddressAtom atom,
                                            int objectOffset,
                                            int cityOffset,
                                            String lang) throws IOException {
        CodedInputStream codedIS = index.getInputStream();
        if (atom.type == BinaryMapAddressReaderAdapter.CityBlocks.STREET_TYPE.index) {
            City city = cityOffset > 0 ? loadCity(index, region, cityOffset) : null;
            if (city == null) {
                return null;
            }
            codedIS.seek(objectOffset);
            int length = codedIS.readRawVarint32();
            long oldLimit = codedIS.pushLimitLong(length);
            try {
                Street street = readStreetAtOffset(codedIS, city,
                        (int) MapUtils.getTileNumberX(24, city.getLocation().getLongitude()),
                        (int) MapUtils.getTileNumberY(24, city.getLocation().getLatitude()),
                        region.getAttributeTagsTable());
                return toAddressObjectAddress(street, lang, "street", objectOffset, length, atom.nameIndexDataOffset);
            } finally {
                codedIS.popLimit(oldLimit);
            }
        }
        City city = loadCity(index, region, objectOffset);
        return city == null ? null : toAddressObjectAddress(city, lang,
                city.getType() == null ? "address" : city.getType().name().toLowerCase(Locale.ROOT),
                objectOffset, 0, atom.nameIndexDataOffset);
    }

    private City loadCity(BinaryMapIndexReaderExt index,
                          BinaryMapAddressReaderAdapter.AddressRegion region,
                          int offset) throws IOException {
        CodedInputStream codedIS = index.getInputStream();
        codedIS.seek(offset);
        int length = codedIS.readRawVarint32();
        long oldLimit = codedIS.pushLimitLong(length);
        try {
            return readCityAtOffset(codedIS, offset, region.getAttributeTagsTable());
        } finally {
            codedIS.popLimit(oldLimit);
        }
    }

    private ObjectAddress toAddressObjectAddress(MapObject object, String lang, String type,
                                                 int payloadOffset, int payloadSize, int sourceOffset) {
        if (object == null) {
            return null;
        }
        Map<String, String> values = new LinkedHashMap<>();
        String name = object.getName(lang);
        if (!Algorithms.isEmpty(object.getName())) {
            values.put(Amenity.NAME, object.getName());
        }
        if (!Algorithms.isEmpty(object.getEnName(false))) {
            values.put("name:en", object.getEnName(false));
        }
        Map<String, String> names = object.getNamesMap(false);
        if (names != null) {
            for (Map.Entry<String, String> entry : names.entrySet()) {
                if (!Algorithms.isEmpty(entry.getKey()) && !Algorithms.isEmpty(entry.getValue())) {
                    values.put(entry.getKey().startsWith("name:") ? entry.getKey() : "name:" + entry.getKey(), entry.getValue());
                }
            }
        }
        Long osmId = object.getId() == null ? null : object.getId();
        return new ObjectAddress(0, Algorithms.isEmpty(name) ? object.getName() : name, object.getLocation(),
                arrangeObjectAddressValues(values), false, false, type, osmId, null, payloadOffset, payloadSize, sourceOffset, "");
    }

    private void collectPoiObjects(BinaryMapIndexReaderExt index, IndexToken token, List<ObjectAddress> results, String lang) throws IOException {
        if (token == null || token.atoms() == null) {
            return;
        }
        Map<BinaryMapPoiReaderAdapter.PoiRegion, Map<Integer, List<POIAtom>>> atomsByRegionAndShift = new LinkedHashMap<>();
        for (Atom atom : token.atoms()) {
            if (!(atom instanceof POIAtom poiAtom) || poiAtom.shiftTo == 0) {
                continue;
            }
            BinaryMapPoiReaderAdapter.PoiRegion region = findPoiRegion(index, poiAtom);
            if (region != null) {
                int absoluteShiftTo = resolvePoiShiftTo(region, poiAtom);
                atomsByRegionAndShift.computeIfAbsent(region, ignored -> new LinkedHashMap<>())
                        .computeIfAbsent(absoluteShiftTo, ignored -> new ArrayList<>()).add(poiAtom);
            }
        }
        Set<String> visited = new LinkedHashSet<>();
        for (Map.Entry<BinaryMapPoiReaderAdapter.PoiRegion, Map<Integer, List<POIAtom>>> regionEntry : atomsByRegionAndShift.entrySet()) {
            BinaryMapPoiReaderAdapter.PoiRegion region = regionEntry.getKey();
            index.initCategories(region);
            Map<Integer, List<TagValuePair>> tagGroups = preloadPoiTagGroups(index.getInputStream(), region);
            for (Map.Entry<Integer, List<POIAtom>> shiftEntry : regionEntry.getValue().entrySet()) {
                collectPoiObjectsAtShift(index, region, tagGroups, shiftEntry.getKey(), shiftEntry.getValue(), results, visited, lang);
            }
        }
    }

    private BinaryMapPoiReaderAdapter.PoiRegion findPoiRegion(BinaryMapIndexReaderExt index, POIAtom atom) {
        int absoluteOffset = atom.shiftTo;
        for (BinaryMapPoiReaderAdapter.PoiRegion region : index.getPoiIndexes()) {
            long start = region.getFilePointer();
            long end = start + region.getLength();
            long candidate = start + atom.shiftTo;
            if (absoluteOffset >= start && absoluteOffset < end) {
                return region;
            }
            if (candidate >= start && candidate < end) {
                return region;
            }
        }
        return null;
    }

    private int resolvePoiShiftTo(BinaryMapPoiReaderAdapter.PoiRegion region, POIAtom atom) {
        long start = region.getFilePointer();
        long end = start + region.getLength();
        if (atom.shiftTo >= start && atom.shiftTo < end) {
            return atom.shiftTo;
        }
        return (int) (start + atom.shiftTo);
    }

    private void collectPoiObjectsAtShift(BinaryMapIndexReaderExt index,
                                          BinaryMapPoiReaderAdapter.PoiRegion region,
                                          Map<Integer, List<TagValuePair>> tagGroups,
                                          int shiftTo,
                                          List<POIAtom> atoms,
                                          List<ObjectAddress> results,
                                          Set<String> visited,
                                          String lang) throws IOException {
        Set<Integer> requestedIndexes = new LinkedHashSet<>();
        for (POIAtom atom : atoms) {
            if (atom.poiIndInBlock.length == 0) {
                requestedIndexes.add(-1);
            } else {
                for (int poiIndex : atom.poiIndInBlock) {
                    requestedIndexes.add(poiIndex);
                }
            }
        }
        CodedInputStream codedIS = index.getInputStream();
        codedIS.seek(Math.max(0, shiftTo - 4));
        int length = (int) OBFService.readFixed32Length(codedIS);
        if (codedIS.getTotalBytesRead() != shiftTo) {
            codedIS.seek(shiftTo);
            length = Integer.MAX_VALUE;
        }
        long oldLimit = length == Integer.MAX_VALUE ? -1 : codedIS.pushLimitLong(length);
        try {
            int x = 0;
            int y = 0;
            int zoom = 0;
            int poiIndex = 0;
            while (true) {
                int tagValue = codedIS.readTag();
                int tag = com.google.protobuf.WireFormat.getTagFieldNumber(tagValue);
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
                        int atomOffset = (int) codedIS.getTotalBytesRead();
                        int atomLength = codedIS.readRawVarint32();
                        long atomOldLimit = codedIS.pushLimitLong(atomLength);
                        try {
                            if (requestedIndexes.contains(-1) || requestedIndexes.contains(poiIndex)) {
                                RawPoiObject rawPoiObject = readRawPoiObject(codedIS, x, y, zoom, region, tagGroups);
                                if (rawPoiObject != null) {
                                    String key = rawPoiObject.id + ":" + atomOffset;
                                    if (visited.add(key)) {
                                        ObjectAddress objectAddress = toPoiObjectAddress(rawPoiObject, lang);
                                        results.add(new ObjectAddress(0, objectAddress.name(), objectAddress.point(),
                                                objectAddress.commonTags(), objectAddress.isPoi(), objectAddress.isAlone(), objectAddress.type(),
                                                objectAddress.osmId(), objectAddress.osmType(), shiftTo, length == Integer.MAX_VALUE ? 0 : length,
                                                atomOffset, ""));
                                    }
                                }
                            } else {
                                codedIS.skipRawBytes(codedIS.getBytesUntilLimit());
                            }
                        } finally {
                            codedIS.popLimit(atomOldLimit);
                        }
                        poiIndex++;
                        break;
                    }
                    default:
                        OBFService.skipUnknownField(codedIS, tagValue);
                        break;
                }
            }
        } finally {
            if (oldLimit >= 0) {
                codedIS.popLimit(oldLimit);
            }
        }
    }

    default ObjectAddressPage getObjects(IndexToken token, String lang, String regExp, int pageToShow,
                                         int pageSizeLimit, String sortBy, String sortOrder, boolean isPOI) {
        return getObjects(getIndexTokenObfs(token), token == null ? null : token.name(), lang, regExp, pageToShow,
                pageSizeLimit, sortBy, sortOrder, isPOI);
    }

    default ObjectAddressPage getObjects(List<String> obfs, IndexTokenRequest token, String lang, String regExp, int pageToShow,
                                         int pageSizeLimit, String sortBy, String sortOrder, boolean isPOI) {
        return getObjects(obfs, token == null ? null : token.name(), lang, regExp, pageToShow,
                pageSizeLimit, sortBy, sortOrder, isPOI);
    }

    private ObjectAddressPage getObjects(List<String> targetObfs, String tokenName, String lang, String regExp, int pageToShow,
                                         int pageSizeLimit, String sortBy, String sortOrder, boolean isPOI) {
        long startedNs = System.nanoTime();
        final int safePage = Math.max(pageToShow, 0);
        final int safeSize = Math.max(pageSizeLimit, 1);
        if (Algorithms.isEmpty(tokenName)) {
            return new ObjectAddressPage(List.of(), safePage, safeSize, 0, 0, new int[10], new int[15], 0, 0);
        }
        List<ObjectAddress> content = new ArrayList<>();
        int[] countMetrics = new int[10];
        int[] sizeMetrics = new int[15];
        int aloneCount = 0;
        int aloneSize = 0;
        long collectStartedNs = System.nanoTime();
        int loadedObfs = 0;
        for (String obf : targetObfs == null ? List.<String>of() : targetObfs) {
            if (Algorithms.isEmpty(obf)) {
                continue;
            }
            IndexToken obfToken = findIndexTokenByName(obf, tokenName, isPOI);
            if (obfToken == null) {
                continue;
            }
            loadedObfs++;
            ObjectAddressPage page = getObjects(obf, obfToken, lang, regExp, 0, Integer.MAX_VALUE, sortBy, sortOrder, isPOI);
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
        long collectNs = System.nanoTime() - collectStartedNs;
        long sortStartedNs = System.nanoTime();
        content.sort(buildObjectAddressComparator(sortBy, sortOrder));
        long sortNs = System.nanoTime() - sortStartedNs;
        long pageStartedNs = System.nanoTime();
        long totalElements = content.size();
        int totalPages = totalElements == 0 ? 0 : (int) ((totalElements + safeSize - 1) / safeSize);
        int fromIndex = Math.min(safePage * safeSize, content.size());
        int toIndex = Math.min(fromIndex + safeSize, content.size());
        List<ObjectAddress> pageContent = fromIndex >= toIndex
                ? List.of()
                : new ArrayList<>(content.subList(fromIndex, toIndex));
        long pageNs = System.nanoTime() - pageStartedNs;
        getLogger().info("getObjects: token={} poi={} obfs={} page={}/{} size={} total={} content={}, timings (ms): collect={} sort={} page={} total={}",
                tokenName, isPOI ? "poi" : "address", loadedObfs, safePage, totalPages, safeSize,
                totalElements, pageContent.size(), elapsedMs(collectNs), elapsedMs(sortNs), elapsedMs(pageNs),
                elapsedMs(System.nanoTime() - startedNs));
        return new ObjectAddressPage(pageContent, safePage, safeSize, totalElements, totalPages, countMetrics, sizeMetrics, aloneCount, aloneSize);
    }

    private IndexToken findIndexTokenByName(String obf, String tokenName, boolean isPoi) {
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

    private List<String> getIndexTokenObfs(IndexToken token) {
        if (token == null || token.atoms() == null || token.atoms().length == 0) {
            return List.of();
        }
        Set<String> obfs = new LinkedHashSet<>();
        for (Atom atom : token.atoms()) {
            if (atom != null && !Algorithms.isEmpty(atom.obf)) {
                obfs.add(atom.obf);
            }
        }
        return new ArrayList<>(obfs);
    }

    private static void addMetrics(int[] target, int[] source) {
        if (target == null || source == null) {
            return;
        }
        for (int i = 0; i < Math.min(target.length, source.length); i++) {
            target[i] = safeMetricInt((long) target[i] + source[i]);
        }
    }

    private static ObjectAddress withObf(ObjectAddress objectAddress, String obf) {
        if (objectAddress == null) {
            return null;
        }
        return new ObjectAddress(objectAddress.sequenceId(), objectAddress.name(), objectAddress.point(),
                objectAddress.commonTags(), objectAddress.isPoi(), objectAddress.isAlone(), objectAddress.type(), objectAddress.osmId(),
                objectAddress.osmType(), objectAddress.payloadOffset(), objectAddress.payloadSize(),
                objectAddress.sourceOffset(), obf);
    }

    private static Comparator<ObjectAddress> buildObjectAddressComparator(String sortBy, String sortOrder) {
        String[] sortKeys = Algorithms.isEmpty(sortBy) ? new String[]{"sequenceid"} : sortBy.split(",");
        String[] sortOrders = Algorithms.isEmpty(sortOrder) ? new String[0] : sortOrder.split(",");
        Comparator<ObjectAddress> comparator = null;
        for (int i = 0; i < sortKeys.length; i++) {
            String normalizedSortBy = sortKeys[i] == null ? "" : sortKeys[i].trim().toLowerCase(Locale.ROOT);
            if (Algorithms.isEmpty(normalizedSortBy)) {
                continue;
            }
            Comparator<ObjectAddress> next = buildObjectAddressSingleComparator(normalizedSortBy);
            String direction = i < sortOrders.length ? sortOrders[i].trim() : "";
            if ("desc".equalsIgnoreCase(direction)) {
                next = next.reversed();
            }
            comparator = comparator == null ? next : comparator.thenComparing(next);
        }
        if (comparator == null) {
            comparator = buildObjectAddressSingleComparator("sequenceid");
        }
        return comparator.thenComparingInt(object -> object == null ? Integer.MAX_VALUE : object.sequenceId())
                .thenComparing(object -> object == null || object.name() == null ? "" : object.name(), String.CASE_INSENSITIVE_ORDER);
    }

    private static Comparator<ObjectAddress> buildObjectAddressSingleComparator(String normalizedSortBy) {
        Comparator<ObjectAddress> comparator = switch (normalizedSortBy) {
            case "#", "sequence", "sequenceid" ->
                    Comparator.comparingInt(object -> object == null ? Integer.MAX_VALUE : object.sequenceId());
            case "type" ->
                    Comparator.comparing(object -> object == null || object.type() == null ? "" : object.type(), String.CASE_INSENSITIVE_ORDER);
            case "osmid", "osm id", "osm_id" ->
                    Comparator.comparingLong(object -> object == null || object.osmId() == null ? Long.MAX_VALUE : object.osmId());
            case "matched" -> Comparator.comparingInt(object -> object != null ? 1 : 0);
            case "alone", "isalone" -> Comparator.comparingInt(object -> object != null && object.isAlone() ? 1 : 0);
            default ->
                    Comparator.comparing(object -> object == null || object.name() == null ? "" : object.name(), String.CASE_INSENSITIVE_ORDER);
        };
        return comparator;
    }
}
