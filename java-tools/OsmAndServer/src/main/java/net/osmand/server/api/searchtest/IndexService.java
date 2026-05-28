package net.osmand.server.api.searchtest;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import net.osmand.CollatorStringMatcher;
import net.osmand.binary.*;
import net.osmand.data.*;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import net.osmand.util.SearchAlgorithms;
import net.osmand.util.TransliterationHelper;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static net.osmand.binary.ObfConstants.isTagIndexedAsSearchRelated;
import static net.osmand.binary.ObfConstants.isTagIndexedForSearchAsId;
import static net.osmand.binary.ObfConstants.isTagIndexedForSearchAsName;

public interface IndexService extends OBFService {
	private void collectAddressObjects(BinaryMapIndexReaderExt index,
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

	private Comparator<IndexToken> buildIndexTokenComparator(String sortBy, String sortOrder) {
		String normalizedSortBy = Algorithms.isEmpty(sortBy) ? "name" : sortBy.trim().toLowerCase(Locale.ROOT);
		Comparator<IndexToken> comparator = switch (normalizedSortBy) {
			case "poi" -> Comparator.comparingInt(this::getIndexTokenPoiCount);
			case "address" -> Comparator.comparingInt(this::getIndexTokenAddressCount);
			case "common" -> Comparator.comparingInt(token -> token != null && token.isCommon() ? 1 : 0);
			case "frequent" -> Comparator.comparingInt(token -> token != null && token.isFrequent() ? 1 : 0);
			case "count" -> Comparator.comparingInt(token -> getIndexTokenPoiCount(token) + getIndexTokenAddressCount(token));
			default -> Comparator.comparing(token -> token == null || token.name() == null ? "" : token.name(), String.CASE_INSENSITIVE_ORDER);
		};
		comparator = comparator.thenComparing(token -> token == null || token.name() == null ? "" : token.name(), String.CASE_INSENSITIVE_ORDER);
		return "desc".equalsIgnoreCase(sortOrder) ? comparator.reversed() : comparator;
	}

	private IndexTokenSummary buildIndexTokenSummary(List<IndexToken> tokens) {
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

	private int getIndexTokenPoiCount(IndexToken token) {
		return token == null || token.poiRefs() == null ? 0 : token.poiRefs().length;
	}

	private int getIndexTokenAddressCount(IndexToken token) {
		return token == null || token.addressRefs() == null ? 0 : token.addressRefs().length;
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

	private void collectPoiIndexTokens(BinaryMapIndexReaderExt index, BinaryMapPoiReaderAdapter.PoiRegion region,
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

	private void readAddressNameIndexTokens(BinaryMapIndexReaderExt index, Map<String, IndexTokenBuilder> tokens) throws IOException {
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

	private void readPoiNameIndexTokens(BinaryMapIndexReaderExt index, Map<String, IndexTokenBuilder> tokens) throws IOException {
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

	private void readNameIndexTableTokens(BinaryMapIndexReaderExt index, Map<String, IndexTokenBuilder> tokens, boolean poi) throws IOException {
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
					InspectorService.skipUnknownField(index.getInputStream(), tagWithType);
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
				InspectorService.skipUnknownField(index.getInputStream(), tagWithType);
			}
		} finally {
			index.getInputStream().popLimit(oldLimit);
		}
	}

	private void addIndexToken(Map<String, IndexTokenBuilder> tokens, String name, int offset, int suffixIndex, boolean poi, int[] poiRefs, int[] poiAtomSizes) {
		IndexTokenBuilder existing = tokens.get(name);
		if (existing == null) {
			tokens.put(name, poi
					? new IndexTokenBuilder(name, new int[0], new int[0], poiRefs == null ? new int[0] : distinctOffsets(poiRefs), poiRefs == null ? new int[0] : poiRefs, poiAtomSizes == null ? new int[0] : poiAtomSizes)
					: new IndexTokenBuilder(name, new int[] {offset}, new int[] {suffixIndex}, new int[0], new int[0], new int[0]));
			return;
		}
		int[] mergedAddressOffsets = poi ? existing.addressOffsets() : appendDistinctOffset(existing.addressOffsets(), offset);
		int[] mergedAddressSuffixIndexes = poi ? existing.addressSuffixIndexes() : appendAddressSuffixIndex(existing.addressOffsets(), existing.addressSuffixIndexes(), offset, suffixIndex);
		int[] mergedPoiRefs = poi ? appendDistinctOffsets(existing.poiRefs(), poiRefs) : existing.poiRefs();
		int[] mergedPoiAtomRefs = poi ? appendOffsets(existing.poiAtomRefs(), poiRefs) : existing.poiAtomRefs();
		int[] mergedPoiAtomSizes = poi ? appendOffsets(existing.poiAtomSizes(), poiAtomSizes) : existing.poiAtomSizes();
		tokens.put(name, new IndexTokenBuilder(name, mergedAddressOffsets, mergedAddressSuffixIndexes, mergedPoiRefs, mergedPoiAtomRefs, mergedPoiAtomSizes));
	}

	private int[] appendAddressSuffixIndex(int[] addressOffsets, int[] suffixIndexes, int offset, int suffixIndex) {
		if (addressOffsets != null) {
			for (int existingOffset : addressOffsets) {
				if (existingOffset == offset) {
					return suffixIndexes == null ? new int[0] : suffixIndexes;
				}
			}
		}
		return appendOffset(suffixIndexes, suffixIndex);
	}

	private List<IndexToken> buildIndexTokensWithRefs(BinaryMapIndexReaderExt index,
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

	private AddressRef[] collectAddressRefs(BinaryMapIndexReaderExt index, IndexTokenBuilder token) throws IOException {
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

	private ObjectAddressStats calculateObjectAddressStats(BinaryMapIndexReaderExt index,
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

	private ObjectAddressStats calculateAloneTokenStats(BinaryMapIndexReaderExt index,
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

	private boolean isAloneTokenObject(ObjectAddress object, String tokenName) {
		if (Algorithms.isEmpty(tokenName)) {
			return false;
		}
		Set<String> candidateNames = new LinkedHashSet<>();
		if (!Algorithms.isEmpty(object.name())) {
			candidateNames.add(object.name());
		}
		Map<String, String> values = object.values();
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
			List<String> tokens = SearchAlgorithms.splitAndNormalize(candidateName);
			SearchAlgorithms.removeCommonWords(tokens);
			if (tokens.contains(tokenName)) {
				return true;
			}
		}
		return false;
	}

	private int sumAddressAtomSizes(AddressRef[] addressRefs) {
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

	private int sumExactAddressAtomSizes(AddressRef[] addressRefs, List<ObjectAddress> exactResults) {
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

	private int sumExactPoiAtomSizes(IndexToken token,
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

	private int sumIntValues(int[] values) {
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

	private int[] appendDistinctOffsets(int[] offsets, int[] values) {
		if (values == null || values.length == 0) {
			return offsets == null ? new int[0] : offsets;
		}
		int[] mergedOffsets = offsets == null ? new int[0] : offsets;
		for (int value : values) {
			mergedOffsets = appendDistinctOffset(mergedOffsets, value);
		}
		return mergedOffsets;
	}

	private int[] distinctOffsets(int[] values) {
		return appendDistinctOffsets(new int[0], values);
	}

	private int[] appendOffsets(int[] offsets, int[] values) {
		if (values == null || values.length == 0) {
			return offsets == null ? new int[0] : offsets;
		}
		int[] existingOffsets = offsets == null ? new int[0] : offsets;
		int[] mergedOffsets = Arrays.copyOf(existingOffsets, existingOffsets.length + values.length);
		System.arraycopy(values, 0, mergedOffsets, existingOffsets.length, values.length);
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

	private AddressTokenRefs readAddressTokenRefs(BinaryMapIndexReaderExt index,
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

	private void readAddressTokenAtom(BinaryMapIndexReaderExt index,
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
		return new ObjectAddress(0, name, city.getLocation(), arrangeObjectAddressValues(values), false, isMatched, false, false, type, city.getId(), null, offset, payloadSize, offset);
	}

	private ObjectAddress loadStreetObjectAddress(BinaryMapIndexReaderExt index,
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
			return new ObjectAddress(0, name, street.getLocation(), arrangeObjectAddressValues(values), false, isMatched, false, false, "Street", street.getId(), null, offset, payloadSize, offset);
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
					InspectorService.skipUnknownField(codedIS, tagWithType);
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
					InspectorService.skipUnknownField(codedIS, tagWithType);
					break;
			}
		}
	}

	private boolean matchesLegacyCity(City city, CollatorStringMatcher matcher) {
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
		return matches;
	}

	private boolean matchesLegacyStreet(Street street, CollatorStringMatcher matcher) {
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
		return matches;
	}

	private void collectPoiObjectsByStoredOffsets(BinaryMapIndexReaderExt index,
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
			for (Integer relativeOffset : relativeOffsets) {
				readPoiObjectsAtShift(index, poiRegion, relativeOffset, results, lang, matcher);
			}
		}
	}

	private Map<BinaryMapPoiReaderAdapter.PoiRegion, Set<Integer>> mapPoiRefs(int[] poiRefs,
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

	private PoiTokenRefs readPoiTokenRefs(BinaryMapIndexReaderExt index,
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

	private void readPoiTokenAtom(BinaryMapIndexReaderExt index,
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
					InspectorService.skipUnknownField(index.getInputStream(), tagWithType);
					break;
			}
		}
	}

	private void readPoiObjectsAtShift(BinaryMapIndexReaderExt index,
			BinaryMapPoiReaderAdapter.PoiRegion region,
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
							RawPoiObject rawPoiObject = readRawPoiObject(index.getInputStream(), x, y, zoom, region);
							if (rawPoiObject != null) {
								ObjectAddress objectAddress = toPoiObjectAddress(rawPoiObject, lang);
                                int payloadSize = poiLength + computeVarint32Size(poiLength);
                                int payloadOffset = (int) (index.getInputStream().getTotalBytesRead() - poiLength);
                                boolean isMatched = matchesLegacyPoi(rawPoiObject, matcher);
                                results.add(new ObjectAddress(0, objectAddress.name(), objectAddress.point(), objectAddress.values(), objectAddress.isPoi(), isMatched, false, false, objectAddress.type(), objectAddress.osmId(), objectAddress.osmType(), payloadOffset, payloadSize, (int) (region.getFilePointer() + relativeOffset)));
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
		Long osmId = rawPoiObject.id > 0 ? ObfConstants.getOsmIdFromMapObjectId(rawPoiObject.id) : null;
		String osmType = decodePoiOsmType(rawPoiObject.id);
		return new ObjectAddress(0, displayName, location, arrangeObjectAddressValues(values), true, false, false, false, "POI", osmId, osmType, 0, 0, 0);
	}

	private boolean matchesLegacyPoi(RawPoiObject rawPoiObject, CollatorStringMatcher matcher) {
		if (rawPoiObject == null || matcher == null) {
			return false;
		}
		boolean matches = matcher.matches(safeLowerCase(rawPoiObject.name))
				|| matcher.matches(safeLowerCase(getLegacyPoiEnName(rawPoiObject, true)));
		if (!matches) {
			for (Map.Entry<String, List<String>> entry : rawPoiObject.decodedTextTags.entrySet()) {
				String key = entry.getKey();
				if (key == null || !key.startsWith("name:") || "name:en".equals(key)) {
					continue;
				}
				for (String value : entry.getValue()) {
					matches = matcher.matches(safeLowerCase(value));
					if (matches) {
						break;
					}
				}
				if (matches) {
					break;
				}
			}
			if (!matches) {
				for (Map.Entry<String, List<String>> entry : rawPoiObject.decodedTextTags.entrySet()) {
					String key = entry.getKey();
					if (isTagIndexedForSearchAsName(key) || isTagIndexedForSearchAsId(key)
							|| isTagIndexedAsSearchRelated(key)) {
						for (String value : entry.getValue()) {
							matches = matcher.matches(value);
							if (matches) {
								break;
							}
						}
					}
					if (matches) {
						break;
					}
				}
			}
		}
		return matches;
	}

	private String getLegacyPoiEnName(RawPoiObject rawPoiObject, boolean transliterate) {
		if (!Algorithms.isEmpty(rawPoiObject.nameEn)) {
			return rawPoiObject.nameEn;
		} else if (!Algorithms.isEmpty(rawPoiObject.name) && transliterate) {
			return TransliterationHelper.transliterate(rawPoiObject.name);
		}
		return "";
	}

	private String safeLowerCase(String value) {
		return value == null ? "" : value.toLowerCase(Locale.ROOT);
	}

	private Map<String, String> arrangeObjectAddressValues(Map<String, String> values) {
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

	private List<ObjectAddress> filterMatchedObjects(List<ObjectAddress> results) {
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

	private List<ObjectAddress> markInvalidPoiAtoms(List<ObjectAddress> results) {
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
			markedResults.add(new ObjectAddress(objectAddress.sequenceId(), objectAddress.name(), objectAddress.point(), objectAddress.values(),
					objectAddress.isPoi(), objectAddress.isMatched(), isInvalidAtom, objectAddress.isAlone(), objectAddress.type(),
					objectAddress.osmId(), objectAddress.osmType(), objectAddress.payloadOffset(),
					objectAddress.payloadSize(), objectAddress.sourceOffset()));
		}
		return markedResults;
	}

	private List<ObjectAddress> markAloneObjects(List<ObjectAddress> results, IndexToken token) {
		if (results == null || results.isEmpty()) {
			return new ArrayList<>();
		}
		List<ObjectAddress> markedResults = new ArrayList<>(results.size());
		for (ObjectAddress objectAddress : results) {
			if (objectAddress == null) {
				continue;
			}
			boolean isAlone = isAloneTokenObject(objectAddress, token == null ? null : token.name());
			markedResults.add(new ObjectAddress(objectAddress.sequenceId(), objectAddress.name(), objectAddress.point(), objectAddress.values(),
					objectAddress.isPoi(), objectAddress.isMatched(), objectAddress.isInvalidAtom(), isAlone, objectAddress.type(),
					objectAddress.osmId(), objectAddress.osmType(), objectAddress.payloadOffset(),
					objectAddress.payloadSize(), objectAddress.sourceOffset()));
		}
		return markedResults;
	}

	private List<ObjectAddress> assignObjectSequenceIds(List<ObjectAddress> results) {
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
			numberedResults.add(new ObjectAddress(sequenceId++, objectAddress.name(), objectAddress.point(), objectAddress.values(),
					objectAddress.isPoi(), objectAddress.isMatched(), objectAddress.isInvalidAtom(), objectAddress.isAlone(), objectAddress.type(),
					objectAddress.osmId(), objectAddress.osmType(), objectAddress.payloadOffset(),
					objectAddress.payloadSize(), objectAddress.sourceOffset()));
		}
		return numberedResults;
	}

	private int countInvalidPoiAtoms(List<ObjectAddress> results) {
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

	record AddressTokenRefs(List<Integer> cityOffsets, List<Integer> streetOffsets, List<Integer> streetCityOffsets, List<AddressRef> addressRefs) {
		AddressTokenRefs() {
			this(new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
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
				int[] countMetrics = new int[] {
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
				int[] sizeMetrics = new int[] {
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

	private Boolean parseObjectTypeFilter(String objectType) {
		if (Algorithms.isEmpty(objectType)) {
			return null;
		}
		return switch (objectType.trim().toLowerCase(Locale.ROOT)) {
			case "poi" -> Boolean.TRUE;
			case "address", "addr" -> Boolean.FALSE;
			default -> null;
		};
	}

	private Comparator<ObjectAddress> buildObjectAddressComparator(String sortBy, String sortOrder) {
		String normalizedSortBy = Algorithms.isEmpty(sortBy) ? "sequenceid" : sortBy.trim().toLowerCase(Locale.ROOT);
		Comparator<ObjectAddress> comparator = switch (normalizedSortBy) {
			case "#", "sequence", "sequenceid" -> Comparator.comparingInt(object -> object == null ? Integer.MAX_VALUE : object.sequenceId());
			case "type" -> Comparator.comparing(object -> object == null || object.type() == null ? "" : object.type(), String.CASE_INSENSITIVE_ORDER);
			case "osmid" -> Comparator.comparingLong(object -> object == null || object.osmId() == null ? Long.MAX_VALUE : object.osmId());
			case "matched" -> Comparator.comparingInt(object -> object != null && object.isMatched() ? 1 : 0);
			case "alone", "isalone" -> Comparator.comparingInt(object -> object != null && object.isAlone() ? 1 : 0);
			default -> Comparator.comparing(object -> object == null || object.name() == null ? "" : object.name(), String.CASE_INSENSITIVE_ORDER);
		};
		comparator = comparator.thenComparingInt(object -> object == null ? Integer.MAX_VALUE : object.sequenceId())
				.thenComparing(object -> object == null || object.name() == null ? "" : object.name(), String.CASE_INSENSITIVE_ORDER);
		return "desc".equalsIgnoreCase(sortOrder) ? comparator.reversed() : comparator;
	}
	
	default void generateDb(List<String> obfs, OutputStream out) throws IOException, SQLException {
		generateDb(obfs, out, null);
	}

	default void generateDb(List<String> obfs, OutputStream out, GenerateDbProgressListener progressListener) throws IOException, SQLException {
		if (obfs == null || obfs.isEmpty()) {
			throw new IllegalArgumentException("OBF file list is required");
		}
		Path dbFile = Files.createTempFile("search-test-db-", ".sqlite");
		try {
			try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbFile.toAbsolutePath())) {
				createGenerateDbSchema(conn);
				conn.setAutoCommit(false);
				try {
					populateDb(conn, obfs, progressListener);
					conn.commit();
				} catch (Exception e) {
					conn.rollback();
					if (e instanceof CancellationException cancellationException) {
						throw cancellationException;
					}
					if (e instanceof SQLException sqlException) {
						throw sqlException;
					}
					if (e instanceof IOException ioException) {
						throw ioException;
					}
					throw new IOException("Failed to generate SQLite DB: " + e.getMessage(), e);
				}
			}
			try (ZipOutputStream zip = new ZipOutputStream(out)) {
				zip.putNextEntry(new ZipEntry("db.sqlite"));
				Files.copy(dbFile, zip);
				zip.closeEntry();
				zip.finish();
			}
		} finally {
			try {
				Files.deleteIfExists(dbFile);
			} catch (IOException e) {
				getLogger().warn("Failed to delete temporary generated DB {}", dbFile, e);
			}
		}
	}

	private void createGenerateDbSchema(Connection conn) throws SQLException {
		try (Statement stmt = conn.createStatement()) {
			stmt.execute("PRAGMA foreign_keys = ON");
			stmt.execute("PRAGMA journal_mode = OFF");
			stmt.execute("PRAGMA synchronous = OFF");
			stmt.execute("""
					CREATE TABLE obf (
						id INTEGER PRIMARY KEY AUTOINCREMENT,
						name TEXT NOT NULL
					)
					""");
			stmt.execute("""
					CREATE TABLE token (
						id INTEGER PRIMARY KEY AUTOINCREMENT,
						name TEXT NOT NULL UNIQUE,
						isCommon INTEGER NOT NULL,
						isFrequent INTEGER NOT NULL,
						poiSize INTEGER NOT NULL DEFAULT 0,
						poiCount INTEGER NOT NULL DEFAULT 0,
						addressSize INTEGER NOT NULL DEFAULT 0,
						addressCount INTEGER NOT NULL DEFAULT 0,
						poiRefs INTEGER NOT NULL DEFAULT 0,
						addressRefs INTEGER NOT NULL DEFAULT 0
					)
					""");
			stmt.execute("""
					CREATE TABLE "object" (
						id INTEGER PRIMARY KEY,
						name TEXT,
						lat REAL,
						lon REAL,
						"values" TEXT,
						type TEXT,
						osmType TEXT
					)
					""");
			stmt.execute("""
					CREATE TABLE posting (
						object_id INTEGER NOT NULL,
						token_id INTEGER NOT NULL,
						PRIMARY KEY (token_id, object_id)
					)
					""");
			stmt.execute("""
					CREATE TABLE obf_posting (
						obf_id INTEGER NOT NULL,
						token_id INTEGER NOT NULL,
						object_id INTEGER NOT NULL,
						sequenceId INTEGER NOT NULL,
						PRIMARY KEY (obf_id, token_id, sequenceId)
					)
					""");
		}
	}

	private void populateDb(Connection conn, List<String> obfs, GenerateDbProgressListener progressListener) throws SQLException, IOException {
		Map<String, Long> tokenIds = new HashMap<>();
		try (PreparedStatement insertObf = conn.prepareStatement("INSERT INTO OBF(name) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
		     PreparedStatement insertToken = conn.prepareStatement("""
				     INSERT INTO token(name, isCommon, isFrequent, poiSize, poiCount, addressSize, addressCount, poiRefs, addressRefs)
				     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
				     ON CONFLICT(name) DO UPDATE SET
				     	isCommon = CASE WHEN excluded.isCommon = 1 THEN 1 ELSE Token.isCommon END,
				     	isFrequent = CASE WHEN excluded.isFrequent = 1 THEN 1 ELSE Token.isFrequent END,
				     	poiSize = Token.poiSize + excluded.poiSize,
				     	poiCount = Token.poiCount + excluded.poiCount,
				     	addressSize = Token.addressSize + excluded.addressSize,
				     	addressCount = Token.addressCount + excluded.addressCount,
				     	poiRefs = Token.poiRefs + excluded.poiRefs,
				     	addressRefs = Token.addressRefs + excluded.addressRefs
				     """);
		     PreparedStatement selectTokenId = conn.prepareStatement("SELECT id FROM token WHERE name = ?");
		     PreparedStatement insertObject = conn.prepareStatement("""
				     INSERT OR IGNORE INTO "object"(id, name, lat, lon, "values", type, osmType)
				     VALUES (?, ?, ?, ?, ?, ?, ?)
				     """);
		     PreparedStatement insertPosting = conn.prepareStatement("""
				     INSERT OR IGNORE INTO posting(token_id, object_id)
				     VALUES (?, ?)
				     """);
		     PreparedStatement insertObfPosting = conn.prepareStatement("""
				     INSERT INTO obf_posting(obf_id, token_id, object_id, sequenceId)
				     VALUES (?, ?, ?, ?)
				     """)) {
			int totalObfs = obfs.size();
			Map<Integer, GenerateDbObfState> progressStates = new LinkedHashMap<>();
			Map<Integer, Long> obfIds = new HashMap<>();
			Map<Integer, AtomicInteger> objectCounts = new HashMap<>();
			Map<Integer, AtomicInteger> skippedWithoutOsmIds = new HashMap<>();
			for (int obfIndex = 0; obfIndex < obfs.size(); obfIndex++) {
				String obf = obfs.get(obfIndex);
				if (Algorithms.isEmpty(obf)) {
					continue;
				}
				String obfName = OBFService.getObfFileName(obf);
				long obfId = insertObfRow(insertObf, obfName);
				int displayIndex = obfIndex + 1;
				obfIds.put(displayIndex, obfId);
				objectCounts.put(displayIndex, new AtomicInteger());
				skippedWithoutOsmIds.put(displayIndex, new AtomicInteger());
				progressStates.put(displayIndex, new GenerateDbObfState(obfName, displayIndex, System.currentTimeMillis()));
			}
			ExecutorService executor = createGenerateDbExecutor();
			try {
				List<Future<GenerateDbObfTokens>> tokenFutures = new ArrayList<>();
				for (int obfIndex = 0; obfIndex < obfs.size(); obfIndex++) {
					String obf = obfs.get(obfIndex);
					if (Algorithms.isEmpty(obf)) {
						continue;
					}
					int displayIndex = obfIndex + 1;
					String obfName = OBFService.getObfFileName(obf);
					tokenFutures.add(executor.submit(() -> {
						long startMs = System.currentTimeMillis();
						List<IndexToken> tokens = loadAllGenerateDbTokens(obf);
						getLogger().info("generateDb: loaded {} tokens for OBF {}", tokens.size(), obfName);
						return new GenerateDbObfTokens(obf, obfName, displayIndex, startMs, tokens);
					}));
				}

				CompletionService<GenerateDbTokenChunk> chunkService = new ExecutorCompletionService<>(executor);
				int submittedChunks = 0;
				for (Future<GenerateDbObfTokens> tokenFuture : tokenFutures) {
					GenerateDbObfTokens obfTokens = getGenerateDbFuture(tokenFuture);
					GenerateDbObfState state = progressStates.get(obfTokens.obfIndex());
					if (state != null) {
						state.startMs = obfTokens.startMs();
						state.totalTokens = obfTokens.tokens().size();
						state.status = "RUNNING";
					}
					notifyGenerateDbProgress(progressListener, "RUNNING", obfTokens.obfName(), obfTokens.obfIndex(), totalObfs,
							0, obfTokens.tokens().size(), obfTokens.startMs(), null, progressStates);
					for (int start = 0; start < obfTokens.tokens().size(); start += 10) {
                        int to = Math.min(start + 10, obfTokens.tokens().size());
						List<IndexToken> tokenChunk = obfTokens.tokens().subList(start, to);
						chunkService.submit(() -> loadGenerateDbTokenChunk(obfTokens, tokenChunk));
						submittedChunks++;
					}
					if (obfTokens.tokens().isEmpty()) {
						if (state != null) {
							state.markDone();
						}
						notifyGenerateDbProgress(progressListener, "RUNNING", obfTokens.obfName(), obfTokens.obfIndex(), totalObfs,
								0, 0, obfTokens.startMs(), null, progressStates);
					}
				}

				for (int completedChunks = 0; completedChunks < submittedChunks; completedChunks++) {
					GenerateDbTokenChunk chunk = getGenerateDbFuture(chunkService.take());
					long obfId = obfIds.get(chunk.obfIndex());
					GenerateDbObfState state = progressStates.get(chunk.obfIndex());
					for (GenerateDbTokenObjects tokenObjects : chunk.tokens()) {
						IndexToken token = tokenObjects.token();
						if (token == null || Algorithms.isEmpty(token.name())) {
							incrementGenerateDbProgress(state, progressListener, chunk, totalObfs, progressStates);
							continue;
						}
						ObjectAddressPage objectsPage = tokenObjects.objectsPage();
						long tokenId = upsertGenerateDbToken(insertToken, selectTokenId, tokenIds, token, objectsPage);
						for (ObjectAddress objectAddress : objectsPage.content()) {
							if (objectAddress == null || !objectAddress.isMatched()) {
								continue;
							}
							if (objectAddress.osmId() == null) {
								skippedWithoutOsmIds.get(chunk.obfIndex()).incrementAndGet();
								continue;
							}
							insertGenerateDbObject(insertObject, objectAddress);
							objectCounts.get(chunk.obfIndex()).incrementAndGet();
							insertGenerateDbPosting(insertPosting, tokenId, objectAddress);
							insertGenerateDbObfPosting(insertObfPosting, obfId, tokenId, objectAddress);
						}
						incrementGenerateDbProgress(state, progressListener, chunk, totalObfs, progressStates);
					}
				}
				for (GenerateDbObfState state : progressStates.values()) {
					state.markDone();
					notifyGenerateDbProgress(progressListener, "RUNNING", state.obfName, state.obfIndex, totalObfs,
							state.processedTokens, state.totalTokens, state.startMs, null, progressStates);
					getLogger().info("generateDb: completed {} objects for OBF {}", objectCounts.get(state.obfIndex).get(), state.obfName);
					int skipped = skippedWithoutOsmIds.get(state.obfIndex).get();
					if (skipped > 0) {
						getLogger().info("generateDb: skipped {} matched objects without OSM ID for OBF {}", skipped, state.obfName);
					}
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IOException("Interrupted while generating SQLite DB", e);
			} finally {
				executor.shutdownNow();
			}
		}
	}

	private ExecutorService createGenerateDbExecutor() {
		int maxCount = Algorithms.parseIntSilently(System.getenv("MAX_THREAD_NUMBER"),
				Math.max(1, Runtime.getRuntime().availableProcessors()));
		maxCount = Math.max(1, maxCount);
		ThreadFactory tf = r -> {
			Thread t = new Thread(r);
			t.setName("search-test-generate-db-" + t.getId());
			t.setDaemon(true);
			return t;
		};
		return Executors.newFixedThreadPool(maxCount, tf);
	}

	private GenerateDbTokenChunk loadGenerateDbTokenChunk(GenerateDbObfTokens obfTokens, List<IndexToken> tokens) {
		List<GenerateDbTokenObjects> result = new ArrayList<>(tokens.size());
		for (IndexToken token : tokens) {
			if (token == null || Algorithms.isEmpty(token.name())) {
				result.add(new GenerateDbTokenObjects(obfTokens.obf(), obfTokens.obfName(), obfTokens.obfIndex(), obfTokens.startMs(), token,
						new ObjectAddressPage(Collections.emptyList(), 0, 0, 0, 0, new int[0], new int[0], 0, 0)));
				continue;
			}
			ObjectAddressPage objectsPage = getObjects(obfTokens.obf(), "en", token, null, 0, Integer.MAX_VALUE, null, null, true, false, null);
			result.add(new GenerateDbTokenObjects(obfTokens.obf(), obfTokens.obfName(), obfTokens.obfIndex(), obfTokens.startMs(), token, objectsPage));
		}
		return new GenerateDbTokenChunk(obfTokens.obf(), obfTokens.obfName(), obfTokens.obfIndex(), obfTokens.startMs(), result);
	}

	private <T> T getGenerateDbFuture(Future<T> future) throws IOException {
		try {
			return future.get();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IOException("Interrupted while generating SQLite DB", e);
		} catch (ExecutionException e) {
			Throwable cause = e.getCause();
			if (cause instanceof RuntimeException runtimeException) {
				throw runtimeException;
			}
			if (cause instanceof IOException ioException) {
				throw ioException;
			}
			throw new IOException("Failed to generate SQLite DB: " + cause.getMessage(), cause);
		}
	}

	private void incrementGenerateDbProgress(GenerateDbObfState state,
			GenerateDbProgressListener progressListener,
			GenerateDbTokenChunk chunk,
			int totalObfs,
			Map<Integer, GenerateDbObfState> progressStates) {
		if (state == null) {
			return;
		}
		state.processedTokens++;
		if (state.processedTokens >= state.totalTokens) {
			state.markDone();
		}
		notifyGenerateDbProgress(progressListener, "RUNNING", chunk.obfName(), chunk.obfIndex(), totalObfs,
				state.processedTokens, state.totalTokens, state.startMs, null, progressStates);
		if (state.processedTokens % 1000 == 0 || state.processedTokens == state.totalTokens) {
			getLogger().info("generateDb: processed {} of {} tokens for OBF {}", state.processedTokens, state.totalTokens, state.obfName);
		}
	}

	class GenerateDbObfState {
		final String obfName;
		final int obfIndex;
		long startMs;
		long completedElapsedMs = -1;
		int totalTokens;
		int processedTokens;
		String status = "PENDING";

		GenerateDbObfState(String obfName, int obfIndex, long startMs) {
			this.obfName = obfName;
			this.obfIndex = obfIndex;
			this.startMs = startMs;
		}

		void markDone() {
			if (!"DONE".equals(status)) {
				completedElapsedMs = Math.max(0, System.currentTimeMillis() - startMs);
			}
			status = "DONE";
		}
	}

	private void notifyGenerateDbProgress(GenerateDbProgressListener progressListener,
			String status,
			String obfName,
			int obfIndex,
			int totalObfs,
			int processedTokens,
			int totalTokens,
			long startMs,
			String error,
			Map<Integer, GenerateDbObfState> progressStates) {
		if (progressListener == null) {
			return;
		}
		long elapsedMs = Math.max(0, System.currentTimeMillis() - startMs);
		long estimatedMs = processedTokens > 0 && totalTokens > 0
				? Math.max(0, (elapsedMs * totalTokens / processedTokens) - elapsedMs)
				: -1;
		List<GenerateDbObfProgress> obfProgress = new ArrayList<>();
		if (progressStates != null) {
			long now = System.currentTimeMillis();
			for (GenerateDbObfState state : progressStates.values()) {
				long stateElapsedMs = "DONE".equals(state.status) && state.completedElapsedMs >= 0
						? state.completedElapsedMs
						: Math.max(0, now - state.startMs);
				long stateEstimatedMs = state.processedTokens > 0 && state.totalTokens > 0 && !"DONE".equals(state.status)
						? Math.max(0, (stateElapsedMs * state.totalTokens / state.processedTokens) - stateElapsedMs)
						: ("DONE".equals(state.status) ? 0 : -1);
				obfProgress.add(new GenerateDbObfProgress(state.obfName, state.obfIndex, state.totalTokens,
						state.processedTokens, stateElapsedMs, stateEstimatedMs, state.status));
			}
		}
		progressListener.onProgress(new GenerateDbProgress(status, obfName, obfIndex, totalObfs,
				processedTokens, totalTokens, elapsedMs, estimatedMs, error, obfProgress));
	}

	private long insertObfRow(PreparedStatement insertObf, String name) throws SQLException {
		insertObf.setString(1, name);
		insertObf.executeUpdate();
		try (java.sql.ResultSet rs = insertObf.getGeneratedKeys()) {
			if (rs.next()) {
				return rs.getLong(1);
			}
		}
		throw new SQLException("Failed to insert OBF row");
	}

	private List<IndexToken> loadAllGenerateDbTokens(String obf) {
		List<IndexToken> tokens = new ArrayList<>();
		int page = 0;
		int pageSize = 100;
		while (true) {
			IndexTokenPage tokenPage = getIndex(obf, null, page, pageSize, "name", "asc");
			tokens.addAll(tokenPage.content());
			if (page + 1 >= tokenPage.totalPages()) {
				break;
			}
			page++;
		}
		return tokens;
	}

	private long upsertGenerateDbToken(PreparedStatement insertToken,
			PreparedStatement selectTokenId,
			Map<String, Long> tokenIds,
			IndexToken token,
			ObjectAddressPage objectsPage) throws SQLException {
		int[] countMetrics = objectsPage == null ? null : objectsPage.countMetrics();
		int[] sizeMetrics = objectsPage == null ? null : objectsPage.sizeMetrics();
		insertToken.setString(1, token.name());
		insertToken.setInt(2, token.isCommon() ? 1 : 0);
		insertToken.setInt(3, token.isFrequent() ? 1 : 0);
		insertToken.setInt(4, metricValue(sizeMetrics, 6));
		insertToken.setInt(5, metricValue(countMetrics, 3));
		insertToken.setInt(6, metricValue(sizeMetrics, 8));
		insertToken.setInt(7, metricValue(countMetrics, 5));
		insertToken.setInt(8, token.poiRefs() == null ? 0 : token.poiRefs().length);
		insertToken.setInt(9, token.addressRefs() == null ? 0 : token.addressRefs().length);
		insertToken.executeUpdate();
		Long cachedId = tokenIds.get(token.name());
		if (cachedId != null) {
			return cachedId;
		}
		selectTokenId.setString(1, token.name());
		try (java.sql.ResultSet rs = selectTokenId.executeQuery()) {
			if (rs.next()) {
				long id = rs.getLong(1);
				tokenIds.put(token.name(), id);
				return id;
			}
		}
		throw new SQLException("Failed to resolve token id for " + token.name());
	}

	private int metricValue(int[] metrics, int index) {
		return metrics == null || index < 0 || index >= metrics.length ? 0 : metrics[index];
	}

	private void insertGenerateDbObject(PreparedStatement insertObject, ObjectAddress objectAddress) throws SQLException, IOException {
		LatLon point = objectAddress.point();
		insertObject.setLong(1, objectAddress.osmId());
		insertObject.setString(2, objectAddress.name());
		if (point == null) {
			insertObject.setNull(3, java.sql.Types.REAL);
			insertObject.setNull(4, java.sql.Types.REAL);
		} else {
			insertObject.setDouble(3, point.getLatitude());
			insertObject.setDouble(4, point.getLongitude());
		}
		insertObject.setString(5, getObjectMapper().writeValueAsString(objectAddress.values()));
		insertObject.setString(6, objectAddress.type());
		insertObject.setString(7, objectAddress.osmType());
		insertObject.executeUpdate();
	}

	private void insertGenerateDbPosting(PreparedStatement insertPosting,
			long tokenId,
			ObjectAddress objectAddress) throws SQLException {
		insertPosting.setLong(1, tokenId);
		insertPosting.setLong(2, objectAddress.osmId());
		insertPosting.executeUpdate();
	}

	private void insertGenerateDbObfPosting(PreparedStatement insertObfPosting,
			long obfId,
			long tokenId,
			ObjectAddress objectAddress) throws SQLException {
		insertObfPosting.setLong(1, obfId);
		insertObfPosting.setLong(2, tokenId);
		insertObfPosting.setLong(3, objectAddress.osmId());
		insertObfPosting.setInt(4, objectAddress.sequenceId());
		insertObfPosting.executeUpdate();
	}
}
