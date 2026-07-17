package net.osmand.obf.preparation;


import java.text.Collator;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import gnu.trove.list.array.TIntArrayList;
import net.osmand.CollatorStringMatcher;
import net.osmand.binary.CommonWords;
import net.osmand.binary.NameIndexReader;
import net.osmand.data.City;
import net.osmand.data.City.CityType;
import net.osmand.data.MapObject;
import net.osmand.data.Street;
import net.osmand.obf.preparation.IndexPoiCreator.PoiAdditionalType;
import net.osmand.obf.preparation.IndexPoiCreator.PoiTileBox;
import net.osmand.osm.AbstractPoiType;
import net.osmand.osm.MapPoiTypes;
import net.osmand.search.core.TopIndexFilter;
import net.osmand.util.Algorithms;
import net.osmand.util.SearchAlgorithms;

public class NameIndexCreator<T> {

	private static final int NAMED_WORDS_SEPARATOR = 0;
	private static final int MIN_LIMIT_FREQ_COMMON = 10; // minimum required for common to have at least
	// Large ADD_TOP_X_FREQ_WORDS many will cause to add many common words to index !
	private static final int ADD_TOP_X_FREQ_WORDS = 10; // minimum required  for frequent to be added and indexed 
	
	private static final int POI_CATEGORY_PREFIX_LENGTH = 5;
	public static boolean NOT_INDEX_COMMON_IF_THERE_ARE_RARE = true;
	public static boolean INDEX_RARE_WORDS_FOR_COMMON = false;
	public static boolean INDEX_RARE_WORDS_FOR_NON_COMMON = false;
	
	Map<String, NamedObjectsByPrefix<T>> namesIndex = new TreeMap<>(Collator.getInstance());
	
	PrepareWordsIndex commonWords;
	CommonWords predefinedWords;
	
	Map<String, Integer> tokenFrequencies = new HashMap<String, Integer>();
	Map<String, Integer> commonNonIndexedFrequencies = new HashMap<String, Integer>();
	
	
	public NameIndexCreator(CommonWords c) {
		this.predefinedWords = c;
	}
	
	public record PoiNameObject(PoiTileBox tileBox, int ind, int eloRating, 
			String type, String subtype, Set<PoiAdditionalType> additionalTags) {
	}
	
	// common words
	public record PrepareWordIndex (int index, String word, int frequency, int nonindexed) { }
	
	public record PrepareWordsIndex (Map<String, PrepareWordIndex> words, List<PrepareWordIndex> wordsLst) { }
	
	
	// suffix dictionary
	public record SuffixEntry(String resolvedSuffix, String encodedSuffix) {}
    public static class SuffixDictionary<T> {
        public final List<SuffixEntry> dictionaryEntries = new ArrayList<>();
        public final Map<String, Integer> resolvedSuffixToIndex = new HashMap<>();
        List<Integer> commonsRef = new ArrayList<Integer>();
        Map<String, Integer> usedCommons = new TreeMap<>(); 
    }

    // single objects
	public record NameObjectSingleNameIndex(String token, Set<String> setNames, List<String> listNames) {}
	
    public static class NamedObject<T> {
    	T object;
    	TIntArrayList bitsetIndex = new TIntArrayList();
    	TIntArrayList otherWordsCount = new TIntArrayList();
    	List<String> extraSuffixes = new ArrayList<>();
    	List<NameObjectSingleNameIndex> singleNames = new ArrayList<>();
    	
		public boolean isOtherWordsNonZeros() {
			for (int k = 0; k < otherWordsCount.size(); k++) {
				if (otherWordsCount.get(k) != 0) {
					return true;
				}
			}
			return false;
		}
	}
	
	public static class NamedObjectsByPrefix<T> {
		public String prefix;
		public final List<NamedObject<T>> namedObjects = new ArrayList<>();
		public SuffixDictionary<T> suffixes = null;

		
		public boolean addToken(T object, String token, List<String> allNames) {
			NamedObject<T> last = namedObjects.size() == 0 ? null : namedObjects.get(namedObjects.size() - 1);
			Set<String> setNames = allNames.size() == 1 ? Collections.emptySet() : new TreeSet<String>(allNames);
			if (last == null || last.object != object) {
				// reuse last
				last = new NamedObject<>();
				last.object = object;
				namedObjects.add(last);
			} else {
				for (NameObjectSingleNameIndex st : last.singleNames) {
					if (st.token.equals(token) && st.setNames.equals(setNames)) {
						return false;
					}
				}
			}
			last.singleNames.add(new NameObjectSingleNameIndex(token, setNames, allNames));
			return true;
		}
		
		// COMMON WORDS
		public SuffixDictionary<T> build(PrepareWordsIndex commonWords) {
			suffixes = new SuffixDictionary<>();
			TreeSet<String> allSortedSuffixes = new TreeSet<>();
			// 1. collect all suffixes to be stored
			for (NamedObject<T> namesList : namedObjects) {
				for (NameObjectSingleNameIndex singleName : namesList.singleNames) {
					boolean isCommonWord = commonWords.words.containsKey(singleName.token);
					String suffix = calculateSuffix(singleName.token, prefix);
					allSortedSuffixes.add(suffix);
					for (String otherName : singleName.setNames) {
						if (!otherName.equals(singleName.token)) {
							if (SearchAlgorithms.isNumber2Letters(otherName)) {
//								if (parsePureIntegerSuffix(otherName) != null) {	
								// skip encode as int and extra word
							} else if(commonWords.words.containsKey(otherName)){
								// skip indexed common words
							} else {
								if (INDEX_RARE_WORDS_FOR_COMMON && isCommonWord) {
									allSortedSuffixes.add(" " + otherName);
								}
								if (INDEX_RARE_WORDS_FOR_NON_COMMON && !isCommonWord)  {
									allSortedSuffixes.add(" " + otherName);
								}
							}
						}
					}
				}
			}
			// 2. prepare all suffixes list
			String previousSuffix = null;
			for (String suffix : allSortedSuffixes) {
				String encodedSuffix = SearchAlgorithms.nameIndexEncodeSuffix(suffix, previousSuffix);
				SuffixEntry entry = new SuffixEntry(suffix, encodedSuffix);
				suffixes.resolvedSuffixToIndex.put(entry.resolvedSuffix(), suffixes.dictionaryEntries.size());
				suffixes.dictionaryEntries.add(entry);
				previousSuffix = suffix;
			}
			// 3. prepare for 1 object bitsets 
			for (NamedObject<T> namedObject : namedObjects) {
				boolean allEmptyExtra = true;
				for (NameObjectSingleNameIndex singleName : namedObject.singleNames) {
					PrepareWordIndex word = commonWords.words.get(singleName.token);
					boolean isCommon = word != null && word.nonindexed != 0;
					if (isCommon) {
						boolean rare = false;
						for (String r : singleName.setNames) {
							if (!commonWords.words.containsKey(r) &&
									!SearchAlgorithms.isNumber2Letters(r)) {
								rare = true;
								break;
							}
						}
						if (rare && NOT_INDEX_COMMON_IF_THERE_ARE_RARE) {
							continue;
						}
					}
					if (namedObject.bitsetIndex.size() != 0) {
						namedObject.bitsetIndex.add(NAMED_WORDS_SEPARATOR); // separator
					}
					boolean isCommonWord = commonWords.words.containsKey(singleName.token);
					String suffix = calculateSuffix(singleName.token, prefix);
					namedObject.bitsetIndex.add((suffixes.resolvedSuffixToIndex.get(suffix) + 1) << 1);
					int otherWords = 0;
					String extraToken = "";
					for (String otherName : singleName.setNames) {
						if (!otherName.equals(singleName.token)) {
							if (SearchAlgorithms.isNumber2Letters(otherName)) {
								Integer pInteger = parsePureIntegerSuffix(otherName);
								if (pInteger != null) {
									// partial number ends with 11 but separated number as 01
									namedObject.bitsetIndex.add((pInteger << 2) + 1);
								} else {
									extraToken += " " + otherName;
								}
							} else if (commonWords.words.containsKey(otherName)) {
								if (!suffixes.usedCommons.containsKey(otherName)) {
									int indx = commonWords.words.get(otherName).index;
									suffixes.commonsRef.add(indx);
									suffixes.usedCommons.put(otherName, suffixes.usedCommons.size());
								}
								int calcRef = suffixes.dictionaryEntries.size();
								calcRef += suffixes.usedCommons.get(otherName);
								namedObject.bitsetIndex.add((calcRef + 1) << 1);
								// skip indexed common words
							} else if (isCommonWord) {
								if (INDEX_RARE_WORDS_FOR_COMMON) {
									Integer ind = suffixes.resolvedSuffixToIndex.get(" " + otherName);
									namedObject.bitsetIndex.add((ind + 1) << 1);
								} else {
									otherWords++;
								}
							} else {
								if (INDEX_RARE_WORDS_FOR_NON_COMMON) {
									Integer ind = suffixes.resolvedSuffixToIndex.get(" " + otherName);
									namedObject.bitsetIndex.add((ind + 1) << 1);
								} else {
									otherWords++;
								}
							}
						} else if (singleName.setNames.size() < singleName.listNames.size()) {
							int i1 = singleName.listNames.indexOf(otherName);
							int i2 = singleName.listNames.lastIndexOf(otherName);
							// duplicate word store as separate name
							if (i1 != i2) {
								extraToken += " " + otherName;
							}
						}
					}
					// extraSuffixes are not used for now?
					namedObject.otherWordsCount.add(otherWords);
					namedObject.extraSuffixes.add(extraToken);
					if (extraToken.length() > 0) {
						allEmptyExtra = false;
					}
				}
				if (allEmptyExtra) {
					namedObject.extraSuffixes.clear();
				}
				
			}
			return this.suffixes;

		}
		
	}
	
	public PrepareWordsIndex buildCommonWords(Map<String, NamedObjectsByPrefix<T>> map) {
		List<String> commonStrings = new ArrayList<String>();
		Set<String> topXFrequent = new HashSet<>();
		for (int i = 0; i < ADD_TOP_X_FREQ_WORDS; i++) {
			int max = -1;
			String top = null;
			for (Map.Entry<String, Integer> e : tokenFrequencies.entrySet()) {
				if (e.getValue() > max && !topXFrequent.contains(e.getKey())
						&& !e.getKey().startsWith(NameIndexReader.POI_CATEGORY_PREFIX)) {
					max = e.getValue();
					top = e.getKey();
				}
			}
			if (top != null) {
				topXFrequent.add(top);
			}
		}
		
		// Here we could also compute and add some top 5 frequent
		for (Map.Entry<String, Integer> e : tokenFrequencies.entrySet()) {
			String s = e.getKey();
			if (s.equalsIgnoreCase(NameIndexReader.CITY_AS_STREET_COMMON)) {
				commonStrings.add(s);
				continue;
			}
			// don't add all common ! for some maps they could have different meaning
			if (e.getValue() < MIN_LIMIT_FREQ_COMMON) {
				continue;
			}
			boolean common = predefinedWords.isCommon(s);
			boolean freq = predefinedWords.getFrequentlyUsed(s) >= 0;
			if (common || freq || topXFrequent.contains(e.getKey())) {
				commonStrings.add(s);
			}
		}
		
		Collections.sort(commonStrings);
		Map<String, PrepareWordIndex> words = new HashMap<String, NameIndexCreator.PrepareWordIndex>();
		List<PrepareWordIndex> wordsList = new ArrayList<>();
		int ind = 0;
		for (String c : commonStrings) {
			Integer matched = tokenFrequencies.get(c);
			Integer nonIndexed = commonNonIndexedFrequencies.get(c);
			PrepareWordIndex word = new PrepareWordIndex(ind++, c, matched, nonIndexed == null ? 0 : nonIndexed);
			words.put(c, word);
			wordsList.add(word);
		}
		commonWords = new PrepareWordsIndex(words, wordsList);
		return commonWords;
	}
	
	
	public void cleanupPoiNames(int max) {
		for (String prefix : new ArrayList<>(namesIndex.keySet())) {
			if (prefix.startsWith(NameIndexReader.POI_CATEGORY_PREFIX)) {
				NamedObjectsByPrefix<T> objects = namesIndex.get(prefix);
				Iterator<NamedObject<T>> it = objects.namedObjects.iterator();
				Map<String, Integer> counts = new HashMap<String, Integer>();
				while (it.hasNext()) {
					NamedObject<T> obj = it.next();
					for (NameObjectSingleNameIndex t : obj.singleNames) {
						counts.compute(t.token, (_t, u) -> u == null ? 1 : u + 1);
					}
				}
				it = objects.namedObjects.iterator();
				while (it.hasNext()) {
					NamedObject<T> obj = it.next();
					Iterator<NameObjectSingleNameIndex> its = obj.singleNames.iterator();
					while (its.hasNext()) {
						NameObjectSingleNameIndex t = its.next();
						String token = t.token;
						if (counts.get(token) >= max) {
							tokenFrequencies.remove(token);
							its.remove();
						}
					}
					if (obj.singleNames.isEmpty()) {
						it.remove();
					}
				}
				if (objects.namedObjects.isEmpty()) {
					namesIndex.remove(prefix);
				}
			}
		}
	}
	
	public static void addPoiCategories(NameIndexCreator<PoiNameObject> th, PoiNameObject obj, MapPoiTypes poiTypes) {
		addPoiCategory(th, obj, obj.subtype, poiTypes);
		if (obj.additionalTags != null) {
			for (PoiAdditionalType o : obj.additionalTags) {
				String key = o.getTag();
				if (o.getTag().startsWith(MapPoiTypes.TOP_INDEX_ADDITIONAL_PREFIX)) {
					key = o.getTag() + "_" + TopIndexFilter.getValueKey(o.getValue());
				}
				addPoiCategory(th, obj, key, poiTypes);
			}
		}
	}

	private static void addPoiCategory(NameIndexCreator<PoiNameObject> th, PoiNameObject obj, String token, MapPoiTypes poiTypes) {
		AbstractPoiType pt = poiTypes.getAnyPoiTypeByKey(token);
		if (pt != null && pt.isNonIndx()) {
			return;
		}
		token = NameIndexReader.POI_CATEGORY_PREFIX + token;
		String prefix = token.substring(0, Math.min(token.length(), POI_CATEGORY_PREFIX_LENGTH));
		NamedObjectsByPrefix<PoiNameObject> entry = th.namesIndex.get(prefix);
		if (entry == null) {
			entry = new NamedObjectsByPrefix<PoiNameObject>();
			entry.prefix = prefix;
			th.namesIndex.put(prefix, entry);
		}
		boolean added = entry.addToken(obj, token, Collections.singletonList(token));
		if (added) {
			th.tokenFrequencies.compute(token, (t, u) -> u == null ? 1 : u + 1);
		}
	}
	
	public void addToNameIndex(String name, T obj, int maxPrefixLength, boolean indexNumbers) {
		if (obj instanceof Street s) {
//			if(name.startsWith("<") && name.trim().endsWith(">") && 
//				!name.startsWith("<<")) {
//				name += " " + NameIndexReader.CITY_AS_STREET_COMMON;
//			} else 
			if (s.getNamesMap(false).containsKey(MapObject.NAME_PLACE_ATTR)) {
				name += " " + NameIndexReader.CITY_AS_STREET_COMMON;
			}
		}
		List<String> uniqueNames = SearchAlgorithms.splitAndNormalize(name, true);
		List<String> allNames = SearchAlgorithms.splitAndNormalize(name, false);
		boolean hasRareName = false;
		for (String token : uniqueNames) {
			if (!token.equalsIgnoreCase(NameIndexReader.CITY_AS_STREET_COMMON) &&
					!predefinedWords.isCommon(token) && predefinedWords.getFrequentlyUsed(token) <= 0) {
				hasRareName = true;
				break;
			}
		}
		for (String token : uniqueNames) {
			if (Algorithms.isEmpty(token)) {
				continue;
			}
			if (token.equalsIgnoreCase(NameIndexReader.CITY_AS_STREET_COMMON)) {
				tokenFrequencies.compute(token, (t, u) -> u == null ? 1 : u + 1);
				commonNonIndexedFrequencies.compute(token, (t, u) -> u == null ? 1 : u + 1);
				continue;
			}
			String prefix = nameIndexPreparePrefix(token, maxPrefixLength);
			if (Algorithms.isEmpty(prefix)) {
				continue;
			}
			if (!indexNumbers && SearchAlgorithms.isNumber2Letters(token)) {
				continue;
			}
			NamedObjectsByPrefix<T> entry = namesIndex.get(prefix);
			if (entry == null) {
				entry = new NamedObjectsByPrefix<T>();
				entry.prefix = prefix;
				namesIndex.put(prefix, entry);
			}
			boolean added = entry.addToken(obj, token, allNames);
			if (!added) {
				continue;
			}
			tokenFrequencies.compute(token, (t, u) -> u == null ? 1 : u + 1);
			boolean c = predefinedWords.isCommon(token);
			if (c && hasRareName) {
				commonNonIndexedFrequencies.compute(token, (t, u) -> u == null ? 1 : u + 1);
			}
			
		}		
	}

	
	private static String substringByCodePoints(String value, int codePointCount) {
        if (codePointCount <= 0 || value.isEmpty()) {
            return "";
        }
        int availableCodePointCount = value.codePointCount(0, value.length());
        if (codePointCount >= availableCodePointCount) {
            return value;
        }
        return value.substring(0, value.offsetByCodePoints(0, codePointCount));
    }

	
    public static String nameIndexPreparePrefix(String token, int maxPrefixLength) {
        String normalizedToken = SearchAlgorithms.normalizeToken(token);
	    if (maxPrefixLength <= 0) {
		    return "";
	    }
        if (normalizedToken.codePointCount(0, normalizedToken.length()) > maxPrefixLength) {
	        return substringByCodePoints(normalizedToken, maxPrefixLength);
        }
        return normalizedToken;
    }
	

	private static String removeBraces(String localeName) {
		int i = localeName.indexOf('(');
		String retName = localeName;
		if (i > -1) {
			retName = localeName.substring(0, i);
			int j = localeName.indexOf(')', i);
			if (j > -1) {
				// remove
				retName = retName.trim() + ' ' + localeName.substring(j + 1).trim();
			}
		}
		return retName;
	}
	

	private static Integer parsePureIntegerSuffix(String token) {
		if (token == null || token.length() == 0) {
			return null;
		}
		try {
			if (token.charAt(token.length() - 1) == CollatorStringMatcher.INCOMPLETE_DOT) {
				token = token.substring(0, token.length() - 1);
			}
			int int1 = Integer.parseInt(token);
			if (!token.equals(int1 + "") || int1 > (Integer.MAX_VALUE >> 2)) {
				return null;
			}
			return int1;
		} catch (NumberFormatException e) {
			return null;
		}
	}

	private static String calculateSuffix(String token, String prefix) {
		int suffixOffset = suffixOffsetAfterPrefix(token, prefix);
        String suffix;
        if (suffixOffset < 0) {
            if (!Objects.equals(token, prefix)) {
                return null;
            }
            suffix = "";
        } else {
            suffix = Normalizer.normalize(token.substring(suffixOffset), Normalizer.Form.NFC);
        }
        return suffix;
	}
	
	private record CodePointPrefixMatch(int leftOffset, int rightOffset, int commonPrefixCodePointLength) {
	}

    private static CodePointPrefixMatch startWith(String token, String prefix) {
        int leftOffset = 0;
        int rightOffset = 0;
        int commonPrefixCodePointLength = 0;
        while (leftOffset < token.length() && rightOffset < prefix.length()) {
            int leftCodePoint = token.codePointAt(leftOffset);
            int rightCodePoint = prefix.codePointAt(rightOffset);
            if (leftCodePoint != rightCodePoint) {
                break;
            }
            leftOffset += Character.charCount(leftCodePoint);
            rightOffset += Character.charCount(rightCodePoint);
            commonPrefixCodePointLength++;
        }
        return new CodePointPrefixMatch(leftOffset, rightOffset, commonPrefixCodePointLength);
    }

    private static int suffixOffsetAfterPrefix(String token, String prefix) {
        CodePointPrefixMatch prefixMatch = startWith(token, prefix);
        if (prefixMatch.rightOffset != prefix.length()) {
            return -1;
        }
        return prefixMatch.leftOffset < token.length() ? prefixMatch.leftOffset : -1;
    }


    public static void putAddrNamedMapObject(NameIndexCreator<MapObject> nameIndex, MapObject o, long fileOffset, IndexCreatorSettings settings) {
		String name = o.getName();
		// getOtherNames ignores "admin_level", "place"
		boolean postcode = (o instanceof City c && c.getType() == CityType.POSTCODE);
		nameIndex.addToNameIndex(removeBraces(name), o,  settings.charsToBuildAddressNameIndex, postcode);
		for (String oName : o.getOtherNames(true, name)) {
			if (!oName.equalsIgnoreCase(name)) {
				nameIndex.addToNameIndex(removeBraces(oName), o,  settings.charsToBuildAddressNameIndex, postcode);
			}
		}
		if (fileOffset > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("File offset > 2 GB.");
		}
		o.setFileOffset((int) fileOffset);
	}



}
