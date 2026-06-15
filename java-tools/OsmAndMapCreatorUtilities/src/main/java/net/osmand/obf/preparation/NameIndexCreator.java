package net.osmand.obf.preparation;


import java.text.Collator;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import gnu.trove.list.array.TIntArrayList;
import net.osmand.binary.CommonWords;
import net.osmand.data.City;
import net.osmand.data.City.CityType;
import net.osmand.data.MapObject;
import net.osmand.obf.preparation.IndexPoiCreator.PoiTileBox;
import net.osmand.util.Algorithms;
import net.osmand.util.SearchAlgorithms;

public class NameIndexCreator<T> {

	private static final int NAMED_WORDS_SEPARATOR = 0;
	public static boolean NOT_INDEX_COMMON_IF_THERE_ARE_RARE = true;
	public static boolean INDEX_RARE_WORDS_FOR_COMMON = false;
	public static boolean INDEX_RARE_WORDS_FOR_NON_COMMON = false;
	
	Map<String, Integer> tokenFrequencies = new HashMap<String, Integer>();

	PrepareWordsIndex commonWords;
	
	Map<String, NamedObjectsByPrefix<T>> namesIndex = new TreeMap<>(Collator.getInstance());
	
	
	public record PoiNameObject(PoiTileBox tileBox, int ind) {  }
	
	// common words
	public record PrepareWordIndex (String word, int frequency, int nonmatched, int index) { }
	
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
	public record NameObjectSingleNameIndex(String token, Set<String> allNames) {}
	
    public static class NamedObject<T> {
    	T object;
    	TIntArrayList bitsetIndex = new TIntArrayList();
    	TIntArrayList otherWordsCount = new TIntArrayList();
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

		
		public void addToken(T object, String token, Collection<String> allNames) {
			NamedObject<T> last = namedObjects.size() == 0  ? null : namedObjects.get(namedObjects.size() -1 );
			if(last == null || last.object != object) {
				// reuse last
				last = new NamedObject<>();
				last.object = object;
				namedObjects.add(last);
			}
			last.singleNames.add(new NameObjectSingleNameIndex(token, allNames.size() == 1 ? Collections.emptySet() : 
				new TreeSet<String>(allNames)));
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
					for (String otherName : singleName.allNames) {
						if (!otherName.equals(singleName.token)) {
							if (parsePureIntegerSuffix(otherName) != null) {
								// skip encode as int
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
				for (NameObjectSingleNameIndex singleName : namedObject.singleNames) {
					PrepareWordIndex word = commonWords.words.get(singleName.token);
					boolean isCommon = word != null && word.nonmatched != 0;
					if (isCommon) {
						boolean rare = false;
						for (String r : singleName.allNames) {
							if (!commonWords.words.containsKey(r)) {
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
					namedObject.bitsetIndex.add(suffixes.resolvedSuffixToIndex.get(suffix) << 1);
					int otherWords = 0;
					for (String otherName : singleName.allNames) {
						if (!otherName.equals(singleName.token)) {
							Integer pInteger = parsePureIntegerSuffix(otherName);
							if (pInteger != null) {
								// partial number ends with 11 but separated number as 01
								namedObject.bitsetIndex.add((pInteger << 2) + 1);
							} else if (commonWords.words.containsKey(otherName)) {
								if(!suffixes.usedCommons.containsKey(otherName)) {
									int indx = commonWords.words.get(otherName).index;
									suffixes.commonsRef.add(indx);
									suffixes.usedCommons.put(otherName, suffixes.usedCommons.size());
								}
								int calcRef = suffixes.dictionaryEntries.size();
								calcRef += suffixes.usedCommons.get(otherName);
								namedObject.bitsetIndex.add(calcRef << 1);
								// skip indexed common words
							} else if(isCommonWord){
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
						}
					}
					// extraSuffixes are not used for now?
					namedObject.otherWordsCount.add(otherWords);
				}
				
			}
			return this.suffixes;

		}
		
	}
	
	public PrepareWordsIndex buildCommonWords(Map<String, NamedObjectsByPrefix<T>> map) {
		// TODO Auto-generated method stub
		return commonWords;
	}
	
	
	public void addToNameIndex(String name, T obj, int maxPrefixLength, boolean indexNumbers) {
		List<String> splitName = SearchAlgorithms.splitAndNormalize(name);
		for (String token : splitName) {
			if (Algorithms.isEmpty(token)) {
				continue;
			}
			String prefix = nameIndexPreparePrefix(token, maxPrefixLength);
			if (Algorithms.isEmpty(prefix)) {
				continue;
			}
			if (indexNumbers && CommonWords.isNumber2Letters(token)) {
				continue;
			}
			NamedObjectsByPrefix<T> entry = namesIndex.get(prefix);
			if (entry == null) {
				entry = new NamedObjectsByPrefix<T>();
				entry.prefix = prefix;
				namesIndex.put(prefix, entry);
			}
			tokenFrequencies.compute(token, (t, u) -> u == null ? 1 : u + 1);
			entry.addToken(obj, token, splitName);
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
		try {
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
