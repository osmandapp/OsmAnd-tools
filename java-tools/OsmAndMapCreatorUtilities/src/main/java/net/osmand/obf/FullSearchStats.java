package net.osmand.obf;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.osmand.binary.CommonWords;
import net.osmand.binary.NameIndexReader.ValueFreq;
import net.osmand.data.Building;
import net.osmand.data.City;
import net.osmand.data.City.CityType;
import net.osmand.data.MapObject;
import net.osmand.data.Street;
import net.osmand.obf.BinaryInspector.AddressStats;
import net.osmand.obf.BinaryInspector.PoiStats;
import net.osmand.util.Algorithms;
import net.osmand.util.SearchAlgorithms;

public class FullSearchStats {

	Map<String, List<String>> errors = new HashMap<>();
	Map<String, ValueFreq> allFreqs = new HashMap<>();
	Map<String, Map<String, ValueFreq>> objFreqs = new HashMap<>();
	int files = 0;

	public void merge(FullSearchStats s) {
		files++;
		for(String key : s.errors.keySet()) {
			if(!this.errors.containsKey(key)) {
				errors.put(key, s.errors.get(key));
			} else {
				errors.get(key).addAll(s.errors.get(key));
			}
		}
		ValueFreq.mergeArray(allFreqs, s.allFreqs);
		for (String t : s.objFreqs.keySet()) {
			if (!objFreqs.containsKey(t)) {
				objFreqs.put(t, s.objFreqs.get(t));
			} else {
				ValueFreq.mergeArray(objFreqs.get(t), s.objFreqs.get(t));
			}
		}
	}

	public void analyze(String descr, MapObject obj, MapObject extra) {
		descr += " " + String.format("%.5f,%.5f", obj.getLocation().getLatitude(), obj.getLocation().getLongitude());
		analyze(obj.getName(), descr, obj, extra, true);
		for (String oname : obj.getOtherNames(true, obj.getName())) {
			analyze(oname, descr, obj, extra, false);
		}
	}
	private void analyze(String name, String descr, MapObject obj, MapObject extra, boolean def) {
		List<String> splitAndNormalize = SearchAlgorithms.splitAndNormalize(name, true);
		int cmn = 0;
		int num = 0;
		String objType = obj.getClass().getSimpleName();
//		if (obj instanceof City c) {
//			objType = c.getType().toString();
//		}
		CommonWords cw = CommonWords.getInstance();
		boolean bld = obj instanceof Building;
		for (String word : splitAndNormalize) {
			int commonIndex = cw.getCommon(word);
			if (commonIndex >= 0 || bld) {
				boolean number = SearchAlgorithms.isNumber2Letters(word);
				String n = word;
				if (number) {
					if (Algorithms.parseIntSilently(n, -1) < 0) {
						n = bld ? "NUMBLD": "NUM2LETTER";
					} else {
						n = bld ? "NUMBLD": "INTEGER";
					}
				} else if (bld) {
					n = "OTHERBLD";
				}
				if (!allFreqs.containsKey(n)) {
					allFreqs.put(n, new ValueFreq(n, 0));
				}
				allFreqs.get(n).freq++;
				if (!objFreqs.containsKey(objType)) {
					objFreqs.put(objType, new HashMap<>());
				}
				if (!objFreqs.get(objType).containsKey(n)) {
					objFreqs.get(objType).put(n, new ValueFreq(n, 0));
				}
				objFreqs.get(objType).get(n).freq++;
				if (number) {
					num++;
				}
				if (commonIndex >= 0) {
					cmn++;
				}
			}	
		}
		if (def && obj instanceof City c && cmn == splitAndNormalize.size()) {
			CityType t = c.getType();
			if (t == CityType.CITY || t == CityType.TOWN || t == CityType.VILLAGE) {
				addError("CITY_COMMON", name + " " + descr);
			}
		}
		if (def && obj instanceof Street s && num == splitAndNormalize.size()) {
			addError("STR_NUM", name + " " + descr + " " + s.getCity());
		}
		if (def && bld && cmn == 0) {
			if (extra instanceof Street s) {
				if (s.getName().length() > name.length()) {
					return;
				}
			}
			addError("BUILD_NAME", name + " " + descr);
		}
	}

	private void addError(String key, String error) {
		if (!errors.containsKey(key)) {
			errors.put(key, new ArrayList<String>());
		}
		errors.get(key).add(error);
	}

	@Override
	public String toString() {
		StringBuilder b = info("", null, null);
		return b.toString();
	}

	public StringBuilder info(String prefixnl, PoiStats poiStats, AddressStats addressStats) {
		StringBuilder b = new StringBuilder(String.format("Full Name Search analysis. Known Search issues (%d):  " , errors.size()));
		for (String e : errors.keySet()) {
			b.append(String.format(prefixnl + "%s (%,d):", e, errors.get(e).size()));
			int i = 0;
			for (String err : errors.get(e)) {
				if (i++ > 10) {
					b.append("...");
					break;
				}
				b.append(String.format(prefixnl + "  %s", err));
			}
		}
		for (String type : objFreqs.keySet()) {
			if (type.equalsIgnoreCase("amenity")) {
				appendCommonWords(prefixnl, objFreqs.get(type), type, b, poiStats, null);
			} else {
				appendCommonWords(prefixnl, objFreqs.get(type), type, b, null, addressStats);
			}
		}
		int limit = 1000;
		appendCommonWords(prefixnl, allFreqs, "All", b, poiStats, addressStats);
		b.append(prefixnl);
		calculateMergedWords(b, poiStats, addressStats, allFreqs, limit);
		b.append("\n");
		return b;
	}

	private void calculateMergedWords(StringBuilder b, PoiStats poiStats, AddressStats addressStats,
			Map<String, ValueFreq> allFreqs, int limit) {
		Map<String, ValueFreq> merged = new HashMap<>();
		if (poiStats != null) {
			ValueFreq.mergeFlatten(merged, poiStats.nameIndex.values());
		}
		if (addressStats != null) {
			ValueFreq.mergeFlatten(merged, addressStats.nameIndex.values());
		}
		for (ValueFreq v : allFreqs.values()) {
			merged.put(v.value, v.copy());
		}
		List<ValueFreq> mergedAllWords = new ArrayList<ValueFreq>(merged.values());
		Collections.sort(mergedAllWords);
		b.append("All name words: ");
		int ind = 0;
		for (ValueFreq v : mergedAllWords) {
			if (ind++ > limit) {
				break;
			}
			if (!allFreqs.containsKey(v.value)) {
				b.append(String.format("\"%s\" [%d], ", v.value, v.freq));
			} else {
				b.append(String.format("\"%s\" [%d -> %d], ", v.value, v.freq, v.extra));
			}
		}
	}

	private List<ValueFreq> appendCommonWords(String prefixnl, Map<String, ValueFreq> freqs, String type, StringBuilder b, 
			PoiStats poiStats, AddressStats addressStats) {
		List<ValueFreq> lst = new ArrayList<>(freqs.values());
		Collections.sort(lst);
		for (ValueFreq v : lst) {
			v.extra = 0;
			if (poiStats != null) {
				v.extra += calcIndexed(poiStats.nameIndex.values(), v.value);
			}
			if (addressStats != null) {
				v.extra += calcIndexed(addressStats.nameIndex.values(), v.value);
			}
		}
		int allFreq = 0, allIndexed = 0;
		int fFreq = 0, fIndexed = 0, rFreq = 0, rIndexed = 0;
		int ind = 0;
		StringBuilder freq = new StringBuilder();
		StringBuilder rare = new StringBuilder();
		for (ValueFreq v : lst) {
			ind++;
			allFreq += v.freq;
			allIndexed += v.extra;
			StringBuilder s = null;
			if (ind < 300) {
				s = freq;
				fFreq += v.freq;
				fIndexed += v.extra;
			} else if (lst.size() - 100 < ind) {
				s = rare;
				rFreq += v.freq;
				rIndexed += v.extra;
			}

			if (s != null) {
				if (v.extra == 0) {
					s.append(String.format("%s (%d), ", v.value, v.freq));
				} else {
					s.append(String.format("%s (%d, %d), ", v.value, v.freq, v.extra));
				}
			}
		}
		b.append(String.format(prefixnl + "%s Common words - %,d (matched %,d, indexed %,d). ", type, lst.size(),
				allFreq, allIndexed));

		if (rare.length() > 0) {
			b.append(String.format(" Rare (matched %,d, indexed %,d): %s", rFreq, rIndexed, rare.toString()));
		}
		b.append(prefixnl).append(String.format("  Frequent common words (matched %,d, indexed %,d): %s", fFreq,
				fIndexed, freq.toString()));
		return lst;
	}

	private int calcIndexed(Collection<ValueFreq> nameIndex, String value) {
		int indexed = 0;
		if (nameIndex != null) {
			for (ValueFreq key : nameIndex) {
				if (value.equalsIgnoreCase(key.value) && key.subValues == null) {
					indexed += key.freq;
				} else if (value.startsWith(key.value)) {
					indexed += calcIndexed(key.subValues, value);
				}
			}
		}
		return indexed;
	}
	

}