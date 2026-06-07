package net.osmand.obf;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.osmand.binary.CommonWords;
import net.osmand.binary.NameIndexInspector.ValueFreq;
import net.osmand.data.Building;
import net.osmand.data.City;
import net.osmand.data.City.CityType;
import net.osmand.data.MapObject;
import net.osmand.data.Street;
import net.osmand.obf.BinaryInspector.AddressStats;
import net.osmand.obf.BinaryInspector.PoiStats;
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

	public void analyze(String name, String descr, MapObject obj, MapObject extra) {
		// all names could be used
		descr += " " + String.format("%.5f,%.5f", obj.getLocation().getLatitude(), obj.getLocation().getLongitude());
		List<String> splitAndNormalize = SearchAlgorithms.splitAndNormalize(name);
		int cmn = 0;
		int num = 0;
		String objType = obj.getClass().getSimpleName();
//		if (obj instanceof City c) {
//			objType = c.getType().toString();
//		}
		for (String n : splitAndNormalize) {
			if (CommonWords.getCommon(n) > 0) {
				boolean number = CommonWords.isNumber2Letters(n);
				if (!number) {
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
				}
				if (number) {
					num++;
				}
				cmn++;
			}
		}
		if (obj instanceof City c && cmn == splitAndNormalize.size()) {
			CityType t = c.getType();
			if (t == CityType.CITY || t == CityType.TOWN || t == CityType.VILLAGE) {
				addError("CITY_COMMON", name + " " + descr);
			}
		}
		if (obj instanceof Street s && num == splitAndNormalize.size()) {
			addError("STR_NUM", name + " " + descr + " " + s.getCity());
		}
		if (obj instanceof Building && cmn == 0) {
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
		StringBuilder b = info(null, null);
		return b.toString();
	}

	public StringBuilder info(PoiStats poiStats, AddressStats addressStats) {
		StringBuilder b = new StringBuilder(String.format("Search issues (%d):  \n", errors.size()));
		for (String type : objFreqs.keySet()) {
			if (type.equalsIgnoreCase("amenity")) {
				appendCommonWords(objFreqs.get(type), type, b, poiStats, null);
			} else {
				appendCommonWords(objFreqs.get(type), type, b, null, addressStats);
			}
		}
		appendCommonWords(allFreqs, "All", b, poiStats, addressStats);
		
		for (String e : errors.keySet()) {
			b.append(String.format("\t %s (%,d):", e, errors.get(e).size()));
			int i = 0;
			for (String err : errors.get(e)) {
				if (i++ > 10) {
					b.append("....");
					break;
				}
				b.append(String.format("\n\t\t %s", err));
			}
		}
		b.append("\n");
		return b;
	}

	private void appendCommonWords(Map<String, ValueFreq> freqs, String type, StringBuilder b, 
			PoiStats poiStats, AddressStats addressStats) {
		List<ValueFreq> lst = new ArrayList<>(freqs.values());
		Collections.sort(lst);
		List<ValueFreq> rare = new ArrayList<>(lst);
		Collections.reverse(rare);
		appendWithIndexed(b, 50, "Frequent " + type, poiStats, addressStats, lst);
		appendWithIndexed(b, 50, "Rare " + type, poiStats, addressStats, rare);
	}

	private void appendWithIndexed(StringBuilder str, int limit, String prefix, 
			PoiStats poiStats, AddressStats addressStats, List<ValueFreq> lst) {
		
		StringBuilder b = new StringBuilder();
		int ind = 0, allFreq = 0, allIndexed = 0;
		for (ValueFreq v : lst) {
			int indexed = 0;
			if (poiStats != null) {
				indexed += calcIndexed(poiStats.nameIndex.values(), v.value);
			}
			if (addressStats != null) {
				indexed += calcIndexed(addressStats.nameIndex.values(), v.value);
			}
			if (ind < limit) {
				ind++;
				allFreq += v.freq;
				allIndexed += indexed;
				if (indexed == 0) {
					b.append(String.format("%s (%d), ", v.value, v.freq));
				} else {
					b.append(String.format("%s (%d, %d), ", v.value, v.freq, indexed));
				}
			}
		}
		str.append(String.format("\t %s Common words (distinct %,d, all %,d, indexed %,d): %s\n", prefix, ind, allFreq,
				allIndexed, b.toString()));
		b.append("\n");
	}

	private int calcIndexed(Collection<ValueFreq> nameIndex, String value) {
		int indexed = 0;
		if (nameIndex != null) {
			for (ValueFreq key : nameIndex) {
				if (value.equalsIgnoreCase(key.value)) {
					indexed += key.freq;
				} else if (value.startsWith(key.value)) {
					indexed += calcIndexed(key.subValues, value);
				}
			}
		}
		return indexed;
	}
	

}