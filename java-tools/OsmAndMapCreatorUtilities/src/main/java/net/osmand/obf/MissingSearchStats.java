package net.osmand.obf;

import java.util.ArrayList;
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
import net.osmand.util.SearchAlgorithms;

public class MissingSearchStats {

	Map<String, List<String>> errors = new HashMap<>();
	Map<String, ValueFreq> allFreqs = new HashMap<>();
	Map<String, Map<String, ValueFreq>> objFreqs = new HashMap<>();
	int files = 0;

	public void merge(MissingSearchStats s) {
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
		StringBuilder b = new StringBuilder(String.format("Missing search issues (%d):  \n", errors.size()));
		for (String type : objFreqs.keySet()) {
			appendCommonWords(objFreqs.get(type), type, b);
		}
		appendCommonWords(allFreqs, "All", b);
		
		for (String e : errors.keySet()) {
			b.append(String.format("\t %s (%,d):\n", e, errors.get(e).size()));
			int i = 0;
			for (String err : errors.get(e)) {
				if (i++ > 10) {
					b.append("....\n");
					break;
				}
				b.append(String.format("\t\t %s\n", err));
			}
		}
		return b.toString();
	}

	private void appendCommonWords(Map<String, ValueFreq> freqs, String type, StringBuilder b) {
		List<ValueFreq> lst = new ArrayList<>(freqs.values());
		Collections.sort(lst);
		int cm = lst.size(), token = sumFreq(lst);
		List<ValueFreq> rare = new ArrayList<>(lst);
		Collections.reverse(rare);
		lst = lst.subList(0, Math.min(50, lst.size()));
		rare = rare.subList(0, Math.min(50, rare.size()));
		
		b.append(String.format("\t Frequent %s Common words (%,d, %,d): %s\n", type, cm, token, lst));
		b.append(String.format("\t Rare %s Common words (%,d, %,d): %s\n", type, rare.size(), sumFreq(rare), rare));
	}
	
	private static int sumFreq(List<ValueFreq> refs) {
		int f = 0;
		for (ValueFreq r : refs) {
			f += r.freq;
		}
		return f;
	}
}