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

	Map<String, String> errors = new HashMap<String, String>();
	Map<String, ValueFreq> commonFreqs = new HashMap<>();
	int files = 0;

	public void merge(MissingSearchStats s) {
		files++;
		this.errors.putAll(s.errors);
		ValueFreq.mergeArray(commonFreqs, s.commonFreqs);
	}

	public void analyze(String name, String descr, MapObject obj, MapObject extra) {
		// all names could be used
		descr += " " + String.format("%.5f,%.5f", obj.getLocation().getLatitude(), obj.getLocation().getLongitude());
		List<String> splitAndNormalize = SearchAlgorithms.splitAndNormalize(name);
		int cmn = 0;
		int num = 0;
		for (String n : splitAndNormalize) {
			if (CommonWords.getCommon(n) > 0) {
				boolean number = CommonWords.isNumber2Letters(n);
				if (!number) {
					ValueFreq vf = commonFreqs.get(n);
					if (vf == null) {
						vf = new ValueFreq(n, 0);
						commonFreqs.put(vf.value, vf);
					}
					vf.freq++;
				} else {
					num++;
				}
				cmn++;
			}
		}
		if (obj instanceof City c && cmn == splitAndNormalize.size()) {
			CityType t = c.getType();
			if (t == CityType.CITY || t == CityType.TOWN || t == CityType.VILLAGE) {
				errors.put("CITY_COMMON " + name, descr);
			}
		}
		if (obj instanceof Street s && num == splitAndNormalize.size()) {
			errors.put("STR_NUM " + name, descr + " " + s.getCity());
		}
		if (obj instanceof Building && cmn == 0) {
			if (extra instanceof Street s) {
				if (s.getName().length() > name.length()) {
					return;
				}
			}
			errors.put("BUILD_NAME " + name, descr);
		}
	}

	@Override
	public String toString() {
		List<ValueFreq> lst = new ArrayList<>(commonFreqs.values());
		Collections.sort(lst);
		StringBuilder b = new StringBuilder(String.format("Missing search issues (%d):  \n", errors.size()));
		int cm = lst.size(), token = sumFreq(lst);
		lst = lst.subList(0, Math.min(50, lst.size()));
		b.append(String.format("\t Skipped Common words (%,d, %,d): %s\n", cm, token, lst));
		for (String e : errors.keySet()) {
			b.append(String.format("\t'%s': %s\n", e, errors.get(e)));
		}
		return b.toString();
	}
	
	private static int sumFreq(List<ValueFreq> refs) {
		int f = 0;
		for (ValueFreq r : refs) {
			f += r.freq;
		}
		return f;
	}
}