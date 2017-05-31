package net.osmand.exceptionanalyzer;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

public class AnalyzeEmails {

	private static class PeriodEmails {
		TreeMap<String, List<Long>> users = new TreeMap<String, List<Long>>();
		int[] freq = new int[20];
		TreeMap<String, Integer> domains = new TreeMap<>();
		
		private void calculateDomains() {
			for(String s: users.keySet()) {
				int i = s.indexOf('<');
				int j = s.indexOf('>');
				if(j > i && i > -1) {
					String ml = s.substring(i + 1, j);
					String dm = ml.substring(ml.indexOf('@') + 1, ml.length());
					Integer cnt = domains.get(dm);
					if(cnt == null) {
						domains.put(dm, 1);
					} else {
						domains.put(dm, cnt + 1);
					}
				}
			}
		}
		
		private void printDomainStats(int threshold) {
			Iterator<Entry<String, Integer>> it = domains.entrySet().iterator();
			System.out.println("Domains:");
			while(it.hasNext()) {
				Entry<String, Integer> e = it.next();
				if(e.getValue() > threshold) {
					System.out.println(e.getKey() + " - " + e.getValue());
				}
			}
		}
		
		private void calculateFrequencies() {
			for(List<Long> l : users.values()) {
				if(l.size() > freq.length) {
					freq[freq.length-1]++;
				} else {
					freq[l.size() - 1]++;
				}
			}
		}
	}
	
	public static void main(String[] args) throws ZipException, IOException {
		List<PeriodEmails> years = new ArrayList<>();
		years.add(parseInformation("2012_logs.zip"));
		years.add(parseInformation("2013_logs.zip"));
		years.add(parseInformation("2014_logs.zip"));
		years.add(parseInformation("2015_logs.zip"));
		years.add(parseInformation("2016_logs.zip"));
		years.add(parseInformation("2017_logs.zip"));
		
		int[] freqYearFrequencies = new int[10]; 
		for (int i = 0; i < years.size(); i++) {
			TreeSet<String> cs = new TreeSet<>();
			for(int j = i + 1; j < years.size(); j++) {
				TreeSet<String> ts = new TreeSet<>(years.get(i).users.keySet());
				ts.retainAll(years.get(j).users.keySet());
				cs.addAll(ts);
			}
			freqYearFrequencies[i] = cs.size();
		}
		PeriodEmails pe = new PeriodEmails();
		for (int i = 0; i < years.size(); i++) {
			PeriodEmails p = years.get(i);
			Iterator<Entry<String, List<Long>>> it = p.users.entrySet().iterator();
			while(it.hasNext()) {
				Entry<String, List<Long>> nn = it.next();
				if(!pe.users.containsKey(nn.getKey())) {
					pe.users.put(nn.getKey(), nn.getValue());
				} else {
					pe.users.get(nn.getKey()).addAll(nn.getValue());	
				}
			}
		}
		System.out.println(Arrays.toString(freqYearFrequencies));
		
		pe.calculateFrequencies();
		System.out.println("FREQENCIES "  + Arrays.toString(pe.freq));
		
		pe.calculateDomains();
		pe.printDomainStats(5);
		
	}

	private static PeriodEmails parseInformation(String file) throws ZipException, IOException {
		PeriodEmails users = new PeriodEmails();
		collectUsers(users.users, file);
		users.calculateFrequencies();
		System.out.println("FREQENCIES "  + file + " " + Arrays.toString(users.freq));
		return users;
	}

	

	private static void collectUsers(TreeMap<String, List<Long>> users, String file) throws ZipException, IOException {
		ZipFile zf = new ZipFile(new File(System.getProperty("user.home") + "/" + file));
		Enumeration<? extends ZipEntry> iterator = zf.entries();
		
		while(iterator.hasMoreElements()) {
			ZipEntry ze = iterator.nextElement();
			
			if(!ze.getName().contains(".uid")) {
				continue;
			}
			InputStream is = zf.getInputStream(ze);
			String usr = new BufferedReader(new InputStreamReader(is)).readLine();
			if(!users.containsKey(usr)) {
				users.put(usr, new ArrayList<Long>());
			}
			users.get(usr).add(ze.getTime());
			is.close();
		}
		zf.close();
	}
}
