package net.osmand.obf.diff;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.TreeMap;

import net.osmand.util.Algorithms;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.xmlpull.v1.XmlPullParserException;

public class GenerateMonthlyObf {
	private static final Log log = LogFactory.getLog(GenerateMonthlyObf.class);
	static SimpleDateFormat day = new SimpleDateFormat("yyyy_MM_dd");
	static SimpleDateFormat month = new SimpleDateFormat("yyyy_MM");
	static {
		day.setTimeZone(TimeZone.getTimeZone("UTC"));
		month.setTimeZone(TimeZone.getTimeZone("UTC"));
	}
	public static void main(String[] args) {
		try {
			File dir = new File(args[0]);
			iterateOverDir(dir, args.length > 1 && "--delete".equals(args[1]));
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	private static void iterateOverDir(File dir, boolean delete) throws IOException, SQLException,
			InterruptedException, XmlPullParserException {
		int i = 0;
		List<File> sortFiles = GenerateDailyObf.sortFiles(dir);
		for (File countryF : sortFiles) {
			if (!countryF.isDirectory()) {
				continue;
			}
			i++;
			ArrayList<File> deleteDates = new ArrayList<File>();
			iterateCountry(countryF, deleteDates, (i * 100.f / sortFiles.size()));
			for (File fld : deleteDates) {
				if (fld.isDirectory()) {
					for (File oneF : fld.listFiles()) {
						System.out.println((delete ? "Delete " : "About to delete") + oneF.getAbsolutePath());
						if (delete) {
							oneF.delete();
						}
					}
				}

				System.out.println((delete ? "Delete " : "About to delete") + fld.getAbsolutePath());
				if (delete) {
					fld.delete();
				}
			}
		}
	}
	
	private static class Month {
		TreeMap<String, List<File>> files = new TreeMap<String, List<File>>();
		long targetTimestamp;
		String monthName;
		
		public String getTargetName(File countryF) {
			String name = countryF.getName() + "_" + monthName + "_00";
			name = Algorithms.capitalizeFirstLetterAndLowercase(name);
			return name;
		}
	}

	private static void iterateCountry(File countryF, List<File> deleteFiles, float prog) throws IOException, SQLException, InterruptedException, XmlPullParserException {
		Map<String, Month> filesByMonth = groupFilesByMonth(countryF, deleteFiles);
		for(Month m : filesByMonth.values()) {
			File targetObf = new File(countryF, m.getTargetName(countryF) + ".obf.gz");
			if (!targetObf.exists() || targetObf.lastModified() != m.targetTimestamp) {
				if(!targetObf.exists()) {
					log.info("The file " + targetObf.getName() + " doesn't exist");
				} else {
					log.info("The file " + targetObf.getName() + " was updated for " + (targetObf.lastModified() - m.targetTimestamp) / 1000
							+ " seconds");
				}
				System.out.println("Processing " + targetObf.getName() + " " + new Date() + " " + prog +"%");
				List<File> osmFiles = new ArrayList<File>();
				Iterator<Entry<String, List<File>>> it = m.files.entrySet().iterator();
				while(it.hasNext()) {
					Entry<String, List<File>> e = it.next();
					osmFiles.addAll(e.getValue());
				}
				GenerateDailyObf.generateCountry(m.getTargetName(countryF), 
						targetObf, osmFiles.toArray(new File[osmFiles.size()]), m.targetTimestamp, getOdbFile(countryF, m));
			}
			
		}
		
//		
//		
	}

	private static File getOdbFile(File countryF, Month m) {
		return new File(countryF, "osm_" + m.monthName + ".odb");
	}

	private static Map<String, Month> groupFilesByMonth(File countryF, List<File> deleteFiles) {
		Date currentDate = new Date();
		String cdate = day.format(currentDate);
		String cmnt = month.format(currentDate);
//		System.out.println(cdate);
		Map<String, Month> filesByMonth = new TreeMap<String, Month>();
		for(File date : countryF.listFiles()) {
			if(date.getName().length() == 10 && !date.getName().equals(cdate)) {
				String day = date.getName().substring(2).replace('-', '_');
				String month = day.substring(0, day.length() - 3);
				Month m = filesByMonth.get(month);
				if(m == null) {
					m = new Month();
					m.monthName = month;
					filesByMonth.put(month, m);
					if(!date.getName().startsWith(cmnt)) {
						deleteFiles.add(getOdbFile(countryF, m));
					}
				}
				File odbDay = new File(date, GenerateDailyObf.OSM_ODB_FILE);
				if(odbDay.exists()) {
					odbDay.delete();
				}
				List<File> osmFiles = new ArrayList<File>();
				for(File f : date.listFiles()) {
					if(f.getName().endsWith(".osm.gz")) {
						m.targetTimestamp = Math.max(m.targetTimestamp, f.lastModified());
						osmFiles.add(f);
					}
				}
				if(osmFiles.size() > 0) {
					Collections.sort(osmFiles, new Comparator<File>(){
						@Override
						public int compare(File o1, File o2) {
							return o1.getName().compareTo(o2.getName());
						}
					});
					m.files.put(date.getName(), osmFiles);
				}
				if(!date.getName().startsWith(cmnt)) {
					deleteFiles.add(date);
				}
				
			}
		}
		return filesByMonth;
	}

	
}
