package net.osmand.osm.util;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.osmand.IndexConstants;
import net.osmand.binary.BinaryInspector;
import net.osmand.data.LatLon;
import net.osmand.data.index.IndexBatchCreator;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.map.OsmandRegions;
import net.osmand.map.RegionCountry;
import net.osmand.map.RegionRegistry;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.OsmMapUtils;
import net.osmand.osm.edit.Way;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.osm.io.OsmStorageWriter;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import org.apache.tools.bzip2.CBZip2InputStream;
import org.xml.sax.SAXException;

import javax.xml.stream.XMLStreamException;
import java.io.*;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

public class CombineSRTMIntoFile {

	public static void main(String[] args) throws IOException, SAXException, XMLStreamException {
		File directoryWithSRTMFiles = new File(args[0]);
		File directoryWithTargetFiles = new File(args[1]);
//		File directoryWithSRTMFiles = null;
//		File directoryWithTargetFiles = null;
//		OsmandRegions or = new OsmandRegions();
//		or.prepareFile("/home/victor/projects/osmand/osm-gen/Osmand_regions.obf");
//		or.cacheAllCountries();


		final RegionRegistry rr = RegionRegistry.getRegionRegistry();
		final List<RegionCountry> rcs = rr.getCountries();
		for(RegionCountry rc : rcs) {
//			final String stdname = rc.name.toLowerCase() + '_' + rc.continentName.toLowerCase();
//			if(!or.containsCountry(stdname)){
//				System.out.println("##MISSING " + rc.name);
//			}
			if (rc.getTileSize() > 35 && rc.getSubRegions().size() > 0) {
				for(RegionCountry c : rc.getSubRegions()) {
					process(c, rc, directoryWithSRTMFiles, directoryWithTargetFiles);
				}
			} else {
				process(rc, null, directoryWithSRTMFiles, directoryWithTargetFiles);
			}
		}
	}

	private static void process(RegionCountry country, RegionCountry parent, File directoryWithSRTMFiles, File directoryWithTargetFiles) throws IOException {
		String continentName = country.continentName;
		if(parent != null){
			continentName = parent.continentName;
		}

		final String continentSuffix = "_" + continentName + "_" + IndexConstants.BINARY_MAP_VERSION + IndexConstants.BINARY_SRTM_MAP_INDEX_EXT;
		final String suffix = "_" + IndexConstants.BINARY_MAP_VERSION + IndexConstants.BINARY_MAP_INDEX_EXT;
		String countryName = country.name;
		if(parent != null) {
			countryName = parent.name + "_" + countryName;
		}
		String name = Algorithms.capitalizeFirstLetterAndLowercase(countryName + continentSuffix);
		final File targetFile = new File(directoryWithTargetFiles, name);
		if(targetFile.exists()) {
			System.out.println("Already processed "+ name);
			return;
		}

		Set<String> srtmFileNames = new TreeSet<String>();
		Set<String> NESrtmFileNames = new TreeSet<String>();
		Set<String> NWSrtmFileNames = new TreeSet<String>();
		Set<String> SESrtmFileNames = new TreeSet<String>();
		Set<String> SWSrtmFileNames = new TreeSet<String>();
		final TIntArrayList singleTiles = country.getSingleTiles();
		int cx = 0;
		int cy = 0;
		for (int j = 0; j < singleTiles.size(); j+=2) {
			cx += singleTiles.get(j);
			cy += singleTiles.get(j + 1);
		}
		cx /= (singleTiles.size()/2);
		cy /= (singleTiles.size()/2);

		for (int j = 0; j < singleTiles.size(); j+=2) {
			final int lon = singleTiles.get(j);
			final int lat = singleTiles.get(j + 1);
			final String filename = getFileName(lon, lat) + suffix;
			srtmFileNames.add(filename);
			if(lon <= cx) {
				if(lat <= cy) {
					SWSrtmFileNames.add(filename);
				} else {
					NWSrtmFileNames.add(filename);
				}
			} else {
				if(lat <= cy) {
					SESrtmFileNames.add(filename);
				} else {
					NESrtmFileNames.add(filename);
				}
			}
		}
		final File work = new File(directoryWithTargetFiles, "work");
		System.out.println("Process "+ name);
		Map<File, String> mp = new HashMap<File, String>();
		long length = 0;
		for(String file : srtmFileNames) {
			final File fl = new File(directoryWithSRTMFiles, file + ".zip");
			if(!fl.exists()) {
				System.err.println("!! Missing " + name + " because " + file + " doesn't exist");
			} else {
				ZipFile zipFile = new ZipFile(fl);
				Enumeration<? extends ZipEntry> entries = zipFile.entries();
				if (entries.hasMoreElements()) {
					ZipEntry entry = entries.nextElement();
					File entryDestination = new File(work, file);
					entryDestination.getParentFile().mkdirs();
					InputStream in = zipFile.getInputStream(entry);
					OutputStream out = new FileOutputStream(entryDestination);
					Algorithms.streamCopy(in, out);
					in.close();
					out.close();
					zipFile.close();
					length += entryDestination.length();
					mp.put(entryDestination, null);
				} else {
					System.err.println("!! Can't process " + name + " because " + file + " nothing found");
					return;
				}
			}
		}
		if(length > Integer.MAX_VALUE) {
			splitAndCombineParts(getFile(directoryWithTargetFiles, continentSuffix, countryName, "NE"), NESrtmFileNames, work);
			splitAndCombineParts(getFile(directoryWithTargetFiles, continentSuffix, countryName, "NW"), NWSrtmFileNames, work);
			splitAndCombineParts(getFile(directoryWithTargetFiles, continentSuffix, countryName, "SE"), SESrtmFileNames, work);
			splitAndCombineParts(getFile(directoryWithTargetFiles, continentSuffix, countryName, "SW"), SWSrtmFileNames, work);
		} else {
			BinaryInspector.combineParts(targetFile, mp);
		}
		for(String file : srtmFileNames) {
			final File fl = new File(work, file);
			fl.delete();
		}

	}

	private static File getFile(File directoryWithTargetFiles, String continentSuffix, String countryName, String middle) {
		return new File(directoryWithTargetFiles, Algorithms.capitalizeFirstLetterAndLowercase(countryName + middle + continentSuffix));
	}

	private static void splitAndCombineParts(File targetFile, Set<String> srtmFileNames, File work) throws IOException {
		if(targetFile.exists()) {
			System.out.println("Already processed "+ targetFile.getName());
			return;
		}
		Map<File, String> mp = new HashMap<File, String>();
		for(String s : srtmFileNames) {
			mp.put(new File(work, s), null);
		}
		BinaryInspector.combineParts(targetFile, mp);
	}

	private static String getFileName(int lon, int lat) {
		String fn = lat >= 0 ? "N" : "S";
		if(Math.abs(lat) < 10) {
			fn += "0";
		}
		fn += Math.abs(lat);
		fn += lon >= 0 ? "e" : "w";
		if(Math.abs(lon) < 10) {
			fn += "00";
		} else if(Math.abs(lon) < 100) {
			fn += "0";
		}
		fn += Math.abs(lon);
		return fn;
	}

}
