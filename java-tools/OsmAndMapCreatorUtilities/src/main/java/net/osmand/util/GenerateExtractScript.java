package net.osmand.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.xmlpull.v1.XmlPullParserException;

import net.osmand.util.Algorithms;
import net.osmand.util.CountryOcbfGeneration.CountryRegion;

public class GenerateExtractScript {

	public static void main(String[] args) throws IOException, XmlPullParserException {
		String location = "/Users/victorshcherb/osmand/temp/osmconvert/";
		if (args.length > 0) {
			location = args[0];
		}

		String repo = "/Users/victorshcherb/osmand/repos/";
		if (args.length > 1) {
			repo = args[1];
		}

		String binaryFolder = "/Users/victorshcherb/bin/";
		if (args.length > 2) {
			binaryFolder = args[2];
		}
		new GenerateExtractScript().process(location, repo, binaryFolder);
	}

	private void process(String location, String repo, String binaryFolder) throws IOException, XmlPullParserException {
		CountryOcbfGeneration ocbfGeneration = new CountryOcbfGeneration();
		CountryRegion regionStructure = ocbfGeneration.parseRegionStructureFromRepo(repo);
		Map<String, File> polygons = ocbfGeneration.getPolygons(repo);
		List<CountryRegion> rt = new ArrayList<CountryRegion>();
		Iterator<CountryRegion> it = regionStructure.iterator();
		while(it.hasNext()) {
			CountryRegion reg = it.next();
			if(reg.getParent() != null) {
				rt.add(reg);
			}
		}
		int md = 0;
		for (CountryRegion reg : rt) {
			File countryFolder = new File(location, reg.getDownloadName());
			File polygonFile = getPolygonFile(polygons, reg, countryFolder, reg.getDownloadName());
			if (polygonFile == null) {
				System.err.println("Boundary doesn't exist " + reg.getDownloadName());
				continue;
			}
			int depth = 0;
			CountryRegion r = reg;
			while(r.getParent() != null) {
				depth++;
				r = r.getParent();
			}
			md = Math.max(md, depth);
			writeToFile(countryFolder, ".depth", depth+"");
			if(reg.hasMapFiles()) {
				writeToFile(countryFolder, ".map", "1");
			}
			System.out.print(reg.getDownloadName());
			if (reg.getParent() != null) {
				writeToFile(countryFolder, ".parent", reg.getParent().getDownloadName());
			}
			if (!Algorithms.isEmpty(reg.getSinglePolyExtract()) || reg.getParent().boundary == null) {
				writeToFile(countryFolder, ".polyextract", reg.getPolyExtract());
				System.out.println(" - extract from " + reg.getPolyExtract());
			} else {
				System.out.println(" - extract from " + reg.getParent().getDownloadName());
			}

		}
		// System.out.println("Max depth " + md); // 5
	}

	private void writeToFile(File countryFolder, String fn, String cont) throws IOException {
		File fl = new File(countryFolder, fn);
		FileOutputStream fous = new FileOutputStream(fl);
		if(cont != null) {
			fous.write(cont.getBytes());
		}
		fous.close();
	}

	private File getPolygonFile(Map<String, File> polygons, CountryRegion reg, File countryFolder, String regFile) throws IOException {
		File file = polygons.get(reg.boundary);
		if(file != null) {
			File polygonFile = new File(countryFolder, regFile + ".poly");
			countryFolder.mkdirs();
			if(!polygonFile.exists() || polygonFile.length() != file.length()) {
				Algorithms.fileCopy(file, polygonFile);
			}
			return polygonFile;
		}
		return null;
	}
}
