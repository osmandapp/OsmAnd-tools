package net.osmand.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.xmlpull.v1.XmlPullParserException;

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
		new GenerateExtractScript().process(location, repo);
	}

	private void process(String location, String repo) throws IOException, XmlPullParserException {
		CountryOcbfGeneration ocbfGeneration = new CountryOcbfGeneration();
		CountryRegion regionStructure = ocbfGeneration.parseRegionStructureFromRepo(repo);
		Map<String, File> polygons = ocbfGeneration.getPolygons(repo);
		List<CountryRegion> rt = new ArrayList<CountryRegion>();
		Iterator<CountryRegion> it = regionStructure.iterator();
		while (it.hasNext()) {
			CountryRegion reg = it.next();
			if (reg.getParent() != null) {
				rt.add(reg);
			}
		}
		int md = 0;
		for (CountryRegion reg : rt) {
			File regionFolder = new File(location, reg.getDownloadName());
			String boundary = reg.boundary;
			if (boundary == null && !Algorithms.isEmpty(reg.getSinglePolyExtract())) {
				boundary = reg.name;
			}
			File polygonFile = null;
			if (boundary != null) {
				polygonFile = getPolygonFile(polygons, reg, regionFolder, boundary, reg.getDownloadName());
			}
			if (polygonFile == null) {
				System.err.println("Boundary doesn't exist " + reg.getDownloadName());
				continue;
			}
			if (Algorithms.isEmpty(reg.getSinglePolyExtract()) && (reg.getParent() == null || reg.getParent().boundary == null || 
					Algorithms.isEmpty(reg.getParent().getSinglePolyExtract()))) {
				System.err.println("Parent Boundary doesn't exist " + reg.getDownloadName());
				continue;
			}
			int depth = 0;
			CountryRegion r = reg;
			while (r.getParent() != null) {
				depth++;
				r = r.getParent();
			}
			md = Math.max(md, depth);
			writeToFile(regionFolder, ".depth", depth + "");
			if (reg.hasMapFiles()) {
				writeToFile(regionFolder, ".map", "1");
			}
			if (reg.getParent() != null) {
				writeToFile(regionFolder, ".parent", reg.getParent().getDownloadName());
			}
			if (reg.getParent() == null || !Algorithms.isEmpty(reg.getSinglePolyExtract())) {
				System.out.println(reg.getDownloadName() + " - extract from " + reg.getSinglePolyExtract());
			} else {
				System.out.println(reg.getDownloadName() + " - extract from " + reg.getParent().getDownloadName());
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

	private File getPolygonFile(Map<String, File> polygons, CountryRegion reg, File countryFolder, String boundary, String regFile) throws IOException {
		File file = polygons.get(boundary);
		if (file != null) {
			File polygonFile = new File(countryFolder, regFile + ".poly");
			countryFolder.mkdirs();
			if (!polygonFile.exists() || polygonFile.length() != file.length()) {
				Algorithms.fileCopy(file, polygonFile);
			}
			return polygonFile;
		}
		return null;
	}
}
