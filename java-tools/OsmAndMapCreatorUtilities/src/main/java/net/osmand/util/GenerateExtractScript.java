package net.osmand.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.xmlpull.v1.XmlPullParserException;

import net.osmand.util.CountryOcbfGeneration.CountryRegion;

public class GenerateExtractScript {

	private static final String PLANET_CONST = "planet";

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
		List<CountryRegion> parentRegions = new ArrayList<>();
		parentRegions.addAll(regionStructure.getChildren());
		Map<String, List<String>> existingParentRegions = new TreeMap<>();
		Map<String, Integer> regionsDepths = new TreeMap<>();
		existingParentRegions.put(PLANET_CONST, new ArrayList<String>());
		regionsDepths.put(PLANET_CONST, 0);
		while (!parentRegions.isEmpty()) {
			List<CountryRegion> children = new ArrayList<>();
			for (CountryRegion reg : parentRegions) {
				File regionFolder = new File(location, reg.getDownloadName());
				String boundary = reg.boundary;
				if (boundary == null && !Algorithms.isEmpty(reg.getSinglePolyExtract())) {
					boundary = reg.name;
				}
				children.addAll(reg.getChildren());
				if (reg.getDownloadName().equals(reg.getParent().getDownloadName())) {
					// case antarctica continent vs region
					continue;
				}
				if (existingParentRegions.containsKey(reg.getDownloadName())) {
					throw new IllegalStateException("Already processed " + reg.getDownloadName());
				}
				String parentExtract = null;
				if (!Algorithms.isEmpty(reg.getSinglePolyExtract())) {
					parentExtract = reg.getSinglePolyExtract();
				} else if (reg.getParent() != null && (reg.getParent().boundary != null
						|| !Algorithms.isEmpty(reg.getParent().getSinglePolyExtract()))) {
					parentExtract = reg.getParent().getDownloadName();
				}
				File polygonFile = null;
				if (boundary != null) {
					polygonFile = getPolygonFile(polygons, reg, regionFolder, boundary, reg.getDownloadName());
				}
				if (polygonFile == null) {
					System.err.println("WARN: Boundary doesn't exist " + reg.getDownloadName());
					continue;
				}

				if (Algorithms.isEmpty(parentExtract) || !existingParentRegions.containsKey(parentExtract)) {
					System.err.println("WARN: Parent Boundary doesn't exist " + reg.getDownloadName() + " from " + parentExtract);
					continue;
				}
				if (reg.hasMapFiles()) {
					writeToFile(regionFolder, ".map", "1");
				}
				if (reg.getParent() != null) {
					writeToFile(regionFolder, ".parent", parentExtract);
				}
				existingParentRegions.get(parentExtract).add(reg.getDownloadName());
				existingParentRegions.put(reg.getDownloadName(), new ArrayList<String>());
				int depth = regionsDepths.get(parentExtract) + 1;
				regionsDepths.put(reg.getDownloadName(), depth);
				System.out.println(reg.getDownloadName() + " - extract from " + parentExtract + " " + depth);
				writeToFile(regionFolder, ".depth", depth + "");
			}
			parentRegions = children;
		}
		
		for (String file : existingParentRegions.keySet()) {
			File regionFolder = file.equals(PLANET_CONST) ? new File(location) : new File(location, file);
			List<String> children = existingParentRegions.get(file);
			
			if (children.size() > 0) {
				StringBuilder bld = new StringBuilder();
				for (String child : children) {
					if (bld.length() == 0) {
						bld.append("{").append("\n");
						bld.append("\n  ").append(String.format(" \"directory\": \"%s\", ", location));
						bld.append("\n  ").append(String.format(" \"extracts\": [ \n", location));
					} else {
						bld.append(",").append("\n");
					}
					bld.append("     {");
					bld.append("\n        ")
							.append(String.format("\"output\": \"%s\", ", child + "/" + child + ".pbf"));
					bld.append("\n        ")
							.append(String.format("\"polygon\": { \"%s\" } ", child + "/" + child + ".poly"));
					bld.append("\n     }");
				}
				bld.append("\n}");
				System.out.println("Extract " + children.size() + " files from " + file );
				writeToFile(regionFolder, "extract.json", bld.toString());
			}
		}
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
