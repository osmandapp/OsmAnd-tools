package net.osmand.obf;


import net.osmand.PlatformUtil;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexTestReader;
import net.osmand.binary.OsmandOdb;
import net.osmand.util.Algorithms;

import org.apache.commons.compress.compressors.gzip.GzipUtils;
import org.apache.commons.logging.Log;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

class SearchDataCreator extends BinaryMerger {

	private final static Log log = PlatformUtil.getLog(SearchDataCreator.class);
	public static final String helpMessage = "output_file.obf [--address] [--poi] [input_file.json] ...: create and merge all search test json files and merges poi & address structure into 1";
	private static final Map<String, Integer> COMBINE_ARGS = new HashMap<String, Integer>();

	static {
		COMBINE_ARGS.put("--address", OsmandOdb.OsmAndStructure.ADDRESSINDEX_FIELD_NUMBER);
		COMBINE_ARGS.put("--poi", OsmandOdb.OsmAndStructure.POIINDEX_FIELD_NUMBER);
	}

	public static void main(String[] args) throws IOException, SQLException {
		SearchDataCreator in = new SearchDataCreator();
		// test cases show info
		if (args.length == 1 && "test".equals(args[0])) {
			in.merger(new String[]{
					"D:\\Projects\\git\\Osmand\\resources\\test-resources\\search\\eiffel.obf",
					"D:\\Projects\\OsmAnd\\test-cases\\eiffel.json",
					"D:\\Projects\\OsmAnd\\test-cases\\louvre.json",
			});
		} else {
			in.merger(args);
		}
	}

	public void merger(String[] args) throws IOException, SQLException {
		if (args == null || args.length == 0) {
			System.out.println(helpMessage);
			return;
		}
		File outputFile = null;
		String outputFilePath = null;
		List<BinaryMapIndexReader> readers = new ArrayList<BinaryMapIndexReader>();
		Set<Integer> combineParts = new HashSet<Integer>();
		for (int i = 0; i < args.length; i++) {
			if (args[i].startsWith("--")) {
				combineParts.add(COMBINE_ARGS.get(args[i]));
			} else if (outputFile == null) {
				outputFilePath = args[i];
				outputFile = new File(outputFilePath);
			} else {
				File file = new File(args[i]);
				if (file.exists() && file.getName().endsWith(".json")) {
					BinaryMapIndexReader reader = BinaryMapIndexTestReader.buildTestReader(file);
					readers.add(reader);
				}
			}
		}
		if (combineParts.isEmpty()) {
			combineParts.addAll(COMBINE_ARGS.values());
		}
		if (outputFile.exists()) {
			if (!outputFile.delete()) {
				throw new IOException("Cannot delete file " + outputFile);
			}
		}
		if (readers.isEmpty()) {
			throw new IOException("No data for merge");
		}
		combineParts(outputFile, null, readers, combineParts);

		FileInputStream inputStream = new FileInputStream(outputFile);
		File outFile = new File(outputFilePath + ".gz");
		GZIPOutputStream outputStream = new GZIPOutputStream(new FileOutputStream(outFile));
		Algorithms.streamCopy(inputStream, outputStream);
		outputStream.close();
		inputStream.close();

		if (!outputFile.delete()) {
			//throw new IOException("Cannot delete file " + outputFile);
		}
	}
}
