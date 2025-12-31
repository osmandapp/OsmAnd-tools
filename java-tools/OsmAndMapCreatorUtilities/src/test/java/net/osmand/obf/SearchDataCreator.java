package net.osmand.obf;


import net.osmand.PlatformUtil;
import org.apache.commons.logging.Log;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class SearchDataCreator extends OBFDataCreator {

	private final static Log log = PlatformUtil.getLog(SearchDataCreator.class);
	public static final String helpMessage = "output_file.obf [--address] [--poi] [input_file.json] ...: create and merge all search test json files and merges poi & address structure into 1";

	public static void main(String[] args) throws IOException, SQLException {
		SearchDataCreator in = new SearchDataCreator();
		// test cases show info
		if (args.length == 1 && "test".equals(args[0])) {
			in.merger(new String[]{
					"/Users/alexey/OsmAnd/resources/test-resources/search/hisar.obf",
					"/Users/alexey/OsmAnd/resources/test-resources/search/hisar.data.json",
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
		Set<Integer> combineParts = new HashSet<>();
		String outputFilePath = null;
		List<String> jsonPaths = new ArrayList<>();
		for (String arg : args) {
			if (arg.startsWith("--")) {
				combineParts.add(COMBINE_ARGS.get(arg));
			} else if (outputFilePath == null) {
				outputFilePath = arg;
			} else {
				jsonPaths.add(arg);
			}
		}
		if (combineParts.isEmpty()) {
			combineParts.addAll(COMBINE_ARGS.values());
		}
		if (outputFilePath == null || jsonPaths.isEmpty()) {
			throw new IOException("Output file or input JSON files are not specified");
		}

		File outputFile = create(outputFilePath, jsonPaths.toArray(new String[0]), combineParts);
		if (!outputFile.delete()) {
			throw new IOException("Cannot delete file " + outputFile);
		}
	}
}
