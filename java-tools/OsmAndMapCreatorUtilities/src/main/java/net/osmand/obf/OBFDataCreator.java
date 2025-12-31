package net.osmand.obf;

import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexTestReader;
import net.osmand.binary.OsmandOdb;
import net.osmand.util.Algorithms;

import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

public class OBFDataCreator extends BinaryMerger {
	protected static final Map<String, Integer> COMBINE_ARGS = new HashMap<String, Integer>();

	static {
		COMBINE_ARGS.put("--address", OsmandOdb.OsmAndStructure.ADDRESSINDEX_FIELD_NUMBER);
		COMBINE_ARGS.put("--poi", OsmandOdb.OsmAndStructure.POIINDEX_FIELD_NUMBER);
	}

	public File create(String obfFilePath, String[] jsonFilePaths) throws IOException, SQLException {
		Set<Integer> combineParts = new HashSet<>(COMBINE_ARGS.values());
		return create(obfFilePath, jsonFilePaths, combineParts);
	}

	public File create(String obfFilePath, String[] jsonFilePaths, Set<Integer> combineParts) throws IOException, SQLException {
		File outputFile = new File(obfFilePath);
		List<BinaryMapIndexReader> readers = new ArrayList<>();
		for (String jsonFilePath : jsonFilePaths) {
			File jsonFile = new File(jsonFilePath);
			if (jsonFile.exists() && jsonFile.getName().endsWith(".json")) {
				BinaryMapIndexReader reader = BinaryMapIndexTestReader.buildTestReader(jsonFile);
				readers.add(reader);
			}
		}

		if (readers.isEmpty()) {
			throw new IOException("No data for merge");
		}
		if (outputFile.exists()) {
			if (!outputFile.delete()) {
				throw new IOException("Cannot delete file " + outputFile);
			}
		}
		Set<Integer> cp = combineParts == null || combineParts.isEmpty()
				? new HashSet<>(COMBINE_ARGS.values())
				: new HashSet<>(combineParts);
		combineParts(outputFile, null, readers, cp);

		File outFile = new File(obfFilePath + ".gz");
		try (FileInputStream inputStream = new FileInputStream(outputFile);
		     FileOutputStream fileOutputStream = new FileOutputStream(outFile);
		     GZIPOutputStream gzipOutputStream = new GZIPOutputStream(fileOutputStream)) {
			Algorithms.streamCopy(inputStream, gzipOutputStream);
		}

		return outFile;
	}
}
