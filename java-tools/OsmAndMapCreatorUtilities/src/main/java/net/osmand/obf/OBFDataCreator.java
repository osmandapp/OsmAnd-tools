package net.osmand.obf;

import gnu.trove.map.hash.TLongObjectHashMap;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexTestReader;
import net.osmand.binary.OsmandOdb;
import net.osmand.binary.RouteDataObject;
import net.osmand.obf.diff.ObfFileInMemory;
import net.osmand.util.Algorithms;
import rtree.RTreeException;

import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

public class OBFDataCreator extends BinaryMerger {
	protected static final Map<String, Integer> COMBINE_ARGS = new HashMap<String, Integer>();

	static {
		COMBINE_ARGS.put("--address", OsmandOdb.OsmAndStructure.ADDRESSINDEX_FIELD_NUMBER);
		COMBINE_ARGS.put("--poi", OsmandOdb.OsmAndStructure.POIINDEX_FIELD_NUMBER);
		COMBINE_ARGS.put("--route", OsmandOdb.OsmAndStructure.ROUTINGINDEX_FIELD_NUMBER);
	}

	public File create(String obfFilePath, String[] jsonFilePaths) throws IOException, SQLException {
		Set<Integer> combineParts = new HashSet<>(COMBINE_ARGS.values());
		return create(obfFilePath, jsonFilePaths, combineParts);
	}

	public File create(String obfFilePath, String[] jsonFilePaths, Set<Integer> combineParts) throws IOException, SQLException {
		File outputFile = new File(obfFilePath);
		List<BinaryMapIndexReader> readers = new ArrayList<>();
		List<BinaryMapIndexTestReader> testReaders = new ArrayList<>();
		for (String jsonFilePath : jsonFilePaths) {
			File jsonFile = new File(jsonFilePath);
			if (jsonFile.exists() && jsonFile.getName().endsWith(".json")) {
				BinaryMapIndexReader reader = BinaryMapIndexTestReader.buildTestReader(jsonFile);
				readers.add(reader);
				if (reader instanceof BinaryMapIndexTestReader testReader) {
					testReaders.add(testReader);
				}
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
		boolean includeRouting = cp.contains(OsmandOdb.OsmAndStructure.ROUTINGINDEX_FIELD_NUMBER);
		boolean hasRoutingData = includeRouting && hasRoutingData(testReaders);
		if (hasRoutingData) {
			createWithRouting(outputFile, readers, testReaders, cp);
		} else {
			Set<Integer> cpWithoutRouting = new HashSet<>(cp);
			cpWithoutRouting.remove(OsmandOdb.OsmAndStructure.ROUTINGINDEX_FIELD_NUMBER);
			combineParts(outputFile, null, readers, cpWithoutRouting);
		}

		File outFile = new File(obfFilePath + ".gz");
		try (FileInputStream inputStream = new FileInputStream(outputFile);
		     FileOutputStream fileOutputStream = new FileOutputStream(outFile);
		     GZIPOutputStream gzipOutputStream = new GZIPOutputStream(fileOutputStream)) {
			Algorithms.streamCopy(inputStream, gzipOutputStream);
		}

		return outFile;
	}

	private boolean hasRoutingData(List<BinaryMapIndexTestReader> readers) {
		for (BinaryMapIndexTestReader reader : readers) {
			if (!reader.getRoutingData().isEmpty()) {
				return true;
			}
		}
		return false;
	}

	private void createWithRouting(File outputFile, List<BinaryMapIndexReader> readers,
			List<BinaryMapIndexTestReader> testReaders, Set<Integer> combineParts) throws IOException, SQLException {
		Set<Integer> combineWithoutRouting = new HashSet<>(combineParts);
		combineWithoutRouting.remove(OsmandOdb.OsmAndStructure.ROUTINGINDEX_FIELD_NUMBER);
		if (combineWithoutRouting.isEmpty()) {
			createRoutingFile(outputFile, testReaders);
			return;
		}
		File baseFile = new File(outputFile.getParentFile(), outputFile.getName() + ".base.obf");
		File routeFile = new File(outputFile.getParentFile(), outputFile.getName() + ".route.obf");
		try {
			if (baseFile.exists() && !baseFile.delete()) {
				throw new IOException("Cannot delete file " + baseFile);
			}
			if (routeFile.exists() && !routeFile.delete()) {
				throw new IOException("Cannot delete file " + routeFile);
			}
			combineParts(baseFile, null, readers, combineWithoutRouting);
			createRoutingFile(routeFile, testReaders);
			combineParts(outputFile, List.of(baseFile, routeFile), null, Collections.emptySet());
		} finally {
			if (baseFile.exists() && !baseFile.delete()) {
				baseFile.deleteOnExit();
			}
			if (routeFile.exists() && !routeFile.delete()) {
				routeFile.deleteOnExit();
			}
		}
	}

	private void createRoutingFile(File routeFile, List<BinaryMapIndexTestReader> readers) throws IOException {
		if (routeFile.exists() && !routeFile.delete()) {
			throw new IOException("Cannot delete file " + routeFile);
		}
		ObfFileInMemory routeObf = new ObfFileInMemory();
		TLongObjectHashMap<RouteDataObject> routeObjects = new TLongObjectHashMap<>();
		long nextRouteId = 1;
		for (BinaryMapIndexTestReader reader : readers) {
			for (RouteDataObject routeDataObject : reader.getRoutingData()) {
				RouteDataObject copy = new RouteDataObject(routeDataObject);
				while (routeObjects.containsKey(nextRouteId)) {
					nextRouteId++;
				}
				copy.id = nextRouteId;
				routeObjects.put(nextRouteId, copy);
				nextRouteId++;
			}
		}
		routeObf.putRoutingData(routeObjects, true);
		try {
			routeObf.writeFile(routeFile, true);
		} catch (RTreeException | SQLException e) {
			throw new IOException("Failed to write route test obf", e);
		}
	}
}
