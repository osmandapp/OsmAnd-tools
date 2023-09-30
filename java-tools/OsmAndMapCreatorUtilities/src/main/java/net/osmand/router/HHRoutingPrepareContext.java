package net.osmand.router;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.router.HHRoutingPreparationDB.NetworkRouteRegion;
import net.osmand.router.RoutePlannerFrontEnd.RouteCalculationMode;
import net.osmand.router.RoutingConfiguration.Builder;
import net.osmand.router.RoutingConfiguration.RoutingMemoryLimits;

public class HHRoutingPrepareContext {

	final static int MEMORY_RELOAD_MB = 1000; //
	final static int MEMORY_RELOAD_TIMEOUT_SECONDS = 120;
	static final int ROUTING_MEMORY_LIMIT = 1024;
	static long MEMEORY_LAST_RELOAD = System.currentTimeMillis();
	static long MEMORY_LAST_USED_MB;
	static long DEBUG_START_TIME = 0;

	private String ROUTING_PROFILE = "car";
	private List<File> FILE_SOURCES = new ArrayList<File>();
	
	public HHRoutingPrepareContext(File obfFile, String routingProfile) {
		if (routingProfile != null) {
			ROUTING_PROFILE = routingProfile;
		}
		if (obfFile.isDirectory()) {
			for (File f : obfFile.listFiles()) {
				if (!f.getName().endsWith(".obf")) {
					continue;
				}
				FILE_SOURCES.add(f);
			}
		} else {
			FILE_SOURCES.add(obfFile);
		}
	}
	

	
	public RoutingContext gcMemoryLimitToUnloadAll(RoutingContext ctx, List<NetworkRouteRegion> subRegions,
			boolean force) throws IOException {
		long usedMemory = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) >> 20;
		if (force || ((usedMemory - MEMORY_LAST_USED_MB) > MEMORY_RELOAD_MB
				&& (System.currentTimeMillis() - MEMEORY_LAST_RELOAD) > MEMORY_RELOAD_TIMEOUT_SECONDS * 1000)) {
			long nt = System.nanoTime();
			System.gc();
			long ntusedMemory = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) >> 20;
			if (!force && (ntusedMemory - MEMORY_LAST_USED_MB) < MEMORY_RELOAD_MB) {
				return ctx;
			}
			Set<File> fls = null;
			if (subRegions != null) {
				fls = new LinkedHashSet<>();
				for (NetworkRouteRegion r : subRegions) {
					fls.add(r.file);
				}
			}
			ctx = prepareContext(fls, ctx);
			ctx.calculationProgress = new RouteCalculationProgress();
			System.gc();
			MEMORY_LAST_USED_MB = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) >> 20;
			MEMEORY_LAST_RELOAD = System.currentTimeMillis();
			logf("***** Reload memory used before %d MB -> GC %d MB -> reload ctx %d MB (%.1f s) *****\n", usedMemory,
					ntusedMemory, MEMORY_LAST_USED_MB, (System.nanoTime() - nt) / 1e9);
		}
		return ctx;
	}

	public static void logf(String string, Object... a) {
		if (DEBUG_START_TIME == 0) {
			DEBUG_START_TIME = System.currentTimeMillis();
		}
		String ms = String.format("%3.1fs ", (System.currentTimeMillis() - DEBUG_START_TIME) / 1000.f);
		System.out.printf(ms + string + "\n", a);

	}

	private List<BinaryMapIndexReader> initReaders(Collection<File> hints) throws IOException {
		List<BinaryMapIndexReader> readers = new ArrayList<BinaryMapIndexReader>();
		for (File source : (hints != null ? hints : FILE_SOURCES)) {
			BinaryMapIndexReader reader = new BinaryMapIndexReader(new RandomAccessFile(source, "r"), source);
			readers.add(reader);
		}
		return readers;
	}

	RoutingContext prepareContext(Collection<File> fileSources, RoutingContext oldCtx) throws IOException {
		List<BinaryMapIndexReader> readers = initReaders(fileSources);
		if (oldCtx != null) {
			for (BinaryMapIndexReader r : oldCtx.map.keySet()) {
				r.close();
			}
		}
		RoutePlannerFrontEnd router = new RoutePlannerFrontEnd();
		Builder builder = RoutingConfiguration.parseDefault();
		RoutingMemoryLimits memoryLimit = new RoutingMemoryLimits(ROUTING_MEMORY_LIMIT, ROUTING_MEMORY_LIMIT);
		Map<String, String> map = new TreeMap<String, String>();
		// map.put("avoid_ferries", "true");
		RoutingConfiguration config = builder.build(ROUTING_PROFILE, memoryLimit, map);
		config.planRoadDirection = 1;
		config.heuristicCoefficient = 0; // dijkstra
		return router.buildRoutingContext(config, null, readers.toArray(new BinaryMapIndexReader[readers.size()]),
				RouteCalculationMode.NORMAL);
	}


}
