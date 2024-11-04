package net.osmand.router;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import static net.osmand.router.HHRoutingUtilities.logf;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.router.HHRoutingPreparationDB.NetworkRouteRegion;
import net.osmand.router.RoutePlannerFrontEnd.RouteCalculationMode;
import net.osmand.router.RoutingConfiguration.Builder;
import net.osmand.router.RoutingConfiguration.RoutingMemoryLimits;

public class HHRoutingPrepareContext {

	static final int ROUTING_MEMORY_LIMIT = 3000;
	static int MEMORY_RELOAD_TIMEOUT_SECONDS = 120;
	static int MEMORY_RELOAD_MB = 1000; //
	static long MEMEORY_LAST_RELOAD = System.currentTimeMillis();
	static long MEMORY_LAST_USED_MB;

	private String ROUTING_PROFILE = "car";
	private Map<String, String> PROFILE_SETTINGS = new TreeMap<>();
	private List<File> FILE_SOURCES = new ArrayList<File>();
	
	public HHRoutingPrepareContext(File obfFile, String routingProfile, String... profileSettings) {
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
		if (profileSettings != null) {
			for (String sp : profileSettings) {
				String[] s = sp.split("=");
				String val = "true";
				if (s.length > 1) {
					val = s[1];
				}
				PROFILE_SETTINGS.put(s[0], val);
			}
		}
		Collections.sort(FILE_SOURCES, new Comparator<File>() {

			@Override
			public int compare(File o1, File o2) {
				return Long.compare(o1.length(), o2.length());
			}
		});
	}
	

	
	RoutingContext gcMemoryLimitToUnloadAll(RoutingContext ctx, List<NetworkRouteRegion> subRegions,
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
			double reloadTimeSeconds = (System.nanoTime() - nt) / 1e9;
			logf("***** Reload memory used before %d MB -> GC %d MB -> reload ctx %d MB (%.1f s) *****\n", usedMemory,
					ntusedMemory, MEMORY_LAST_USED_MB, reloadTimeSeconds);
			if (reloadTimeSeconds * 8 > MEMORY_RELOAD_TIMEOUT_SECONDS) {
				MEMORY_RELOAD_TIMEOUT_SECONDS = (int) (reloadTimeSeconds * 16);
				logf("New reload memory time %d seconds", MEMORY_RELOAD_TIMEOUT_SECONDS);
			}
		}
		return ctx;
	}

	RoutingContext prepareContext(Collection<File> fileSources, RoutingContext oldCtx) throws IOException {
		List<BinaryMapIndexReader> readers = initReaders(fileSources);
		if (oldCtx != null) {
			for (BinaryMapIndexReader r : oldCtx.map.keySet()) {
				r.close();
			}
		}
		RoutePlannerFrontEnd router = new RoutePlannerFrontEnd();
		RoutingConfiguration config = getRoutingConfig();
		return router.buildRoutingContext(config, null, readers.toArray(new BinaryMapIndexReader[readers.size()]),
				RouteCalculationMode.NORMAL);
	}



	public RoutingConfiguration getRoutingConfig() {
		Builder builder = RoutingConfiguration.parseDefault();
		RoutingMemoryLimits memoryLimit = new RoutingMemoryLimits(ROUTING_MEMORY_LIMIT, ROUTING_MEMORY_LIMIT);
		RoutingConfiguration config = builder.build(ROUTING_PROFILE, memoryLimit, PROFILE_SETTINGS);
		config.planRoadDirection = 1;
		config.heuristicCoefficient = 0; // dijkstra
		config.routeCalculationTime = -1; // boostMaxspeedByMaxConditional
		return config;
	}


	private List<BinaryMapIndexReader> initReaders(Collection<File> hints) throws IOException {
		List<BinaryMapIndexReader> readers = new ArrayList<BinaryMapIndexReader>();
		for (File source : (hints != null ? hints : FILE_SOURCES)) {
			BinaryMapIndexReader reader = new BinaryMapIndexReader(new RandomAccessFile(source, "r"), source);
			readers.add(reader);
		}
		return readers;
	}



}
