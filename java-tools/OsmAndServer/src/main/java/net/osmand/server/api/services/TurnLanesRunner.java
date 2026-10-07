package net.osmand.server.api.services;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Replays turn-lanes drives on another OsmAndMapCreator build, in a JVM of its own: {@code java -cp
 * <this class>:<build>/lib/* net.osmand.server.api.services.TurnLanesRunner <profile>}.
 *
 * It names no OsmAnd class, so it loads against a build of any year - the routing API has changed under it
 * (searchRoute returned a List until 2023, a RouteCalcResult since) - and nothing of the server: the class file is
 * all it takes. The instructions are keyed and written exactly as GenerateTurnLanesTest.instructions does it.
 *
 * stdin, a drive per line: {@code num<TAB>obf path<TAB>lat,lon<TAB>lat,lon<TAB>left side}.
 * stdout, a line per drive: {@code {"num":"1","results":{"123":"TL:+TL|C"},"roads":["123","456"]}} or
 * {@code {"num":"1","error":"..."}}.
 */
public class TurnLanesRunner {

	private final String profile;
	private final Class<?> latLonClass;
	private final Class<?> readerClass;
	private final Object frontEnd;
	private Object reader;
	private String readerPath;

	private TurnLanesRunner(String profile) throws Exception {
		this.profile = profile;
		latLonClass = Class.forName("net.osmand.data.LatLon");
		readerClass = Class.forName("net.osmand.binary.BinaryMapIndexReader");
		frontEnd = Class.forName("net.osmand.router.RoutePlannerFrontEnd").getConstructor().newInstance();
	}

	public static void main(String[] args) throws Exception {
		PrintStream out = new PrintStream(new FileOutputStream(FileDescriptor.out), true, "UTF-8");
		// the router prints as it goes: keep that out of the answers
		System.setOut(System.err);
		TurnLanesRunner runner = new TurnLanesRunner(args.length > 0 ? args[0] : "car");
		BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
		for (String line; (line = in.readLine()) != null; ) {
			if (line.isEmpty()) {
				continue;
			}
			String[] p = line.split("\t");
			StringBuilder sb = new StringBuilder("{\"num\":").append(json(p[0]));
			try {
				List<?> route = runner.route(p[1], p[2], p[3], Boolean.parseBoolean(p[4]));
				if (route == null) {
					sb.append(",\"error\":\"No route\"");
				} else {
					sb.append(",\"results\":{");
					boolean first = true;
					for (Map.Entry<String, String> e : instructions(route).entrySet()) {
						sb.append(first ? "" : ",").append(json(e.getKey())).append(':').append(json(e.getValue()));
						first = false;
					}
					// every road the drive takes: an instruction gone from one of them is not the drive going elsewhere
					sb.append("},\"roads\":[");
					Set<Long> roads = new LinkedHashSet<>();
					for (Object segment : route) {
						roads.add(osmId(call(segment, "getObject")));
					}
					first = true;
					for (long id : roads) {
						sb.append(first ? "" : ",").append('"').append(id).append('"');
						first = false;
					}
					sb.append(']');
				}
			} catch (Throwable e) {
				Throwable t = e instanceof InvocationTargetException && e.getCause() != null ? e.getCause() : e;
				t.printStackTrace();
				sb.append(",\"error\":").append(json(t.getClass().getSimpleName() + ": " + t.getMessage()));
			}
			out.println(sb.append('}'));
		}
		runner.closeReader();
	}

	private static String json(String s) {
		StringBuilder sb = new StringBuilder("\"");
		for (char c : String.valueOf(s).toCharArray()) {
			switch (c) {
				case '"' -> sb.append("\\\"");
				case '\\' -> sb.append("\\\\");
				case '\n' -> sb.append("\\n");
				case '\r' -> sb.append("\\r");
				case '\t' -> sb.append("\\t");
				default -> {
					if (c < 0x20) {
						sb.append(String.format("\\u%04x", (int) c));
					} else {
						sb.append(c);
					}
				}
			}
		}
		return sb.append('"').toString();
	}

	/** the drives come map by map: one map open at a time */
	private Object reader(String path) throws Exception {
		if (!path.equals(readerPath)) {
			closeReader();
			File f = new File(path);
			reader = readerClass.getConstructor(RandomAccessFile.class, File.class)
					.newInstance(new RandomAccessFile(f, "r"), f);
			readerPath = path;
		}
		return reader;
	}

	private void closeReader() throws Exception {
		if (reader != null) {
			readerClass.getMethod("close").invoke(reader);
			reader = null;
			readerPath = null;
		}
	}

	private Object latLon(String s) throws Exception {
		String[] p = s.split(",");
		return latLonClass.getConstructor(double.class, double.class)
				.newInstance(Double.parseDouble(p[0].trim()), Double.parseDouble(p[1].trim()));
	}

	private List<?> route(String obf, String from, String to, boolean leftSide) throws Exception {
		Object readers = Array.newInstance(readerClass, 1);
		Array.set(readers, 0, reader(obf));
		Object ctx = context(readers, leftSide);
		Method search = null;
		for (Method m : frontEnd.getClass().getMethods()) {
			if (m.getName().equals("searchRoute") && m.getParameterCount() == 4
					&& m.getParameterTypes()[1] == latLonClass) {
				search = m;
			}
		}
		if (search == null) {
			throw new NoSuchMethodException("RoutePlannerFrontEnd.searchRoute(ctx, LatLon, LatLon, List)");
		}
		Object result = search.invoke(frontEnd, ctx, latLon(from), latLon(to), null);
		// a List until 2023, a RouteCalcResult carrying it since
		List<?> route = result == null || result instanceof List ? (List<?>) result
				: (List<?>) result.getClass().getMethod("getList").invoke(result);
		return route == null || route.isEmpty() ? null : route;
	}

	private Object context(Object readers, boolean leftSide) throws Exception {
		Class<?> config = Class.forName("net.osmand.router.RoutingConfiguration");
		Object builder = config.getMethod("getDefault").invoke(null);
		Map<String, String> params = new HashMap<>();
		params.put(profile, "true");
		// GenerateTurnLanesTest.MEMORY_LIMIT_MB: the same budget as the dataset was made with
		int memory = 512;
		int nativeMemory = defaultLimit(config, "DEFAULT_NATIVE_MEMORY_LIMIT", 256);
		Object cfg;
		Class<?> limitsClass;
		try {
			limitsClass = Class.forName("net.osmand.router.RoutingConfiguration$RoutingMemoryLimits");
		} catch (ClassNotFoundException e) {
			limitsClass = null; // before 2022: the limit is a plain number of megabytes
		}
		if (limitsClass == null) {
			try {
				cfg = builder.getClass().getMethod("build", String.class, int.class, Map.class)
						.invoke(builder, profile, memory, params);
			} catch (NoSuchMethodException e) {
				cfg = builder.getClass().getMethod("build", String.class, int.class).invoke(builder, profile, memory);
			}
		} else {
			Object limits = null;
			for (Constructor<?> c : limitsClass.getConstructors()) {
				Class<?>[] t = c.getParameterTypes();
				if (t.length == 2 && t[0] == int.class) {
					limits = c.newInstance(memory, nativeMemory);
				} else if (t.length == 2 && t[0] == long.class) {
					limits = c.newInstance((long) memory, (long) nativeMemory);
				}
			}
			try {
				cfg = builder.getClass().getMethod("build", String.class, limitsClass, Map.class)
						.invoke(builder, profile, limits, params);
			} catch (NoSuchMethodException e) {
				cfg = builder.getClass().getMethod("build", String.class, limitsClass)
						.invoke(builder, profile, limits);
			}
		}
		Class<?> nativeLib = Class.forName("net.osmand.NativeLibrary");
		Object ctx;
		try {
			Class<?> mode = Class.forName("net.osmand.router.RoutePlannerFrontEnd$RouteCalculationMode");
			Object normal = mode.getField("NORMAL").get(null);
			ctx = frontEnd.getClass().getMethod("buildRoutingContext", config, nativeLib, readers.getClass(), mode)
					.invoke(frontEnd, cfg, null, readers, normal);
		} catch (ClassNotFoundException | NoSuchMethodException e) {
			ctx = frontEnd.getClass().getMethod("buildRoutingContext", config, nativeLib, readers.getClass())
					.invoke(frontEnd, cfg, null, readers);
		}
		try {
			ctx.getClass().getField("leftSideNavigation").setBoolean(ctx, leftSide);
		} catch (NoSuchFieldException e) {
			// an old build: right-hand everywhere
		}
		return ctx;
	}

	private static int defaultLimit(Class<?> config, String field, int fallback) {
		try {
			return ((Number) config.getField(field).get(null)).intValue();
		} catch (ReflectiveOperationException e) {
			return fallback;
		}
	}

	/** GenerateTurnLanesTest.instructions, over reflection */
	private static Map<String, String> instructions(List<?> route) throws Exception {
		Map<String, String> results = new LinkedHashMap<>();
		Map<Long, Integer> seen = new HashMap<>();
		seen.put(osmId(call(route.get(0), "getObject")), (Integer) call(route.get(0), "getStartPointIndex"));
		for (int i = 1; i < route.size(); i++) {
			Object segment = route.get(i);
			Object turn = call(segment, "getTurnType");
			if (turn == null) {
				continue;
			}
			long id = osmId(call(segment, "getObject"));
			int[] lanesArr = (int[]) call(turn, "getLanes");
			String lanes = lanesArr == null ? null : lanesToString(turn.getClass(), lanesArr);
			String xml = (String) call(turn, "toXmlString");
			String value = lanes == null ? xml : (skipToSpeak(turn) ? "[MUTE] " : "")
					+ xml + ":" + lanes;
			int start = (Integer) call(segment, "getStartPointIndex");
			Integer had = seen.put(id, start);
			String key = had == null ? String.valueOf(id) : id + ":" + start;
			if (had != null && results.containsKey(String.valueOf(id))) {
				results.put(id + ":" + had, results.remove(String.valueOf(id)));
			}
			results.put(key, value);
		}
		return results;
	}

	/** TurnType.lanesToString, which builds before 2020 do not have: the same, from the build's own lane bits */
	private static String lanesToString(Class<?> turnType, int[] lanes) throws Exception {
		try {
			return (String) turnType.getMethod("lanesToString", int[].class).invoke(null, (Object) lanes);
		} catch (NoSuchMethodException e) {
			// below
		}
		Method valueOf = turnType.getMethod("valueOf", int.class, boolean.class);
		Method xml = turnType.getMethod("toXmlString");
		String[] parts = {"getPrimaryTurn", "getSecondaryTurn", "getTertiaryTurn"};
		StringBuilder s = new StringBuilder();
		for (int h = 0; h < lanes.length; h++) {
			if (h > 0) {
				s.append('|');
			}
			if (lanes[h] % 2 == 1) {
				s.append('+');
			}
			for (int i = 0; i < parts.length; i++) {
				int t = (Integer) turnType.getMethod(parts[i], int.class).invoke(null, lanes[h]);
				if (i == 0 && t == 0) {
					t = 1; // an unmarked lane reads as straight on
				} else if (t == 0) {
					continue;
				}
				s.append(i == 0 ? "" : ",").append(xml.invoke(valueOf.invoke(null, t, false)));
			}
		}
		return s.toString();
	}

	/** builds that did not mute anything have no such flag */
	private static boolean skipToSpeak(Object turn) throws Exception {
		try {
			return Boolean.TRUE.equals(turn.getClass().getMethod("isSkipToSpeak").invoke(turn));
		} catch (NoSuchMethodException e) {
			return false;
		}
	}

	private static Object call(Object o, String method) throws Exception {
		return o.getClass().getMethod(method).invoke(o);
	}

	private static long osmId(Object road) throws Exception {
		try {
			Class<?> obf = Class.forName("net.osmand.binary.ObfConstants");
			return (Long) obf.getMethod("getOsmObjectId", road.getClass()).invoke(null, road);
		} catch (ClassNotFoundException | NoSuchMethodException e) {
			// before ObfConstants: the id is the osm one shifted by six bits
			return ((Long) call(road, "getId")) >> 6;
		}
	}
}
