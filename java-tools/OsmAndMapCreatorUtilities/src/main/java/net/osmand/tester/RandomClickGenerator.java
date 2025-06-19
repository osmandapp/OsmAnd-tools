package net.osmand.tester;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;
import net.osmand.MainUtilities;
import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryIndexPart;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexReader.MapIndex;
import net.osmand.render.RenderingRuleSearchRequest;
import net.osmand.render.RenderingRulesStorage;
import net.osmand.routes.RouteRelationExtractor;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.stream.IntStream;

public class RandomClickGenerator {
	private int zoom = 17;
	private int maxClicks = 1000;
	private int maxShiftMeters = 5;
	private PseudoRandom pseudoRandom = PseudoRandom.MONTH;
	private final List<String> obfFileNames = new ArrayList<>();

	private static final int EXIT_SUCCESS = 0;
	private static final int EXIT_FAILED = 1;

	private final RenderingRuleSearchRequest renderingSearchRequest;
	private final Map<String, List<ClickableObject>> allObjectsMap = new HashMap<>();
	private final List<ClickableObject> generatedRandomClicks = new ArrayList<>();

	public static void main(String[] args) throws IOException {
		long started = System.currentTimeMillis();
		List<ClickableObject> result = new RandomClickGenerator(args).generate();

		System.out.println(new ObjectMapper()
				.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
				.writerWithDefaultPrettyPrinter()
				.writeValueAsString(Map.of("clicks", result)));

		System.err.printf("Finished in %.2f seconds\n", (System.currentTimeMillis() - started) / 1000.0);
		System.exit(result.isEmpty() ? EXIT_FAILED : EXIT_SUCCESS);
	}

	private RandomClickGenerator(String[] args) {
		applyCommandLineOpts(new MainUtilities.CommandLineOpts(args));

		String[] styles = RouteRelationExtractor.customStyles;
		Map<String, String> properties = RouteRelationExtractor.customProperties;
		RenderingRulesStorage renderingRules = RenderingRulesStorage.initWithStylesFromResources(styles);
		renderingSearchRequest = RenderingRuleSearchRequest.initWithCustomProperties(renderingRules, zoom, properties);
	}

	private List<ClickableObject> generate() throws IOException {
		readObfFilesIndexes();
		generateRandomClicks();
		return generatedRandomClicks;
	}

	private void generateRandomClicks() {
		List<String> orderedTypes = new ArrayList<>(allObjectsMap.keySet());
		Collections.shuffle(orderedTypes, pseudoRandom.getNextRandom());

		for (String type : orderedTypes) {
			Collections.shuffle(allObjectsMap.get(type), pseudoRandom.getNextRandom());
		}

		boolean added;
		do {
			added = false;
			for (String type : orderedTypes) {
				if (generatedRandomClicks.size() >= maxClicks) {
					break;
				}
				List<ClickableObject> objects = allObjectsMap.get(type);
				if (!objects.isEmpty()) {
					generatedRandomClicks.add(objects.remove(objects.size() - 1));
					added = true;
				}
			}
		} while(added);
	}

	private void readObfFilesIndexes() throws IOException {
		for (String fileName : obfFileNames) {
			File file = new File(fileName);
			RandomAccessFile raf = new RandomAccessFile(file, "r");
			BinaryMapIndexReader reader = new BinaryMapIndexReader(raf, file);
			for (BinaryIndexPart part : reader.getIndexes()) {
				if (part instanceof MapIndex) {
					readMapSection(reader, (MapIndex) part);
				}
			}
			reader.close();
		}
	}

	private void readMapSection(BinaryMapIndexReader reader, MapIndex map) throws IOException {
		BinaryMapIndexReader.SearchRequest<BinaryMapDataObject> req = BinaryMapIndexReader.buildSearchRequest(
				0, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, zoom, null,
				new ResultMatcher<>() {
					@Override
					public boolean publish(BinaryMapDataObject object) {
						processBinaryMapDataObject(object);
						return false;
					}

					@Override
					public boolean isCancelled() {
						return false;
					}
				});
		reader.searchMapIndex(req, map);
	}

	private void processBinaryMapDataObject(BinaryMapDataObject object) {
		if (object.getPolygonInnerCoordinates() != null && object.getPolygonInnerCoordinates().length > 0) {
			return; // Multipolygon
		} else if (object.isArea()) {
			return; // Area
		} else if (object.getPointsLength() > 1) {
			return; // Way
		} else if (object.getPointsLength() == 1) {
			processMapPoint(object);
		}
	}

	private Map<String, String> collectBinaryDataObjectOrderedTags(BinaryMapDataObject object) {
		Map<String, String> tags = new LinkedHashMap<>();

		int[] types = object.getTypes();
		int[] additionalTypes = object.getAdditionalTypes();

		int[] allObjectTypes = IntStream.concat(
						Arrays.stream(types != null ? types : new int[0]),
						Arrays.stream(additionalTypes != null ? additionalTypes : new int[0]))
				.toArray();

		for (int type : allObjectTypes) {
			BinaryMapIndexReader.TagValuePair pair = object.getMapIndex().decodeType(type);
			if (pair != null && pair.tag != null && pair.value != null) {
				tags.put(pair.tag, pair.value);
			}
		}

		return tags;
	}

	private void processMapPoint(BinaryMapDataObject point) {
		Map<String, String> tags = collectBinaryDataObjectOrderedTags(point);
		String icon = renderingSearchRequest.searchIconByTags(tags);
		boolean isClickable = icon != null && !tags.isEmpty();

		if (isClickable) {
			float lat = (float) MapUtils.get31LatitudeY(point.getPoint31YTile(0));
			float lon = (float) MapUtils.get31LongitudeX(point.getPoint31XTile(0));

			if (maxShiftMeters > 0) {
				lat = shiftLatOrLon(lat);
				lon = shiftLatOrLon(lon);
			}

			Map.Entry<String, String> firstTag = tags.entrySet().iterator().next();
			String mainTagValue = firstTag.getKey() + "=" + firstTag.getValue();

			String mainName = "";
			TIntArrayList order = point.getNamesOrder();
			TIntObjectHashMap<String> names = point.getObjectNames();
			if (names != null && !names.isEmpty() && order != null && !order.isEmpty()) {
				BinaryMapIndexReader.TagValuePair pair = point.getMapIndex().decodeType(order.get(0));
				if (pair != null) {
					String tag = pair.tag;
					String name = names.get(order.get(0));
					tags.putIfAbsent(tag, name);
					mainName = name;
				}
			}

			ClickableObject clickable = new ClickableObject(zoom, lat, lon, icon, mainName, mainTagValue, tags);
			allObjectsMap.computeIfAbsent(mainTagValue, k -> new ArrayList<>()).add(clickable);
		}

	}

	private float shiftLatOrLon(float ll) {
		float maxDegree = maxShiftMeters / (float) MapUtils.METERS_IN_DEGREE;
		float offset = (pseudoRandom.getNextFloat() * 2 - 1) * maxDegree;
		return ll + offset; // returns ll +- maxShiftMeters in degrees
	}

	private void applyCommandLineOpts(MainUtilities.CommandLineOpts opts) {
		if (opts.getOpt("--help") != null) {
			printHelpAndExit();
		}

		zoom = Algorithms.parseIntSilently(opts.getOpt("--zoom"), zoom);
		maxClicks = Algorithms.parseIntSilently(opts.getOpt("--max-clicks"), maxClicks);
		maxShiftMeters = Algorithms.parseIntSilently(opts.getOpt("--max-shift"), maxShiftMeters);

		String optRandomSeed = Objects.requireNonNullElse(opts.getOpt("--random-seed"), "month");
		switch (optRandomSeed) {
			case "random" -> pseudoRandom = PseudoRandom.RANDOM;
			case "month" -> pseudoRandom = PseudoRandom.MONTH;
			case "week" -> pseudoRandom = PseudoRandom.WEEK;
			case "day" -> pseudoRandom = PseudoRandom.DAY;
		}

		obfFileNames.addAll(opts.getStrings());
	}

	private void printHelpAndExit() {
		System.err.printf("%s\n", String.join("\n",
				"",
				"Usage: random-click-generator [--options] [OBF-FILE(s)...]",
				"",
				"--random-seed=month|week|day|random (default based on month)",
				"--max-clicks=N (default 1000 clicks)",
				"--max-shift=N (default 5 meters)",
				"--zoom=N (default 17 zoom)",
				"",
				"--help show help",
				""
		));
		System.exit(EXIT_SUCCESS);
	}

	private enum PseudoRandom {
		MONTH(1000L * 3600 * 24 * 31),
		WEEK(1000L * 3600 * 24 * 7),
		DAY(1000L * 3600 * 24),
		RANDOM(1L);

		private final Random floatRandom;
		private final Random longRandom;

		PseudoRandom(long millisPeriod) {
			long seed = System.currentTimeMillis() / millisPeriod;
			floatRandom = new Random(seed);
			longRandom = new Random(seed);
		}

		private float getNextFloat() {
			return floatRandom.nextFloat();
		}

		private long getNextLong() {
			return longRandom.nextLong();
		}

		private Random getNextRandom() {
			return new Random(getNextLong());
		}
	}

	private record ClickableObject(
			int zoom,
			float latitude,
			float longitude,
			String icon,
			String mainName,
			String mainTagValue,
			Map<String, String> tags
	) {
		@Override
		public String toString() {
			return String.format("%.5f,%.5f z%s %s %s (%s)", latitude, longitude, zoom, mainTagValue, icon, mainName);
		}
	}
}
