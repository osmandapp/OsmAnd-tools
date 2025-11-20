package net.osmand.router.tester;

import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryIndexPart;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapRouteReaderAdapter;
import net.osmand.binary.RouteDataObject;
import net.osmand.data.LatLon;
import net.osmand.util.MapUtils;

import java.io.IOException;
import java.util.*;

class RandomRouteGenerator {
	private final RandomRouteTester.GeneratorConfig config;
	private final List<RandomRouteEntry> testList = new ArrayList<>();
	private List<BinaryMapIndexReader> obfReaders = new ArrayList<>();

	RandomRouteGenerator(RandomRouteTester.GeneratorConfig config) {
		this.config = config;
	}

	List<RandomRouteEntry> generateTestList(List<BinaryMapIndexReader> obfReaders) {
		this.obfReaders = obfReaders;
		if (config.PREDEFINED_TESTS.length > 0) {
			parsePredefinedTests();
		} else {
			try {
				generateRandomTests();
			} catch (IOException e) {
				throw new IllegalStateException("generateRandomTests() failed");
			}
		}
		return testList;
	}

	private void parsePredefinedTests() {
		for (String url : config.PREDEFINED_TESTS) {
			RandomRouteEntry entry = new RandomRouteEntry();
			String opts = url.replaceAll(".*\\?", "").replaceAll("#.*", "");
			if (opts.contains("&")) {
				for (String keyval : opts.split("&")) {
					if (keyval.contains("=")) {
						String k = keyval.split("=")[0];
						String v = keyval.split("=")[1];
						if ("profile".equals(k)) { // profile=string
							entry.profile = v;
						} else if ("start".equals(k) && v.contains(",")) { // start=L,L
							double lat = Double.parseDouble(v.split(",")[0]);
							double lon = Double.parseDouble(v.split(",")[1]);
							entry.start = new LatLon(lat, lon);
						} else if (("finish".equals(k) || "end".equals(k)) && v.contains(",")) { // finish=L,L end=L,L
							double lat = Double.parseDouble(v.split(",")[0]);
							double lon = Double.parseDouble(v.split(",")[1]);
							entry.finish = new LatLon(lat, lon);
						} else if ("via".equals(k)) { // via=L,L;L,L...
							for (String ll : v.split(";")) {
								if (ll.contains(",")) {
									double lat = Double.parseDouble(ll.split(",")[0]);
									double lon = Double.parseDouble(ll.split(",")[1]);
									entry.via.add(new LatLon(lat, lon));
								}
							}
						} else if ("params".equals(k)) { // params=string,string...
							for (String param : v.split(",")) {
								if (entry.profile.equals(param)) {
									continue; // /profile/,param1,param2 -> param1,param2 (ignore profile in params)
								}
								if (param.startsWith("hhoff") || param.startsWith("hhonly") ||
										param.startsWith("nativerouting") || param.startsWith("calcmode") ||
										param.startsWith("noglobalfile") || param.startsWith("routing")) {
									continue; // do not use mode-specific or web-specific params from url
								}
								entry.params.add(param);
							}
						}
					}
				}
			}
			if (entry.start != null && entry.finish != null) {
				testList.add(entry);
			}
		}
	}

	private enum RandomActions {
		HIGHWAY_SKIP_DIV,
		HIGHWAY_TO_POINT,
		N_INTER_POINTS,
		GET_START,
		GET_POINTS,
		GET_PROFILE,
		SHIFT_METERS,
	}

	// return fixed (pseudo) random int >=0 and < bound
	// use current week number + action (enum) + i + j as the random seed
	private int fixedRandom(int bound, RandomActions action, long i, long j) {
		final long week = Calendar.getInstance().get(Calendar.WEEK_OF_YEAR); // 1-52 (reset seed every week)
		final long seed = (week << 56) + ((long) action.ordinal() << 48) + (i << 1) + j;
		return bound > 0 ? Math.abs(new Random(seed).nextInt()) % bound : 0;
	}

	private void getObfHighwayRoadRandomPoints(
			BinaryMapIndexReader index, List<LatLon> randomPoints, int limit, int seed) throws IOException {


		class Counter {
			private int value;
		}
		Counter added = new Counter();

		// pointSkipDivisor used to hop over sequential points to enlarge distances between them
		// The idea is to read only 1 of 100 points, but the different 1 each method call (seed)
		int pointSkipDivisor = 1 + fixedRandom(100, RandomActions.HIGHWAY_SKIP_DIV, 0, seed);

		for (BinaryIndexPart p : index.getIndexes()) {
			if (p instanceof BinaryMapRouteReaderAdapter.RouteRegion) {
				List<BinaryMapRouteReaderAdapter.RouteSubregion> regions =
						index.searchRouteIndexTree(
								BinaryMapIndexReader.buildSearchRequest(
										0, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, 15, null
								),
								((BinaryMapRouteReaderAdapter.RouteRegion) p).getSubregions()
						);
				index.loadRouteIndexData(regions, new ResultMatcher<>() {
					@Override
					public boolean publish(RouteDataObject obj) {
						for (int i = 0; i < obj.getTypes().length; i++) {
							BinaryMapRouteReaderAdapter.RouteTypeRule rr =
									obj.region.quickGetEncodingRule(obj.getTypes()[i]);
							// use highway=primary|secondary as a universally suitable way for any profile
							if ("highway".equals(rr.getTag()) &&
									("primary".equals(rr.getValue()) || "secondary".equals(rr.getValue()))
							) {
								final int SHIFT_ID = 6;
								final long osmId = obj.getId() >> SHIFT_ID;
								if (osmId % pointSkipDivisor == 0) {
									int nPoints = obj.pointsX.length;
									// use object id and seed (number of class randomPoints) as a unique random seed
									int pointIndex = fixedRandom(nPoints, RandomActions.HIGHWAY_TO_POINT, osmId, seed);
									double lat = MapUtils.get31LatitudeY(obj.pointsY[pointIndex]);
									double lon = MapUtils.get31LongitudeX(obj.pointsX[pointIndex]);
									randomPoints.add(new LatLon(lat, lon));
									added.value++;
									break;
								}
							}
						}
						return true;
					}

					@Override
					public boolean isCancelled() {
						return added.value > limit;
					}
				});
			}
		}
	}

	private void replenishRandomPoints(List<LatLon> randomPoints) throws IOException {
		if (obfReaders.isEmpty()) {
			throw new IllegalStateException("OBF files not initialized (replenishRandomPoints)");
		}

		int seed = randomPoints.size(); // second random seed (unique for every method call)

		int pointsToRead = 30 * Math.min(config.ITERATIONS, 10); // read up to 30 x ITERATIONS points every time
		int pointsPerObf = pointsToRead / obfReaders.size(); // how many to read per one obf
		pointsPerObf = Math.max(pointsPerObf, 10); // as max as 10

		for (BinaryMapIndexReader obfReader : obfReaders) {
			getObfHighwayRoadRandomPoints(obfReader, randomPoints, pointsPerObf, seed);
		}
	}

	// cut down LatLon precision via %f
	private LatLon roundLatLonViaString(LatLon ll) {
		String str = String.format("%f,%f", ll.getLatitude(), ll.getLongitude());
		double lat = Double.parseDouble(str.split(",")[0]);
		double lon = Double.parseDouble(str.split(",")[1]);
		return new LatLon(lat, lon);
	}

	private void generateRandomTests() throws IOException {
		List<LatLon> randomPoints = new ArrayList<>();
		Set<LatLon> avoidDupes = new HashSet<>();

		int replenishCounter = 0;
		final int REPLENISH_LIMIT = 10; // avoid looping in case of bad config
		replenishRandomPoints(randomPoints); // read initial random points list

		for (int i = 0; i < config.ITERATIONS; i++) {
			RandomRouteEntry entry = new RandomRouteEntry();

			// 1) select profile,params
			if (config.RANDOM_PROFILES.length > 0) {
				boolean isProfileName = true; // "profile[,params]"
				int profileIndex = fixedRandom(config.RANDOM_PROFILES.length, RandomActions.GET_PROFILE, i, 0);
				for (String param : config.RANDOM_PROFILES[profileIndex].split(",")) {
					if (isProfileName) {
						entry.profile = param;
						isProfileName = false;
					} else {
						entry.params.add(param);
					}
				}
			}

			// 2) select start
			for (int j = 0; j < randomPoints.size(); j++) {
				int startIndex = fixedRandom(randomPoints.size(), RandomActions.GET_START, i, j);
				entry.start = roundLatLonViaString(randomPoints.get(startIndex));
				if (!avoidDupes.contains(entry.start)) {
					break;
				}
			}
			avoidDupes.add(entry.start);

			// 3) select via (inter points) and finish points, restart if no suitable points found
			int nInterpoints = fixedRandom(config.MAX_INTER_POINTS + 1, RandomActions.N_INTER_POINTS, i, 0);
			int nNextPoints = 1 + nInterpoints; // as minimum, the one (finish) point must be added
			int minDistanceKm = config.MIN_DISTANCE_KM / nNextPoints;
			int maxDistanceKm = config.MAX_DISTANCE_KM / nNextPoints;
			LatLon prevPoint = entry.start;

			boolean restart = false;
			while (nNextPoints-- > 0) {
				LatLon point = null;
				boolean pointFound = false;
				for (int j = 0; j < randomPoints.size(); j++) {
					int pointIndex = fixedRandom(randomPoints.size(), RandomActions.GET_POINTS, i, nNextPoints + j);
					point = roundLatLonViaString(randomPoints.get(pointIndex));
					double km = MapUtils.getDistance(prevPoint, point) / 1000;
					if (km >= minDistanceKm && km <= maxDistanceKm && !avoidDupes.contains(point)) {
						pointFound = true;
						break;
					}
				}
				if (!pointFound) {
					restart = true;
					break;
				} else {
					prevPoint = point;
					avoidDupes.add(point);
					if (nNextPoints > 0) {
						entry.via.add(point);
					} else {
						entry.finish = point;
					}
				}
			}

			if (restart) {
				if (replenishCounter++ >= REPLENISH_LIMIT) {
					throw new IllegalStateException(
							"Random routes not generated. Check min/max dist, region size, and OBF connectivity.");
				}
				replenishRandomPoints(randomPoints); // read more points
//				System.err.printf("Read more points i=%d size=%d\n", i, randomPoints.size());
				i--; // retry
				continue;
			}

			// 4) shift points from their exact LatLon
			if (config.MAX_SHIFT_ALL_POINTS_M > 0) {
				class Shifter {
					LatLon shiftLatLon(LatLon ll, int i, int j) {
						int meters = fixedRandom(config.MAX_SHIFT_ALL_POINTS_M, RandomActions.SHIFT_METERS, i, j);
						double shift = meters / 111_000F; // enough approx meters to lat/lon
						double lat = ll.getLatitude() + shift;
						double lon = ll.getLongitude() + shift;
						return roundLatLonViaString(new LatLon(lat, lon));
					}
				}
				int n = 0;
				Shifter shifter = new Shifter();
				entry.start = shifter.shiftLatLon(entry.start, i, n++);
				entry.finish = shifter.shiftLatLon(entry.finish, i, n++);
				for (int j = 0; j < entry.via.size(); j++) {
					entry.via.set(j, shifter.shiftLatLon(entry.via.get(j), i, n++));
				}
			}

			// 5) finally, add TestEntry to the testList
			if (entry.start != null && entry.finish != null) {
				replenishCounter = 0;
				testList.add(entry);
			}
		}
	}
}
