package net.osmand.server.api.searchtest;

import net.osmand.binary.BinaryIndexPart;
import net.osmand.binary.BinaryMapAddressReaderAdapter.AddressRegion;
import net.osmand.binary.BinaryMapAddressReaderAdapter.CityBlocks;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexReaderStats.SearchStat;
import net.osmand.data.Building;
import net.osmand.data.City;
import net.osmand.data.LatLon;
import net.osmand.data.Street;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import org.jetbrains.annotations.NotNull;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * - Builds and keeps a lightweight spatial index of buildings around tiles ("radius" cells) to serve
 *   nearest-building queries quickly during Search Test runs.
 * - Provides comparison flags for a found building against provided fields (house/street/city/unit/postcode)
 *   using normalized string equality for names.
 * - Exposes a per-obf-file singleton via getInstance(reader) to reuse warmed caches safely across calls.
 */
public class BinaryNodeInspector {
    public record BuildingFinding(Building b, String path) {
        @NotNull
        public String toString() {
            return (b != null ? "" : "-: ") + path;
        }
    }
	private record BuildingEntry(Building building, Street street, City city) {	}

	private static class BuildingSpatialIndex {
		private static final double METERS_PER_DEG_LAT = 111_320.0;
		// Approximate meters per degree lon near mid-latitudes to size grid; final filter uses isIn()
		private static final double METERS_PER_DEG_LON_MIDLAT = 78_000.0; // ~cos(45°) * 111.32 km

		private final Map<Long, List<BuildingEntry>> buckets;
		private final double cellLatDeg;
		private final double cellLonDeg;

		private BuildingSpatialIndex(double radiusMeters) {
			this.cellLatDeg = radiusMeters / METERS_PER_DEG_LAT;
			this.cellLonDeg = radiusMeters / METERS_PER_DEG_LON_MIDLAT;
			this.buckets = new HashMap<>();
		}

		private long keyFor(int cx, int cy) {
			return (((long) cx) << 32) ^ (cy & 0xffff_ffffL);
		}

		private int cellX(double lon) { return (int) Math.floor(lon / cellLonDeg); }
		private int cellY(double lat) { return (int) Math.floor(lat / cellLatDeg); }

		public static BuildingSpatialIndex build(BinaryMapIndexReader reader, double radiusMeters, SearchStat stat) {
			BuildingSpatialIndex idx = new BuildingSpatialIndex(radiusMeters);
			try {
				for (BinaryIndexPart p : reader.getIndexes()) {
					if (p instanceof AddressRegion region) {
						for (CityBlocks type : CityBlocks.values()) {
							if (type == CityBlocks.UNKNOWN_TYPE) {
								continue;
							}
							List<City> cities = reader.getCities(null, type, region, stat);
							for (City c : cities) {
								reader.preloadStreets(c, null, stat);
								for (Street s : new ArrayList<>(c.getStreets())) {
									reader.preloadBuildings(s, null, stat);
									List<Building> bs = s.getBuildings();
									if (bs == null || bs.isEmpty()) {
										continue;
									}
									for (Building b : bs) {
										LatLon bl = b.getLocation();
										if (bl == null) { continue; }
										double lat = bl.getLatitude();
										double lon = bl.getLongitude();
										int minCx = idx.cellX(lon - idx.cellLonDeg);
										int maxCx = idx.cellX(lon + idx.cellLonDeg);
										int minCy = idx.cellY(lat - idx.cellLatDeg);
										int maxCy = idx.cellY(lat + idx.cellLatDeg);
										BuildingEntry entry = new BuildingEntry(b, s, c);
										for (int cx = minCx; cx <= maxCx; cx++) {
											for (int cy = minCy; cy <= maxCy; cy++) {
												long k = idx.keyFor(cx, cy);
												idx.buckets.computeIfAbsent(k, kk -> new ArrayList<>()).add(entry);
											}
										}
									}
								}
							}
						}
					}
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			return idx;
		}

		public List<BuildingEntry> getBuildings(LatLon point) {
			if (point == null) {
				return Collections.emptyList();
			}
			double lat = point.getLatitude();
			double lon = point.getLongitude();
			int cx = cellX(lon);
			int cy = cellY(lat);
			List<BuildingEntry> out = new ArrayList<>();
			for (int dx = -1; dx <= 1; dx++) {
				for (int dy = -1; dy <= 1; dy++) {
					long k = keyFor(cx + dx, cy + dy);
					List<BuildingEntry> lst = buckets.get(k);
					if (lst != null) {
						out.addAll(lst);
					}
				}
			}
			return out;
		}
	}

	private static final double RADIUS = 5000.0;
    private final SearchStat stat = new SearchStat();
    private long duration = 0;
    private final BuildingSpatialIndex buildingIndex;
    private static final Map<File, BinaryNodeInspector> CACHE = new ConcurrentHashMap<>();

	private BinaryNodeInspector(BinaryMapIndexReader reader) {
        // Warm up the spatial cache using the provided reader
        this.buildingIndex = BuildingSpatialIndex.build(reader, RADIUS, stat);
    }

	private BinaryNodeInspector(String fileName) {
		File file = new File(fileName);
		if (!file.exists()) {
			throw new RuntimeException("Binary OsmAnd index was not found: " + fileName);
		}
		        // Warm up the spatial cache by opening a temporary reader
        try (RandomAccessFile raf = new RandomAccessFile(file.getAbsolutePath(), "r")) {
            BinaryMapIndexReader index = new BinaryMapIndexReader(raf, file);
            this.buildingIndex = BuildingSpatialIndex.build(index, RADIUS, stat);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

	public static BinaryNodeInspector getInstance(BinaryMapIndexReader reader) {
		if (reader == null || reader.getFile() == null) {
			throw new IllegalArgumentException("BinaryMapIndexReader is null");
		}
		File key = reader.getFile();
		return CACHE.computeIfAbsent(key, k -> new BinaryNodeInspector(reader));
	}

	private List<BuildingEntry> getBuildings(@NotNull LatLon point) {
		BuildingSpatialIndex idx = buildingIndex;
		if (idx == null) {
			return Collections.emptyList();
		}
		List<BuildingEntry> out = new ArrayList<>();
		for (BuildingEntry e : idx.getBuildings(point))
			if (isIn(e.building, point))
				out.add(e);
		return out;
	}

	private static boolean isIn(Building b, LatLon point) {
		double d = MapUtils.getDistance(b.getLocation(), point);
		return d <= RADIUS;
	}

    public BuildingFinding find(LatLon point, Map<String, Object> row) {
        if (row == null) {
            return new BuildingFinding(null, null);
        }
	    String city = firstNonEmpty(row,
			    "addr_city", "addr_town", "addr_village", "addr_hamlet", "addr_municipality", "city");
	    String street = firstNonEmpty(row, "addr_street", "street");
        String houseNumber = firstNonEmpty(row, "addr_housenumber", "addr_house_number", "housenumber", "number");
        String postcode = firstNonEmpty(row, "addr_postcode", "addr_postal_code", "postcode");
        String unit = firstNonEmpty(row, "addr_unit", "addr_flats", "addr_door", "addr_apt", "addr_apartment", "addr_flat", "unit");
        return find(point, city, street, unit, postcode, houseNumber);
    }

    private static String firstNonEmpty(Map<String, Object> map, String... keys) {
        for (String k : keys) {
            Object v = map.get(k);
            if (v == null) {
	            v = map.get(k.toUpperCase());
	            if (v == null)
		            continue;
            }
            String s = v.toString().trim();
            if (!s.isEmpty()) {
                return s;
            }
        }
        return null;
    }

	public BuildingFinding find(@NotNull LatLon point, String city, String street, String unit, String postcode, String houseNumber) {
		long start = System.currentTimeMillis();

		List<BuildingEntry> foundBuildings = getBuildings(point);
		BuildingEntry e = findClosestBuilding(point, foundBuildings);
		duration = System.currentTimeMillis() - start;

		return new BuildingFinding(e == null ? null : e.building, getComparison(e, houseNumber, city, street, unit, postcode));
	}

    private BuildingEntry findClosestBuilding(LatLon point, List<BuildingEntry> matches) {
        if (matches.isEmpty()) {
            return null;
        }

        BuildingEntry best = null;
        double bestDist = Double.MAX_VALUE;
        for (BuildingEntry e : matches) {
            double dist = Double.MAX_VALUE;
            LatLon loc = e.building().getLocation();
            if (loc != null && point != null) {
                dist = MapUtils.getDistance(point.getLatitude(), point.getLongitude(),
                        loc.getLatitude(), loc.getLongitude());
            }

            if (dist < bestDist) {
                best = e;
                bestDist = dist;
            }
        }

        return best;
    }

    private String getComparison(BuildingEntry e, String houseNumber, String cityName, String streetName, String unit, String postcode) {
        boolean isUnit = false, isPost = false, isCity = false, isStreet = false, isHouse = false;
        boolean checkUnit = unit != null && !unit.isEmpty();
        boolean checkPost = postcode != null && !postcode.isEmpty();

        if (checkUnit) {
            Map<String, LatLon> entrances = e.building().getEntrances();
            isUnit = entrances != null && entrances.containsKey(unit);
        }

        if (checkPost) {
            isPost = postcode.equals(e.building().getPostcode());
        }

        // House number exact match
        if (houseNumber != null && !houseNumber.isEmpty()) {
            String hno = e.building().getName();
            isHouse = hno != null && hno.equals(houseNumber);
        }

		// Derive city/street match from building relations if available
		Street st = e.street();
		if (streetName != null && !streetName.isEmpty() && st != null) {
			String normProvided = normalizeToken(streetName);
			String normActual = normalizeStreet(st.getName());
			if (normProvided != null && normActual != null) {
				isStreet = normProvided.equals(normActual);
			}
		}

		if (cityName != null && !cityName.isEmpty() && st != null) {
			City city = st.getCity();
			if (city != null && city.getName() != null) {
				String normProvidedCity = normalizeToken(cityName);
				String normActualCity = normalizeToken(city.getName());
				if (normProvidedCity != null && normActualCity != null) {
					isCity = normProvidedCity.equals(normActualCity);
				}
			}
		}

        return (isHouse ? "+" : "-") + "h," +
		        (isStreet ? "+" : "-") + "s," +
		        (isCity ? "+" : "-") + "c," +
               (isUnit ? "+" : "-") + "u," +
               (isPost ? "+" : "-") + "p";
    }

    private static String normalizeStreet(String s) {
        if (s == null) return null;
        // Remove trailing " (...)" suffix once
        String t = s.replaceFirst("\\s*\\([^)]*\\)$", "");
        return normalizeToken(t);
    }

	private static String normalizeToken(String s) {
		if (s == null)
			return null;
		s = Algorithms.normalizeSearchText(s);
		String t = s.toLowerCase();
		t = t.replaceAll("[^\\p{L}\\p{Nd}]+", ""); // keep letters/digits only
		return t;
	}

    public static void main(String[] args) throws IOException {
        if (args == null || args.length == 0) {
            return;
        }
        BinaryNodeInspector in = new BinaryNodeInspector(args[0]);
	    BuildingFinding b = in.find(new LatLon(0, 0), "Altoona", "Pleasant Valley Boulevard", "#1", "16602", "3119");

		System.out.println(b);
	    System.out.println(in.duration);
	    System.out.println(in.stat);
    }

}
