package net.osmand.obf;


import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;
import gnu.trove.set.hash.TLongHashSet;
import net.osmand.IndexConstants;
import net.osmand.PlatformUtil;
import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryIndexPart;
import net.osmand.binary.BinaryMapAddressReaderAdapter.AddressRegion;
import net.osmand.binary.BinaryMapAddressReaderAdapter.CityBlocks;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapPoiReaderAdapter.PoiRegion;
import net.osmand.binary.OsmandOdb;
import net.osmand.data.*;
import net.osmand.obf.preparation.*;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.edit.Node;
import net.osmand.util.Algorithms;
import net.osmand.util.CountryOcbfGeneration;
import net.osmand.util.CountryOcbfGeneration.CountryRegion;
import net.osmand.util.IndexUploader;
import net.osmand.util.MapUtils;
import org.apache.commons.logging.Log;
import org.xmlpull.v1.XmlPullParserException;
import rtree.RTreeException;

import java.io.*;
import java.sql.SQLException;
import java.text.Collator;
import java.text.MessageFormat;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static net.osmand.obf.preparation.IndexCreator.REMOVE_POI_DB;

public class BinaryMerger {

	public static final int BUFFER_SIZE = 1 << 20;
	private final static Log log = PlatformUtil.getLog(BinaryMerger.class);
	public static final String helpMessage = "output_file.obf [--address] [--poi] [input_file.obf] ...: merges all obf files and merges poi & address structure into 1";
	private static final Map<String, Integer> COMBINE_ARGS = new HashMap<String, Integer>();
	private BinaryMapIndexReader.OsmAndOwner osmAndOwner;

	static {
		COMBINE_ARGS.put("--address", OsmandOdb.OsmAndStructure.ADDRESSINDEX_FIELD_NUMBER);
		COMBINE_ARGS.put("--poi", OsmandOdb.OsmAndStructure.POIINDEX_FIELD_NUMBER);
		COMBINE_ARGS.put("--hhindex", OsmandOdb.OsmAndStructure.HHROUTINGINDEX_FIELD_NUMBER);
		COMBINE_ARGS.put("--owner", OsmandOdb.OsmAndStructure.OWNER_FIELD_NUMBER);
	}

	public static void main(String[] args) throws IOException, SQLException {
		BinaryMerger in = new BinaryMerger();
		// test cases show info
		if (args.length == 1 && "test".equals(args[0])) {
			in.merger(new String[]{
					"/Users/macmini/OsmAnd/maps/Merged.obf",
					"/Users/macmini/OsmAnd/maps/Ukraine_khmelnytskyy_europe_2.obf",
					"/Users/macmini/OsmAnd/maps/Ukraine_vinnytsya_europe_2.obf",
					"/Users/macmini/OsmAnd/maps/Ukraine_zhytomyr_europe_2.obf"
			});
		} else {
			in.merger(args);
		}
	}

	public static void signObfFile(String[] args) throws IOException, SQLException {
		String pathToObf = "";
		String name = "";
		String pluginid = "";
		String description = "";
		String resource = "";
		String usage = "Usage: <path to obf> name=\"Owner name\" resource=\"Link to resource\"(optional) pluginid=\"Plugin name\"(optional) description>\"Description (any text)\"(optional)";
		if(args.length == 1 && args[0].equals("test")) {
			pathToObf = "/Users/macmini/OsmAnd/maps/Ukraine_vinnytsya_europe.obf";
			name = "owner=John";
			pluginid = "pluginid=Offroad tracks";
			description = "description=Offroad tracks of Middle-earth";
			resource = "https://osmand.net OsmAnd";
		} else {
			if (args.length < 2) {
				System.out.println(usage);
				System.exit(1);
				return;
			}
			pathToObf = args[0];
			for(int i = 1; i < args.length; i++) {
				String arg = args[i];
				if (arg.startsWith("name=")) {
					name = arg.replace("name=", "");
					continue;
				}
				if (arg.startsWith("pluginid=")) {
					pluginid = arg.replace("pluginid=", "");
					continue;
				}
				if (arg.startsWith("description=")) {
					description = arg.replace("description=", "");
					continue;
				}
				if (arg.startsWith("resource=")) {
					resource = arg.replace("resource=", "");
					continue;
				}
			}
		}
		if (pathToObf.isEmpty() || name.isEmpty()) {
			System.out.println(usage);
			System.exit(1);
			return;
		}

		if (!pathToObf.endsWith(".obf") && !pathToObf.endsWith(".obf.zip")) {
			System.out.println("Supported file formats are: *.obf, *.obf.zip");
			System.exit(1);
			return;
		}

		BinaryMerger in = new BinaryMerger();
		in.addOsmAndOwner(pathToObf, name, resource, description, pluginid);
	}

	public void addOsmAndOwner(String pathToFile, String name, String resource, String description, String pluginid) throws IOException, SQLException {
		String signed = "";
		boolean zip = false;
		if (pathToFile.endsWith(".obf.zip")) {
			signed = pathToFile.replace(".obf.zip", "_signed.obf");
			zip = true;
		} else {
			signed = pathToFile.replace(".obf", "_signed.obf");
		}
		String[] args = new String[]{
				signed,
				pathToFile
		};
		BinaryMerger in = new BinaryMerger();
		in.osmAndOwner = new BinaryMapIndexReader.OsmAndOwner(name, resource, pluginid, description);
		in.merger(args);

		File sFile = new File(signed);
		if (zip) {
			try {
				File zFile = new File(pathToFile);
				zFile.delete();
				sFile.renameTo(new File(pathToFile.replace(".obf.zip", ".obf")));
				IndexUploader.zip(sFile, zFile, "", System.currentTimeMillis());
				sFile.delete();
			} catch (IndexUploader.OneFileException e) {
				e.printStackTrace();
			}
		} else {
			File oFile = new File(pathToFile);
			oFile.delete();
			sFile.renameTo(oFile);
		}
		System.out.println("Sign added to file:" + pathToFile + " . Sign name=" + name + ", resource=" + resource + ", pluginid=" + pluginid + ", description=" + description);
	}

	public static void mergeStandardFiles(String[] args) throws IOException, SQLException, XmlPullParserException, RTreeException {
		BinaryMerger in = new BinaryMerger();
		String pathWithGeneratedMapZips = args[0];
		String pathToPutJointFiles = args[1];
		boolean mapFiles = false;
		boolean roadFiles = false;
		boolean skipExisting = false;
		boolean ignoreFailures = true;
		String filter = null;
		for (String arg : args) {
			String val = null;
			String[] s = arg.split("=");
			String key = s[0];
			if (s.length > 1) {
				val = s[1].trim();
			}
			if (key.equals("--map")) {
				mapFiles = true;
				roadFiles = false;
			}
			if (key.equals("--road")) {
				roadFiles = true;
				mapFiles = false;
			} else if (key.equals("--filter")) {
				filter = val;
			} else if (key.equals("--skip-existing")) {
				skipExisting = true;
			} else if (key.equals("--fail-fast")) {
				ignoreFailures = false;
			}
		}
		List<String> failedCountries = new ArrayList<String>();
		CountryRegion world = new CountryOcbfGeneration().parseDefaultOsmAndRegionStructure();
		Iterator<CountryRegion> it = world.iterator();
		while (it.hasNext()) {
			CountryRegion cr = it.next();
			String roadExt = "_" + IndexConstants.BINARY_MAP_VERSION + IndexConstants.BINARY_ROAD_MAP_INDEX_EXT;
			String mapExt = "_" + IndexConstants.BINARY_MAP_VERSION + IndexConstants.BINARY_MAP_INDEX_EXT;
			if ((cr.jointMap && mapFiles) || (cr.jointRoads && roadFiles && !cr.jointMap)) {
				boolean road = cr.jointRoads && !cr.jointMap;
				if (!Algorithms.isEmpty(filter)
						&& !cr.getDownloadName().toLowerCase().startsWith(filter.toLowerCase())) {
					continue;
				}
				List<CountryRegion> list = cr.getChildren();
				List<String> sargs = new ArrayList<String>();
				String targetFileName = Algorithms.capitalizeFirstLetterAndLowercase(cr.getDownloadName()) + (road ? roadExt : mapExt);
				File targetFile = new File(pathToPutJointFiles, targetFileName);
				File targetUploadedFile = new File(pathWithGeneratedMapZips, targetFileName + ".zip");
				System.out.println("Checking " + targetFileName);
				if (skipExisting && targetFile.exists()) {
					continue;
				}
				if (skipExisting && targetUploadedFile.exists()) {
					continue;
				}
				sargs.add(targetFileName);
				sargs.add("--address");
				sargs.add("--poi");
				sargs.add("--hhindex");
				for (CountryRegion reg : list) {
					if (!reg.map) {
						continue;
					}
					File fl = getExistingFile(pathWithGeneratedMapZips, reg, road ? roadExt : mapExt);
					if (!fl.exists() && road) {
						fl = getExistingFile(pathWithGeneratedMapZips, reg, mapExt);
						if (fl.exists()) {
							File target = new File(fl.getParentFile(), fl.getName() + ".roadtmp");
							IndexUploader.extractRoadOnlyFile(fl, target);
							fl = target;
						}
					}
					sargs.add(fl.getAbsolutePath());
				}
				log.info("Merge file with arguments: " + sargs);
				try {
					in.merger(sargs.toArray(new String[sargs.size()]));
					File genFile = new File(targetFileName);
					boolean moved = genFile.renameTo(targetFile);
					if (!moved) {
						Algorithms.fileCopy(genFile, targetFile);
						genFile.delete();
					}
				} catch (IOException | SQLException e) {
					if (!ignoreFailures) {
						throw e;
					}
					log.error(e.getMessage(), e);
					failedCountries.add(cr.getDownloadName());
				}
			}
		}
		if (!failedCountries.isEmpty()) {
			String msg = "Failed generation for such countries: " + failedCountries;
			log.error(msg);
			throw new RuntimeException(msg);
		}
	}

	private static File getExistingFile(String pathWithZips, CountryRegion reg, String mapExt) {
		File fl = new File(pathWithZips, Algorithms.capitalizeFirstLetterAndLowercase(reg.getDownloadName()) + mapExt);
		if (!fl.exists()) {
			fl = new File(fl.getParentFile(), fl.getName() + ".zip");
		}
		return fl;
	}

	public static final void writeInt(CodedOutputStream ous, long v) throws IOException {
		BinaryInspector.writeInt(ous, v);
	}

	private void mergeCitiesByNameDistance(List<City> orderedCities, Map<City, List<City>> mergeGroup,
			Map<City, BinaryMapIndexReader> cityMap, boolean rename) {
		for (int i = 0; i < orderedCities.size() - 1; i++) {
			int j = i;
			City oc = orderedCities.get(i);
			City nc = orderedCities.get(j);
			BinaryMapIndexReader ocIndexReader = cityMap.get(oc);
			List<City> uniqueNamesakes = new ArrayList<City>();
			boolean renameGroup = false;
			while (MapObject.BY_NAME_COMPARATOR.areEqual(nc, oc)) {
				boolean isUniqueCity = true;
				for (ListIterator<City> uci = uniqueNamesakes.listIterator(); uci.hasNext(); ) {
					City uc = uci.next();
					if (isSameCity(uc, nc)) {
						// Prefer cities with shortest names ("1101DL" instead of "1101 DL")
						boolean shorter = nc.getName().length() < uc.getName().length();
						if (shorter) {
							mergeGroup.put(nc, mergeGroup.remove(uc));
							uniqueNamesakes.remove(uc);
							uniqueNamesakes.add(nc);
							City tmp = uc;
							uc = nc;
							nc = tmp;
						}
						orderedCities.remove(nc);
						mergeGroup.get(uc).add(nc);
						isUniqueCity = false;
						break;
					}
				}
				if (isUniqueCity) {
					uniqueNamesakes.add(nc);
					mergeGroup.put(nc, new ArrayList<City>());
					j++;
				}
				boolean areCitiesInSameRegion = ocIndexReader == cityMap.get(nc);
				renameGroup = renameGroup || (rename && !areCitiesInSameRegion && uniqueNamesakes.size() > 1);
				nc = (j < orderedCities.size()) ? orderedCities.get(j) : null;
			}
			if (uniqueNamesakes.size() == 1 && mergeGroup.get(uniqueNamesakes.get(0)).size() == 0) {
				mergeGroup.remove(uniqueNamesakes.get(0));
			} else {
				if (renameGroup) {
					for (City uc : uniqueNamesakes) {
						for (City c : mergeGroup.get(uc)) {
							addRegionToCityName(c, cityMap.get(c));
						}
						addRegionToCityName(uc, cityMap.get(uc));
					}
				}
			}
		}
	}

	private List<String> extractCountryAndRegionNames(BinaryMapIndexReader index) {
		List<String> name = index.getRegionNames();
		if (name.size() > 0) {
			return Arrays.asList(name.get(0).split("_"));
		}
		return Collections.emptyList();
	}

	private String extractCountryName(BinaryMapIndexReader index) {
		List<String> lst = extractCountryAndRegionNames(index);
		if (lst.size() > 0) {
			return lst.get(0);
		}
		return null;
	}

	private String extractRegionName(BinaryMapIndexReader index) {
		List<String> names = extractCountryAndRegionNames(index);
		if (names.size() >= 2) {
			String region = names.get(1);
			region = region.substring(0, 1).toUpperCase() + region.substring(1);
			return region;
		} else {
			return null;
		}
	}

	private void addRegionToCityName(City city, BinaryMapIndexReader index) {
		String region = extractRegionName(index);
		city.setName((region == null) ? city.getName() : city.getName() + " (" + region + ")");
	}

	private static boolean isSameCity(City namesake0, City namesake1) {
		double sameCityDistance = namesake0.isPostcode() ? 50000 : 5000;
		return MapUtils.getDistance(namesake0.getLocation(), namesake1.getLocation()) < sameCityDistance;
	}

	private static City mergeCities(City city, City namesake, Map<City, Map<Street, List<Node>>> namesakesStreetNodes) {
		Map<Street, Street> smap = city.mergeWith(namesake);
		Map<Street, List<Node>> wayNodes = namesakesStreetNodes.get(city);
		Map<Street, List<Node>> owayNodes = namesakesStreetNodes.get(namesake);
		for(Street o : smap.keySet()) {
			List<Node> nodes = owayNodes.get(o);
			wayNodes.put(smap.get(o), nodes);
		}
		return city;
	}

	private void normalizePostcode(City city, String country) {
		String normalizedPostcode;
		if (city.isPostcode()) {
			normalizedPostcode = Postcode.normalize(city.getName(), country);
			city.setName(normalizedPostcode);
			city.setEnName(normalizedPostcode);
		}
		if (city.getPostcode() != null) {
			normalizedPostcode = Postcode.normalize(city.getPostcode(), country);
			city.setPostcode(normalizedPostcode);
		}
	}

	private void preloadStreetsAndBuildings(BinaryMapIndexReader rindex, City city,
			Map<City, Map<Street, List<Node>>> namesakesStreetNodes) throws IOException {
		rindex.preloadStreets(city, null, null);
		Map<Street, List<Node>> streetNodes = new LinkedHashMap<Street, List<Node>>();
		Map<String, List<Node>> streetNodesN = new LinkedHashMap<String, List<Node>>();
		for (Street street : city.getStreets()) {
			rindex.preloadBuildings(street, null, null);
			ArrayList<Node> nns = new ArrayList<Node>();
			for (Street is : street.getIntersectedStreets()) {
				List<Node> list = streetNodesN.get(is.getName());
				Node nn = null;
				if (list != null) {
					boolean nameEqual = false;
					double dd = 100000;
					for (Node n : list) {
						double d = MapUtils.getDistance(n.getLatLon(), is.getLocation());
						if (Algorithms.objectEquals(street.getName(), n.getTag("name")) &&
								(d < dd || !nameEqual)) {
							nn = n;
							dd = d;
							nameEqual = true;
						} else if (!nameEqual && d < dd) {
							nn = n;
							dd = d;
						}
					}
				}
				if (nn == null) {
					int ty = (int) MapUtils.getTileNumberY(24, is.getLocation().getLatitude());
					int tx = (int) MapUtils.getTileNumberY(24, is.getLocation().getLongitude());
					long id = (((long) tx << 32)) | ty;
					nn = new Node(is.getLocation().getLatitude(), is.getLocation().getLongitude(), id);
					nn.putTag("name", is.getName());
				}
				nns.add(nn);
			}
			streetNodes.put(street, nns);
			if (streetNodesN.containsKey(street.getName())) {
				streetNodesN.get(street.getName()).addAll(nns);
			} else {
				streetNodesN.put(street.getName(), nns);
			}
		}
		namesakesStreetNodes.put(city, streetNodes);
	}

	private void combineAddressIndex(String name, BinaryMapIndexWriter writer, AddressRegion[] addressRegions, BinaryMapIndexReader[] indexes)
			throws IOException {
		IndexCreatorSettings settings = new IndexCreatorSettings();
		Set<String> attributeTagsTableSet = new TreeSet<String>();
		for (int i = 0; i < addressRegions.length; i++) {
			AddressRegion region = addressRegions[i];
			if (region == null) {
				continue;
			}
			attributeTagsTableSet.addAll(region.getAttributeTagsTable());
		}
		writer.startWriteAddressIndex(name, attributeTagsTableSet);
		List<String> attributeTagsTable = new ArrayList<String>();
		attributeTagsTable.addAll(attributeTagsTableSet);
		Map<String, Integer> tagRules = new HashMap<String, Integer>();
		Map<String, List<MapObject>> namesIndex = new TreeMap<String, List<MapObject>>(Collator.getInstance());
		ListIterator<String> it = attributeTagsTable.listIterator();
		while (it.hasNext()) {
			tagRules.put(it.next(), it.previousIndex());
		}
		for (CityBlocks cityBlockType : CityBlocks.values()) {
			if (!cityBlockType.cityGroupType && cityBlockType != CityBlocks.BOUNDARY_TYPE) {
				continue;
			}
			Map<City, BinaryMapIndexReader> cityMap = new HashMap<City, BinaryMapIndexReader>();
			Map<Long, City> cityIds = new HashMap<Long, City>();
			for (int i = 0; i < addressRegions.length; i++) {
				AddressRegion region = addressRegions[i];
				if (region == null) {
					continue;
				}
				final BinaryMapIndexReader index = indexes[i];
				for (City city : index.getCities(null, cityBlockType, region, null)) {
					normalizePostcode(city, extractCountryName(index));
					// weird code cause city ids can overlap
					// probably code to merge cities below is not needed (it called mostly for postcodes)
					if(cityIds.containsKey(city.getId())) {
						index.preloadStreets(city, null, null);
						City city2 = cityIds.get(city.getId());
						cityMap.get(city2).preloadStreets(city2, null, null);
						if(city.getStreets().size() > city2.getStreets().size()) {
							cityMap.remove(city2);
							cityIds.put(city.getId(), city);
							cityMap.put(city, index);
						}
					} else {
						cityMap.put(city, index);
						cityIds.put(city.getId(), city);
					}
				}
			}
			List<City> cities = new ArrayList<City>(cityMap.keySet());
			Map<City, List<City>> mergeCityGroup = new HashMap<City, List<City>>();
			Collections.sort(cities, MapObject.BY_NAME_COMPARATOR);
			mergeCitiesByNameDistance(cities, mergeCityGroup, cityMap, cityBlockType == CityBlocks.CITY_TOWN_TYPE);
			List<BinaryFileReference> refs = new ArrayList<BinaryFileReference>();
			// 1. write cities
			writer.startCityBlockIndex(cityBlockType.index);
			Map<City, Map<Street, List<Node>>> namesakesStreetNodes = new HashMap<City, Map<Street, List<Node>>>();
			for (int i = 0; i < cities.size(); i++) {
				City city = cities.get(i);
				BinaryMapIndexReader rindex = cityMap.get(city);
				preloadStreetsAndBuildings(rindex, city, namesakesStreetNodes);
				List<City> namesakes = mergeCityGroup.get(city);
				if (namesakes != null) {
					for (City namesake : namesakes) {
						preloadStreetsAndBuildings(cityMap.get(namesake), namesake, namesakesStreetNodes);
						city = mergeCities(city, namesake, namesakesStreetNodes);
					}
				}
				BinaryFileReference ref = writer.writeCityHeader(city, city.getType().ordinal(), tagRules);
				refs.add(ref);
				writer.writeCityIndex(city, city.getStreets(), namesakesStreetNodes.get(city), ref, tagRules);
				IndexAddressCreator.putNamedMapObject(namesIndex, city, ref.getStartPointer(), settings);
				if (!city.isPostcode()) {
					for (Street s : city.getStreets()) {
						IndexAddressCreator.putNamedMapObject(namesIndex, s, s.getFileOffset(), settings);
					}
				}

				city.getStreets().clear();
				namesakesStreetNodes.clear();
			}

			writer.endCityBlockIndex();
		}
		writer.writeAddressNameIndex(namesIndex);
		writer.endWriteAddressIndex();
	}

	public static long latlon(Amenity amenity) {
		LatLon loc = amenity.getLocation();
		return ((long) MapUtils.get31TileNumberX(loc.getLongitude()) << 31 | (long) MapUtils.get31TileNumberY(loc.getLatitude()));
	}

	private void combinePoiIndex(String name, BinaryMapIndexWriter writer, long dateCreated, PoiRegion[] poiRegions, BinaryMapIndexReader[] indexes)
			throws IOException, SQLException {
		final int[] writtenPoiCount = {0};
		MapRenderingTypesEncoder renderingTypes = new MapRenderingTypesEncoder(null, name);
		IndexCreatorSettings settings = new IndexCreatorSettings();
		settings.indexPOI = true;

		final IndexPoiCreator indexPoiCreator = new IndexPoiCreator(settings, renderingTypes);
		indexPoiCreator.createDatabaseStructure(new File(new File(System.getProperty("user.dir")), IndexCreator.getPoiFileName(name)));
		final Map<Long, List<Amenity>> amenityRelations = new HashMap<Long, List<Amenity>>();
		final TLongHashSet set = new TLongHashSet();
		final long[] generatedRelationId = {-1};
		for (int i = 0; i < poiRegions.length; i++) {
			BinaryMapIndexReader index = indexes[i];
			final TLongHashSet file = new TLongHashSet();
			log.info("Region: " + extractRegionName(index));
			index.searchPoi(BinaryMapIndexReader.buildSearchPoiRequest(
					0, Integer.MAX_VALUE, 0, Integer.MAX_VALUE, -1,
					BinaryMapIndexReader.ACCEPT_ALL_POI_TYPE_FILTER,
					new ResultMatcher<Amenity>() {
						@Override
						public boolean publish(Amenity amenity) {
							try {
								boolean isRelation = amenity.getId() < 0;
								if (isRelation) {
									long j = latlon(amenity);
									List<Amenity> list;
									if (!amenityRelations.containsKey(j)) {
										list = new ArrayList<Amenity>(1);
										amenityRelations.put(j, list);
									} else {
										list = amenityRelations.get(j);
									}
									boolean unique = true;
									for (Amenity a : list) {
										if (a.getType() == amenity.getType() &&
												Algorithms.objectEquals(a.getSubType(), amenity.getSubType())) {
											unique = false;
											break;
										}
									}
									if (unique) {
										amenity.setId(generatedRelationId[0]--);
										amenityRelations.get(j).add(amenity);
                                        indexPoiCreator.resetOrder(amenity);
										indexPoiCreator.insertAmenityIntoPoi(amenity);
										writtenPoiCount[0]++;
									}
								} else {
									if (!set.contains(amenity.getId())) {
										file.add(amenity.getId());
                                        indexPoiCreator.resetOrder(amenity);
										indexPoiCreator.insertAmenityIntoPoi(amenity);
										writtenPoiCount[0]++;
									}
								}
								return false;
							} catch (SQLException e) {
								throw new RuntimeException(e);
							}

						}

						@Override
						public boolean isCancelled() {
							return false;
						}
					}));
			set.addAll(file);
		}
		indexPoiCreator.writeBinaryPoiIndex(null, writer, name, null);
		indexPoiCreator.commitAndClosePoiFile(dateCreated);
//		REMOVE_POI_DB = false;
		if (REMOVE_POI_DB) {
			indexPoiCreator.removePoiFile();
		}
		log.info("Written " + writtenPoiCount[0] + " POI.");
	}

	public static void copyBinaryPart(CodedOutputStream ous, byte[] BUFFER, RandomAccessFile raf, long fp, long length)
			throws IOException {
		BinaryInspector.copyBinaryPart(ous, BUFFER, raf, fp, length);
	}

	public void combineParts(File fileToExtract, List<File> files, List<BinaryMapIndexReader> readers, Set<Integer> combineParts) throws IOException, SQLException {
		boolean combineFiles = files != null;
		BinaryMapIndexReader[] indexes =  combineFiles ? new BinaryMapIndexReader[files.size()] : readers.toArray(new BinaryMapIndexReader[readers.size()]);
		RandomAccessFile[] rafs = combineFiles ? new RandomAccessFile[files.size()] : null;
		long dateCreated = 0;
		int version = -1;
		// Go through all files and validate consistency
		int c = 0;
		if (combineFiles) {
			for (File f : files) {
				if (f.getAbsolutePath().equals(fileToExtract.getAbsolutePath())) {
					System.err.println("Error : Input file is equal to output file " + f.getAbsolutePath());
					return;
				}
				rafs[c] = new RandomAccessFile(f.getAbsolutePath(), "r");
				indexes[c] = new BinaryMapIndexReader(rafs[c], f);
				dateCreated = Math.max(dateCreated, indexes[c].getDateCreated());
				if (version == -1) {
					version = indexes[c].getVersion();
				} else {
					if (indexes[c].getVersion() != version) {
						System.err.println("Error : Different input files has different input versions " + indexes[c].getVersion() + " != " + version);
						return;
					}
				}
				c++;
			}
		} else {
			for (BinaryMapIndexReader index : indexes) {
				dateCreated = Math.max(dateCreated, index.getDateCreated());
				if (version == -1) {
					version = index.getVersion();
				} else {
					if (index.getVersion() != version) {
						System.err.println("Error : Different input files has different input versions " + index.getVersion() + " != " + version);
						return;
					}
				}
			}
		}

		// write files
		RandomAccessFile rafToExtract = new RandomAccessFile(fileToExtract, "rw");
		BinaryMapIndexWriter writer = new BinaryMapIndexWriter(rafToExtract, dateCreated);
		CodedOutputStream ous = writer.getCodedOutStream();
		byte[] BUFFER_TO_READ = new byte[BUFFER_SIZE];
		AddressRegion[] addressRegions = new AddressRegion[combineFiles ? files.size() : readers.size()];
		PoiRegion[] poiRegions = new PoiRegion[combineFiles ? files.size() : readers.size()];
		for (int k = 0; k < indexes.length; k++) {
			BinaryMapIndexReader index = indexes[k];
			RandomAccessFile raf = rafs != null ? rafs[k] : null;
			for (int i = 0; i < index.getIndexes().size(); i++) {
				BinaryIndexPart part = index.getIndexes().get(i);
				if (combineParts.contains(part.getFieldNumber())) {
					if (part.getFieldNumber() == OsmandOdb.OsmAndStructure.ADDRESSINDEX_FIELD_NUMBER) {
						addressRegions[k] = (AddressRegion) part;
					} else if (part.getFieldNumber() == OsmandOdb.OsmAndStructure.POIINDEX_FIELD_NUMBER) {
						poiRegions[k] = (PoiRegion) part;
					} else if (part.getFieldNumber() == OsmandOdb.OsmAndStructure.HHROUTINGINDEX_FIELD_NUMBER) {
						// ignore as we don't know how to merge
					}
				} else if (raf != null) {
					ous.writeTag(part.getFieldNumber(), WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
					writeInt(ous, part.getLength());
					copyBinaryPart(ous, BUFFER_TO_READ, raf, part.getFilePointer(), part.getLength());
					System.out.println(MessageFormat.format("{2} part {0} is extracted {1} bytes",
							new Object[]{part.getName(), part.getLength(), part.getPartName()}));
				}
			}
		}
		String nm = fileToExtract.getName();
		int i = nm.indexOf('_');
		if (i > 0) {
			nm = nm.substring(0, i);
		}
		if (combineParts.contains(OsmandOdb.OsmAndStructure.ADDRESSINDEX_FIELD_NUMBER)) {
			combineAddressIndex(nm, writer, addressRegions, indexes);
		}
		if (combineParts.contains(OsmandOdb.OsmAndStructure.POIINDEX_FIELD_NUMBER)) {
			combinePoiIndex(nm, writer, dateCreated, poiRegions, indexes);
		}
		if (combineParts.contains(OsmandOdb.OsmAndStructure.OWNER_FIELD_NUMBER) && osmAndOwner != null) {
			writer.writeOsmAndOwner(osmAndOwner);
		}
		ous.writeInt32(OsmandOdb.OsmAndStructure.VERSIONCONFIRM_FIELD_NUMBER, version);
		ous.flush();
	}

	public void merger(String[] args) throws IOException, SQLException {
		if (args == null || args.length == 0) {
			System.out.println(helpMessage);
			return;
		}
		File outputFile = null;
		List<File> parts = new ArrayList<File>();
		List<File> toDelete = new ArrayList<File>();
		Set<Integer> combineParts = new HashSet<Integer>();
		for (int i = 0; i < args.length; i++) {
			if (args[i].startsWith("--")) {
				combineParts.add(COMBINE_ARGS.get(args[i]));
			} else if (outputFile == null) {
				outputFile = new File(args[i]);
			} else {
				File file = new File(args[i]);
				if (file.getName().endsWith(".zip")) {
					File tmp = File.createTempFile(file.getName(), "obf");
					ZipInputStream zis = new ZipInputStream(new FileInputStream(file));
					ZipEntry ze;
					while ((ze = zis.getNextEntry()) != null) {
						String name = ze.getName();
						if (!ze.isDirectory() && name.endsWith(".obf")) {
							FileOutputStream fout = new FileOutputStream(tmp);
							Algorithms.streamCopy(zis, fout);
							fout.close();
							parts.add(tmp);
						}
					}
					zis.close();
					tmp.deleteOnExit();
					toDelete.add(tmp);

				} else {
					parts.add(file);
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
		combineParts(outputFile, parts, null, combineParts);
		for (File f : toDelete) {
			if (!f.delete()) {
				throw new IOException("Cannot delete file " + outputFile);
			}
		}
	}

}
