package net.osmand.obf;


import static net.osmand.obf.preparation.IndexCreator.REMOVE_POI_DB;
import gnu.trove.set.hash.TLongHashSet;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.SQLException;
import java.text.Collator;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import net.osmand.PlatformUtil;
import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryIndexPart;
import net.osmand.binary.BinaryMapAddressReaderAdapter;
import net.osmand.binary.BinaryMapAddressReaderAdapter.AddressRegion;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapPoiReaderAdapter.PoiRegion;
import net.osmand.binary.OsmandOdb;
import net.osmand.data.Amenity;
import net.osmand.data.City;
import net.osmand.data.LatLon;
import net.osmand.data.MapObject;
import net.osmand.data.Postcode;
import net.osmand.data.Street;
import net.osmand.obf.preparation.BinaryFileReference;
import net.osmand.obf.preparation.BinaryMapIndexWriter;
import net.osmand.obf.preparation.IndexAddressCreator;
import net.osmand.obf.preparation.IndexCreator;
import net.osmand.obf.preparation.IndexCreatorSettings;
import net.osmand.obf.preparation.IndexPoiCreator;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.util.Algorithms;
import net.osmand.util.CountryOcbfGeneration;
import net.osmand.util.MapUtils;
import net.osmand.util.CountryOcbfGeneration.CountryRegion;

import org.apache.commons.logging.Log;
import org.xmlpull.v1.XmlPullParserException;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;

public class BinaryMerger {

	public static final int BUFFER_SIZE = 1 << 20;
	private final static Log log = PlatformUtil.getLog(BinaryMerger.class);
	public static final String helpMessage = "output_file.obf [--address] [--poi] [input_file.obf] ...: merges all obf files and merges poi & address structure into 1";
	private static final Map<String, Integer> COMBINE_ARGS = new HashMap<String, Integer>();

	static {
		COMBINE_ARGS.put("--address", OsmandOdb.OsmAndStructure.ADDRESSINDEX_FIELD_NUMBER);
		COMBINE_ARGS.put("--poi", OsmandOdb.OsmAndStructure.POIINDEX_FIELD_NUMBER);
	}

	public static void main(String[] args) throws IOException, SQLException {
		BinaryMerger in = new BinaryMerger();
		// test cases show info
		if (args.length == 1 && "test".equals(args[0])) {
			in.merger(new String[]{
					System.getProperty("maps.dir") + "Switzerland_europe_merge.obf",
					System.getProperty("maps.dir") + "Switzerland_basel_europe_2.obf_",
					System.getProperty("maps.dir") + "Switzerland_bern_europe_2.obf_",
					System.getProperty("maps.dir") + "Switzerland_central_europe_2.obf_",
					System.getProperty("maps.dir") + "Switzerland_eastern_europe_2.obf_",
					System.getProperty("maps.dir") + "Switzerland_lake-geneva_europe_2.obf_",
					System.getProperty("maps.dir") + "Switzerland_ticino_europe_2.obf_",
					System.getProperty("maps.dir") + "Switzerland_zurich_europe_2.obf_",
			});
		} else {
			in.merger(args);
		}
	}
	
	public static void mergeStandardFiles(String[] args) throws IOException, SQLException, XmlPullParserException {
		BinaryMerger in = new BinaryMerger();
		String pathWithGeneratedMapZips = args[0];
		String pathToPutJointFiles = args[1];
		boolean mapFiles = args.length > 2 && args[2].equals("--map");
		String filter = args.length > 3? args[3] : null;
		CountryRegion world = new CountryOcbfGeneration().parseDefaultOsmAndRegionStructure();
		Iterator<CountryRegion> it = world.iterator();
		while(it.hasNext()) {
			CountryRegion cr = it.next();
			if((cr.jointMap && mapFiles) || (cr.jointRoads && !mapFiles)) {
				if(!Algorithms.isEmpty(filter) && !cr.getDownloadName().toLowerCase().startsWith(filter.toLowerCase())) {
					continue;
				}
				List<CountryRegion> list = cr.getChildren();
				List<String> sargs = new ArrayList<String>();
				String ext = "_2" + (mapFiles ? ".obf" : ".road.obf");
				String targetFileName = Algorithms.capitalizeFirstLetterAndLowercase(cr.getDownloadName()) + ext;
				sargs.add(targetFileName);
				sargs.add("--address");
				sargs.add("--poi");
				for (CountryRegion reg : list) {
					if(reg.map || (!mapFiles && reg.roads)) {
						sargs.add(pathWithGeneratedMapZips + Algorithms.capitalizeFirstLetterAndLowercase(reg.getDownloadName()) + ext + ".zip");
					}
				}
				log.info("Merge file with arguments: " + sargs);
				in.merger(sargs.toArray(new String[sargs.size()]));
				new File(targetFileName).renameTo(new File(pathToPutJointFiles, targetFileName));
			}
		}
	}

	public static final void writeInt(CodedOutputStream ous, int v) throws IOException {
		ous.writeRawByte((v >>> 24) & 0xFF);
		ous.writeRawByte((v >>> 16) & 0xFF);
		ous.writeRawByte((v >>>  8) & 0xFF);
		ous.writeRawByte(v & 0xFF);
		//written += 4;
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
		return new ArrayList<String>(Arrays.asList(index.getRegionNames().get(0).split("_")));
	}

	private String extractCountryName(BinaryMapIndexReader index) {
		return extractCountryAndRegionNames(index).get(0);
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
		rindex.preloadStreets(city, null);
		Map<Street, List<Node>> streetNodes = new LinkedHashMap<Street, List<Node>>();
		Map<String, List<Node>> streetNodesN = new LinkedHashMap<String, List<Node>>();
		for (Street street : city.getStreets()) {
			rindex.preloadBuildings(street, null);
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
		Set<String> attributeTagsTableSet = new TreeSet<String>();
		for (int i = 0; i != addressRegions.length; i++) {
			AddressRegion region = addressRegions[i];
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
		for (int type : BinaryMapAddressReaderAdapter.CITY_TYPES) {
			Map<City, BinaryMapIndexReader> cityMap = new HashMap<City, BinaryMapIndexReader>();
			Map<Long, City> cityIds = new HashMap<Long, City>();
			for (int i = 0; i < addressRegions.length; i++) {
				AddressRegion region = addressRegions[i];
				final BinaryMapIndexReader index = indexes[i];
				for (City city : index.getCities(region, null, type)) {
					normalizePostcode(city, extractCountryName(index));
					// weird code cause city ids can overlap
					// probably code to merge cities below is not needed (it called mostly for postcodes)
					if(cityIds.containsKey(city.getId())) {
						index.preloadStreets(city, null);
						City city2 = cityIds.get(city.getId());
						cityMap.get(city2).preloadStreets(city2, null);
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
			mergeCitiesByNameDistance(cities, mergeCityGroup, cityMap, type == BinaryMapAddressReaderAdapter.CITY_TOWN_TYPE);
			List<BinaryFileReference> refs = new ArrayList<BinaryFileReference>();
			// 1. write cities
			writer.startCityBlockIndex(type);
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

				int cityType = city.isPostcode() ? -1 : city.getType().ordinal();
				BinaryFileReference ref = writer.writeCityHeader(city, cityType, tagRules);
				refs.add(ref);
				writer.writeCityIndex(city, city.getStreets(), namesakesStreetNodes.get(city), ref, tagRules);
				IndexAddressCreator.putNamedMapObject(namesIndex, city, ref.getStartPointer());
				if (!city.isPostcode()) {
					for (Street s : city.getStreets()) {
						IndexAddressCreator.putNamedMapObject(namesIndex, s, s.getFileOffset());
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
		boolean overwriteIds = false;
		IndexCreatorSettings settings = new IndexCreatorSettings();
		settings.indexPOI = true;
		
		final IndexPoiCreator indexPoiCreator = new IndexPoiCreator(settings, renderingTypes, overwriteIds);
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
										indexPoiCreator.insertAmenityIntoPoi(amenity);
										writtenPoiCount[0]++;
									}
								} else {
									if (!set.contains(amenity.getId())) {
										file.add(amenity.getId());
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
		indexPoiCreator.writeBinaryPoiIndex(writer, name, null);
		indexPoiCreator.commitAndClosePoiFile(dateCreated);
//		REMOVE_POI_DB = false;
		if (REMOVE_POI_DB) {
			indexPoiCreator.removePoiFile();
		}
		log.info("Written " + writtenPoiCount[0] + " POI.");
	}

	public static void copyBinaryPart(CodedOutputStream ous, byte[] BUFFER, RandomAccessFile raf, long fp, int length)
			throws IOException {
		long old = raf.getFilePointer();
		raf.seek(fp);
		int toRead = length;
		while (toRead > 0) {
			int read = raf.read(BUFFER);
			if (read == -1) {
				throw new IllegalArgumentException("Unexpected end of file");
			}
			if (toRead < read) {
				read = toRead;
			}
			ous.writeRawBytes(BUFFER, 0, read);
			toRead -= read;
		}
		raf.seek(old);
	}

	public void combineParts(File fileToExtract, List<File> files, Set<Integer> combineParts) throws IOException, SQLException {
		BinaryMapIndexReader[] indexes = new BinaryMapIndexReader[files.size()];
		RandomAccessFile[] rafs = new RandomAccessFile[files.size()];
		long dateCreated = 0;
		int version = -1;
		// Go through all files and validate consistency
		int c = 0;
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

		// write files
		RandomAccessFile rafToExtract = new RandomAccessFile(fileToExtract, "rw");
		BinaryMapIndexWriter writer = new BinaryMapIndexWriter(rafToExtract, dateCreated);
		CodedOutputStream ous = writer.getCodedOutStream();
		byte[] BUFFER_TO_READ = new byte[BUFFER_SIZE];
		AddressRegion[] addressRegions = new AddressRegion[files.size()];
		PoiRegion[] poiRegions = new PoiRegion[files.size()];
		for (int k = 0; k < indexes.length; k++) {
			BinaryMapIndexReader index = indexes[k];
			RandomAccessFile raf = rafs[k];
			for (int i = 0; i < index.getIndexes().size(); i++) {
				BinaryIndexPart part = index.getIndexes().get(i);
				if (combineParts.contains(part.getFieldNumber())) {
					if (part.getFieldNumber() == OsmandOdb.OsmAndStructure.ADDRESSINDEX_FIELD_NUMBER) {
						addressRegions[k] = (AddressRegion) part;
					} else if (part.getFieldNumber() == OsmandOdb.OsmAndStructure.POIINDEX_FIELD_NUMBER) {
						poiRegions[k] = (PoiRegion) part;
					}
				} else {
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
		combineParts(outputFile, parts, combineParts);
		for (File f : toDelete) {
			if (!f.delete()) {
				throw new IOException("Cannot delete file " + outputFile);
			}
		}
	}

}
