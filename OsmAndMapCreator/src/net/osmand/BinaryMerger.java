package net.osmand;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.Collator;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import net.osmand.binary.BinaryIndexPart;
import net.osmand.binary.BinaryMapAddressReaderAdapter;
import net.osmand.binary.BinaryMapAddressReaderAdapter.AddressRegion;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.OsmandOdb;
import net.osmand.data.City;
import net.osmand.data.MapObject;
import net.osmand.data.Postcode;
import net.osmand.data.Street;
import net.osmand.data.preparation.BinaryFileReference;
import net.osmand.data.preparation.BinaryMapIndexWriter;
import net.osmand.data.preparation.address.IndexAddressCreator;
import net.osmand.osm.edit.Node;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

import org.apache.commons.logging.Log;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;

public class BinaryMerger {

	public static final int BUFFER_SIZE = 1 << 20;
	private final static Log log = PlatformUtil.getLog(BinaryMerger.class);
	public static final String helpMessage = "output_file.obf [input_file.obf] ...: merges all obf files and merges address structure into 1";

	public static void main(String[] args) throws IOException {
		BinaryMerger in = new BinaryMerger();
		// test cases show info
		if (args.length == 1 && "test".equals(args[0])) {
			in.merger(new String[]{
					System.getProperty("maps.dir") + "Ukraine_merge.road.obf",
					System.getProperty("maps.dir") + "Ukraine_kherson_europe_2.road.obf"
					});
		} else {
			in.merger(args);
		}
	}

	public void merger(String[] args) throws IOException {
		if (args == null || args.length == 0) {
			System.out.println(helpMessage);
			return;
		}
		File outputFile = new File(args[0]);
		List<File> parts = new ArrayList<File>();
		List<File> toDelete = new ArrayList<File>();
		for (int i = 1; i < args.length; i++) {
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
		if (outputFile.exists()) {
			if (!outputFile.delete()) {
				throw new IOException("Cannot delete file " + outputFile);
			}
		}
		combineParts(outputFile, parts);
		for (File f : toDelete) {
			if (!f.delete()) {
				throw new IOException("Cannot delete file " + outputFile);
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
		city.mergeWith(namesake);
		namesakesStreetNodes.get(city).putAll(namesakesStreetNodes.get(namesake));
		return city;
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
			for (int i = 0; i < addressRegions.length; i++) {
				AddressRegion region = addressRegions[i];
				final BinaryMapIndexReader index = indexes[i];
				for (City city : index.getCities(region, null, type)) {
					normalizePostcode(city, extractCountryName(index));
					if (cityMap.containsKey(city)) {
						cityMap.remove(city);
					}
					cityMap.put(city, index);
				}
			}
			List<City> cities = new ArrayList<City>(cityMap.keySet());
			Map<City, List<City>> mergeCityGroup = new HashMap<City, List<City>>();
			Collections.sort(cities, MapObject.BY_NAME_COMPARATOR);
			mergeCitiesByNameDistance(cities, mergeCityGroup, cityMap, type == BinaryMapAddressReaderAdapter.CITY_TOWN_TYPE);
			List<BinaryFileReference> refs = new ArrayList<BinaryFileReference>();
			// 1. write cities
			writer.startCityBlockIndex(type);
			for (City city : cities) {
				int cityType = city.isPostcode() ? -1 : city.getType().ordinal();
				refs.add(writer.writeCityHeader(city, cityType, tagRules));
			}
			Map<City, Map<Street, List<Node>>> namesakesStreetNodes = new HashMap<City, Map<Street, List<Node>>>();
			for (int i = 0; i < refs.size(); i++) {
				BinaryFileReference ref = refs.get(i);
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

				writer.writeCityIndex(city, city.getStreets(), namesakesStreetNodes.get(city), ref, tagRules);
				IndexAddressCreator.putNamedMapObject(namesIndex, city, ref.getStartPointer());
				for (Street s : city.getStreets()) {
					IndexAddressCreator.putNamedMapObject(namesIndex, s, s.getFileOffset());
				}

				city.getStreets().clear();
				namesakesStreetNodes.clear();
			}

			writer.endCityBlockIndex();
		}
		writer.writeAddressNameIndex(namesIndex);
		writer.endWriteAddressIndex();
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

	private void mergeCitiesByNameDistance(List<City> orderedCities, Map<City, List<City>> mergeCityGroup, 
			Map<City, BinaryMapIndexReader> cityMap, boolean rename) {
		for (int i = 0; i < orderedCities.size(); i++) {
			boolean renameGroup = false;
			int j = i + 1;
			City oc = orderedCities.get(i);
			City nc = (j < orderedCities.size()) ? orderedCities.get(j) : null;
			while (MapObject.BY_NAME_COMPARATOR.areEqual(nc, oc)) {
				if (isSameCity(oc, nc)) {
					if (!mergeCityGroup.containsKey(oc)) {
						mergeCityGroup.put(oc, new ArrayList<City>());
					}
					boolean shorter = nc.getName().length() < oc.getName().length();
					// Prefer cities with shortest names ("1101DL" instead "1101 DL")
					if (shorter) {
						orderedCities.remove(i);
						mergeCityGroup.put(nc, mergeCityGroup.remove(oc));
						City tmp = oc;
						oc = nc;
						nc = tmp;
					} else {
						orderedCities.remove(j);
					}
					mergeCityGroup.get(oc).add(nc);
				} else {
					boolean areCitiesInSameRegion = cityMap.get(oc) == cityMap.get(nc);
					renameGroup = renameGroup || (rename && !areCitiesInSameRegion);
					j++;
				}
				nc = (j < orderedCities.size()) ? orderedCities.get(j) : null;
			}
			if (renameGroup) {
				for (City c : orderedCities.subList(i, j)) {
					addRegionToCityName(c, cityMap.get(c));
				}
			}
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
					boolean  nameEqual = false;
					double dd = 100000;
					for (Node n : list) {
						double d = MapUtils.getDistance(n.getLatLon(), is.getLocation());
						if(Algorithms.objectEquals(street.getName(), n.getTag("name")) && 
								(d < dd || !nameEqual)) {
							nn = n;
							dd = d;
							nameEqual = true;
						} else if(!nameEqual && d < dd ) {
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
			if(streetNodesN.containsKey(street.getName())) {
				streetNodesN.get(street.getName()).addAll(nns);
			} else {
				streetNodesN.put(street.getName(), nns);
			}
		}
		namesakesStreetNodes.put(city, streetNodes);
	}


	public void combineParts(File fileToExtract, List<File> files) throws IOException {
		BinaryMapIndexReader[] indexes = new BinaryMapIndexReader[files.size()];
		RandomAccessFile[] rafs = new RandomAccessFile[files.size()];
		long dateCreated = 0;
		int version = -1;
		// Go through all files and validate conistency
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
		for (int k = 0; k < indexes.length; k++) {
			BinaryMapIndexReader index = indexes[k];
			RandomAccessFile raf = rafs[k];
			for (int i = 0; i < index.getIndexes().size(); i++) {
				BinaryIndexPart part = index.getIndexes().get(i);
				if (part.getFieldNumber() == OsmandOdb.OsmAndStructure.ADDRESSINDEX_FIELD_NUMBER) {
					addressRegions[k] = (AddressRegion) part;
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
		combineAddressIndex(nm, writer, addressRegions, indexes);
		ous.writeInt32(OsmandOdb.OsmAndStructure.VERSIONCONFIRM_FIELD_NUMBER, version);
		ous.flush();
	}


	public static void copyBinaryPart(CodedOutputStream ous, byte[] BUFFER, RandomAccessFile raf, long fp, int length)
			throws IOException {
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
	}


}
