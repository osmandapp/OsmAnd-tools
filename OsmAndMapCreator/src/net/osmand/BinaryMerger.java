package net.osmand;


import java.io.*;
import java.text.Collator;
import java.text.MessageFormat;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import net.osmand.util.MapUtils;
import org.apache.commons.logging.Log;

import net.osmand.binary.BinaryIndexPart;
import net.osmand.binary.BinaryMapAddressReaderAdapter;
import net.osmand.binary.BinaryMapAddressReaderAdapter.AddressRegion;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.OsmandOdb;
import net.osmand.data.Building;
import net.osmand.data.City;
import net.osmand.data.MapObject;
import net.osmand.data.Street;
import net.osmand.data.preparation.BinaryFileReference;
import net.osmand.data.preparation.BinaryMapIndexWriter;
import net.osmand.data.preparation.address.IndexAddressCreator;
import net.osmand.osm.edit.Node;
import net.osmand.util.Algorithms;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;

public class BinaryMerger {

	public static final int BUFFER_SIZE = 1 << 20;
	private final static Log log = PlatformUtil.getLog(BinaryMerger.class);

	public static void main(String[] args) throws IOException {
		BinaryMerger in = new BinaryMerger();
		// test cases show info
		if (args.length == 1 && "test".equals(args[0])) {
			in.merger(new String[]{
					System.getProperty("maps.dir") + "Argentina_southamerica_2.obf",
					System.getProperty("maps.dir") + "Argentina_cordoba_southamerica_2.obf",
					System.getProperty("maps.dir") + "Argentina_chubut_southamerica_2.obf"});
		} else {
			in.merger(args);
		}
	}

	public void merger(String[] args) throws IOException {
		if (args == null || args.length == 0) {
			System.out.println("[output file] [input files]");
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
					if(!ze.isDirectory() && name.endsWith(".obf")) {
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
			f.delete();
		}
	}

	public static final void writeInt(CodedOutputStream ous, int v) throws IOException {
		ous.writeRawByte((v >>> 24) & 0xFF);
		ous.writeRawByte((v >>> 16) & 0xFF);
		ous.writeRawByte((v >>>  8) & 0xFF);
		ous.writeRawByte(v & 0xFF);
		//written += 4;
	}

	private static void addRegionToCityName(City city, BinaryMapIndexReader index) {
		String region = index.getRegionNames().get(0).split("_")[1];
		region = region.substring(0, 1).toUpperCase() + region.substring(1);
		city.setName(city.getName() + " (" + region + ")");
	}

	private static boolean isSameCity(City namesake0, City namesake1) {
		double sameCityDistance = 1000;
		return MapUtils.getDistance(namesake0.getLocation(), namesake1.getLocation()) < sameCityDistance;
	}

	private static void combineAddressIndex(String name, BinaryMapIndexWriter writer, AddressRegion[] addressRegions, BinaryMapIndexReader[] indexes)
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
		Map<String, City> postcodes = new TreeMap<String, City>();
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
					if (cityMap.containsKey(city)) {
						cityMap.remove(city);
					}
					cityMap.put(city, index);
				}
			}
			List<City> cities = new ArrayList<City>(cityMap.keySet());
			Collections.sort(cities, MapObject.BY_NAME_COMPARATOR);
			List<BinaryFileReference> refs = new ArrayList<BinaryFileReference>();
			// 1. write cities
			writer.startCityBlockIndex(type);
			for (City city : cities) {
				int cityType = city.isPostcode() ? BinaryMapAddressReaderAdapter.POSTCODES_TYPE : city.getType().ordinal();
				refs.add(writer.writeCityHeader(city, cityType, tagRules));
			}
			List<List<City>> namesakes = new ArrayList<>();
			String namesakesName = null;
			Map<City, Map<Street, List<Node>>> namesakesStreetNodes = new HashMap<>();
			for (int i = 0; i != refs.size(); i++) {
				BinaryFileReference ref = refs.get(i);
				City city = cities.get(i);

				if (!city.getName().equals(namesakesName)) {
					boolean rename = (type == BinaryMapAddressReaderAdapter.CITY_TOWN_TYPE) && (namesakes.size() > 1);
					for (List<City> namesakeGroup : namesakes) {
						City mainCity = namesakeGroup.get(0);
						for (City namesake : namesakeGroup.subList(1, namesakeGroup.size())) {
							mainCity.mergeWith(namesake);
							namesakesStreetNodes.get(mainCity).putAll(namesakesStreetNodes.get(namesake));
							city.getStreets().clear();
						}
						if (rename) {
							addRegionToCityName(mainCity, cityMap.get(mainCity));
						}

						List<Street> streets = new ArrayList<Street>(mainCity.getStreets());
						Map<Street, List<Node>> streetNodes = namesakesStreetNodes.get(mainCity);
						writer.writeCityIndex(mainCity, streets, streetNodes, ref, tagRules);
						IndexAddressCreator.putNamedMapObject(namesIndex, mainCity, ref.getStartPointer());
						mainCity.getStreets().clear();
						// register postcodes and name index
						for (Street s : streets) {
							IndexAddressCreator.putNamedMapObject(namesIndex, s, s.getFileOffset());
							for (Building b : s.getBuildings()) {
								if (city.getPostcode() != null && b.getPostcode() == null) {
									b.setPostcode(city.getPostcode());
								}
								if (b.getPostcode() != null) {
									if (!postcodes.containsKey(b.getPostcode())) {
										City p = City.createPostcode(b.getPostcode());
										p.setLocation(b.getLocation().getLatitude(), b.getLocation().getLongitude());
										postcodes.put(b.getPostcode(), p);
									}
									City post = postcodes.get(b.getPostcode());
									Street newS = post.getStreetByName(s.getName());
									if (newS == null) {
										newS = new Street(post);
										newS.copyNames(s);
										newS.setLocation(s.getLocation().getLatitude(), s.getLocation().getLongitude());
										newS.setId(s.getId());
										post.registerStreet(newS);
									}
									newS.addBuildingCheckById(b);
								}
							}
						}
					}

					namesakesName = city.getName();
					namesakes.clear();
					namesakesStreetNodes.clear();
				}

				BinaryMapIndexReader rindex = cityMap.get(city);
				rindex.preloadStreets(city, null);
				boolean isDuplicate = false;
				for (List<City> namesakeGroup : namesakes) {
					City mainCity = namesakeGroup.get(0);
					if (isSameCity(mainCity, city)) {
						isDuplicate = true;
						if (city.getStreets().size() > mainCity.getStreets().size()) {
							namesakeGroup.add(0, city);
						} else {
							namesakeGroup.add(city);
						}
						break;
					}
				}
				if (!isDuplicate) {
					namesakes.add(new ArrayList<City>(Collections.singletonList(city)));
				}
				Map<Street, List<Node>> streetNodes = new LinkedHashMap<Street, List<Node>>();
				for (Street street : city.getStreets()) {
					rindex.preloadBuildings(street, null);
					ArrayList<Node> nns = new ArrayList<Node>();
					for (Street is : street.getIntersectedStreets()) {
						double lat = is.getLocation().getLatitude();
						double lon = is.getLocation().getLongitude();
						long id = (Float.floatToIntBits((float) lat) << 32) | Float.floatToIntBits((float) lon);
						Node nn = new Node(is.getLocation().getLatitude(), is.getLocation().getLongitude(), id);
						nns.add(nn);
					}
					streetNodes.put(street, nns);
				}
				namesakesStreetNodes.put(city, streetNodes);
			}
			writer.endCityBlockIndex();
		}
		writer.writeAddressNameIndex(namesIndex);
		writer.endWriteAddressIndex();
	}

	public static void combineParts(File fileToExtract, List<File> files) throws IOException {
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
					addressRegions[k] = (AddressRegion)part;
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
