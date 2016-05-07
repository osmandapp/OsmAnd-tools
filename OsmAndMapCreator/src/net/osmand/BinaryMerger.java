package net.osmand;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.Collator;
import java.text.MessageFormat;
import java.util.*;

import net.osmand.binary.BinaryIndexPart;
import net.osmand.binary.BinaryMapAddressReaderAdapter;
import net.osmand.binary.BinaryMapAddressReaderAdapter.AddressRegion;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexReader.MapIndex;
import net.osmand.binary.BinaryMapIndexReader.MapRoot;
import net.osmand.binary.OsmandOdb;
import net.osmand.data.Building;
import net.osmand.data.City;
import net.osmand.data.MapObject;
import net.osmand.data.Street;
import net.osmand.data.preparation.BinaryFileReference;
import net.osmand.data.preparation.BinaryMapIndexWriter;
import net.osmand.data.preparation.address.IndexAddressCreator;
import net.osmand.osm.edit.Node;
import net.osmand.util.MapUtils;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;

public class BinaryMerger {


	public static final int BUFFER_SIZE = 1 << 20;

	public static void main(String[] args) throws IOException {
		BinaryMerger in = new BinaryMerger();
		// test cases show info
		if (args.length == 1 && "test".equals(args[0])) {
			in.merger(new String[]{""});
		} else {
			in.merger(args);
		}
	}

	public void merger(String[] args) throws IOException {
		if (args == null || args.length == 0) {
			printUsage(null);
			return;
		}
		String f = args[0];
		if (f.charAt(0) == '-') {
			// command
			if (f.equals("-c") || f.equals("-combine")) {
				if (args.length < 4) {
					printUsage("Too few parameters to extract (require minimum 4)");
				} else {
					Map<File, String> parts = new HashMap<File, String>();
					for (int i = 2; i < args.length; i++) {
						File file = new File(args[i]);
						if (!file.exists()) {
							System.err.println("File to extract from doesn't exist " + args[i]);
							return;
						}
						parts.put(file, null);
						if (i < args.length - 1) {
							if (args[i + 1].startsWith("-") || args[i + 1].startsWith("+")) {
								parts.put(file, args[i + 1]);
								i++;
							}
						}
					}
					List<Float> extracted = combineParts(new File(args[1]), parts);
					if (extracted != null) {
						System.out.println("\n" + extracted.size() + " parts were successfully extracted to " + args[1]);
					}
				}
			} else {
				printUsage("Unknown command : " + f);
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

	public static void combineAddressIndex(String name, BinaryMapIndexWriter writer, AddressRegion[] addressRegions, BinaryMapIndexReader[] indexes)
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
			Set<City> citiesSet = new TreeSet<City>();
			for (int i = 0; i != addressRegions.length; i++) {
				AddressRegion region = addressRegions[i];
				BinaryMapIndexReader index = indexes[i];
				citiesSet.addAll(index.getCities(region, null, type));
			}
			List<City> cities = new ArrayList<City>();
			cities.addAll(citiesSet);
			List<BinaryFileReference> refs = new ArrayList<BinaryFileReference>();
			// 1. write cities
			writer.startCityBlockIndex(type);
			for (City city : cities) {
				refs.add(writer.writeCityHeader(city, city.getType().ordinal(), tagRules));
			}
			for (int i = 0; i != refs.size(); i++) {
				BinaryFileReference ref = refs.get(i);
				City city = cities.get(i);
				IndexAddressCreator.putNamedMapObject(namesIndex, city, ref.getStartPointer());
				List<Street> streets = new ArrayList(city.getStreets());
				Map<Street, List<Node>> streetNodes = new LinkedHashMap<Street, List<Node>>();
				writer.writeCityIndex(city, streets, streetNodes, ref, tagRules);
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
							if(newS == null) {
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
			writer.endCityBlockIndex();
		}
		writer.writeAddressNameIndex(namesIndex);
		writer.endWriteAddressIndex();
	}

	@SuppressWarnings("unchecked")
	public static List<Float> combineParts(File fileToExtract, Map<File, String> partsToExtractFrom) throws IOException {
		BinaryMapIndexReader[] indexes = new BinaryMapIndexReader[partsToExtractFrom.size()];
		RandomAccessFile[] rafs = new RandomAccessFile[partsToExtractFrom.size()];

		LinkedHashSet<Float>[] partsSet = new LinkedHashSet[partsToExtractFrom.size()];
		int c = 0;
//		Set<String> addressNames = new LinkedHashSet<String>();

		long dateCreated = 0;
		int version = -1;
		// Go through all files and validate conistency
		for (File f : partsToExtractFrom.keySet()) {
			if (f.getAbsolutePath().equals(fileToExtract.getAbsolutePath())) {
				System.err.println("Error : Input file is equal to output file " + f.getAbsolutePath());
				return null;
			}
			rafs[c] = new RandomAccessFile(f.getAbsolutePath(), "r");
			indexes[c] = new BinaryMapIndexReader(rafs[c], f);
			partsSet[c] = new LinkedHashSet<Float>();
			dateCreated = Math.max(dateCreated, indexes[c].getDateCreated());
			if (version == -1) {
				version = indexes[c].getVersion();
			} else {
				if (indexes[c].getVersion() != version) {
					System.err.println("Error : Different input files has different input versions " + indexes[c].getVersion() + " != " + version);
					return null;
				}
			}

			LinkedHashSet<Float> temp = new LinkedHashSet<Float>();
			String pattern = partsToExtractFrom.get(f);
			boolean minus = true;
			for (int i = 0; i < indexes[c].getIndexes().size(); i++) {
				partsSet[c].add(i + 1f);
				BinaryIndexPart part = indexes[c].getIndexes().get(i);
				if (part instanceof MapIndex) {
					List<MapRoot> roots = ((MapIndex) part).getRoots();
					int rsize = roots.size();
					for (int j = 0; j < rsize; j++) {
						partsSet[c].add((i + 1f) + (j + 1) / 10f);
					}
				}
			}
			if (pattern != null) {
				minus = pattern.startsWith("-");
				String[] split = pattern.substring(1).split(",");
				for (String s : split) {
					temp.add(Float.valueOf(s));
				}
			}

			Iterator<Float> p = partsSet[c].iterator();
			while (p.hasNext()) {
				Float part = p.next();
				if (minus) {
					if (temp.contains(part)) {
						p.remove();
					}
				} else {
					if (!temp.contains(part)) {
						p.remove();
					}
				}
			}

			c++;
		}

		// write files
		RandomAccessFile rafToExtract = new RandomAccessFile(fileToExtract, "rw");
		BinaryMapIndexWriter writer = new BinaryMapIndexWriter(rafToExtract, dateCreated);
		CodedOutputStream ous = writer.getCodedOutStream();
		List<Float> list = new ArrayList<Float>();
		byte[] BUFFER_TO_READ = new byte[BUFFER_SIZE];

		AddressRegion[] addressRegions = new AddressRegion[partsToExtractFrom.size()];
		for (int k = 0; k < indexes.length; k++) {
			LinkedHashSet<Float> partSet = partsSet[k];
			BinaryMapIndexReader index = indexes[k];
			RandomAccessFile raf = rafs[k];
			for (int i = 0; i < index.getIndexes().size(); i++) {
				if (!partSet.contains(Float.valueOf(i + 1f))) {
					continue;
				}
				list.add(i + 1f);

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
//				} else if (part instanceof AddressRegion) {
//					if (addressNames.contains(part.getName())) {
//						System.err.println("Error : going to merge 2 addresses with same names. Skip " + part.getName());
//						continue;
//					}
//					addressNames.add(part.getName());
			}
		}
		combineAddressIndex(fileToExtract.getName(), writer, addressRegions, indexes);

		ous.writeInt32(OsmandOdb.OsmAndStructure.VERSIONCONFIRM_FIELD_NUMBER, version);
		ous.flush();

		return list;
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


	protected String formatBounds(int left, int right, int top, int bottom) {
		double l = MapUtils.get31LongitudeX(left);
		double r = MapUtils.get31LongitudeX(right);
		double t = MapUtils.get31LatitudeY(top);
		double b = MapUtils.get31LatitudeY(bottom);
		return formatLatBounds(l, r, t, b);
	}

	protected String formatLatBounds(double l, double r, double t, double b) {
		MessageFormat format = new MessageFormat("(left top - right bottom) : {0,number,#.####}, {1,number,#.####} NE - {2,number,#.####}, {3,number,#.####} NE", new Locale("EN", "US"));
		return format.format(new Object[]{l, t, r, b});
	}

	public void printUsage(String warning) {
		if (warning != null) {
			System.out.println(warning);
		}
		System.out.println("Merger is console utility for working with binary indexes of OsmAnd.");
		System.out.println("It allows to merge indexes.");
		System.out.println("\nUsage for combining indexes : merger -c file_to_create (file_from_extract ((+|-)parts_to_extract)? )*");
		System.out.println("\tCreate new file of extracted parts from input file. [parts_to_extract] could be parts to include or exclude.");
		System.out.println("  Example : merger -c output_file input_file +1,2,3\n\tExtracts 1, 2, 3 parts (could be find in print info)");
		System.out.println("  Example : merger -c output_file input_file -2,3\n\tExtracts all  parts excluding 2, 3");
		System.out.println("  Example : merger -c output_file input_file1 input_file2 input_file3\n\tSimply combine 3 files");
		System.out.println("  Example : merger -c output_file input_file1 input_file2 -4\n\tCombine all parts of 1st file and all parts excluding 4th part of 2nd file");
	}

}
