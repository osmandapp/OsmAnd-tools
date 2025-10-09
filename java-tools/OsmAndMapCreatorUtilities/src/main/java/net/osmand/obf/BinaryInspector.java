package net.osmand.obf;


import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.osmand.IndexConstants;
import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryHHRouteReaderAdapter.HHRouteRegion;
import net.osmand.binary.*;
import net.osmand.binary.BinaryMapAddressReaderAdapter.AddressRegion;
import net.osmand.binary.BinaryMapAddressReaderAdapter.CitiesBlock;
import net.osmand.binary.BinaryMapAddressReaderAdapter.CityBlocks;
import net.osmand.binary.BinaryMapIndexReader.*;
import net.osmand.binary.BinaryMapPoiReaderAdapter.PoiRegion;
import net.osmand.binary.BinaryMapPoiReaderAdapter.PoiSubType;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteRegion;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteSubregion;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteTypeRule;
import net.osmand.binary.BinaryMapTransportReaderAdapter.TransportIndex;
import net.osmand.data.*;
import net.osmand.osm.MapPoiTypes;
import net.osmand.osm.MapRenderingTypes;
import net.osmand.osm.PoiType;
import net.osmand.router.HHRouteDataStructure.NetworkDBPoint;
import net.osmand.router.RoutingContext;
import net.osmand.router.TransportRoutePlanner;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

import java.io.*;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class BinaryInspector {


	public static final int BUFFER_SIZE = 1 << 20;
	public static final int SHIFT_ID = 6;
	
	protected static final boolean DETECT_POI_ADDRESS = false;
	
	private VerboseInfo vInfo;
	public static void main(String[] args) throws IOException {
		BinaryInspector in = new BinaryInspector();
		if (args == null || args.length == 0) {
			BinaryInspector.printUsage(null);
			return;
		}
		// test cases show info
		if ("test".equals(args[0])) {
			in.inspector(new String[] {
//					"-vpoi",
//					"-vmap", "-vmapobjects",
//					"-vmapcoordinates",
//					"-vrouting",
//					"-vtransport", "-vtransportschedule",
//					"-vaddress", "-vcities", //"-vstreetgroups",
//					"-vstreets", //"-vbuildings", "-vintersections",
//					"-lang=ru",
//					"-zoom=15",
					// road
//					"-latlon=41.4,-75.7,0.05",
//					"-latlon=42.060294,-77.498224,0.05",
					
					//"-xyz=12071,26142,16",
//					"-c",
//					"-osm="+System.getProperty("maps.dir")+"World_lightsectors_src_0.osm",
					System.getProperty("maps.dir") + "Map.obf"
//					System.getProperty("maps.dir") + "Germany_nordrhein-westfalen_cologne-government-region_europe_3.obf"
//					System.getProperty("maps.dir") + "../basemap/World_basemap_mini_2.obf"
//					System.getProperty("maps.dir")+"/../repos/resources/countries-info/regions.ocbf"
			});
		} else {
			in.inspector(args);
		}
	}

	private void printToFile(String s) throws IOException {
		if (vInfo.osmOut != null) {
			vInfo.osmOut.write(s.getBytes());
		} else {
			System.out.println(s);
		}
	}

	private void println(String s) {
		if (vInfo != null && vInfo.osm && vInfo.osmOut == null) {
			// ignore
		} else {
			System.out.println(s);
		}

	}

	private void print(String s) {
		if (vInfo != null && vInfo.osm && vInfo.osmOut == null) {
			// ignore
		} else {
			System.out.print(s);
		}
	}

	protected static class VerboseInfo {
		boolean vaddress;
		boolean vcities;
		boolean vcitynames;
		boolean vstreetgroups;
		boolean vstreets;
		boolean vbuildings;
		boolean vintersections;
		boolean vtransport;
		boolean vtransportschedule;
		boolean vpoi;
		boolean vmap;
		boolean vrouting;
		boolean vhhrouting;
		boolean vmapObjects;
		boolean vmapCoordinates;
		boolean vstats;
		boolean osm;
		FileOutputStream osmOut = null;
		double lattop = 85;
		double latbottom = -85;
		double lonleft = -179.9;
		double lonright = 179.9;
		String lang = null;
		int zoom = 15;

		public boolean isVaddress() {
			return vaddress;
		}

		public int getZoom() {
			return zoom;
		}

		public boolean isVmap() {
			return vmap;
		}

		public boolean isVrouting() {
			return vrouting;
		}

		public boolean isVpoi() {
			return vpoi;
		}

		public boolean isVHHrouting() {
			return vhhrouting;
		}

		public boolean isVtransport() {
			return vtransport;
		}

		public boolean isVStats() {
			return vstats;
		}

		public VerboseInfo(String[] params) throws FileNotFoundException {
			for (int i = 0; i < params.length; i++) {
				if (params[i].equals("-vaddress")) {
					vaddress = true;
				} else if (params[i].equals("-vstreets")) {
					vstreets = true;
				} else if (params[i].equals("-vstreetgroups")) {
					vstreetgroups = true;
				} else if (params[i].equals("-vcities")) {
					vcities = true;
				} else if (params[i].equals("-vcitynames")) {
					vcitynames = true;
				} else if (params[i].equals("-vbuildings")) {
					vbuildings = true;
				} else if (params[i].equals("-vintersections")) {
					vintersections = true;
				} else if (params[i].equals("-vmap")) {
					vmap = true;
				} else if (params[i].equals("-vstats")) {
					vstats = true;
				} else if (params[i].equals("-vrouting")) {
					vrouting = true;
				} else if (params[i].equals("-vhhrouting")) {
					vhhrouting = true;
				} else if (params[i].equals("-vmapobjects")) {
					vmapObjects = true;
				} else if (params[i].equals("-vmapcoordinates")) {
					vmapCoordinates = true;
				} else if (params[i].equals("-vpoi")) {
					vpoi = true;
				} else if (params[i].startsWith("-osm")) {
					osm = true;
					if (params[i].startsWith("-osm=")) {
						osmOut = new FileOutputStream(params[i].substring(5));
					}
				} else if (params[i].equals("-vtransport")) {
					vtransport = true;
				} else if (params[i].equals("-vtransportschedule")) {
					vtransportschedule = true;
				} else if (params[i].startsWith("-lang=")) {
					lang = params[i].substring("-lang=".length());
				} else if (params[i].startsWith("-zoom=")) {
					zoom = Integer.parseInt(params[i].substring("-zoom=".length()));
				} else if (params[i].startsWith("-latlon=")) {
					String[] values = params[i].substring("-latlon=".length()).split(",");
					double latmid = Double.parseDouble(values[0]);
					double lonmid = Double.parseDouble(values[1]);
					double dist = 0.005;
					if (values.length > 2) {
						dist = Double.parseDouble(values[2]);
					}
					lonleft = lonmid - dist;
					lattop = latmid + dist;
					lonright = lonmid + dist;
					latbottom = latmid - dist;
				} else if (params[i].startsWith("-xyz=")) {
					String[] values = params[i].substring("-xyz=".length()).split(",");
					int tileX = Integer.parseInt(values[0]);
					int tileY = Integer.parseInt(values[1]);
					int z = Integer.parseInt(values[2]);
					lonleft = MapUtils.getLongitudeFromTile(z, tileX);
					lonright = MapUtils.getLongitudeFromTile(z, tileX + 1);
					lattop = MapUtils.getLatitudeFromTile(z, tileY);
					latbottom = MapUtils.getLatitudeFromTile(z, tileY + 1);
				} else if (params[i].startsWith("-bbox=")) {
					String[] values = params[i].substring("-bbox=".length()).split(",");
					lonleft = Double.parseDouble(values[0]);
					lattop = Double.parseDouble(values[1]);
					lonright = Double.parseDouble(values[2]);
					latbottom = Double.parseDouble(values[3]);
				}
			}
		}

		public boolean contains(MapObject o) {
			return lattop >= o.getLocation().getLatitude() && latbottom <= o.getLocation().getLatitude()
					&& lonleft <= o.getLocation().getLongitude() && lonright >= o.getLocation().getLongitude();

		}

		public void close() throws IOException {
			if (osmOut != null) {
				osmOut.close();
				osmOut = null;
			}

		}
	}

	public static class FileExtractFrom {
		public List<File> from = new ArrayList<File>();
		public Set<Integer> excludeParts;
		public Set<Integer> includeParts;
		public Set<Integer> excludeIndex;
		public Set<Integer> includeIndex;

	}

	public void inspector(String[] args) throws IOException {
		String f = args[0];
		if (f.charAt(0) == '-') {
			// command
			if (f.equals("-c") || f.equals("-combine")) {
				if (args.length < 3) {
					printUsage("Too few parameters to extract (require minimum 3)");
				} else {
					List<FileExtractFrom> parts = new ArrayList<>();
					FileExtractFrom lastPart = null;
					String date = null;
					for (int i = 2; i < args.length; i++) {
						if ((args[i].startsWith("-") || args[i].startsWith("+"))) {
							if(lastPart == null) {
								System.err.println("Expected file name instead of " + args[i]);
								return;
							}
							if (args[i].startsWith("--date")) {
								date = args[i].replace("--date", "").replace("=", "");
							} else if (args[i].startsWith("--") || args[i].startsWith("++")) {
								String[] st = args[i].substring(2).split(",");
								TreeSet<Integer> ts = new TreeSet<>();
								for (String s : st) {
									int t = 0;
									if (s.equals("address")) {
										t = OsmandOdb.OsmAndStructure.ADDRESSINDEX_FIELD_NUMBER;
									} else if (s.equals("routing")) {
										t = OsmandOdb.OsmAndStructure.ROUTINGINDEX_FIELD_NUMBER;
									} else if (s.equals("hhrouting")) {
										t = OsmandOdb.OsmAndStructure.HHROUTINGINDEX_FIELD_NUMBER;
									} else if (s.equals("map")) {
										t = OsmandOdb.OsmAndStructure.MAPINDEX_FIELD_NUMBER;
									} else if (s.equals("poi")) {
										t = OsmandOdb.OsmAndStructure.POIINDEX_FIELD_NUMBER;
									} else if (s.equals("transport")) {
										t = OsmandOdb.OsmAndStructure.TRANSPORTINDEX_FIELD_NUMBER;
									} else {
										throw new IllegalArgumentException(s);
									}
									ts.add(t);
								}
								if (args[i].startsWith("-")) {
									lastPart.excludeIndex = ts;
								} else {
									lastPart.includeIndex = ts;
								}
							} else {
								String[] st = args[i].substring(1).split(",");
								TreeSet<Integer> ts = new TreeSet<>();
								for (String s : st) {
									ts.add(Integer.parseInt(s));
								}
								if (args[i].startsWith("-")) {
									lastPart.excludeParts = ts;
								} else {
									lastPart.includeParts = ts;
								}
							}
						} else {
							File file = new File(args[i]);
							if (!file.exists()) {
								System.err.println("File to extract from doesn't exist " + args[i]);
								return;
							}
							lastPart = new FileExtractFrom();
							if (file.isDirectory()) {
								for (File ch : file.listFiles()) {
									if (ch.getName().endsWith(".obf")) {
										lastPart.from.add(ch);
									}
								}
								Collections.sort(lastPart.from, new Comparator<File>() {

									@Override
									public int compare(File o1, File o2) {
										return o1.getName().compareTo(o2.getName());
									}
								});
							} else {
								lastPart.from.add(file);
							}
							if (!lastPart.from.isEmpty()) {
								parts.add(lastPart);
							}
						}
					}
					File to = new File(args[1]);
					int partsS = combineParts(to, parts, date);
					if (partsS > 0) {
						println("\n" + partsS + " parts were successfully extracted to " + to.getName());
					}
				}
			} else if (f.startsWith("-v") || f.startsWith("-osm") || f.startsWith("-zoom")) {
				if (args.length < 2) {
					printUsage("Missing file parameter");
				} else {
					vInfo = new VerboseInfo(args);
					printFileInformation(args[args.length - 1]);
					vInfo.close();
				}
			} else {
				printUsage("Unknown command : " + f);
			}
		} else {
			vInfo = null;
			printFileInformation(f);
		}
	}

	public static final void writeInt(CodedOutputStream ous, long v) throws IOException {
		if (v > Integer.MAX_VALUE) {
			// mark highest bit to 1 as long
			ous.writeRawByte(((v >> 54) & 0xFF) | 0x80);
			ous.writeRawByte((v >> 48) & 0xFF);
			ous.writeRawByte((v >> 40) & 0xFF);
			ous.writeRawByte((v >> 32) & 0xFF);
		}
		ous.writeRawByte((v >> 24) & 0xFF);
		ous.writeRawByte((v >> 16) & 0xFF);
		ous.writeRawByte((v >>  8) & 0xFF);
		ous.writeRawByte(v & 0xFF);

		//written += 4;
	}

	public  static int combineParts(File fileToCreate, List<FileExtractFrom> partsToExtractFrom, String date) throws IOException {
		Set<String> uniqueNames = new LinkedHashSet<String>();

		int version = IndexConstants.BINARY_MAP_VERSION;

		// write files
		FileOutputStream fout = new FileOutputStream(fileToCreate);
		CodedOutputStream ous = CodedOutputStream.newInstance(fout, BUFFER_SIZE);
		byte[] BUFFER_TO_READ = new byte[BUFFER_SIZE];

		long dateCreated = System.currentTimeMillis();
		if (!Algorithms.isEmpty(date)) {
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-ddHH:mm");
			try {
				Date d = dateFormat.parse(date);
				dateCreated = d.getTime();
			} catch (ParseException e) {
				System.err.println("Date is wrong! Right format is yyyy-MM-ddHH:mm");
			}
		}

		ous.writeInt32(OsmandOdb.OsmAndStructure.VERSION_FIELD_NUMBER, version);
		ous.writeInt64(OsmandOdb.OsmAndStructure.DATECREATED_FIELD_NUMBER, dateCreated);
		// Go through all files and validate conistency
		int parts = 0;
		for (FileExtractFrom extract : partsToExtractFrom) {
			for (File f : extract.from) {
				if (f.getAbsolutePath().equals(fileToCreate.getAbsolutePath())) {
					System.err.println("Error : Input file is equal to output file " + f.getAbsolutePath());
					continue;
				}
				RandomAccessFile raf = new RandomAccessFile(f.getAbsolutePath(), "r");
				BinaryMapIndexReader bmir = new BinaryMapIndexReader(raf, f);
				if (bmir.getVersion() != version) {
					System.err.println("Error : Different input files has different input versions " + bmir.getVersion()
							+ " != " + version);
					continue;
				}
				for (int i = 0; i < bmir.getIndexes().size(); i++) {
					BinaryIndexPart part = bmir.getIndexes().get(i);
					int fieldNumber = part.getFieldNumber();
					String uniqueName = fieldNumber + " " + part.getName();
					int ind = i + 1;
					if (extract.excludeParts != null && extract.excludeParts.contains(ind)) {
						continue;
					}
					if (extract.includeParts != null && !extract.includeParts.contains(ind)) {
						continue;
					}
					if (extract.excludeIndex != null && extract.excludeIndex.contains(fieldNumber)) {
						continue;
					}
					if (extract.includeIndex != null && !extract.includeIndex.contains(fieldNumber)) {
						continue;
					}
					if (!uniqueNames.add(uniqueName)) {
						System.out.printf("Skip %s %s from %s as duplicate \n", part.getPartName(), part.getName(), f.getName());
						continue;
					}
					parts++;
					ous.writeTag(fieldNumber, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
					writeInt(ous, part.getLength());
					copyBinaryPart(ous, BUFFER_TO_READ, raf, part.getFilePointer(), part.getLength());
					System.out.printf("%s %s from %s is extracted %,d bytes\n", part.getPartName(), part.getName(), f.getName(),
							part.getLength());
				}
				raf.close();
			}
		}



		ous.writeInt32(OsmandOdb.OsmAndStructure.VERSIONCONFIRM_FIELD_NUMBER, version);
		ous.flush();
		fout.close();
		return parts;
	}


	public static void copyBinaryPart(CodedOutputStream ous, byte[] BUFFER, RandomAccessFile raf, long fp, long length)
			throws IOException {
		long old = raf.getFilePointer();
		raf.seek(fp);
		long toRead = length;
		while (toRead > 0) {
			int read = raf.read(BUFFER);
			if (read == -1) {
				throw new IllegalArgumentException("Unexpected end of file");
			}
			if (toRead < read) {
				read = (int) toRead;
			}
			ous.writeRawBytes(BUFFER, 0, read);
			toRead -= read;
		}
		raf.seek(old);
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

	public void printFileInformation(String fileName) throws IOException {
		File file = new File(fileName);
		if (!file.exists()) {
			println("Binary OsmAnd index " + fileName + " was not found.");
			return;
		}
		if (file.isDirectory()) {
			for(File f : file.listFiles()) {
				if(f.getName().endsWith(".obf")) {
					printFileInformation(f);
				}
			}
		} else {
			printFileInformation(file);
		}
	}

	public void printFileInformation(File file) throws IOException {
		RandomAccessFile r = new RandomAccessFile(file.getAbsolutePath(), "r");
		printFileInformation(r, file);
	}

	public void printFileInformation(RandomAccessFile r, File file) throws IOException {
		String filename = file.getName();
		try {
			BinaryMapIndexReader index = new BinaryMapIndexReader(r, file);
			String owner = index.getOwner() != null ? "\n" + index.getOwner().toString() : "";
			int i = 1;
			println("Binary index " + filename + " version = " + index.getVersion() + " edition = " + new Date(index.getDateCreated()) + owner);
			for (BinaryIndexPart p : index.getIndexes()) {
				String partname = p.getPartName();
				String name = p.getName() == null ? "" : p.getName();
				println(MessageFormat.format("{0} {1} data {3} - {2,number,#,###} bytes",
						new Object[]{i, partname, p.getLength(), name}));
				if(p instanceof TransportIndex){
					TransportIndex ti = ((TransportIndex) p);
					int sh = (31 - BinaryMapIndexReader.TRANSPORT_STOP_ZOOM);
					println("\tBounds " + formatBounds(ti.getLeft() << sh, ti.getRight() << sh,
							ti.getTop() << sh, ti.getBottom() << sh));
					if ((vInfo != null && vInfo.isVtransport())) {
						printTransportDetailInfo(vInfo, index, (TransportIndex) p);
					}
				} else if (p instanceof HHRouteRegion) {
					HHRouteRegion ri = ((HHRouteRegion) p);
					QuadRect rt = ri.getLatLonBbox();
					println(String.format("\tBounds %s profile '%s' %s edition = %s", formatLatBounds(rt.left, rt.right,
							rt.top, rt.bottom), ri.profile, ri.profileParams, new Date(ri.edition)));
					if ((vInfo != null && vInfo.isVHHrouting())) {
						TLongObjectHashMap<NetworkDBPoint> pnts = index.initHHPoints(ri, (short) 0, NetworkDBPoint.class);
						for (NetworkDBPoint pnt : pnts.valueCollection()) {
							System.out.println(String.format("\t\t %s - cluster %d (dual point %d, %d) - %d,%d -> %d,%d", pnt,
									pnt.clusterId, pnt.dualPoint == null ? 0 : pnt.dualPoint.index,
									pnt.dualPoint == null ? 0 : pnt.dualPoint.clusterId, pnt.startX, pnt.startY, pnt.endX, pnt.endY));
						}
					}
				} else if (p instanceof RouteRegion) {
					RouteRegion ri = ((RouteRegion) p);
					println("\tBounds " + formatLatBounds(ri.getLeftLongitude(), ri.getRightLongitude(),
							ri.getTopLatitude(), ri.getBottomLatitude()));
					if ((vInfo != null && vInfo.isVrouting())) {
						printRouteEncodingRules(ri);
						printRouteDetailInfo(index, (RouteRegion) p);
					}
				} else if (p instanceof MapIndex) {
					MapIndex m = ((MapIndex) p);
					int j = 1;

					for (MapRoot mi : m.getRoots()) {
						println(MessageFormat.format("\t{4}.{5} Map level minZoom = {0}, maxZoom = {1}, size = {2,number,#,###} bytes \n\t\tBounds {3}",
								new Object[] {
								mi.getMinZoom(), mi.getMaxZoom(), mi.getLength(),
								formatBounds(mi.getLeft(), mi.getRight(), mi.getTop(), mi.getBottom()),
								i, j++}));
					}
					if ((vInfo != null && vInfo.isVmap())) {
						printMapDetailInfo(index, m);
						printMapEncodingRules(m);
					}
				} else if (p instanceof PoiRegion && (vInfo != null && vInfo.isVpoi())) {
					printPOIDetailInfo(vInfo, index, (PoiRegion) p);
				} else if (p instanceof AddressRegion) {
					List<CitiesBlock> cities = ((AddressRegion) p).getCities();
					int ind = 0;
					for (CitiesBlock c : cities) {
						ind++;
						CityBlocks block = CityBlocks.getByType(c.getType()); 
						println(String.format("\t %d.%d Address %s part size=%,d bytes",i , ind, block.toString(), c.getLength()));
					}
					if (vInfo != null && vInfo.isVaddress()) {
						printAddressDetailedInfo(vInfo, index, (AddressRegion) p);
					}
				}
				i++;
			}


		} catch (IOException e) {
			System.err.println("File doesn't have valid structure : " + filename + " " + e.getMessage());
			throw e;
		}

	}

	/**
	 * @param ri
	 */
	private void printRouteEncodingRules(RouteRegion ri) {
		Map<String, Integer> mp = new HashMap<String, Integer>();
		int ind = 0;
		for (RouteTypeRule rtr : ri.routeEncodingRules) {
			if (rtr == null) {
				continue;
			}
			String t = rtr.getTag();
			if (t.contains(":")) {
				t = t.substring(0, t.indexOf(":"));
			}
			t += "-" + ind++;
			if (mp.containsKey(t)) {
				mp.put(t, mp.get(t) + 1);
			} else {
				mp.put(t, 1);
			}
		}
		List<String> tagvalues = new ArrayList<>(mp.keySet());
		tagvalues.sort(new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				return -Integer.compare(mp.get(o1), mp.get(o2));
			}
		});
		Map<String, Integer> fmt = new LinkedHashMap<String, Integer>();
		for (String key : tagvalues) {
			fmt.put(key, mp.get(key));
		}
		println(String.format("\tEncoding rules %d (%d KB): %s", ri.routeEncodingRules.size(), ri.routeEncodingRulesBytes / 1024, fmt.toString()));
	}

	private void printMapEncodingRules(MapIndex ri) {
		Map<String, Integer> mp = new HashMap<String, Integer>();
		TIntObjectIterator<TagValuePair> it = ri.decodingRules.iterator();
		while(it.hasNext()) {
//		for (TagValuePair rtr : ri.decodingRules.valueCollection()) {
			it.advance();
			TagValuePair rtr = it.value();
			if (rtr == null) {
				continue;
			}
			String t = rtr.tag;
			if (t.contains(":")) {
				t = t.substring(0, t.indexOf(":"));
			}
//			System.out.println(rtr.tag + " " + rtr.value);
//			t += "-" + rtr.value;
			if (mp.containsKey(t)) {
				mp.put(t, mp.get(t) + 1);
			} else {
				mp.put(t, 1);
			}
		}
		List<String> tagvalues = new ArrayList<>(mp.keySet());
		tagvalues.sort(new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				return -Integer.compare(mp.get(o1), mp.get(o2));
			}
		});
		Map<String, Integer> fmt = new LinkedHashMap<String, Integer>();

		for (String key : tagvalues) {
			fmt.put(key, mp.get(key));
		}
		println(String.format("\tEncoding rules %d (%d KB): %s", ri.decodingRules.size(), ri.encodingRulesSizeBytes / 1024, fmt.toString()));
	}

	private void printRouteDetailInfo(BinaryMapIndexReader index, RouteRegion p) throws IOException {
		final DamnCounter mapObjectsCounter = new DamnCounter();
		final StringBuilder b = new StringBuilder();
		List<RouteSubregion> regions = index.searchRouteIndexTree(
				BinaryMapIndexReader.buildSearchRequest(MapUtils.get31TileNumberX(vInfo.lonleft),
						MapUtils.get31TileNumberX(vInfo.lonright), MapUtils.get31TileNumberY(vInfo.lattop),
						MapUtils.get31TileNumberY(vInfo.latbottom), vInfo.getZoom(), null),
				vInfo.getZoom() < 15 ? p.getBaseSubregions() : p.getSubregions());
		if (vInfo.osm) {
			printToFile("<?xml version='1.0' encoding='UTF-8'?>\n" +
					"<osm version='0.6'>\n");
		}
		index.loadRouteIndexData(regions, new ResultMatcher<RouteDataObject>() {
			@Override
			public boolean publish(RouteDataObject obj) {
				mapObjectsCounter.value++;
				mapObjectsCounter.pntValue += obj.getPointsLength();
				if (vInfo.osm) {
					b.setLength(0);
					printOsmRouteDetails(obj, b);
					try {
						printToFile(b.toString());
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
				b.setLength(0);
				b.append("Road ");
				b.append(obj.id);
				b.append(" osmid ").append(obj.getId() >> (SHIFT_ID));

				for (int i = 0; i < obj.getTypes().length; i++) {
					RouteTypeRule rr = obj.region.quickGetEncodingRule(obj.getTypes()[i]);
					b.append(" ").append(rr.getTag()).append("='").append(rr.getValue()).append("'");
				}
				int[] nameIds = obj.getNameIds();
				if (nameIds != null) {
					for (int key : nameIds) {
						RouteTypeRule rr = obj.region.quickGetEncodingRule(key);
						b.append(" ").append(rr.getTag()).append("=\"").append(obj.getNames().get(key)).append("\"");
					}
				}
				int pointsLength = obj.getPointsLength();
				if(obj.hasPointNames() || obj.hasPointTypes()) {
					b.append(" pointtypes [");
					for (int i = 0; i < pointsLength; i++) {
						String[] names = obj.getPointNames(i);
						int[] nametypes = obj.getPointNameTypes(i);
						int[] types = obj.getPointTypes(i);
						if (types != null || names != null) {
							b.append("[" + (i + 1) + ". ");
							if (names != null) {
								for (int k = 0; k < names.length; k++) {
									RouteTypeRule rr = obj.region.quickGetEncodingRule(nametypes[k]);
									b.append(rr.getTag()).append("=\"").append(names[k]).append("\" ");
								}
							}
							if (types != null) {
								for (int k = 0; k < types.length; k++) {
									RouteTypeRule rr = obj.region.quickGetEncodingRule(types[k]);
									b.append(rr.getTag()).append("='").append(rr.getValue()).append("' ");
								}
							}
							if (vInfo.vmapCoordinates) {
								float x = (float) MapUtils.get31LongitudeX(obj.getPoint31XTile(i));
								float y = (float) MapUtils.get31LatitudeY(obj.getPoint31YTile(i));
								b.append(y).append(" / ").append(x).append(" ");
							}
							b.append("]");
						}
					}
					b.append("]");
				}
				if (obj.restrictions != null) {
					b.append(" restrictions [");
					for (int i = 0; i < obj.restrictions.length; i++) {
						if (i > 0) {
							b.append(", ");
						}
						b.append(obj.getRestrictionId(i)).append(" (").append(
								MapRenderingTypes.getRestrictionValue(
								obj.getRestrictionType(i))).append(")");
						if(obj.getRestrictionVia(i) != 0) {
							b.append(" via ").append(obj.getRestrictionVia(i));
						}

					}
					b.append(" ]");
				}
				if (vInfo.vmapCoordinates) {
					b.append(" lat/lon : ");
					for (int i = 0; i < obj.getPointsLength(); i++) {
						float x = (float) MapUtils.get31LongitudeX(obj.getPoint31XTile(i));
						float y = (float) MapUtils.get31LatitudeY(obj.getPoint31YTile(i));
						b.append(y).append(" / ").append(x).append(" , ");
					}
				}
				println(b.toString());
				return false;
			}

			@Override
			public boolean isCancelled() {
				return false;
			}
		});
		println("\tTotal map objects: " + mapObjectsCounter.value + " points " + mapObjectsCounter.pntValue);
		if (vInfo.osm) {
			printToFile("</osm >\n");
		}
	}

	private void printAddressDetailedInfo(VerboseInfo verbose, BinaryMapIndexReader index, AddressRegion region) throws IOException {
		for (CityBlocks type : CityBlocks.values()) {
			if (type == CityBlocks.UNKNOWN_TYPE) {
				continue;
			}
			final List<City> cities = index.getCities(region, null, type);
			
			print(String.format("\t %s %d entities", type.toString(), cities.size()));
			if (CityBlocks.CITY_TOWN_TYPE == type) {
				if (!verbose.vstreetgroups && !verbose.vcities) {
					println("");
					continue;
				}
			} else if (!verbose.vstreetgroups) {
				println("");
				continue;
			}
			println(":");
			for (City c : cities) {
				int size = 0;
				if (type != CityBlocks.BOUNDARY_TYPE) {
					size = index.preloadStreets(c, null);
				}
				List<Street> streets = new ArrayList<Street>(c.getStreets());
				String name = c.getName(verbose.lang);
				if (verbose.vcitynames) {
					boolean includeEnName = verbose.lang == null || !verbose.lang.equals("en");
					name += " " + c.getNamesMap(includeEnName).toString();
				}
				String bboxStr = "";
				if(c.getBbox31() != null) {
					bboxStr = String.format("%.5f, %.5f - %.5f, %.5f",
							MapUtils.get31LatitudeY(c.getBbox31()[1]),
							MapUtils.get31LongitudeX(c.getBbox31()[0]),
							MapUtils.get31LatitudeY(c.getBbox31()[3]),
							MapUtils.get31LongitudeX(c.getBbox31()[2]));
				}
				String cityDescription = (type == CityBlocks.POSTCODES_TYPE ?
						String.format("\t\t'%s' %d street(s) size %,d bytes %s", name, streets.size(), size, bboxStr) :
						String.format("\t\t'%s' [%d], %d street(s) size %,d bytes %s", name, c.getId(), streets.size(), size, bboxStr));
				print(cityDescription);
				if (!verbose.vstreets) {
					println("");
		            continue;
		        }
				println(":");
				if (!verbose.contains(c))
					continue;

				for (Street t : streets) {
					if (!verbose.contains(t))
						continue;
					index.preloadBuildings(t, null);
					final List<Building> buildings = t.getBuildings();
					final List<Street> intersections = t.getIntersectedStreets();

					println(MessageFormat.format("\t\t\t''{0}'' [{1,number,#}], {2,number,#} building(s), {3,number,#} intersections(s)",
							new Object[]{t.getName(verbose.lang), t.getId(), buildings.size(), intersections.size()}));
					if (buildings != null && !buildings.isEmpty() && verbose.vbuildings) {
						println("\t\t\t\tBuildings:");
						for (Building b : buildings) {
							println("\t\t\t\t" + b.getName(verbose.lang)
									+ (b.getPostcode() == null ? "" : " postcode:" + b.getPostcode()));
						}
					}

					if (intersections != null && !intersections.isEmpty() && verbose.vintersections) {
						println("\t\t\t\tIntersects with:");
						for (Street s : intersections) {
							println("\t\t\t\t\t" + s.getName(verbose.lang));
						}
					}
				}
			}
		}
	}

	private static class DamnCounter {
		int value;
		int pntValue;
	}

	private static class MapStatKey {
		String key = "";
		long statCoordinates;
		long statCoordinatesCount;
		long statObjectSize;
		int count;
		int namesLength;
	}

	private class MapStats {
		public int lastStringNamesSize;
		public int lastObjectIdSize;
		public int lastObjectHeaderInfo;
		public int lastObjectAdditionalTypes;
		public int lastObjectTypes;
		public int lastObjectCoordinates;
		public int lastObjectCoordinatesCount;
		public int lastObjectLabelCoordinates;
		public int lastObjectSize;

		private Map<String, MapStatKey> types = new LinkedHashMap<String, BinaryInspector.MapStatKey>();
		private SearchRequest<BinaryMapDataObject> req;

		public void processKey(String simpleString, MapObjectStat st, TIntObjectHashMap<String> objectNames,
		                       int coordinates, boolean names) {
			TIntObjectIterator<String> it = objectNames.iterator();
			int nameLen = 0;
			while (it.hasNext()) {
				it.advance();
				nameLen++;
				nameLen += it.value().length();
			}
			if (!types.containsKey(simpleString)) {
				MapStatKey stt = new MapStatKey();
				stt.key = simpleString;
				types.put(simpleString, stt);
			}
			MapStatKey key = types.get(simpleString);
			if (names) {
				key.namesLength += nameLen;
			} else {
				key.statCoordinates += st.lastObjectCoordinates;
				key.statCoordinatesCount += coordinates;
				key.statObjectSize += st.lastObjectSize;
				key.count++;
			}
		}


		public void process(BinaryMapDataObject obj) {
			MapObjectStat st = req.getStat();
			int cnt = 0;
			boolean names = st.lastObjectCoordinates == 0;
			if (!names) {
				this.lastStringNamesSize += st.lastStringNamesSize;
				this.lastObjectIdSize += st.lastObjectIdSize;
				this.lastObjectHeaderInfo += st.lastObjectHeaderInfo;
				this.lastObjectAdditionalTypes += st.lastObjectAdditionalTypes;
				this.lastObjectTypes += st.lastObjectTypes;
				this.lastObjectCoordinates += st.lastObjectCoordinates;
				this.lastObjectLabelCoordinates += st.lastObjectLabelCoordinates;
				cnt = obj.getPointsLength();
				this.lastObjectSize += st.lastObjectSize;
				if (obj.getPolygonInnerCoordinates() != null) {
					for (int[] i : obj.getPolygonInnerCoordinates()) {
						cnt += i.length;
					}
				}
				this.lastObjectCoordinatesCount += cnt;
			}
			for (int i = 0; i < obj.getTypes().length; i++) {
				int tp = obj.getTypes()[i];
				TagValuePair pair = obj.getMapIndex().decodeType(tp);
				if (pair == null) {
					continue;
				}
				processKey(pair.toSimpleString(), st, obj.getObjectNames(), cnt, names);
			}
			st.clearObjectStats();
			st.lastObjectSize = 0;

		}

		public void print() {
			MapObjectStat st = req.getStat();
			println("MAP BLOCK INFO:");
			long b = 0;
			b += out("Header", st.lastBlockHeaderInfo);
			b += out("String table", st.lastBlockStringTableSize);
			b += out("Map Objects", lastObjectSize);
			out("TOTAL", b);
			println("\nMAP OBJECTS INFO:");
			b = 0;
			b += out("Header", lastObjectHeaderInfo);
			b += out("Coordinates", lastObjectCoordinates);
			b += out("Label coordinates", lastObjectLabelCoordinates);
			b += out("Coordinates Count (pairs)", lastObjectCoordinatesCount);
			b += out("Types", lastObjectTypes);
			b += out("Additonal Types", lastObjectAdditionalTypes);
			b += out("Ids", lastObjectIdSize);
			b += out("String names", lastStringNamesSize);
			out("TOTAL", b);

			println("\n\nOBJECT BY TYPE STATS: ");
			ArrayList<MapStatKey> stats = new ArrayList<MapStatKey>(types.values());
			Collections.sort(stats, new Comparator<MapStatKey>() {

				@Override
				public int compare(MapStatKey o1, MapStatKey o2) {
					return compare(o1.statObjectSize, o2.statObjectSize);
				}

				public int compare(long x, long y) {
			        return -Long.compare(x, y);
			    }
			});

			for (MapStatKey s : stats) {
				println(String.format("%-35s [%7d] %8d KB: coord [%7d] %8d KB, names %8d KB  ",s.key, s.count,
						s.statObjectSize >> 10, s.statCoordinatesCount, s.statCoordinates >> 10, s.namesLength >> 10));
			}

		}

		private long out(String s, long i) {
			while (s.length() < 25) {
				s += " ";
			}
			DecimalFormat df = new DecimalFormat("0,000,000,000");
			println(s + ": " + df.format(i));
			return i;
		}


		public void setReq(SearchRequest<BinaryMapDataObject> req) {
			this.req = req;
		}

	}

	private void printMapDetailInfo(BinaryMapIndexReader index, MapIndex mapIndex) throws IOException {
		final StringBuilder b = new StringBuilder();
		final DamnCounter mapObjectsCounter = new DamnCounter();
		final MapStats mapObjectStats = new MapStats();
		if (vInfo.osm) {
			printToFile("<?xml version='1.0' encoding='UTF-8'?>\n" +
					"<osm version='0.6'>\n");
		}
		if (vInfo.isVStats()) {
			BinaryMapIndexReader.READ_STATS = true;
		}
		final SearchRequest<BinaryMapDataObject> req = BinaryMapIndexReader.buildSearchRequest(
				MapUtils.get31TileNumberX(vInfo.lonleft),
				MapUtils.get31TileNumberX(vInfo.lonright),
				MapUtils.get31TileNumberY(vInfo.lattop),
				MapUtils.get31TileNumberY(vInfo.latbottom),
				vInfo.getZoom(),
				new SearchFilter() {
					@Override
					public boolean accept(TIntArrayList types, MapIndex index) {
						return true;
					}
				},
				new ResultMatcher<BinaryMapDataObject>() {
					@Override
					public boolean publish(BinaryMapDataObject obj) {
						mapObjectsCounter.value++;
						mapObjectsCounter.pntValue += obj.getPointsLength();
						if (vInfo.isVStats()) {
							mapObjectStats.process(obj);
						} else if (vInfo.vmapObjects) {
							b.setLength(0);
							if (vInfo.osm) {
								printOsmMapDetails(obj, b);
								try {
									printToFile(b.toString());
								} catch (IOException e) {
									throw new RuntimeException(e);
								}
							} else {
								printMapDetails(obj, b, vInfo.vmapCoordinates);
								println(b.toString());
							}
						}
						return false;
					}

					@Override
					public boolean isCancelled() {
						return false;
					}
				});
		if (vInfo.vstats) {
			mapObjectStats.setReq(req);
		}
		index.searchMapIndex(req, mapIndex);
		if (vInfo.osm) {
			printToFile("</osm >\n");
		}
		if (vInfo.vstats) {
			mapObjectStats.print();
		}
		println("\tTotal map objects: " + mapObjectsCounter.value);
	}


	public static void printMapDetails(BinaryMapDataObject obj, StringBuilder b, boolean vmapCoordinates) {
		boolean multipolygon = obj.getPolygonInnerCoordinates() != null && obj.getPolygonInnerCoordinates().length > 0;
		if (multipolygon) {
			b.append("Multipolygon");
		} else {
			b.append(obj.isArea() ? "Area" : (obj.getPointsLength() > 1 ? "Way" : "Point"));
		}
		int[] types = obj.getTypes();
		if(obj.isLabelSpecified()) {
			b.append(" ").append(new LatLon(MapUtils.get31LatitudeY(obj.getLabelY()),
					MapUtils.get31LongitudeX(obj.getLabelX())));
		}

		b.append(" types [");
		for (int j = 0; j < types.length; j++) {
			if (j > 0) {
				b.append(", ");
			}
			TagValuePair pair = obj.getMapIndex().decodeType(types[j]);
			if (pair == null) {
				System.err.println("Type " + types[j] + "was not found");
				continue;
//								throw new NullPointerException("Type " + obj.getAdditionalTypes()[j] + "was not found");
			}
			b.append(pair.toSimpleString() + " (" + types[j] + ")");
		}
		b.append("]");
		if (obj.getAdditionalTypes() != null && obj.getAdditionalTypes().length > 0) {
			b.append(" add_types [");
			for (int j = 0; j < obj.getAdditionalTypes().length; j++) {
				if (j > 0) {
					b.append(", ");
				}
				TagValuePair pair = obj.getMapIndex().decodeType(obj.getAdditionalTypes()[j]);
				if (pair == null) {
					System.err.println("Type " + obj.getAdditionalTypes()[j] + "was not found");
					continue;
//									throw new NullPointerException("Type " + obj.getAdditionalTypes()[j] + "was not found");
				}
				b.append(pair.toSimpleString() + "(" + obj.getAdditionalTypes()[j] + ")");

			}
			b.append("]");
		}
		TIntObjectHashMap<String> names = obj.getObjectNames();
		TIntArrayList order = obj.getNamesOrder();
		if (names != null && !names.isEmpty()) {
			b.append(" Names [");
			// int[] keys = names.keys();
			for (int j = 0; j < order.size(); j++) {
				if (j > 0) {
					b.append(", ");
				}
				TagValuePair pair = obj.getMapIndex().decodeType(order.get(j));
				if (pair == null) {
					throw new NullPointerException("Type " + order.get(j) + " was not found");
				}
				b.append(pair.toSimpleString() + "(" + order.get(j) + ")");
				b.append(" - ").append(names.get(order.get(j)));
			}
			b.append("]");
		}

		b.append(" id ").append(obj.getId());
		b.append(" osmid ").append((obj.getId() >> (SHIFT_ID + 1)));
		if (obj.getId() > ObfConstants.PROPAGATE_NODE_BIT && obj.getId() < ObfConstants.RELATION_BIT) {
			long wayId = (obj.getId() & ((1L << ObfConstants.SHIFT_PROPAGATED_NODE_IDS) - 1)) >> ObfConstants.SHIFT_PROPAGATED_NODES_BITS;
			wayId = wayId >> 1;
			b.append(" (propagate from way: " + wayId + ") ");
		}
		if (vmapCoordinates) {
			b.append(" lat/lon : ");
			for (int i = 0; i < obj.getPointsLength(); i++) {
				float x = (float) MapUtils.get31LongitudeX(obj.getPoint31XTile(i));
				float y = (float) MapUtils.get31LatitudeY(obj.getPoint31YTile(i));
				b.append(y).append(" / ").append(x).append(" , ");
			}
		}

	}


	private static int OSM_ID = 1;

	private void printOsmRouteDetails(RouteDataObject obj, StringBuilder b) {
		StringBuilder tags = new StringBuilder();
		int[] types = obj.getTypes();
		for (int j = 0; j < types.length; j++) {
			RouteTypeRule rt = obj.region.quickGetEncodingRule(types[j]);
			if (rt == null) {
				throw new NullPointerException("Type " + types[j] + "was not found");
			}
			String value = quoteName(rt.getValue());
			tags.append("\t<tag k='").append(rt.getTag()).append("' v='").append(value).append("' />\n");
		}
		TIntObjectHashMap<String> names = obj.getNames();
		if (names != null && !names.isEmpty()) {
			int[] keys = names.keys();
			for (int j = 0; j < keys.length; j++) {
				RouteTypeRule rt = obj.region.quickGetEncodingRule(keys[j]);
				if (rt == null) {
					throw new NullPointerException("Type " + keys[j] + "was not found");
				}
				String name = quoteName(names.get(keys[j]));
				tags.append("\t<tag k='").append(rt.getTag()).append("' v='").append(name).append("' />\n");
			}
		}

		tags.append("\t<tag k=\'").append("original_id").append("' v='").append(obj.getId() >> (SHIFT_ID))
				.append("'/>\n");
		tags.append("\t<tag k=\'").append("osmand_id").append("' v='").append(obj.getId()).append("'/>\n");

		TLongArrayList ids = new TLongArrayList();
		for (int i = 0; i < obj.getPointsLength(); i++) {
			float lon = (float) MapUtils.get31LongitudeX(obj.getPoint31XTile(i));
			float lat = (float) MapUtils.get31LatitudeY(obj.getPoint31YTile(i));
			int id = OSM_ID++;
			b.append("\t<node id = '" + id + "' version='1' lat='" + lat + "' lon='" + lon + "' >\n");
			if (obj.getPointNames(i) != null) {
				String[] vs = obj.getPointNames(i);
				int[] keys = obj.getPointNameTypes(i);
				for (int j = 0; j < keys.length; j++) {
					RouteTypeRule rt = obj.region.quickGetEncodingRule(keys[j]);
					String name = quoteName(vs[j]);
					b.append("\t\t<tag k='").append(rt.getTag()).append("' v='").append(name).append("' />\n");
				}
			}
			if (obj.getPointTypes(i) != null) {
				int[] keys = obj.getPointTypes(i);
				for (int j = 0; j < keys.length; j++) {
					RouteTypeRule rt = obj.region.quickGetEncodingRule(keys[j]);
					String value = quoteName(rt.getValue());
					b.append("\t\t<tag k='").append(rt.getTag()).append("' v='").append(value).append("' />\n");
				}
			}
			b.append("\t</node >\n");
			ids.add(id);
		}
		long idway = printWay(ids, b, tags);
		if(obj.getRestrictionLength() > 0) {
			for(int i = 0; i < obj.getRestrictionLength(); i ++) {
				long ld = obj.getRestrictionId(i);
				String tp = MapRenderingTypes.getRestrictionValue(obj.getRestrictionType(i));
				int id = OSM_ID++;
				b.append("<relation id = '" + id + "' version='1'>\n");
				b.append("\t<member ref='").append(idway).append("' role='from' type='way' />\n");
				b.append("\t<tag k='").append("from_osmand_id").append("' v='").append(obj.getId()).append("' />\n");
				b.append("\t<tag k='").append("from_id").append("' v='").append(obj.getId() >> SHIFT_ID).append("' />\n");
				b.append("\t<tag k='").append("to_osmand_id").append("' v='").append(ld).append("' />\n");
				b.append("\t<tag k='").append("to_id").append("' v='").append(ld >> SHIFT_ID).append("' />\n");
				b.append("\t<tag k='").append("type").append("' v='").append("restriction").append("' />\n");
				b.append("\t<tag k='").append("restriction").append("' v='").append(tp).append("' />\n");
				b.append("</relation>\n");
			}
		}
	}

	private String quoteName(String name) {
		if(name == null || name.length() == 0) {
			return "EMPTY";
		}
		name = name.replace("'", "&apos;");
		name = name.replace("<", "&lt;");
		name = name.replace(">", "&gt;");
		name = name.replace("&", "&amp;");
		return name;
	}

	private void printOsmMapDetails(BinaryMapDataObject obj, StringBuilder b) {
		boolean multipolygon = obj.getPolygonInnerCoordinates() != null && obj.getPolygonInnerCoordinates().length > 0;
		boolean point = obj.getPointsLength() == 1;
		StringBuilder tags = new StringBuilder();
		int[] types = obj.getTypes();
		for (int j = 0; j < types.length; j++) {
			TagValuePair pair = obj.getMapIndex().decodeType(types[j]);
			if (pair == null) {
				throw new NullPointerException("Type " + types[j] + "was not found");
			}
			tags.append("\t<tag k='").append(pair.tag).append("' v='").append(quoteName(pair.value)).append("' />\n");
		}

		if (obj.getAdditionalTypes() != null && obj.getAdditionalTypes().length > 0) {
			for (int j = 0; j < obj.getAdditionalTypes().length; j++) {
				int addtype = obj.getAdditionalTypes()[j];
				TagValuePair pair = obj.getMapIndex().decodeType(addtype);
				if (pair == null) {
					throw new NullPointerException("Type " + obj.getAdditionalTypes()[j] + "was not found");
				}
				tags.append("\t<tag k='").append(pair.tag).append("' v='").append(quoteName(pair.value)).append("' />\n");
			}
		}
		TIntObjectHashMap<String> names = obj.getObjectNames();
		if (names != null && !names.isEmpty()) {
			int[] keys = names.keys();
			for (int j = 0; j < keys.length; j++) {
				TagValuePair pair = obj.getMapIndex().decodeType(keys[j]);
				if (pair == null) {
					throw new NullPointerException("Type " + keys[j] + "was not found");
				}
				String name = names.get(keys[j]);
				name = quoteName(name);
				tags.append("\t<tag k='").append(pair.tag).append("' v='").append(name).append("' />\n");
			}
		}

		tags.append("\t<tag k=\'").append("original_id").append("' v='").append(obj.getId() >> (SHIFT_ID + 1)).append("'/>\n");
		tags.append("\t<tag k=\'").append("osmand_id").append("' v='").append(obj.getId()).append("'/>\n");

		if(point) {
			float lon= (float) MapUtils.get31LongitudeX(obj.getPoint31XTile(0));
			float lat = (float) MapUtils.get31LatitudeY(obj.getPoint31YTile(0));
			b.append("<node id = '" + OSM_ID++ + "' version='1' lat='" + lat + "' lon='" + lon + "' >\n");
			b.append(tags);
			b.append("</node>\n");
		} else {
			TLongArrayList innerIds = new TLongArrayList();
			TLongArrayList ids = new TLongArrayList();
			for (int i = 0; i < obj.getPointsLength(); i++) {
				float lon = (float) MapUtils.get31LongitudeX(obj.getPoint31XTile(i));
				float lat = (float) MapUtils.get31LatitudeY(obj.getPoint31YTile(i));
				int id = OSM_ID++;
				b.append("\t<node id = '" + id + "' version='1' lat='" + lat + "' lon='" + lon + "' />\n");
				ids.add(id);
			}

			long outerId = printWay(ids, b, multipolygon ? null : tags);
			if (multipolygon) {
				int[][] polygonInnerCoordinates = obj.getPolygonInnerCoordinates();
				for (int j = 0; j < polygonInnerCoordinates.length; j++) {
					ids.clear();
					for (int i = 0; i < polygonInnerCoordinates[j].length; i += 2) {
						float lon = (float) MapUtils.get31LongitudeX(polygonInnerCoordinates[j][i]);
						float lat = (float) MapUtils.get31LatitudeY(polygonInnerCoordinates[j][i + 1]);
						int id = OSM_ID++;
						b.append("<node id = '" + id + "' version='1' lat='" + lat + "' lon='" + lon + "' />\n");
						ids.add(id);
					}
					innerIds.add(printWay(ids, b, null));
				}
				int id = OSM_ID++;
				b.append("<relation id = '" + id + "' version='1'>\n");
				b.append(tags);
				b.append("\t<member type='way' role='outer' ref= '" + outerId + "'/>\n");
				TLongIterator it = innerIds.iterator();
				while (it.hasNext()) {
					long ref = it.next();
					b.append("<member type='way' role='inner' ref= '" + ref + "'/>\n");
				}
				b.append("</relation>\n");
			}
		}

	}


	private long printWay(TLongArrayList ids, StringBuilder b, StringBuilder tags) {
		int id = OSM_ID++;
		b.append("<way id = '" + id + "' version='1'>\n");
		if (tags != null) {
			b.append(tags);
		}
		TLongIterator it = ids.iterator();
		while (it.hasNext()) {
			long ref = it.next();
			b.append("\t<nd ref = '" + ref + "'/>\n");
		}
		b.append("</way>\n");
		return id;
	}

	private void printTransportDetailInfo(VerboseInfo verbose, BinaryMapIndexReader index, TransportIndex p) throws IOException {
		SearchRequest<TransportStop> sr = BinaryMapIndexReader.buildSearchTransportRequest(
				MapUtils.get31TileNumberX(verbose.lonleft),
				MapUtils.get31TileNumberX(verbose.lonright),
				MapUtils.get31TileNumberY(verbose.lattop),
				MapUtils.get31TileNumberY(verbose.latbottom),
				-1, null);
		List<TransportStop> stops = index.searchTransportIndex(sr);
		Map<Long, TransportRoute> rs = new LinkedHashMap<>();
		List<String> lrs = new ArrayList<>();
		println("\nStops:");
		for (TransportStop s : stops) {
			lrs.clear();
			for (long pnt : s.getReferencesToRoutes()) {
				TransportRoute route;
				if (!rs.containsKey(pnt)) {
					TLongObjectHashMap<TransportRoute> pts = index.getTransportRoutes(new long[] { pnt });
					route = pts.valueCollection().iterator().next();
					rs.put(pnt, route);
				} else {
					route = rs.get(pnt);
				}
				if (route != null) {
					//lrs.add(route.getRef() + " " + route.getName(verbose.lang));
					lrs.add(route.getRef() + " " + route.getType());
				}
			}
			if(s.getDeletedRoutesIds() != null) {
				for (long l : s.getDeletedRoutesIds()) {
					lrs.add(" -" + (l / 2));
				}
			}
			String exitsString = s.getExitsString();
			println("  " + s.getName(verbose.lang) + ": " + lrs + " " + s.getLocation() + exitsString + " " + s.getId());
		}
		println("\nRoutes:");
		for(TransportRoute st : rs.values()) {
			List<String> stopsString = new ArrayList<>();
			for (TransportStop stop : st.getForwardStops()) {
				stopsString.add(stop.getName(verbose.lang));
			}
			Map<String, String> tags = st.getTags();
			StringBuilder tagString = new StringBuilder();
			if (tags != null) {
				for (Map.Entry<String, String> tag : tags.entrySet()) {
					tagString.append(tag.getKey()).append(":").append(tag.getValue()).append(" ");
				}
			}
			println("  " + st.getRef() + " " + st.getType() + " " + st.getName(verbose.lang) + ": " + stopsString + " " + tagString);
			if (verbose.vtransportschedule) {
				TransportSchedule sc = st.getSchedule();
				if (sc != null) {
					StringBuilder bld = new StringBuilder();
					int[] tripIntervalsList = sc.getTripIntervals();
					int prevTime = 0;
					for (int i : tripIntervalsList) {
						i = i + prevTime;
						String tm = TransportRoutePlanner.formatTransportTime(i);
						bld.append(tm);
						prevTime = i;
					}
					println("   " + bld.toString());
					bld = new StringBuilder();
					int atm = tripIntervalsList[0];
					int[] avgStopIntervals = sc.getAvgStopIntervals();
					int[] avgWaitIntervals = sc.getAvgWaitIntervals();
					for(int k = 0; k < st.getForwardStops().size(); k++) {
						TransportStop stp = st.getForwardStops().get(k);
						if(k == 0) {
							bld.append(String.format("%6.6s %s, ", stp.getName(), TransportRoutePlanner.formatTransportTime(atm)));
						} else {
							atm += avgStopIntervals[k - 1];
							if(avgWaitIntervals.length > k && avgWaitIntervals[k] > 0)  {
								bld.append(String.format("%6.6s %s - %s, ", stp.getName(), TransportRoutePlanner.formatTransportTime(atm),
										TransportRoutePlanner.formatTransportTime(avgWaitIntervals[k] + atm)));
							} else {
								bld.append(String.format("%6.6s %s, ", stp.getName(), TransportRoutePlanner.formatTransportTime(atm)));
							}
						}
					}
					// %3.3s
					println("   " + bld.toString());
				}
			}
		}
	}



	private void printPOIDetailInfo(VerboseInfo verbose, BinaryMapIndexReader index, PoiRegion p) throws IOException {
		int[] count = new int[3];
		RoutingContext ctx = GeocodingUtilities.buildDefaultContextForPOI(index);
		GeocodingUtilities geocodingUtilities = new GeocodingUtilities();
		SearchRequest<Amenity> req = BinaryMapIndexReader.buildSearchPoiRequest(
				MapUtils.get31TileNumberX(verbose.lonleft),
				MapUtils.get31TileNumberX(verbose.lonright),
				MapUtils.get31TileNumberY(verbose.lattop),
				MapUtils.get31TileNumberY(verbose.latbottom),
				verbose.getZoom(),
				BinaryMapIndexReader.ACCEPT_ALL_POI_TYPE_FILTER,
				new ResultMatcher<>() {
					@Override
					public boolean publish(Amenity amenity) {
						count[0]++;
						String s = String.valueOf(amenity.printNamesAndAdditional());
						long id = (amenity.getId());
						if(id > 0) {
							id = id >> 1;
						}
						Map<Integer, List<TagValuePair>> tagGroups = amenity.getTagGroups();
						if (tagGroups != null) {
							s += " cities:";
							for (Map.Entry<Integer, List<TagValuePair>> entry : tagGroups.entrySet()) {
								s += "[";
								for (TagValuePair p : entry.getValue()) {
									s += p.tag + "=" + p.value + " ";
								}
								s += "]";
							}
						}
						println(amenity.getType().getKeyName() + ": " + amenity.getSubType() + " " + amenity.getName() +
								" " + amenity.getLocation() + " osmid=" + id + " " + s);
						if(!Algorithms.isEmpty(amenity.getStreetName())) {
							count[1] ++; 
						} else if (!Algorithms.isEmpty(amenity.getName())) {
							count[2]++;
						}
						return false;
					}

					@Override
					public boolean isCancelled() {
						return false;
					}
				});

		index.initCategories(p);
		println("\tRegion: " + p.getName());

		println("\t\tBounds " + formatLatBounds(MapUtils.get31LongitudeX(p.getLeft31()),
				MapUtils.get31LongitudeX(p.getRight31()),
				MapUtils.get31LatitudeY(p.getTop31()),
				MapUtils.get31LatitudeY(p.getBottom31())));
		println("\t\tCategories:");
		List<String> cs = p.getCategories();
		List<List<String>> subcategories = p.getSubcategories();
		for (int i = 0; i < cs.size(); i++) {
			println(String.format("\t\t\t%s (%d): %s", cs.get(i), subcategories.get(i).size(), subcategories.get(i)));
		}
		println("\t\tPOI Additionals:");
		List<PoiSubType> subtypes = p.getSubTypes();
		Set<String> text = new TreeSet<String>();
		Set<String> refs = new TreeSet<String>();
		Map<String, List<String>> singleValues = new TreeMap<String, List<String>>();
		int singleVals = 0;
		MapPoiTypes poiTypes = MapPoiTypes.getDefault();
		for (int i = 0; i < subtypes.size(); i++) {
			PoiSubType st = subtypes.get(i);
			if (st.text) {
				PoiType ref = poiTypes.getPoiTypeByKey(st.name);
				if(ref != null && !ref.isAdditional()) {
					refs.add(st.name);
				} else {
					text.add(st.name);
				}
			} else if (st.possibleValues.size() == 1) {
				singleVals++;
				int lastIndexOf = st.name.lastIndexOf('_');
				String key = st.name;
				if (lastIndexOf >= 0) {
					key = key.substring(0, lastIndexOf);
				}
				if (!singleValues.containsKey(key)) {
					singleValues.put(key, new ArrayList<String>());
				}
				singleValues.get(key).add(st.name);
			} else {
				println(String.format("\t\t\t%s (%d): %s",  st.name, st.possibleValues.size(), st.possibleValues));
			}
		}
		StringBuilder singleValuesFmt = new StringBuilder();
		for(String key : singleValues.keySet()) {
			singleValuesFmt.append(key + " (" + singleValues.get(key).size()+ "), ");
		}
		println(String.format("\t\t\tReference to another poi (incorrect?) (%d): %s",  refs.size(), refs));
		println(String.format("\t\t\tText based (%d): %s",  text.size(), text));
		println(String.format("\t\t\tSingle value filters (%d): %s",  singleVals, singleValuesFmt));
//		req.poiTypeFilter = null;//for test only
		index.searchPoi(p, req);
		
		println(String.format("Found %d pois (%d with addr, %d with name without addr)", count[0],
				count[1], count[2]));
	}

	public static void printUsage(String warning) {
		if (warning != null) {
			System.out.println(warning);
		}
		System.out.println("Inspector is console utility for working with binary indexes of OsmAnd.");
		System.out.println("It allows print info about file, extract parts and merge indexes.");
		System.out.println("\nUsage for print info : inspector [-vaddress] [-vcities] [-vcitynames] [-vstreetgroups] [-vstreets] [-vbuildings] [-vintersections] [-vmap] [-vstats] [-vmapobjects] [-vmapcoordinates] [-osm] [-vpoi] [-vrouting] [-vhhrouting] [-vtransport] [-zoom=Zoom] [-bbox=LeftLon,TopLat,RightLon,BottomLat] [file]");
		System.out.println("  Prints information about [file] binary index of OsmAnd.");
		System.out.println("  -v.. more verbose output (like all cities and their streets or all map objects with tags/values and coordinates)");
		System.out.println("\nUsage for combining indexes : inspector -c file_to_create (file_from_extract ((+|-)parts_to_extract)? )* [--date=...]");
		System.out.println("\tCreate new file of extracted parts from input file. [parts_to_extract] could be parts to include or exclude.");
		System.out.println("\nUse optional argument --date=YYYY-MM-DDhh:mm to specify exact edition-date of the output file\n");
		System.out.println("  Example : inspector -c output_file input_file +1,2,3\n\tExtracts 1, 2, 3 parts (could be find in print info)");
		System.out.println("  Example : inspector -c output_file input_file -2,3\n\tExtracts all parts excluding 2, 3");
		System.out.println("  Example : inspector -c output_file input_file1 input_file2 input_file3\n\tSimply combine 3 files");
		System.out.println("  Example : inspector -c output_file input_file1 input_file2 -4\n\tCombine all parts of 1st file and all parts excluding 4th part of 2nd file");
		System.out.println("  Example : inspector -c output_file input_file1 +routing\n\tCopy only routing parts (supports address, poi, routing, hhrouting, transport, map)");
	}

}
