package net.osmand;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import net.osmand.binary.BinaryIndexPart;
import net.osmand.binary.BinaryMapAddressReaderAdapter;
import net.osmand.binary.BinaryMapAddressReaderAdapter.AddressRegion;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.data.City;
import net.osmand.data.MapObject;
import net.osmand.data.Street;
import net.osmand.util.MapUtils;

import org.apache.commons.logging.Log;

public class BinaryComparator {

	public static final int BUFFER_SIZE = 1 << 20;
	private final static Log log = PlatformUtil.getLog(BinaryComparator.class);

	public static void main(String[] args) throws IOException {
		BinaryComparator in = new BinaryComparator();
		// test cases show info
		if (args.length == 1 && "test".equals(args[0])) {
			in.compare(new String[]{
					System.getProperty("maps.dir") +"Ukraine_europe_2.road.obf",
					System.getProperty("maps.dir") + "Ukraine_europe_2_all.road.obf"
					});
		} else {
			in.compare(args);
		}
	}

	private void compare(String[] args) throws IOException {
		BinaryMapIndexReader[] indexes = new BinaryMapIndexReader[args.length];
		RandomAccessFile[] rafs = new RandomAccessFile[args.length];
		for(int i = 0; i < args.length; i++) {
			rafs[i] = new RandomAccessFile(args[i], "r");
			indexes[i] = new BinaryMapIndexReader(rafs[i], new File(args[i]));
		}
		AddressRegion r0 = getAddressRegion(indexes[0]);
		AddressRegion r1 = getAddressRegion(indexes[1]);
		compare(r0, indexes[0], r1, indexes[1]);
		
	}

	private void compare(AddressRegion r0, BinaryMapIndexReader i0, AddressRegion r1, BinaryMapIndexReader i1) throws IOException {
		for (int cityType : BinaryMapAddressReaderAdapter.CITY_TYPES) {
			List<City> ct0 = i0.getCities(null, cityType);
			List<City> ct1 = i1.getCities(null, cityType);
			Comparator<City> c = comparator();
			Collections.sort(ct0, c);
			Collections.sort(ct1, c);
			int i = 0;
			int j = 0;
			System.out.println("CITY TYPE: " + cityType);
			while(i < ct0.size() || j < ct1.size()) {
				City c0 = i >= ct0.size() ? null : ct0.get(i);
				City c1 = j >= ct1.size() ? null : ct1.get(j);
				int cmp = c.compare(c0, c1); 
				if(cmp < 0) {
					while (c.compare(c0, c1) < 0) {
						System.out.println("(1). Extra city in 1st file: " + c0);
						i++;
						c0 = i >= ct0.size() ? null : ct0.get(i);
					}
				} else if (cmp > 0) {
					while (c.compare(c0, c1) > 0) {
						System.out.println("(1). Extra city in 2nd file: " + c1 );
						j++;
						c1 = j >= ct1.size() ? null : ct1.get(j);
					}
				} else {
					//System.out.println("Same city " + c1.getName() );
					i++;
					j++;	
					i0.preloadStreets(c0, null);
					i1.preloadStreets(c1, null);
					if(c0.getStreets().size() != c1.getStreets().size()) {
						System.out.println("(2). City streets " + c1 + ":  " + c0.getStreets().size() + " != " + c1.getStreets().size());
						List<String> s0 = new ArrayList<String>();
						List<String> s1 = new ArrayList<String>();
						for(Street s : c0.getStreets()) {
							if(c1.getStreetByName(s.getName()) == null) {
								s0.add(s.getName());
							}
						}
						for(Street s : c1.getStreets()) {
							if(c0.getStreetByName(s.getName()) == null) {
								s1.add(s.getName());
							}
						}
						if(s0.isEmpty() && s1.isEmpty()) {
							System.out.println("(2)? " + c0.getStreets());
						} else {
							System.out.println("(2).. " + s0 + "!=" + s1);
						}
					} else {
						// compare streets
						// TODO (3) BUILDINGS INEQUAL + POSTCODE
						// TODO (4) Intersections
					}
				}
			}
		}
		// TODO (5) Compare by name index
	}

	private Comparator<City> comparator() {
		return new Comparator<City>() {
			Collator collator = OsmAndCollator.primaryCollator();
			@Override
			public int compare(City o1, City o2) {
				int c = collator.compare(strip(o1.getName()), strip(o2.getName()));
				if(c == 0) {
					if(MapUtils.getDistance(o1.getLocation(), o2.getLocation())  < 5500) {
						return 0;
					}
					c = Double.compare(MapUtils.getDistance(o1.getLocation(), 0, 0),
							MapUtils.getDistance(o2.getLocation(), 0, 0));
				}
				return c;
			}
			private String strip(String name) {
				return name.indexOf('(') != -1 ? name.substring(0, name.indexOf('(')).trim() : name;
			}
		};
	}


	private AddressRegion getAddressRegion(BinaryMapIndexReader i) {
		List<BinaryIndexPart> list = i.getIndexes();
		for(BinaryIndexPart p : list) {
			if(p instanceof AddressRegion) {
				return (AddressRegion) p;
			}
		}
		return null;
	}

	
}
