package net.osmand;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import net.osmand.binary.BinaryIndexPart;
import net.osmand.binary.BinaryMapAddressReaderAdapter;
import net.osmand.binary.BinaryMapAddressReaderAdapter.AddressRegion;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.data.City;
import net.osmand.data.MapObject;
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
						System.out.println("Extra city in 1st file: " + c0.getName() + " " + c0.getLocation() + " " + c1);
						i++;
						c0 = i >= ct0.size() ? null : ct0.get(i);
					}
				} else if (cmp > 0) {
					while (c.compare(c0, c1) > 0) {
						System.out.println("Extra city in 2nd file: " + c1.getName() + " " + c1.getLocation() + " " + c0);
						j++;
						c1 = j >= ct1.size() ? null : ct1.get(j);
					}
				} else {
					//System.out.println("Same city " + c1.getName() );
					i++;
					j++;	
				}
			}
		}
	}

	private Comparator<City> comparator() {
		return new Comparator<City>() {

			@Override
			public int compare(City o1, City o2) {
				int c = MapObject.BY_NAME_COMPARATOR.compare(o1, o2);
				if(c == 0) {
					c = Double.compare(MapUtils.getDistance(o1.getLocation(), 0, 0),
							MapUtils.getDistance(o2.getLocation(), 0, 0));
				}
				return c;
			}
		};
	}

	private int compare(City c0, City c1) {
		if(c0 == null) {
			return c1 == null ? 0 : 1;
		} else if(c1 == null) {
			return -1;
		}
		int cc = c0.getName().compareTo(c1.getName());
		if(cc == 0) {
			cc = MapUtils.getDistance(c0.getLocation(), c1.getLocation()) < 1000 ? 0 : 1;
		}
		return cc;
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
