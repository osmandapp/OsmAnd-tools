package net.osmand.obf.preparation;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

import gnu.trove.set.hash.TLongHashSet;
import net.osmand.binary.BinaryMapAddressReaderAdapter.AddressRegion;
import net.osmand.binary.BinaryMapAddressReaderAdapter.CityBlocks;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapPoiReaderAdapter.PoiRegion;
import net.osmand.binary.NameIndexReader;
import net.osmand.binary.NameIndexReader.PrefixNameValue;
import net.osmand.binary.ObfConstants;
import net.osmand.binary.OsmandOdb.AddressNameIndexDataAtom;
import net.osmand.binary.OsmandOdb.OsmAndPoiNameIndexDataAtom;
import net.osmand.data.Amenity;
import net.osmand.data.City;
import net.osmand.data.MapObject;
import net.osmand.data.Multipolygon;
import net.osmand.data.MultipolygonBuilder;
import net.osmand.data.QuadRect;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Relation.RelationMember;
import net.osmand.osm.edit.Way;
import net.osmand.util.MapUtils;
import net.osmand.util.SearchAlgorithms;

public class OverpassFetcherTest {

	
	public static void main(String[] args) throws IOException {
		String base = "/Users/victorshcherb/osmand/maps/";
		File f = new File(base + "France_occitania_haute-garonne_europe_2.obf");
//		File f = new File(base + "Ukraine_kyiv_europe_2.obf");
		BinaryMapIndexReader bmir = new BinaryMapIndexReader(new RandomAccessFile(f, "r"), f);
		for (AddressRegion a : bmir.getAddressIndexes()) {
//			processAddress(bmir, new NameIndexReader(a));
		}
		for (PoiRegion a : bmir.getPoiIndexes()) {
			processPoi(bmir, new NameIndexReader(a));
		}
	}
	
	private static void processPoi(BinaryMapIndexReader bmir, NameIndexReader nir) throws IOException {
		String query = ".";
		nir.setQuery(query);
		bmir.readFullNameIndex(nir);
		TLongHashSet ids = new TLongHashSet();
		for (PrefixNameValue p : nir.getMatchedPrefixes(query)) {
			List<OsmAndPoiNameIndexDataAtom> atomList = p.poi.getAtomsList();
			for (OsmAndPoiNameIndexDataAtom atom : atomList) {
				if (atom.hasBbox()) {
					int x16 = atom.getX();
					int y16 = atom.getY();
					int[] bbox = SearchAlgorithms.decodeBboxForNameAtomsBytes(atom.getBbox(), x16, y16);
					int shift = BinaryMapIndexReader.convertFixed32ToRef(atom.getShiftTo());
					int poiInd = atom.getPoiIndInBlock(0);
					long intId = shift + poiInd;
					if (!ids.add(intId)) {
						continue;
					}
					List<Amenity> amenities = bmir.readAmenityBlock(nir.poiRegion, shift, poiInd);
					Amenity obj = amenities.get(poiInd);
					analyze(obj, bbox, intId);
				}
			}
		}
	}

	private static void processAddress(BinaryMapIndexReader bmir, NameIndexReader nir) throws IOException {
		String query = ".";
		nir.setQuery(query);
		bmir.readFullNameIndex(nir);
		TLongHashSet ids = new TLongHashSet();
		for (PrefixNameValue p : nir.getMatchedPrefixes(query)) {
			List<AddressNameIndexDataAtom> atomList = p.addr.getAtomList();
			for (AddressNameIndexDataAtom atom : atomList) {
				if (atom.hasBbox()) {
					int xy16 = atom.getXy16(0);
					int x16 = (xy16 >>> 16);
					int y16 = (xy16 & ((1 << 16) - 1));
					int[] bbox = SearchAlgorithms.decodeBboxForNameAtomsBytes(atom.getBbox(), x16, y16);
					if (atom.getType() != CityBlocks.STREET_TYPE.index) {
						long shift = p.shift - atom.getShiftToIndex(0);
						if (!ids.add(shift)) {
							continue;
						}
						City obj = bmir.readCityObject(nir.addressRegion, shift);
						analyze(obj, bbox, shift);
					}
				}
			}
		}
	}

	static int IND = 0;
	private static void analyze(MapObject obj, int[] bbox, long intId) {
		OverpassFetcher inst = OverpassFetcher.getInstance();
		long osmId = ObfConstants.getOsmObjectId(obj);
		EntityType typ = ObfConstants.getOsmEntityType(obj);
		MultipolygonBuilder mb = new MultipolygonBuilder();
		Entity e;
		if (typ == EntityType.RELATION) {
			Relation relation = new Relation(osmId);
			inst.fetchCompleteGeometryRelation(relation, null, 0l);
			for (RelationMember es : ((Relation) relation).getMembers()) {
				if (es.getEntity() instanceof Way) {
					boolean inner = "inner".equals(es.getRole()); //$NON-NLS-1$
					if (inner) {
						mb.addInnerWay((Way) es.getEntity());
					} else if("outer".equals(es.getRole())){
						mb.addOuterWay((Way) es.getEntity());
					}
				}
			}
			e = relation;
		} else if (typ == EntityType.WAY) {
			Way w = new Way(osmId);
			inst.fetchCompleteGeometry(w, null, 0l);
			mb.addOuterWay(w);
			e = w;
		} else {
			return;
		}
		Multipolygon mp = mb.build();
		if (!mp.areRingsComplete() || mp.getOuterRings().isEmpty()) {
			System.out.println("ERROR " + osmId);
			return;
		}
		QuadRect bb = mp.getLatLonBbox();
		int[] actualBbox = new int[] { MapUtils.get31TileNumberX(bb.left), MapUtils.get31TileNumberY(bb.top),
				MapUtils.get31TileNumberX(bb.right), MapUtils.get31TileNumberY(bb.bottom) };
		System.out.printf("%d. %d %s %d. %s\n", IND++, osmId, typ, intId, obj);
		bboxCompare(e, bbox, actualBbox);
	}

	private static void bboxCompare(Entity e, int[] bbox, int[] actualBbox) {
		if (bbox != null && bbox.length >= 4 && actualBbox != null && actualBbox.length >= 4) {
			int paramMinX = Math.min(bbox[0], bbox[2]);
			int paramMaxX = Math.max(bbox[0], bbox[2]);
			int paramMinY = Math.min(bbox[1], bbox[3]);
			int paramMaxY = Math.max(bbox[1], bbox[3]);

			int calcMinX = Math.min(actualBbox[0], actualBbox[2]);
			int calcMaxX = Math.max(actualBbox[0], actualBbox[2]);
			int calcMinY = Math.min(actualBbox[1], actualBbox[3]);
			int calcMaxY = Math.max(actualBbox[1], actualBbox[3]);

			long interMinX = Math.max(paramMinX, calcMinX);
			long interMaxX = Math.min(paramMaxX, calcMaxX);
			long interMinY = Math.max(paramMinY, calcMinY);
			long interMaxY = Math.min(paramMaxY, calcMaxY);
			double atomArea = (double) (paramMaxX - paramMinX) * (paramMaxY - paramMinY);
			double actArea = (double) (calcMaxX - calcMinX) * (calcMaxY - calcMinY);

			double interArea = 0.0;
			if (interMaxX > interMinX && interMaxY > interMinY) {
				interArea = (double) (interMaxX - interMinX) * (interMaxY - interMinY);
			}
			double outsidePercent = ((actArea - interArea) / atomArea) * 100.0;
			double insidePercent = (interArea / atomArea) * 100.0;
//			boolean fullyCovers = paramMinX <= calcMinX && paramMaxX >= calcMaxX 
//					&& paramMinY <= calcMinY && paramMaxY >= calcMaxY;

			System.out.printf(
					"BBox: outside %.2f%%, inside %.2f%% | Atom [%d, %d, %d, %d] vs Actual [%d, %d, %d, %d] \n",
					outsidePercent, insidePercent, bbox[0], bbox[1], bbox[2], bbox[3], actualBbox[0],
					actualBbox[1], actualBbox[2], actualBbox[3]);
		}
	}
}
