package net.osmand.obf.preparation;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import com.google.protobuf.WireFormat;
import com.google.protobuf.WireFormat.FieldType;

import gnu.trove.list.TLongList;
import gnu.trove.list.array.TByteArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TIntObjectMap;
import net.osmand.IndexConstants;
import net.osmand.binary.BinaryMapAddressReaderAdapter.CityBlocks;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexReader.TagValuePair;
import net.osmand.binary.BinaryMapRouteReaderAdapter;
import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteTypeRule;
import net.osmand.binary.OsmandOdb;
import net.osmand.binary.OsmandOdb.AddressNameIndexDataAtom;
import net.osmand.binary.OsmandOdb.CityBlockIndex;
import net.osmand.binary.OsmandOdb.CityIndex;
import net.osmand.binary.OsmandOdb.MapData;
import net.osmand.binary.OsmandOdb.MapDataBlock;
import net.osmand.binary.OsmandOdb.OsmAndAddressIndex;
import net.osmand.binary.OsmandOdb.OsmAndAddressIndex.CitiesIndex;
import net.osmand.binary.OsmandOdb.OsmAndAddressNameIndexData;
import net.osmand.binary.OsmandOdb.OsmAndAddressNameIndexData.AddressNameIndexData;
import net.osmand.binary.OsmandOdb.OsmAndCategoryTable.Builder;
import net.osmand.binary.OsmandOdb.OsmAndHHRoutingIndex;
import net.osmand.binary.OsmandOdb.OsmAndHHRoutingIndex.HHRouteBlockSegments;
import net.osmand.binary.OsmandOdb.OsmAndHHRoutingIndex.HHRouteNetworkPoint;
import net.osmand.binary.OsmandOdb.OsmAndHHRoutingIndex.HHRoutePointSegments;
import net.osmand.binary.OsmandOdb.OsmAndHHRoutingIndex.HHRoutePointsBox;
import net.osmand.binary.OsmandOdb.OsmAndMapIndex;
import net.osmand.binary.OsmandOdb.OsmAndMapIndex.MapDataBox;
import net.osmand.binary.OsmandOdb.OsmAndMapIndex.MapEncodingRule;
import net.osmand.binary.OsmandOdb.OsmAndMapIndex.MapRootLevel;
import net.osmand.binary.OsmandOdb.OsmAndPoiBoxDataAtom;
import net.osmand.binary.OsmandOdb.OsmAndPoiNameIndex;
import net.osmand.binary.OsmandOdb.OsmAndPoiNameIndexDataAtom;
import net.osmand.binary.OsmandOdb.OsmAndPoiSubtype;
import net.osmand.binary.OsmandOdb.OsmAndRoutingIndex;
import net.osmand.binary.OsmandOdb.OsmAndRoutingIndex.RouteDataBlock;
import net.osmand.binary.OsmandOdb.OsmAndRoutingIndex.RouteDataBox;
import net.osmand.binary.OsmandOdb.OsmAndRoutingIndex.RouteEncodingRule;
import net.osmand.binary.OsmandOdb.OsmAndSubtypesTable;
import net.osmand.binary.OsmandOdb.OsmAndTransportIndex;
import net.osmand.binary.OsmandOdb.RouteData;
import net.osmand.binary.OsmandOdb.StreetIndex;
import net.osmand.binary.OsmandOdb.StreetIntersection;
import net.osmand.binary.OsmandOdb.StringTable;
import net.osmand.binary.OsmandOdb.TransportRoute;
import net.osmand.binary.OsmandOdb.TransportRouteSchedule;
import net.osmand.binary.OsmandOdb.TransportRouteStop;
import net.osmand.data.Building;
import net.osmand.data.City;
import net.osmand.data.City.CityType;
import net.osmand.data.LatLon;
import net.osmand.data.MapObject;
import net.osmand.data.Street;
import net.osmand.data.TransportSchedule;
import net.osmand.data.TransportStop;
import net.osmand.data.TransportStopExit;
import net.osmand.obf.preparation.IndexPoiCreator.PoiAdditionalType;
import net.osmand.obf.preparation.IndexPoiCreator.PoiCreatorCategories;
import net.osmand.obf.preparation.IndexPoiCreator.PoiCreatorTagGroups;
import net.osmand.obf.preparation.IndexPoiCreator.PoiTileBox;
import net.osmand.osm.MapRenderingTypes.MapRulType;
import net.osmand.osm.MapRoutingTypes.MapPointName;
import net.osmand.osm.MapRoutingTypes.MapRouteType;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.Node;
import net.osmand.router.HHRouteDataStructure.NetworkDBPoint;
import net.osmand.router.HHRoutingOBFWriter.NetworkDBPointWrite;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;
import net.sf.junidecode.Junidecode;

public class BinaryMapIndexWriter {

	private RandomAccessFile raf;
	private CodedOutputStream codedOutStream;

	protected static final int SHIFT_COORDINATES = BinaryMapIndexReader.SHIFT_COORDINATES;
	public int MASK_TO_READ = ~((1 << SHIFT_COORDINATES) - 1);
	private static final int ROUTE_SHIFT_COORDINATES = 4;
	private static final int LABEL_THRESHOLD = 1024; // 20 meters on equator
	private static final int LABEL_ZOOM_ENCODE = BinaryMapIndexReader.LABEL_ZOOM_ENCODE; 
	private static Log log = LogFactory.getLog(BinaryMapIndexWriter.class);

	private static class Bounds {
		public Bounds(int leftX, int rightX, int topY, int bottomY) {
			super();
			this.bottomY = bottomY;
			this.leftX = leftX;
			this.rightX = rightX;
			this.topY = topY;
		}

		int leftX = 0;
		int rightX = 0;
		int topY = 0;
		int bottomY = 0;

	}

	private Stack<Bounds> stackBounds = new Stack<Bounds>();
	private Stack<Long> stackBaseIds = new Stack<Long>();

	// internal constants to track state of index writing
	private Stack<Integer> state = new Stack<Integer>();
	private Stack<BinaryFileReference> stackSizes = new Stack<BinaryFileReference>();

	private final static int OSMAND_STRUCTURE_INIT = 1;
	private final static int MAP_INDEX_INIT = 2;
	private final static int MAP_ROOT_LEVEL_INIT = 3;
	private final static int MAP_TREE = 4;

	private final static int ADDRESS_INDEX_INIT = 5;
	private final static int CITY_INDEX_INIT = 6;

	private final static int TRANSPORT_INDEX_INIT = 9;
	private final static int TRANSPORT_STOPS_TREE = 10;
	private final static int TRANSPORT_ROUTES = 11;

	private final static int POI_INDEX_INIT = 12;
	private final static int POI_BOX = 13;
	private final static int POI_DATA = 14;

	private final static int ROUTE_INDEX_INIT = 15;
	private final static int ROUTE_TREE = 16;
	
	
	private final static int HH_INDEX_INIT = 17;
	private final static int HH_BLOCK_SEGMENTS =18;


	public BinaryMapIndexWriter(final RandomAccessFile raf, long timestamp) throws IOException {
		this.raf = raf;
		codedOutStream = CodedOutputStream.newInstance(new OutputStream() {
			@Override
			public void write(int b) throws IOException {
				raf.write(b);
			}

			@Override
			public void write(byte[] b) throws IOException {
				raf.write(b);
			}

			@Override
			public void write(byte[] b, int off, int len) throws IOException {
				raf.write(b, off, len);
			}

		});
		codedOutStream.writeUInt32(OsmandOdb.OsmAndStructure.VERSION_FIELD_NUMBER, IndexConstants.BINARY_MAP_VERSION);
		codedOutStream.writeInt64(OsmandOdb.OsmAndStructure.DATECREATED_FIELD_NUMBER, timestamp);
		state.push(OSMAND_STRUCTURE_INIT);
	}


	public BinaryMapIndexWriter(final RandomAccessFile raf, CodedOutputStream cos) throws IOException {
		this.raf = raf;
		codedOutStream = cos;
		state.push(OSMAND_STRUCTURE_INIT);
	}

	private BinaryFileReference preserveInt32Size() throws IOException {
		long filePointer = getFilePointer();
		BinaryFileReference ref = BinaryFileReference.createSizeReference(filePointer);
		stackSizes.push(ref);
		codedOutStream.writeFixed32NoTag(0);
		return ref;
	}
	
	private BinaryFileReference preserveInt64Size() throws IOException {
		long filePointer = getFilePointer();
		BinaryFileReference ref = BinaryFileReference.createLongSizeReference(filePointer);
		stackSizes.push(ref);
		codedOutStream.writeFixed32NoTag(0);
		codedOutStream.writeFixed32NoTag(0);
		return ref;
	}

	public long getFilePointer() throws IOException {
		codedOutStream.flush();
		return raf.getFilePointer();
		// return codedOutStream.getWrittenBytes(); // doesn't work with route section rewrite (should not take into account)
	}

	public CodedOutputStream getCodedOutStream() {
		return codedOutStream;
	}

	private long writeInt32Size() throws IOException {
		long filePointer = getFilePointer();
		BinaryFileReference ref = stackSizes.pop();
		codedOutStream.flush();
		long length = ref.writeReference(raf, filePointer);
		return length;
	}

	private long prewriteInt32Size() throws IOException {
		long filePointer = getFilePointer();
		BinaryFileReference ref = stackSizes.peek();
		codedOutStream.flush();
		long length = ref.writeReference(raf, filePointer);
		return length;
	}

	public void startWriteMapIndex(String name) throws IOException {
		pushState(MAP_INDEX_INIT, OSMAND_STRUCTURE_INIT);
		codedOutStream.writeTag(OsmandOdb.OsmAndStructure.MAPINDEX_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
		preserveInt32Size();
		if (name != null) {
			codedOutStream.writeString(OsmandOdb.OsmAndMapIndex.NAME_FIELD_NUMBER, name);
		}
	}

	public void endWriteMapIndex() throws IOException {
		popState(MAP_INDEX_INIT);
		long len = writeInt32Size();
		log.info("MAP INDEX SIZE : " + len);
	}
	
	
	public void startHHRoutingIndex(long edition, String profile, List<String> stringTable, 
			boolean longIndex, String... params) throws IOException {
		pushState(HH_INDEX_INIT, OSMAND_STRUCTURE_INIT);
		codedOutStream.writeTag(OsmandOdb.OsmAndStructure.HHROUTINGINDEX_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
		if (longIndex) {
			preserveInt64Size();
		} else {
			preserveInt32Size();
		}
		codedOutStream.writeInt64(OsmandOdb.OsmAndHHRoutingIndex.EDITION_FIELD_NUMBER, edition);
		codedOutStream.writeString(OsmandOdb.OsmAndHHRoutingIndex.PROFILE_FIELD_NUMBER, profile);
		for (String s : params) {
			codedOutStream.writeString(OsmandOdb.OsmAndHHRoutingIndex.PROFILEPARAMS_FIELD_NUMBER, s);
		}
		if (stringTable != null && stringTable.size() > 0) {
			OsmandOdb.StringTable.Builder st = OsmandOdb.StringTable.newBuilder();
			for (String s : stringTable) {
				st.addS(s);
			}
			codedOutStream.writeMessage(OsmandOdb.OsmAndHHRoutingIndex.TAGVALUESTABLE_FIELD_NUMBER, st.build());
		}
	}

	public void endHHRoutingIndex() throws IOException {
		popState(HH_INDEX_INIT);
		long len = writeInt32Size();
		log.info("HHROUTING INDEX SIZE : " + len);
	}


	public void startWriteRouteIndex(String name) throws IOException {
		pushState(ROUTE_INDEX_INIT, OSMAND_STRUCTURE_INIT);
		codedOutStream.writeTag(OsmandOdb.OsmAndStructure.ROUTINGINDEX_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
		preserveInt32Size();
		if (name != null) {
			codedOutStream.writeString(OsmandOdb.OsmAndRoutingIndex.NAME_FIELD_NUMBER, name);
		}
	}


	public void endWriteRouteIndex() throws IOException {
		popState(ROUTE_INDEX_INIT);
		long len = writeInt32Size();
		log.info("- ROUTE TYPE SIZE SIZE " + BinaryMapIndexWriter.ROUTE_TYPES_SIZE); //$NON-NLS-1$
		log.info("- ROUTE COORDINATES SIZE " + BinaryMapIndexWriter.ROUTE_COORDINATES_SIZE + " COUNT " + BinaryMapIndexWriter.ROUTE_COORDINATES_COUNT); //$NON-NLS-1$
		log.info("- ROUTE POINTS SIZE " + BinaryMapIndexWriter.ROUTE_POINTS_SIZE);
		log.info("- ROUTE STRING SIZE " + BinaryMapIndexWriter.ROUTE_STRING_DATA_SIZE); //$NON-NLS-1$
		log.info("- ROUTE ID SIZE " + BinaryMapIndexWriter.ROUTE_ID_SIZE); //$NON-NLS-1$
		log.info("-- ROUTE_DATA " + BinaryMapIndexWriter.ROUTE_DATA_SIZE); //$NON-NLS-1$
		ROUTE_TYPES_SIZE = ROUTE_DATA_SIZE = ROUTE_POINTS_SIZE = ROUTE_ID_SIZE =
				ROUTE_COORDINATES_COUNT = ROUTE_COORDINATES_SIZE = 0;
		log.info("ROUTE INDEX SIZE : " + len);
	}

	public void simulateWriteEndRouteIndex() throws IOException {
		checkPeekState(ROUTE_INDEX_INIT);
		long len = prewriteInt32Size();
		log.info("PREROUTE INDEX SIZE : " + len);
	}

	public RandomAccessFile getRaf() {
		return raf;
	}


	public void startWriteMapLevelIndex(int minZoom, int maxZoom, int leftX, int rightX, int topY, int bottomY) throws IOException {
		pushState(MAP_ROOT_LEVEL_INIT, MAP_INDEX_INIT);

		codedOutStream.writeTag(OsmandOdb.OsmAndMapIndex.LEVELS_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
		preserveInt32Size();

		codedOutStream.writeInt32(OsmandOdb.OsmAndMapIndex.MapRootLevel.MAXZOOM_FIELD_NUMBER, maxZoom);
		codedOutStream.writeInt32(OsmandOdb.OsmAndMapIndex.MapRootLevel.MINZOOM_FIELD_NUMBER, minZoom);
		codedOutStream.writeInt32(OsmandOdb.OsmAndMapIndex.MapRootLevel.LEFT_FIELD_NUMBER, leftX);
		codedOutStream.writeInt32(OsmandOdb.OsmAndMapIndex.MapRootLevel.RIGHT_FIELD_NUMBER, rightX);
		codedOutStream.writeInt32(OsmandOdb.OsmAndMapIndex.MapRootLevel.TOP_FIELD_NUMBER, topY);
		codedOutStream.writeInt32(OsmandOdb.OsmAndMapIndex.MapRootLevel.BOTTOM_FIELD_NUMBER, bottomY);
		stackBounds.push(new Bounds(leftX, rightX, topY, bottomY));
	}

	public void endWriteMapLevelIndex() throws IOException {
		popState(MAP_ROOT_LEVEL_INIT);
		stackBounds.pop();
		long len = writeInt32Size();
		log.info("MAP level SIZE : " + len);
	}

	public void writeMapEncodingRules(TIntObjectMap<TagValuePair> decodingRules) throws IOException {
		for (int i = 1; i <= decodingRules.size(); i++) {
			TagValuePair value = decodingRules.get(i);
			MapEncodingRule.Builder builder = OsmandOdb.OsmAndMapIndex.MapEncodingRule.newBuilder();
			if (value == null) {
				break;
			}
			builder.setTag(value.tag);
			if (value.value != null) {
				builder.setValue(value.value);
			}
			builder.setType(value.additionalAttribute);
			MapEncodingRule rulet = builder.build();
			codedOutStream.writeMessage(OsmandOdb.OsmAndMapIndex.RULES_FIELD_NUMBER, rulet);
		}
	}

	public void writeMapEncodingRules(Map<String, MapRulType> types) throws IOException {
		checkPeekState(MAP_INDEX_INIT);

		ArrayList<MapRulType> out = new ArrayList<MapRulType>();
		int highestTargetId = types.size();
		// 1. prepare map rule type to write
		for (MapRulType t : types.values()) {
			if (t.getFreq() == 0 || !t.isMap()) {
				t.setTargetId(highestTargetId++);
			} else {
				out.add(t);
			}
		}

		// 2. sort by frequency and assign ids
		Collections.sort(out, new Comparator<MapRulType>() {
			@Override
			public int compare(MapRulType o1, MapRulType o2) {
				return o2.getFreq() - o1.getFreq();
			}
		});

		for (int i = 0; i < out.size(); i++) {
			MapEncodingRule.Builder builder = OsmandOdb.OsmAndMapIndex.MapEncodingRule.newBuilder();
			MapRulType rule = out.get(i);
			rule.setTargetId(i + 1);

			builder.setTag(rule.getTag());
			if (rule.getValue() != null) {
				builder.setValue(rule.getValue());
			}
			builder.setMinZoom(rule.getMinzoom());
			if (rule.isAdditional()) {
				builder.setType(1);
			} else if (rule.isText()) {
				builder.setType(2);
			}
			MapEncodingRule rulet = builder.build();
			codedOutStream.writeMessage(OsmandOdb.OsmAndMapIndex.RULES_FIELD_NUMBER, rulet);
		}
	}
	
	public void writeRouteEncodingRules(List<MapRouteType> types) throws IOException {
		checkPeekState(ROUTE_INDEX_INIT);

		ArrayList<MapRouteType> out = new ArrayList<MapRouteType>(types);
		// 2. sort by frequency and assign ids
		Collections.sort(out, new Comparator<MapRouteType>() {
			@Override
			public int compare(MapRouteType o1, MapRouteType o2) {
				return o2.getFreq() - o1.getFreq();
			}
		});

		for (int i = 0; i < out.size(); i++) {
			RouteEncodingRule.Builder builder = OsmandOdb.OsmAndRoutingIndex.RouteEncodingRule.newBuilder();
			MapRouteType rule = out.get(i);
			rule.setTargetId(i + 1);

			builder.setTag(rule.getTag());
			if (rule.getValue() != null) {
				builder.setValue(rule.getValue());
			} else {
				builder.setValue("");
			}
			RouteEncodingRule rulet = builder.build();
			codedOutStream.writeMessage(OsmandOdb.OsmAndRoutingIndex.RULES_FIELD_NUMBER, rulet);
		}
	}

	public void writeRouteRawEncodingRules(List<BinaryMapRouteReaderAdapter.RouteTypeRule> types) throws IOException {
		checkPeekState(ROUTE_INDEX_INIT);

		for (int i = 1; i < types.size(); i++) {
			RouteEncodingRule.Builder builder = OsmandOdb.OsmAndRoutingIndex.RouteEncodingRule.newBuilder();
			RouteTypeRule rule = types.get(i);
			builder.setTag(rule.getTag());
			if (rule.getValue() != null) {
				builder.setValue(rule.getValue());
			} else {
				builder.setValue("");
			}
			RouteEncodingRule rulet = builder.build();
			codedOutStream.writeMessage(OsmandOdb.OsmAndRoutingIndex.RULES_FIELD_NUMBER, rulet);
		}
	}

	public BinaryFileReference startRouteTreeElement(int leftX, int rightX, int topY, int bottomY, boolean containsObjects,
			boolean basemap) throws IOException {
		checkPeekState(ROUTE_TREE, ROUTE_INDEX_INIT);
		if (state.peek() == ROUTE_INDEX_INIT) {
			if (basemap) {
				codedOutStream.writeTag(OsmAndRoutingIndex.BASEMAPBOXES_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
			} else {
				codedOutStream.writeTag(OsmAndRoutingIndex.ROOTBOXES_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
			}
		} else {
			codedOutStream.writeTag(RouteDataBox.BOXES_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
		}
		state.push(ROUTE_TREE);
		preserveInt32Size();
		long fp = getFilePointer();


		Bounds bounds;
		if (stackBounds.isEmpty()) {
			bounds = new Bounds(0, 0, 0, 0);
		} else {
			bounds = stackBounds.peek();
		}
		codedOutStream.writeSInt32(RouteDataBox.LEFT_FIELD_NUMBER, leftX - bounds.leftX);
		codedOutStream.writeSInt32(RouteDataBox.RIGHT_FIELD_NUMBER, rightX - bounds.rightX);
		codedOutStream.writeSInt32(RouteDataBox.TOP_FIELD_NUMBER, topY - bounds.topY);
		codedOutStream.writeSInt32(RouteDataBox.BOTTOM_FIELD_NUMBER, bottomY - bounds.bottomY);
		stackBounds.push(new Bounds(leftX, rightX, topY, bottomY));
		BinaryFileReference ref = null;
		if (containsObjects) {
			codedOutStream.writeTag(RouteDataBox.SHIFTTODATA_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
			ref = BinaryFileReference.createShiftReference(getFilePointer(), fp);
			codedOutStream.writeFixed32NoTag(0);
		}
		return ref;
	}

	public void endRouteTreeElement() throws IOException {
		popState(ROUTE_TREE);
		stackBounds.pop();
		writeInt32Size();
	}


	public BinaryFileReference startMapTreeElement(int leftX, int rightX, int topY, int bottomY, boolean containsLeaf) throws IOException {
		return startMapTreeElement(leftX, rightX, topY, bottomY, containsLeaf, 0);
	}

	public BinaryFileReference startMapTreeElement(int leftX, int rightX, int topY, int bottomY, boolean containsObjects, int landCharacteristic) throws IOException {
		checkPeekState(MAP_ROOT_LEVEL_INIT, MAP_TREE);
		if (state.peek() == MAP_ROOT_LEVEL_INIT) {
			codedOutStream.writeTag(MapRootLevel.BOXES_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
		} else {
			codedOutStream.writeTag(MapDataBox.BOXES_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
		}
		state.push(MAP_TREE);
		preserveInt32Size();
		long fp = getFilePointer();

		// align with borders with grid
		leftX &= MASK_TO_READ;
		topY &= MASK_TO_READ;
		Bounds bounds = stackBounds.peek();
		codedOutStream.writeSInt32(MapDataBox.LEFT_FIELD_NUMBER, leftX - bounds.leftX);
		codedOutStream.writeSInt32(MapDataBox.RIGHT_FIELD_NUMBER, rightX - bounds.rightX);
		codedOutStream.writeSInt32(MapDataBox.TOP_FIELD_NUMBER, topY - bounds.topY);
		codedOutStream.writeSInt32(MapDataBox.BOTTOM_FIELD_NUMBER, bottomY - bounds.bottomY);
		if (landCharacteristic != 0) {
			codedOutStream.writeBool(MapDataBox.OCEAN_FIELD_NUMBER, landCharacteristic < 0);
		}
		stackBounds.push(new Bounds(leftX, rightX, topY, bottomY));
		BinaryFileReference ref = null;
		if (containsObjects) {
			codedOutStream.writeTag(MapDataBox.SHIFTTOMAPDATA_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
			ref = BinaryFileReference.createShiftReference(getFilePointer(), fp);
			codedOutStream.writeFixed32NoTag(0);
		}
		return ref;
	}

	public void endWriteMapTreeElement() throws IOException {
		popState(MAP_TREE);
		stackBounds.pop();
		writeInt32Size();
	}
	
	public void startHHRouteTreeElement(int leftX, int rightX, int topY, int bottomY) throws IOException {
		checkPeekState(ROUTE_TREE, HH_INDEX_INIT);
		if (state.peek() == HH_INDEX_INIT) {
			codedOutStream.writeTag(OsmAndHHRoutingIndex.POINTBOXES_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
		} else {
			codedOutStream.writeTag(HHRoutePointsBox.BOXES_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
		}
		state.push(ROUTE_TREE);
		preserveInt32Size();
		Bounds bounds;
		if (stackBounds.isEmpty()) {
			bounds = new Bounds(0, 0, 0, 0);
		} else {
			bounds = stackBounds.peek();
		}
		codedOutStream.writeSInt32(HHRoutePointsBox.LEFT_FIELD_NUMBER, leftX - bounds.leftX);
		codedOutStream.writeSInt32(HHRoutePointsBox.RIGHT_FIELD_NUMBER, rightX - bounds.rightX);
		codedOutStream.writeSInt32(HHRoutePointsBox.TOP_FIELD_NUMBER, topY - bounds.topY);
		codedOutStream.writeSInt32(HHRoutePointsBox.BOTTOM_FIELD_NUMBER, bottomY - bounds.bottomY);
		stackBounds.push(new Bounds(leftX, rightX, topY, bottomY));
	}
	
	public void writeHHRoutePoints(List<NetworkDBPointWrite> l) throws IOException {
		checkPeekState(ROUTE_TREE);
		Bounds bounds = stackBounds.peek();
		for (NetworkDBPointWrite p : l) {
			HHRouteNetworkPoint.Builder builder = HHRouteNetworkPoint.newBuilder();
			NetworkDBPoint pnt = p.pnt;
			builder.setClusterId(pnt.clusterId);
			builder.setGlobalId(pnt.index);
			builder.setId(p.localId);
			builder.setRoadId(pnt.roadId);
			builder.setRoadStartEndIndex((pnt.start << 1) + (pnt.end > pnt.start ? 1 : 0));
			builder.setDx(pnt.startX - bounds.leftX);
			builder.setDy(pnt.startY - bounds.topY);
			if (p.includeFlag > 1) {
				builder.setPartialInd(p.includeFlag - 1);
			} else if (p.includeFlag == 0) {
				throw new IllegalStateException();
			}
			if (pnt.dualPoint != null) {
				builder.setDualClusterId(pnt.dualPoint.clusterId);
				builder.setDualPointId(pnt.dualPoint.index);
			}
			if (p.tagValuesInts != null) {
				for (int tgv : p.tagValuesInts) {
					builder.addTagValueIds(tgv);
				}
			}
			codedOutStream.writeMessage(OsmandOdb.OsmAndHHRoutingIndex.HHRoutePointsBox.POINTS_FIELD_NUMBER, builder.build());
		}
	}

	public void endHHRouteTreeElement() throws IOException {
		popState(ROUTE_TREE);
		stackBounds.pop();
		writeInt32Size();
	}
	
	public void startHHRouteBlockSegments(int idRangeStart, int idRangeLength, int profileId) throws IOException {
		checkPeekState(HH_BLOCK_SEGMENTS, HH_INDEX_INIT);
		if (state.peek() == HH_INDEX_INIT) {
			codedOutStream.writeTag(OsmAndHHRoutingIndex.POINTSEGMENTS_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
		} else {
			codedOutStream.writeTag(HHRouteBlockSegments.INNERBLOCKS_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
		}
		state.push(HH_BLOCK_SEGMENTS);
		preserveInt32Size();
		codedOutStream.writeInt32(HHRouteBlockSegments.IDRANGESTART_FIELD_NUMBER, idRangeStart);
		codedOutStream.writeInt32(HHRouteBlockSegments.IDRANGELENGTH_FIELD_NUMBER, idRangeLength);
		codedOutStream.writeInt32(HHRouteBlockSegments.PROFILEID_FIELD_NUMBER, profileId);
	}
	
	public void writePointSegments(byte[] segmentsIn, byte[] segmentsOut) throws IOException {
		checkPeekState(HH_BLOCK_SEGMENTS);
		HHRoutePointSegments.Builder bld = HHRoutePointSegments.newBuilder();
		bld.setSegmentsIn(ByteString.copyFrom(segmentsIn));
		bld.setSegmentsOut(ByteString.copyFrom(segmentsOut));
		codedOutStream.writeMessage(HHRouteBlockSegments.POINTSEGMENTS_FIELD_NUMBER, bld.build());
	}

	
	public void endHHRouteBlockSegments() throws IOException {
		popState(HH_BLOCK_SEGMENTS);
		writeInt32Size();
	}

	// debug data about size of map index
	public static int COORDINATES_SIZE = 0;
	public static int COORDINATES_COUNT = 0;
	public static int ID_SIZE = 0;
	public static int TYPES_SIZE = 0;
	public static int MAP_DATA_SIZE = 0;
	public static int STRING_TABLE_SIZE = 0;
	public static int LABEL_COORDINATES_SIZE = 0;

	public static int ROUTE_ID_SIZE = 0;
	public static int ROUTE_TYPES_SIZE = 0;
	public static int ROUTE_COORDINATES_SIZE = 0;
	public static int ROUTE_COORDINATES_COUNT = 0;
	public static int ROUTE_POINTS_SIZE = 0;
	public static int ROUTE_DATA_SIZE = 0;
	public static int ROUTE_STRING_DATA_SIZE = 0;

	public MapDataBlock.Builder createWriteMapDataBlock(long baseid) throws IOException {
		MapDataBlock.Builder builder = MapDataBlock.newBuilder();
		builder.setBaseId(baseid);
		return builder;
	}

	public void writeRouteDataBlock(RouteDataBlock.Builder builder, Map<String, Integer> stringTable, BinaryFileReference ref)
			throws IOException {
		checkPeekState(ROUTE_INDEX_INIT);
		if (stringTable != null && stringTable.size() > 0) {
			StringTable.Builder bs = OsmandOdb.StringTable.newBuilder();
			for (String s : stringTable.keySet()) {
				bs.addS(s);
			}
			StringTable st = bs.build();
			builder.setStringTable(st);
			int size = st.getSerializedSize();
			ROUTE_STRING_DATA_SIZE += CodedOutputStream.computeTagSize(OsmandOdb.MapDataBlock.STRINGTABLE_FIELD_NUMBER)
					+ CodedOutputStream.computeRawVarint32Size(size) + size;
		}
		codedOutStream.writeTag(OsmAndMapIndex.MapRootLevel.BLOCKS_FIELD_NUMBER, FieldType.MESSAGE.getWireType());
		codedOutStream.flush();
		ref.writeReference(raf, getFilePointer());
		RouteDataBlock block = builder.build();
		ROUTE_DATA_SIZE += block.getSerializedSize();
		codedOutStream.writeMessageNoTag(block);
	}


	/**
	 * Encode and write a varint. {@code value} is treated as unsigned, so it won't be sign-extended if negative.
	 */
	public void writeRawVarint32(TByteArrayList bf, int value) throws IOException {
		while (true) {
			if ((value & ~0x7F) == 0) {
				writeRawByte(bf, value);
				return;
			} else {
				writeRawByte(bf, (value & 0x7F) | 0x80);
				value >>>= 7;
			}
		}
	}

	/**
	 * Write a single byte.
	 */
	public void writeRawByte(TByteArrayList bf, final int value) throws IOException {
		bf.add((byte) value);
	}

	public RouteData writeRouteData(int diffId, int pleft, int ptop, int[] types, RoutePointToWrite[] points,
			Map<MapRouteType, String> names, Map<String, Integer> stringTable, List<MapPointName> pointNames, RouteDataBlock.Builder dataBlock,
			boolean allowCoordinateSimplification, boolean writePointId)
			throws IOException {
		RouteData.Builder builder = RouteData.newBuilder();
		builder.setRouteId(diffId);
		ROUTE_ID_SIZE += CodedOutputStream.computeInt64Size(RouteData.ROUTEID_FIELD_NUMBER, diffId);
		// types
		mapDataBuf.clear();
		for (int i = 0; i < types.length; i++) {
			writeRawVarint32(mapDataBuf, types[i]);
		}
		builder.setTypes(ByteString.copyFrom(mapDataBuf.toArray()));
		ROUTE_TYPES_SIZE += CodedOutputStream.computeTagSize(RouteData.TYPES_FIELD_NUMBER)
				+ CodedOutputStream.computeRawVarint32Size(mapDataBuf.size()) + mapDataBuf.size();
		// coordinates and point types
		int pcalcx = pleft >> ROUTE_SHIFT_COORDINATES;
		int pcalcy = ptop >> ROUTE_SHIFT_COORDINATES;
		mapDataBuf.clear();
		typesDataBuf.clear();
		for (int k = 0; k < points.length; k++) {
			ROUTE_COORDINATES_COUNT++;

			int tx = (points[k].x >> ROUTE_SHIFT_COORDINATES) - pcalcx;
			int ty = (points[k].y >> ROUTE_SHIFT_COORDINATES) - pcalcy;
			writeRawVarint32(mapDataBuf, CodedOutputStream.encodeZigZag32(tx));
			writeRawVarint32(mapDataBuf, CodedOutputStream.encodeZigZag32(ty));
			pcalcx = pcalcx + tx;
			pcalcy = pcalcy + ty;
			if (points[k].types.size() > 0) {
				typesAddDataBuf.clear();
				for (int ij = 0; ij < points[k].types.size(); ij++) {
					writeRawVarint32(typesAddDataBuf, points[k].types.get(ij));
				}
				writeRawVarint32(typesDataBuf, k);
				writeRawVarint32(typesDataBuf, typesAddDataBuf.size());
				typesDataBuf.add(typesAddDataBuf.toArray());
			}
		}
		builder.setPoints(ByteString.copyFrom(mapDataBuf.toArray()));
		ROUTE_COORDINATES_SIZE += CodedOutputStream.computeTagSize(RouteData.POINTS_FIELD_NUMBER)
				+ CodedOutputStream.computeRawVarint32Size(mapDataBuf.size()) + mapDataBuf.size();
		builder.setPointTypes(ByteString.copyFrom(typesDataBuf.toArray()));
		ROUTE_TYPES_SIZE += CodedOutputStream.computeTagSize(RouteData.POINTTYPES_FIELD_NUMBER)
				+ CodedOutputStream.computeRawVarint32Size(typesDataBuf.size()) + typesDataBuf.size();

		if (pointNames.size() > 0) {
			mapDataBuf.clear();
			for (MapPointName p : pointNames) {
				writeRawVarint32(mapDataBuf, p.pointIndex);
				writeRawVarint32(mapDataBuf, p.nameTypeTargetId);
				Integer ls = stringTable.get(p.name);
				if (ls == null) {
					ls = stringTable.size();
					stringTable.put(p.name, ls);
				}
				writeRawVarint32(mapDataBuf, ls);
			}
			ROUTE_STRING_DATA_SIZE += mapDataBuf.size();
			builder.setPointNames(ByteString.copyFrom(mapDataBuf.toArray()));
		}

		if (names.size() > 0) {
			mapDataBuf.clear();
			for (Entry<MapRouteType, String> s : names.entrySet()) {
				writeRawVarint32(mapDataBuf, s.getKey().getTargetId());
				Integer ls = stringTable.get(s.getValue());
				if (ls == null) {
					ls = stringTable.size();
					stringTable.put(s.getValue(), ls);
				}
				writeRawVarint32(mapDataBuf, ls);
			}
			ROUTE_STRING_DATA_SIZE += mapDataBuf.size();
			builder.setStringNames(ByteString.copyFrom(mapDataBuf.toArray()));
		}

		return builder.build();
	}

	public void writeMapDataBlock(MapDataBlock.Builder builder, Map<String, Integer> stringTable, BinaryFileReference ref)
			throws IOException {

		checkPeekState(MAP_ROOT_LEVEL_INIT);
		StringTable.Builder bs = OsmandOdb.StringTable.newBuilder();
		if (stringTable != null) {
			for (String s : stringTable.keySet()) {
				bs.addS(s);
			}
		}
		StringTable st = bs.build();
		builder.setStringTable(st);
		int size = st.getSerializedSize();
		STRING_TABLE_SIZE += CodedOutputStream.computeTagSize(OsmandOdb.MapDataBlock.STRINGTABLE_FIELD_NUMBER)
				+ CodedOutputStream.computeRawVarint32Size(size) + size;

		codedOutStream.writeTag(OsmAndMapIndex.MapRootLevel.BLOCKS_FIELD_NUMBER, FieldType.MESSAGE.getWireType());

		codedOutStream.flush();
		ref.writeReference(raf, getFilePointer());
		MapDataBlock block = builder.build();
		MAP_DATA_SIZE += block.getSerializedSize();
		codedOutStream.writeMessageNoTag(block);
	}

	private TByteArrayList mapDataBuf = new TByteArrayList();
	private TByteArrayList typesDataBuf = new TByteArrayList();
	private TByteArrayList typesAddDataBuf = new TByteArrayList();

	public MapData writeMapData(long diffId, int pleft, int ptop, boolean area, byte[] coordinates, byte[] innerPolygonTypes, int[] typeUse,
			int[] addtypeUse, Map<MapRulType, String> names, byte[] labelCoordinates, Map<Integer, String> namesDiff, Map<String, Integer> stringTable, MapDataBlock.Builder dataBlock,
			boolean allowCoordinateSimplification)
			throws IOException {
		MapData.Builder data = MapData.newBuilder();
		// calculate size
		mapDataBuf.clear();
		int pcalcx = (pleft >> SHIFT_COORDINATES);
		int pcalcy = (ptop >> SHIFT_COORDINATES);
		int len = coordinates.length / 8;
		int delta = 1;
		long sumLabelX = 0;
		long sumLabelY = 0;
		int sumLabelCount = 0;
		for (int i = 0; i < len; i += delta) {
			int x = Algorithms.parseIntFromBytes(coordinates, i * 8);
			int y = Algorithms.parseIntFromBytes(coordinates, i * 8 + 4);
			int tx = (x >> SHIFT_COORDINATES) - pcalcx;
			int ty = (y >> SHIFT_COORDINATES) - pcalcy;
			writeRawVarint32(mapDataBuf, CodedOutputStream.encodeZigZag32(tx));
			writeRawVarint32(mapDataBuf, CodedOutputStream.encodeZigZag32(ty));
			pcalcx = pcalcx + tx;
			pcalcy = pcalcy + ty;
			sumLabelX += pcalcx;
			sumLabelY += pcalcy;
			sumLabelCount++;
			delta = 1;
			if (allowCoordinateSimplification) {
				delta = skipSomeNodes(coordinates, len, i, x, y, false);
			}
		}
		COORDINATES_SIZE += CodedOutputStream.computeRawVarint32Size(mapDataBuf.size())
				+ CodedOutputStream.computeTagSize(MapData.COORDINATES_FIELD_NUMBER) + mapDataBuf.size();
		if (area) {
			data.setAreaCoordinates(ByteString.copyFrom(mapDataBuf.toArray()));
		} else {
			data.setCoordinates(ByteString.copyFrom(mapDataBuf.toArray()));
		}

		if (innerPolygonTypes != null && innerPolygonTypes.length > 0) {
			mapDataBuf.clear();
			pcalcx = (pleft >> SHIFT_COORDINATES);
			pcalcy = (ptop >> SHIFT_COORDINATES);
			len = innerPolygonTypes.length / 8;
			for (int i = 0; i < len; i += delta) {
				int x = Algorithms.parseIntFromBytes(innerPolygonTypes, i * 8);
				int y = Algorithms.parseIntFromBytes(innerPolygonTypes, i * 8 + 4);
				if (x == 0 && y == 0) {
					if (mapDataBuf.size() > 0) {
						data.addPolygonInnerCoordinates(ByteString.copyFrom(mapDataBuf.toArray()));
						mapDataBuf.clear();
					}
					pcalcx = (pleft >> SHIFT_COORDINATES);
					pcalcy = (ptop >> SHIFT_COORDINATES);
				} else {
					int tx = (x >> SHIFT_COORDINATES) - pcalcx;
					int ty = (y >> SHIFT_COORDINATES) - pcalcy;

					writeRawVarint32(mapDataBuf, CodedOutputStream.encodeZigZag32(tx));
					writeRawVarint32(mapDataBuf, CodedOutputStream.encodeZigZag32(ty));

					pcalcx = pcalcx + tx;
					pcalcy = pcalcy + ty;
					delta = 1;
					if (allowCoordinateSimplification) {
						delta = skipSomeNodes(innerPolygonTypes, len, i, x, y, true);
					}
				}
			}		
		}
		
		if (labelCoordinates != null && labelCoordinates.length > 0 && sumLabelCount > 0) {
			mapDataBuf.clear();
			int LABEL_SHIFT = 31 - LABEL_ZOOM_ENCODE;
			int x = (Algorithms.parseIntFromBytes(labelCoordinates, 0)) >> LABEL_SHIFT;
			int y = (Algorithms.parseIntFromBytes(labelCoordinates, 4)) >> LABEL_SHIFT;
			long labelX = (sumLabelX / sumLabelCount) << (SHIFT_COORDINATES - LABEL_SHIFT);
			long labelY = (sumLabelY / sumLabelCount) << (SHIFT_COORDINATES - LABEL_SHIFT);
			boolean isPOI = true;
			if ((Math.abs(labelX) > LABEL_THRESHOLD && Math.abs(labelY) > LABEL_THRESHOLD) || isPOI) {
				writeRawVarint32(mapDataBuf, CodedOutputStream.encodeZigZag32(x - (int) labelX));
				writeRawVarint32(mapDataBuf, CodedOutputStream.encodeZigZag32(y - (int) labelY));
				data.setLabelcoordinates(ByteString.copyFrom(mapDataBuf.toArray()));
				LABEL_COORDINATES_SIZE += CodedOutputStream.computeRawVarint32Size(mapDataBuf.size())
						+ CodedOutputStream.computeTagSize(MapData.LABELCOORDINATES_FIELD_NUMBER) + mapDataBuf.size();
			}
		}
		
		mapDataBuf.clear();
		for (int i = 0; i < typeUse.length; i++) {
			writeRawVarint32(mapDataBuf, typeUse[i]);
		}
		data.setTypes(ByteString.copyFrom(mapDataBuf.toArray()));
		TYPES_SIZE += CodedOutputStream.computeTagSize(OsmandOdb.MapData.TYPES_FIELD_NUMBER)
				+ CodedOutputStream.computeRawVarint32Size(mapDataBuf.size()) + mapDataBuf.size();
		if (addtypeUse != null && addtypeUse.length > 0) {
			mapDataBuf.clear();
			for (int i = 0; i < addtypeUse.length; i++) {
				writeRawVarint32(mapDataBuf, addtypeUse[i]);
			}
			data.setAdditionalTypes(ByteString.copyFrom(mapDataBuf.toArray()));
			TYPES_SIZE += CodedOutputStream.computeTagSize(OsmandOdb.MapData.ADDITIONALTYPES_FIELD_NUMBER);
		}

		mapDataBuf.clear();
		if (names != null) {
			for (Entry<MapRulType, String> s : names.entrySet()) {
				writeRawVarint32(mapDataBuf, s.getKey().getTargetId());
				Integer ls = stringTable.get(s.getValue());
				if (ls == null) {
					ls = stringTable.size();
					stringTable.put(s.getValue(), ls);
				}
				writeRawVarint32(mapDataBuf, ls);
			}
		}
		if (namesDiff != null) {
			for (Entry<Integer, String> it : namesDiff.entrySet()) {
				writeRawVarint32(mapDataBuf, it.getKey());
				Integer ls = stringTable.get(it.getValue());
				if (ls == null) {
					ls = stringTable.size();
					stringTable.put(it.getValue(), ls);
				}
				writeRawVarint32(mapDataBuf, ls);
			}
		}
		STRING_TABLE_SIZE += mapDataBuf.size();
		data.setStringNames(ByteString.copyFrom(mapDataBuf.toArray()));

		data.setId(diffId);
		ID_SIZE += CodedOutputStream.computeSInt64Size(OsmandOdb.MapData.ID_FIELD_NUMBER, diffId);
		return data.build();
	}

	public static class RoutePointToWrite {
		public TIntArrayList types = new TIntArrayList();
		public int id;
		public int x;
		public int y;
	}

	private static double orthogonalDistance(int x, int y, int x1, int y1, int x2, int y2) {
		long A = (x - x1);
		long B = (y - y1);
		long C = (x2 - x1);
		long D = (y2 - y1);
		return Math.abs(A * D - C * B) / Math.sqrt(C * C + D * D);
	}

	private int skipSomeNodes(byte[] coordinates, int len, int i, int x, int y, boolean multi) {
		int delta;
		delta = 1;
		// keep first/latest point untouched
		// simplified douglas\peuker
		// just try to skip some points very close to this point
		while (i + delta < len - 1) {
			int nx = Algorithms.parseIntFromBytes(coordinates, (i + delta) * 8);
			int ny = Algorithms.parseIntFromBytes(coordinates, (i + delta) * 8 + 4);
			int nnx = Algorithms.parseIntFromBytes(coordinates, (i + delta + 1) * 8);
			int nny = Algorithms.parseIntFromBytes(coordinates, (i + delta + 1) * 8 + 4);
			if (nnx == 0 && nny == 0) {
				break;
			}
			double dist = orthogonalDistance(nx, ny, x, y, nnx, nny);
			if (dist > 31) {
				break;
			}
			delta++;
		}
		return delta;
	}

	public void startWriteAddressIndex(String name, Collection<String> additionalTags) throws IOException {
		pushState(ADDRESS_INDEX_INIT, OSMAND_STRUCTURE_INIT);
		codedOutStream.writeTag(OsmandOdb.OsmAndStructure.ADDRESSINDEX_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
		preserveInt32Size();

		codedOutStream.writeString(OsmandOdb.OsmAndAddressIndex.NAME_FIELD_NUMBER, name);
		codedOutStream.writeString(OsmandOdb.OsmAndAddressIndex.NAME_EN_FIELD_NUMBER, Junidecode.unidecode(name));

		// skip boundaries
		StringTable.Builder bs = OsmandOdb.StringTable.newBuilder();
		for (String s : additionalTags) {
			bs.addS(s);
		}
		codedOutStream.writeTag(OsmandOdb.OsmAndAddressIndex.ATTRIBUTETAGSTABLE_FIELD_NUMBER, FieldType.MESSAGE.getWireType());
		codedOutStream.writeMessageNoTag(bs.build());
	}

	public void endWriteAddressIndex() throws IOException {
		popState(ADDRESS_INDEX_INIT);
		long len = writeInt32Size();
		log.info("ADDRESS INDEX SIZE : " + len);
	}


	public void writeAddressNameIndex(Map<String, List<MapObject>> namesIndex) throws IOException {
		checkPeekState(ADDRESS_INDEX_INIT);
		codedOutStream.writeTag(OsmAndAddressIndex.NAMEINDEX_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
		preserveInt32Size();

		Map<String, BinaryFileReference> res = writeIndexedTable(OsmAndAddressNameIndexData.TABLE_FIELD_NUMBER, namesIndex.keySet());
		for (Entry<String, List<MapObject>> entry : namesIndex.entrySet()) {
			BinaryFileReference ref = res.get(entry.getKey());

			codedOutStream.writeTag(OsmAndAddressNameIndexData.ATOM_FIELD_NUMBER, FieldType.MESSAGE.getWireType());
			codedOutStream.flush();
			long pointer = getFilePointer();
			if (ref != null) {
				ref.writeReference(raf, getFilePointer());
			}
			AddressNameIndexData.Builder builder = AddressNameIndexData.newBuilder();
			// collapse same name ?
			for (MapObject o : entry.getValue()) {
				AddressNameIndexDataAtom.Builder atom = AddressNameIndexDataAtom.newBuilder();
				// this is optional
//				atom.setName(o.getName());
//				if(checkEnNameToWrite(o)){
//					atom.setNameEn(o.getEnName());
//				}
				CityBlocks type = CityBlocks.CITY_TOWN_TYPE;
				if (o instanceof City) {
					CityType ct = ((City) o).getType();
					if (ct == CityType.POSTCODE) {
						type = CityBlocks.BOUNDARY_TYPE;
					} else if (ct == CityType.BOUNDARY) {
						type = CityBlocks.BOUNDARY_TYPE;
					} else if (ct != CityType.CITY && ct != CityType.TOWN) {
						type = CityBlocks.VILLAGES_TYPE;
					}
				} else if (o instanceof Street) {
					type = CityBlocks.STREET_TYPE;
				}
				atom.setType(type.index);
				LatLon ll = o.getLocation();
				int x = (int) MapUtils.getTileNumberX(16, ll.getLongitude());
				int y = (int) MapUtils.getTileNumberY(16, ll.getLatitude());
				atom.addXy16((x << 16) + y);
				atom.addShiftToIndex((int) (pointer - o.getFileOffset()));
				if (o instanceof Street) {
					atom.addShiftToCityIndex((int) (pointer - ((Street) o).getCity().getFileOffset()));
				}
				builder.addAtom(atom.build());
			}
			codedOutStream.writeMessageNoTag(builder.build());
		}

		long len = writeInt32Size();
		log.info("ADDRESS NAME INDEX SIZE : " + len);
	}

	private boolean checkEnNameToWrite(MapObject obj) {
		if (obj.getEnName(false) == null || obj.getEnName(false).length() == 0) {
			return false;
		}
		return true;
	}

	public BinaryFileReference writeCityHeader(City city, int cityType, Map<String, Integer> tagRules) throws IOException {
		checkPeekState(CITY_INDEX_INIT);
		codedOutStream.writeTag(CitiesIndex.CITIES_FIELD_NUMBER, FieldType.MESSAGE.getWireType());
		long startMessage = getFilePointer();


		CityIndex.Builder cityInd = OsmandOdb.CityIndex.newBuilder();
		if (cityType >= 0) {
			cityInd.setCityType(cityType);
		}
		if (city.getId() != null) {
			cityInd.setId(city.getId());
		}

		cityInd.setName(city.getName());
		if (checkEnNameToWrite(city)) {
			cityInd.setNameEn(city.getEnName(false));
		}
		Iterator<Entry<String, String>> it = city.getNamesMap(false).entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, String> next = it.next();
			Integer intg = tagRules.get("name:" + next.getKey());
			if (intg != null) {
				cityInd.addAttributeTagIds(intg);
				cityInd.addAttributeValues(next.getValue());
			}
		}


		int cx = MapUtils.get31TileNumberX(city.getLocation().getLongitude());
		int cy = MapUtils.get31TileNumberY(city.getLocation().getLatitude());
		cityInd.setX(cx);
		cityInd.setY(cy);
		cityInd.setShiftToCityBlockIndex(0);
		int boundarySize = 0;
		if (city.getBbox31() != null) {
			mapDataBuf.clear();
			for (Integer i : city.getBbox31()) {
				writeRawVarint32(mapDataBuf, i);
			}
			cityInd.setBoundary(ByteString.copyFrom(mapDataBuf.toArray()));
			boundarySize = CodedOutputStream.computeRawVarint32Size(mapDataBuf.size())
					+ CodedOutputStream.computeTagSize(CityIndex.BOUNDARY_FIELD_NUMBER) + mapDataBuf.size();
		}
		
		codedOutStream.writeMessageNoTag(cityInd.build());
		codedOutStream.flush();
		return BinaryFileReference.createShiftReference(getFilePointer() - 
				boundarySize - 4, startMessage);

	}

	public void writeCityIndex(City cityOrPostcode, List<Street> streets, Map<Street, List<Node>> wayNodes,
			BinaryFileReference ref, Map<String, Integer> tagRules) throws IOException {
		checkPeekState(CITY_INDEX_INIT);
		codedOutStream.writeTag(CitiesIndex.BLOCKS_FIELD_NUMBER, FieldType.MESSAGE.getWireType());
		codedOutStream.flush();
		long startMessage = getFilePointer();
		long startCityBlock = ref.getStartPointer();
		ref.writeReference(raf, startMessage);
		CityBlockIndex.Builder cityInd = OsmandOdb.CityBlockIndex.newBuilder();
		cityInd.setShiftToCityIndex((int) (startMessage - startCityBlock));
		long currentPointer = startMessage + 4 + CodedOutputStream.computeTagSize(CityBlockIndex.SHIFTTOCITYINDEX_FIELD_NUMBER);

		int cx = MapUtils.get31TileNumberX(cityOrPostcode.getLocation().getLongitude());
		int cy = MapUtils.get31TileNumberY(cityOrPostcode.getLocation().getLatitude());
		Map<Long, Set<Street>> mapNodeToStreet = new LinkedHashMap<Long, Set<Street>>();
		if (wayNodes != null) {
			for (int i = 0; i < streets.size(); i++) {
				for (Node n : wayNodes.get(streets.get(i))) {
					if (!mapNodeToStreet.containsKey(n.getId())) {
						mapNodeToStreet.put(n.getId(), new LinkedHashSet<Street>(3));
					}
					mapNodeToStreet.get(n.getId()).add(streets.get(i));
				}
			}
		}
		String postcodeFilter = cityOrPostcode.isPostcode() ? cityOrPostcode.getName() : null;
		for (Street s : streets) {
			StreetIndex streetInd = createStreetAndBuildings(s, cx, cy, postcodeFilter, mapNodeToStreet, wayNodes, tagRules);
			currentPointer += CodedOutputStream.computeTagSize(CityBlockIndex.STREETS_FIELD_NUMBER);
			if (currentPointer > Integer.MAX_VALUE) {
				throw new IllegalArgumentException("File offset > 2 GB.");
			}
			s.setFileOffset((int) currentPointer);
			currentPointer += CodedOutputStream.computeMessageSizeNoTag(streetInd);
			cityInd.addStreets(streetInd);

		}
		CityBlockIndex block = cityInd.build();
		int size = CodedOutputStream.computeRawVarint32Size(block.getSerializedSize());
		codedOutStream.writeMessageNoTag(block);
		for (Street s : streets) {
			s.setFileOffset(s.getFileOffset() + size);
		}
	}

	public void startCityBlockIndex(int type) throws IOException {
		pushState(CITY_INDEX_INIT, ADDRESS_INDEX_INIT);
		codedOutStream.writeTag(OsmandOdb.OsmAndAddressIndex.CITIES_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
		preserveInt32Size();
		codedOutStream.writeUInt32(CitiesIndex.TYPE_FIELD_NUMBER, type);
	}

	public void endCityBlockIndex() throws IOException {
		popState(CITY_INDEX_INIT);
		long length = writeInt32Size();
		log.info("CITIES size " + length);
	}

	protected StreetIndex createStreetAndBuildings(Street street, int cx, int cy, String postcodeFilter,
			Map<Long, Set<Street>> mapNodeToStreet, Map<Street, List<Node>> wayNodes,
			Map<String, Integer> tagRules) throws IOException {
		checkPeekState(CITY_INDEX_INIT);
		if (street.getName().startsWith("Burnhamthorpe")) {
			System.out.println("--- ");
		}
		StreetIndex.Builder streetBuilder = OsmandOdb.StreetIndex.newBuilder();
		streetBuilder.setName(street.getName());
		if (checkEnNameToWrite(street)) {
			streetBuilder.setNameEn(street.getEnName(false));
		}
		Iterator<Entry<String, String>> it = street.getNamesMap(false).entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, String> next = it.next();
			Integer intg = tagRules.get("name:" + next.getKey());
			if (intg != null) {
				streetBuilder.addAttributeTagIds(intg);
				streetBuilder.addAttributeValues(next.getValue());
			}
		}
		streetBuilder.setId(street.getId());

		int sx = MapUtils.get31TileNumberX(street.getLocation().getLongitude());
		int sy = MapUtils.get31TileNumberY(street.getLocation().getLatitude());
		streetBuilder.setX((sx >> 7) - (cx >> 7));
		streetBuilder.setY((sy >> 7) - (cy >> 7));

		street.sortBuildings();
		for (Building b : street.getBuildings()) {
			if (postcodeFilter != null && !postcodeFilter.equalsIgnoreCase(b.getPostcode())) {
				continue;
			}
			OsmandOdb.BuildingIndex.Builder bbuilder = OsmandOdb.BuildingIndex.newBuilder();
			int bx = MapUtils.get31TileNumberX(b.getLocation().getLongitude());
			int by = MapUtils.get31TileNumberY(b.getLocation().getLatitude());
			bbuilder.setX((bx >> 7) - (sx >> 7));
			bbuilder.setY((by >> 7) - (sy >> 7));

			String number2 = b.getName2();
			if (!Algorithms.isEmpty(number2)) {
				LatLon loc = b.getLatLon2();
				if (loc == null) {
					bbuilder.setX((bx >> 7) - (sx >> 7));
					bbuilder.setY((by >> 7) - (sy >> 7));
				} else {
					int bcx = MapUtils.get31TileNumberX(loc.getLongitude());
					int bcy = MapUtils.get31TileNumberY(loc.getLatitude());
					bbuilder.setX2((bcx >> 7) - (sx >> 7));
					bbuilder.setY2((bcy >> 7) - (sy >> 7));
				}
				bbuilder.setName2(number2);
				if (b.getInterpolationType() != null) {
					bbuilder.setInterpolation(b.getInterpolationType().getValue());
				} else if (b.getInterpolationInterval() > 0) {
					bbuilder.setInterpolation(b.getInterpolationInterval());
				} else {
					bbuilder.setInterpolation(1);
				}
			}
			// bbuilder.setId(b.getId());
			bbuilder.setName(b.getName());
			if (b.getPostcode() != null) {
				bbuilder.setPostcode(b.getPostcode());
			}
			it = b.getNamesMap(false).entrySet().iterator();
			while (it.hasNext()) {
				Entry<String, String> next = it.next();
				Integer intg = tagRules.get("name:" + next.getKey());
				if (intg != null) {
					bbuilder.addAttributeTagIds(intg);
					bbuilder.addAttributeValues(next.getValue());
				}
			}

			if (checkEnNameToWrite(b)) {
				bbuilder.setNameEn(b.getEnName(false));
			}
			if (postcodeFilter == null && b.getPostcode() != null) {
				bbuilder.setPostcode(b.getPostcode());
			}
			streetBuilder.addBuildings(bbuilder.build());
		}

		if (wayNodes != null) {
			Set<Street> checkedStreets = new TreeSet<Street>();
			for (Node intersection : wayNodes.get(street)) {
				for (Street streetJ : mapNodeToStreet.get(intersection.getId())) {
					if (checkedStreets.contains(streetJ) || streetJ.getId().longValue() == street.getId().longValue()) {
						continue;
					}
					checkedStreets.add(streetJ);
					StreetIntersection.Builder builder = OsmandOdb.StreetIntersection.newBuilder();
					int ix = MapUtils.get31TileNumberX(intersection.getLongitude());
					int iy = MapUtils.get31TileNumberY(intersection.getLatitude());
					builder.setIntersectedX((ix - sx) >> 7);
					builder.setIntersectedY((iy - sy) >> 7);
					builder.setName(streetJ.getName());
					if (checkEnNameToWrite(streetJ)) {
						builder.setNameEn(streetJ.getEnName(false));
					}
					it = streetJ.getNamesMap(false).entrySet().iterator();
					while (it.hasNext()) {
						Entry<String, String> next = it.next();
						Integer intg = tagRules.get("name:" + next.getKey());
						if (intg != null) {
							builder.addAttributeTagIds(intg);
							builder.addAttributeValues(next.getValue());
						}
					}

					streetBuilder.addIntersections(builder.build());
				}
			}
		}

		return streetBuilder.build();
	}

	public void startWriteTransportRoutes() throws IOException {
		pushState(TRANSPORT_ROUTES, TRANSPORT_INDEX_INIT);
		codedOutStream.writeTag(OsmandOdb.OsmAndTransportIndex.ROUTES_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
		preserveInt32Size();
	}

	public void endWriteTransportRoutes() throws IOException {
		popState(TRANSPORT_ROUTES);
		writeInt32Size();
	}

	private int registerString(Map<String, Integer> stringTable, String s) {
		if (s == null) {
			s = "";
		}
		if (stringTable.containsKey(s)) {
			return stringTable.get(s);
		}
		int size = stringTable.size();
		stringTable.put(s, size);
		return size;
	}

	public long startWriteTransportIndex(String name) throws IOException {
		pushState(TRANSPORT_INDEX_INIT, OSMAND_STRUCTURE_INIT);
		codedOutStream.writeTag(OsmandOdb.OsmAndStructure.TRANSPORTINDEX_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
		stackBounds.push(new Bounds(0, 0, 0, 0)); // for transport stops tree
		preserveInt32Size();
		long tiOffset = getFilePointer(); //transport index offset
		if (name != null) {
			codedOutStream.writeString(OsmandOdb.OsmAndTransportIndex.NAME_FIELD_NUMBER, name);
		}
		return tiOffset;
	}

	public void endWriteTransportIndex() throws IOException {
		popState(TRANSPORT_INDEX_INIT);
		long len = writeInt32Size();
		stackBounds.pop();
		log.info("TRANSPORT INDEX SIZE : " + len);
	}

	public long writeTransportRoute(long idRoute, String routeName, String routeEnName, String ref, String operator,
	                                String type, int dist, String color, List<TransportStop> directStops, List<byte[]>
	                                directRoute, Map<String, Integer> stringTable, Map<Long, Long> transportRoutesRegistry,
	                                TransportSchedule schedule, TransportTags tags) throws IOException {
		checkPeekState(TRANSPORT_ROUTES);
		TransportRoute.Builder tRoute = OsmandOdb.TransportRoute.newBuilder();
		tRoute.setRef(ref);
		tRoute.setOperator(registerString(stringTable, operator));
		tRoute.setType(registerString(stringTable, type));
		tRoute.setId(idRoute);
		tRoute.setName(registerString(stringTable, routeName));
		tRoute.setDistance(dist);
		tRoute.setColor(registerString(stringTable, color));
		if (tags != null && tags.get(idRoute) != null) {
			TByteArrayList buf = new TByteArrayList();
			for (TransportTags.TransportTagValue tag : tags.get(idRoute)) {
				if (tag.isPopular()) {
					tRoute.addAttributeTagIds(registerString(stringTable, tag.getKey()));
				} else {
					buf.clear();
					int keyId = registerString(stringTable, tag.getTag());
					writeRawVarint32(buf, keyId);
					for (byte rawByte : tag.getValue().getBytes(StandardCharsets.UTF_8)) {
						writeRawByte(buf, rawByte);
					}
					tRoute.addAttributeTextTagValues(ByteString.copyFrom(buf.toArray()));
				}
			}
		}
		if (routeEnName != null) {
			tRoute.setNameEn(registerString(stringTable, routeEnName));
		}
		List<TransportStop> stops = directStops;
		long id = 0;
		int x24 = 0;
		int y24 = 0;
		for (TransportStop st : stops) {
			TransportRouteStop.Builder tStop = OsmandOdb.TransportRouteStop.newBuilder();
			tStop.setId(st.getId() - id);
			id = st.getId();
			int x = (int) MapUtils.getTileNumberX(24, st.getLocation().getLongitude());
			int y = (int) MapUtils.getTileNumberY(24, st.getLocation().getLatitude());
			tStop.setDx(x - x24);
			tStop.setDy(y - y24);
			x24 = x;
			y24 = y;
			tStop.setName(registerString(stringTable, st.getName()));
			if (st.getEnName(false) != null) {
				tStop.setNameEn(registerString(stringTable, st.getEnName(false)));
			}
			tRoute.addDirectStops(tStop.build());
		}
		if (directRoute != null) {
			writeTransportRouteCoordinates(directRoute);
			tRoute.setGeometry(ByteString.copyFrom(mapDataBuf.toArray()));
		}
		if(schedule != null && schedule.tripIntervals.size() > 0) {
			net.osmand.binary.OsmandOdb.TransportRouteSchedule.Builder sched = TransportRouteSchedule.newBuilder();
			TByteArrayList bf = new TByteArrayList();
			for(int i = 0; i < schedule.tripIntervals.size(); i++) {
				writeRawVarint32(bf, schedule.tripIntervals.getQuick(i));
			}
			sched.setTripIntervals(ByteString.copyFrom(bf.toArray()));
			if(schedule.avgStopIntervals.size() > 0) {
				bf.clear();
				for(int i = 0; i < schedule.avgStopIntervals.size(); i++) {
					writeRawVarint32(bf, schedule.avgStopIntervals.getQuick(i));
				}
				sched.setAvgStopIntervals(ByteString.copyFrom(bf.toArray()));
			}
			if(schedule.avgWaitIntervals.size() > 0) {
				bf.clear();
				for(int i = 0; i < schedule.avgWaitIntervals.size(); i++) {
					writeRawVarint32(bf, schedule.avgWaitIntervals.getQuick(i));
				}
				sched.setAvgWaitIntervals(ByteString.copyFrom(bf.toArray()));
			}
			tRoute.addScheduleTrip(sched.build());
		}
		codedOutStream.writeTag(OsmandOdb.TransportRoutes.ROUTES_FIELD_NUMBER, FieldType.MESSAGE.getWireType());
		long ptr = -1;
		if (transportRoutesRegistry != null) {
			ptr = getFilePointer();
			transportRoutesRegistry.put(idRoute, ptr);
		}
		
		codedOutStream.writeMessageNoTag(tRoute.build());
		return ptr;
	}


	private void writeTransportRouteCoordinates(List<byte[]> rt) throws IOException {
		int pcalcx = 0;
		int pcalcy = 0;
		mapDataBuf.clear();
		List<TLongArrayList> coordinates = simplifyCoordinates(rt);
		for (int j = 0; j < coordinates.size(); j++) {
			if(j > 0) {
				writeRawVarint32(mapDataBuf, CodedOutputStream.encodeZigZag32(0));
				writeRawVarint32(mapDataBuf, CodedOutputStream.encodeZigZag32(0));
			}
			TLongArrayList lst = coordinates.get(j);
			for (int i = 0; i < lst.size(); i++) {
				int x = (int) MapUtils.deinterleaveX(lst.get(i));
				int y = (int) MapUtils.deinterleaveY(lst.get(i));
				int tx = (x >> SHIFT_COORDINATES) - pcalcx;
				int ty = (y >> SHIFT_COORDINATES) - pcalcy;
				if (tx != 0 || ty != 0) {
					writeRawVarint32(mapDataBuf, CodedOutputStream.encodeZigZag32(tx));
					writeRawVarint32(mapDataBuf, CodedOutputStream.encodeZigZag32(ty));
				}
				pcalcx = pcalcx + tx;
				pcalcy = pcalcy + ty;
			}
			
		}
	}

	private List<TLongArrayList> simplifyCoordinates(List<byte[]> rt) {
		List<TLongArrayList> ct = new ArrayList<>();
		for(byte[] coordinates : rt) {
			TLongArrayList lst = new TLongArrayList();
			int len = coordinates.length / 8;
			for (int i = 0; i < len; i ++) {
				int x = Algorithms.parseIntFromBytes(coordinates, i * 8);
				int y = Algorithms.parseIntFromBytes(coordinates, i * 8 + 4);
				long clt = MapUtils.interleaveBits(x, y);
				lst.add(clt);
			}
			if(lst.size() > 0) {
				ct.add(lst);
			}
		}
		// simple merge lines
		for (int i = 0; i < ct.size() - 1;) {
			TLongArrayList head = ct.get(i);
			int j = i + 1;
			TLongArrayList tail = ct.get(j);
			if (head.get(head.size() - 1) == tail.get(0)) {
				head.addAll(tail.subList(1, tail.size()));
				ct.remove(j);
			} else if (head.get(head.size() - 1) == tail.get(tail.size() - 1)) {
				TLongList subl = tail.subList(0, tail.size() - 1);
				subl.reverse();
				head.addAll(subl);
				ct.remove(j);
			} else {
				i++;
			}
		}
		return ct;
	}


	public void startTransportTreeElement(int leftX, int rightX, int topY, int bottomY) throws IOException {
		checkPeekState(TRANSPORT_STOPS_TREE, TRANSPORT_INDEX_INIT);
		if (state.peek() == TRANSPORT_STOPS_TREE) {
			codedOutStream.writeTag(OsmandOdb.TransportStopsTree.SUBTREES_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
		} else {
			codedOutStream.writeTag(OsmandOdb.OsmAndTransportIndex.STOPS_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
		}
		state.push(TRANSPORT_STOPS_TREE);
		preserveInt32Size();

		Bounds bounds = stackBounds.peek();

		codedOutStream.writeSInt32(OsmandOdb.TransportStopsTree.LEFT_FIELD_NUMBER, leftX - bounds.leftX);
		codedOutStream.writeSInt32(OsmandOdb.TransportStopsTree.RIGHT_FIELD_NUMBER, rightX - bounds.rightX);
		codedOutStream.writeSInt32(OsmandOdb.TransportStopsTree.TOP_FIELD_NUMBER, topY - bounds.topY);
		codedOutStream.writeSInt32(OsmandOdb.TransportStopsTree.BOTTOM_FIELD_NUMBER, bottomY - bounds.bottomY);
		stackBounds.push(new Bounds(leftX, rightX, topY, bottomY));
		stackBaseIds.push(-1L);

	}

	public void endWriteTransportTreeElement() throws IOException {
		Long baseId = stackBaseIds.pop();
		if (baseId >= 0) {
			codedOutStream.writeUInt64(OsmandOdb.TransportStopsTree.BASEID_FIELD_NUMBER, baseId);
		}
		popState(TRANSPORT_STOPS_TREE);
		stackBounds.pop();
		writeInt32Size();
	}

	public void writeTransportStop(long id, int x24, int y24, String name, String nameEn, Map<String, String> names, Map<String, Integer> stringTable,
			TLongArrayList routesOffsets, TLongArrayList routesIds, TLongArrayList deletedRoutes, Map<Entity.EntityId, List<TransportStopExit>> exits) throws IOException {
		checkPeekState(TRANSPORT_STOPS_TREE);

		Bounds bounds = stackBounds.peek();
		if (stackBaseIds.peek() == -1) {
			stackBaseIds.pop();
			stackBaseIds.push(id);
		}
		codedOutStream.writeTag(OsmandOdb.TransportStopsTree.LEAFS_FIELD_NUMBER, WireFormat.FieldType.MESSAGE.getWireType());
		long fp = getFilePointer();

		OsmandOdb.TransportStop.Builder ts = OsmandOdb.TransportStop.newBuilder();
		ts.setName(registerString(stringTable, name));
		if (nameEn != null) {
			ts.setNameEn(registerString(stringTable, nameEn));
		}
		ts.setDx(x24 - bounds.leftX);
		ts.setDy(y24 - bounds.topY);
		ts.setId(id - stackBaseIds.peek());
		mapDataBuf.clear();
		for (Map.Entry<String, String> entry : names.entrySet()) {
			writeRawVarint32(mapDataBuf, registerString(stringTable,entry.getKey()));
			writeRawVarint32(mapDataBuf, registerString(stringTable,entry.getValue()));
		}
		ts.setAdditionalNamePairs(ByteString.copyFrom(mapDataBuf.toArray()));

		for (long i : routesOffsets.toArray()) {
			ts.addRoutes((int) (fp - i));
		}
		for (long i : routesIds.toArray()) {
			ts.addRoutesIds(i);
		}
		for (long i : deletedRoutes.toArray()) {
			ts.addDeletedRoutesIds(i);
		}
		List<TransportStopExit> list = exits.get(new EntityId(EntityType.NODE, id));
		if (list != null) {
			for (TransportStopExit e : list) {
				OsmandOdb.TransportStopExit.Builder exit = OsmandOdb.TransportStopExit.newBuilder();
				LatLon location = e.getLocation();
				int exitX24 = (int) MapUtils.getTileNumberX(24, location.getLongitude());
				int exitY24 = (int) MapUtils.getTileNumberY(24, location.getLatitude());
				exit.setRef(registerString(stringTable, e.getRef()));
				exit.setDx(exitX24 - bounds.leftX);
				exit.setDy(exitY24 - bounds.topY);
				ts.addExits(exit);
			}
		}


		codedOutStream.writeMessageNoTag(ts.build());
	}
	
	public void writeIncompleteTransportRoutes(Collection<net.osmand.data.TransportRoute> incompleteRoutes, Map<String, Integer> stringTable, long transportIndexOffset) throws IOException {
		checkPeekState(TRANSPORT_INDEX_INIT);
		OsmandOdb.IncompleteTransportRoutes.Builder irs = OsmandOdb.IncompleteTransportRoutes.newBuilder();
		for (net.osmand.data.TransportRoute tr : incompleteRoutes) {
			OsmandOdb.IncompleteTransportRoute.Builder ir = OsmandOdb.IncompleteTransportRoute.newBuilder();
			ir.setId(tr.getId());
			ir.setRouteRef((int) (tr.getFileOffset() - transportIndexOffset));
			ir.setOperator(registerString(stringTable, tr.getOperator()));
			ir.setRef(registerString(stringTable, tr.getRef()));
			ir.setType(registerString(stringTable, tr.getType()));
			irs.addRoutes(ir);
		}
		codedOutStream.writeMessage(OsmAndTransportIndex.INCOMPLETEROUTES_FIELD_NUMBER, irs.build());
	}
	
	public void writeTransportStringTable(Map<String, Integer> stringTable) throws IOException {
		checkPeekState(TRANSPORT_INDEX_INIT);
		// expect linked hash map
		int i = 0;
		OsmandOdb.StringTable.Builder st = OsmandOdb.StringTable.newBuilder();
		for (String s : stringTable.keySet()) {
			if (stringTable.get(s) != i++) {
				throw new IllegalStateException();
			}
			st.addS(s);
		}
		codedOutStream.writeMessage(OsmAndTransportIndex.STRINGTABLE_FIELD_NUMBER, st.build());
	}

	public long startWritePoiIndex(String name, int left31, int right31, int bottom31, int top31) throws IOException {
		pushState(POI_INDEX_INIT, OSMAND_STRUCTURE_INIT);
		codedOutStream.writeTag(OsmandOdb.OsmAndStructure.POIINDEX_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
		stackBounds.push(new Bounds(0, 0, 0, 0)); // for poi index tree
		preserveInt32Size();
		long startPointer = getFilePointer();
		if (name != null) {
			codedOutStream.writeString(OsmandOdb.OsmAndPoiIndex.NAME_FIELD_NUMBER, name);
		}
		OsmandOdb.OsmAndTileBox.Builder builder = OsmandOdb.OsmAndTileBox.newBuilder();
		builder.setLeft(left31);
		builder.setRight(right31);
		builder.setTop(top31);
		builder.setBottom(bottom31);
		codedOutStream.writeMessage(OsmandOdb.OsmAndPoiIndex.BOUNDARIES_FIELD_NUMBER, builder.build());
		return startPointer;
	}

	public void endWritePoiIndex() throws IOException {
		popState(POI_INDEX_INIT);
		long len = writeInt32Size();
		stackBounds.pop();
		log.info("POI INDEX SIZE : " + len);
	}

	public void writePoiCategoriesTable(PoiCreatorCategories cs) throws IOException {
		checkPeekState(POI_INDEX_INIT);
		int i = 0;
		for (String cat : cs.categories.keySet()) {
			Builder builder = OsmandOdb.OsmAndCategoryTable.newBuilder();
			builder.setCategory(cat);
			Set<String> subcatSource = cs.categories.get(cat);
			cs.setCategoryIndex(cat, i);
			int j = 0;
			for (String s : subcatSource) {
				cs.setSubcategoryIndex(cat, s, j);
				builder.addSubcategories(s);
				j++;
			}
			codedOutStream.writeMessage(OsmandOdb.OsmAndPoiIndex.CATEGORIESTABLE_FIELD_NUMBER, builder.build());
			i++;
		}

	}

	public void writePoiSubtypesTable(PoiCreatorCategories cs, Map<String, Set<String>> topIndexAdditional) throws IOException {
		checkPeekState(POI_INDEX_INIT);
		int subcatId = 0;
		OsmAndSubtypesTable.Builder builder = OsmandOdb.OsmAndSubtypesTable.newBuilder();
		Map<String, List<PoiAdditionalType>> groupAdditionalByTagName = new HashMap<String, List<PoiAdditionalType>>();
		for (PoiAdditionalType rt : cs.additionalAttributes) {
			if (!rt.isText()) {
				if (topIndexAdditional.containsKey(rt.getTag())) {
					Set<String> topIndexSet = topIndexAdditional.get(rt.getTag());
					if (!topIndexSet.contains(rt.getValue())) {
						continue;
					}
				}
				if (!groupAdditionalByTagName.containsKey(rt.getTag())) {
					groupAdditionalByTagName.put(rt.getTag(), new ArrayList<PoiAdditionalType>());
				}
				groupAdditionalByTagName.get(rt.getTag()).add(rt);
			} else {
				rt.setTargetPoiId(subcatId++, 0);
				OsmAndPoiSubtype.Builder subType = OsmandOdb.OsmAndPoiSubtype.newBuilder();
				subType.setName(rt.getTag());
				subType.setIsText(true);
				builder.addSubtypes(subType);
			}
		}

		for (String tag : groupAdditionalByTagName.keySet()) {
			int cInd = subcatId++;
			OsmAndPoiSubtype.Builder subType = OsmandOdb.OsmAndPoiSubtype.newBuilder();
			subType.setName(tag);
			subType.setIsText(false);
			List<PoiAdditionalType> list = groupAdditionalByTagName.get(tag);
			subType.setSubtypeValuesSize(list.size());
			int subcInd = 0;
			for (PoiAdditionalType subtypeVal : list) {
				subtypeVal.setTargetPoiId(cInd, subcInd++);
				subType.addSubtypeValue(subtypeVal.getValue());
			}
			builder.addSubtypes(subType);
		}
		codedOutStream.writeMessage(OsmandOdb.OsmAndPoiIndex.SUBTYPESTABLE_FIELD_NUMBER, builder.build());
	}

	public void writePoiCategories(PoiCreatorCategories poiCats) throws IOException {
		checkPeekState(POI_BOX);
		OsmandOdb.OsmAndPoiCategories.Builder builder = OsmandOdb.OsmAndPoiCategories.newBuilder();
		int prev = -1;
		poiCats.cachedCategoriesIds.sort();
		for (int i = 0; i < poiCats.cachedCategoriesIds.size(); i++) {
			// avoid duplicates
			if (i == 0 || prev != poiCats.cachedCategoriesIds.get(i)) {
				builder.addCategories(poiCats.cachedCategoriesIds.get(i));
				prev = poiCats.cachedCategoriesIds.get(i);
			}
		}
		prev = -1;
		poiCats.cachedAdditionalIds.sort();
		for (int i = 0; i < poiCats.cachedAdditionalIds.size(); i++) {
			// avoid duplicates
			if (i == 0 || prev != poiCats.cachedAdditionalIds.get(i)) {
				builder.addSubcategories(poiCats.cachedAdditionalIds.get(i));
				prev = poiCats.cachedAdditionalIds.get(i);
			}
		}
		codedOutStream.writeMessage(OsmandOdb.OsmAndPoiBox.CATEGORIES_FIELD_NUMBER, builder.build());
	}

	public void writePoiTagGroups(PoiCreatorTagGroups tagGroups) throws IOException {
		if (tagGroups.ids == null || tagGroups.ids.size() == 0) {
			return;
		}
		checkPeekState(POI_BOX);
		codedOutStream.writeTag(OsmandOdb.OsmAndPoiBox.TAGGROUPS_FIELD_NUMBER, WireFormat.FieldType.MESSAGE.getWireType());

		OsmandOdb.OsmAndPoiTagGroups.Builder groupsBuilder = OsmandOdb.OsmAndPoiTagGroups.newBuilder();

		for (int id : tagGroups.ids) {
			groupsBuilder.addIds(id);
		}

		for (IndexPoiCreator.PoiCreatorTagGroup tag : tagGroups.tagGroups) {
			OsmandOdb.OsmAndPoiTagGroup.Builder tagBuilder = OsmandOdb.OsmAndPoiTagGroup.newBuilder();
			tagBuilder.setId(tag.id);
			for (String tagValue : tag.tagValues) {
				tagBuilder.addTagValues(tagValue);
			}
			groupsBuilder.addGroups(tagBuilder);
		}
		codedOutStream.writeMessageNoTag(groupsBuilder.build());
	}

	public Map<PoiTileBox, List<BinaryFileReference>> writePoiNameIndex(Map<String, Set<PoiTileBox>> namesIndex, long startPoiIndex) throws IOException {
		checkPeekState(POI_INDEX_INIT);
		codedOutStream.writeTag(OsmandOdb.OsmAndPoiIndex.NAMEINDEX_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
		preserveInt32Size();

		Map<PoiTileBox, List<BinaryFileReference>> fpToWriteSeeks = new LinkedHashMap<PoiTileBox, List<BinaryFileReference>>();
		Map<String, BinaryFileReference> indexedTable = writeIndexedTable(OsmandOdb.OsmAndPoiNameIndex.TABLE_FIELD_NUMBER, namesIndex.keySet());
		for (Map.Entry<String, Set<PoiTileBox>> e : namesIndex.entrySet()) {
			codedOutStream.writeTag(OsmandOdb.OsmAndPoiNameIndex.DATA_FIELD_NUMBER, FieldType.MESSAGE.getWireType());
			BinaryFileReference nameTableRef = indexedTable.get(e.getKey());
			codedOutStream.flush();
			nameTableRef.writeReference(raf, getFilePointer());

			OsmAndPoiNameIndex.OsmAndPoiNameIndexData.Builder builder = OsmAndPoiNameIndex.OsmAndPoiNameIndexData.newBuilder();
			List<PoiTileBox> tileBoxes = new ArrayList<PoiTileBox>(e.getValue());
			for (PoiTileBox box : tileBoxes) {
				OsmandOdb.OsmAndPoiNameIndexDataAtom.Builder bs = OsmandOdb.OsmAndPoiNameIndexDataAtom.newBuilder();
				bs.setX(box.getX());
				bs.setY(box.getY());
				bs.setZoom(box.getZoom());
				bs.setShiftTo(0);
				OsmAndPoiNameIndexDataAtom atom = bs.build();
				builder.addAtoms(atom);
			}
			OsmAndPoiNameIndex.OsmAndPoiNameIndexData msg = builder.build();

			codedOutStream.writeMessageNoTag(msg);
			long endPointer = getFilePointer();

			// first message
			int accumulateSize = 4;
			for (int i = tileBoxes.size() - 1; i >= 0; i--) {
				PoiTileBox box = tileBoxes.get(i);
				if (!fpToWriteSeeks.containsKey(box)) {
					fpToWriteSeeks.put(box, new ArrayList<BinaryFileReference>());
				}
				fpToWriteSeeks.get(box).add(net.osmand.obf.preparation.BinaryFileReference.createShiftReference(endPointer - accumulateSize, startPoiIndex));
				accumulateSize += CodedOutputStream.computeMessageSize(OsmAndPoiNameIndex.OsmAndPoiNameIndexData.ATOMS_FIELD_NUMBER,
						msg.getAtoms(i));

			}
		}

		writeInt32Size();


		return fpToWriteSeeks;
	}

	private Map<String, BinaryFileReference> writeIndexedTable(int tag, Collection<String> indexedTable) throws IOException {
		codedOutStream.writeTag(tag, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
		preserveInt32Size();

		IndexedStringTableNode root = new IndexedStringTableNode();
		for (String key : indexedTable) {
			if (key == null) {
				continue;
			}
			root.addKey(key, 0);
		}

		Map<String, BinaryFileReference> res = new LinkedHashMap<>();
		long init = getFilePointer();
		root.writeNode("", res, init);
		writeInt32Size();
		return res;
	}

	private final class IndexedStringTableNode {
		private final TreeMap<String, IndexedStringTableNode> subNodes = new TreeMap<>();
		private boolean terminal;

		private void addKey(String key, int start) {
			if (start >= key.length()) {
				terminal = true;
				return;
			}
			int end = Math.min(start + 2, key.length());
			String nextKeySuffix = key.substring(start, end);
			IndexedStringTableNode child = subNodes.get(nextKeySuffix);
			if (child == null) {
				child = new IndexedStringTableNode();
				subNodes.put(nextKeySuffix, child);
			}
			child.addKey(key, end);
		}

		private int computeSize() {
			int size = 0;
			for (Map.Entry<String, IndexedStringTableNode> entry : subNodes.entrySet()) {
				String childKey = entry.getKey();
				IndexedStringTableNode child = entry.getValue();
				if (child == null) {
					continue;
				}
				size += CodedOutputStream.computeStringSize(OsmandOdb.IndexedStringTable.KEY_FIELD_NUMBER, childKey);
				if (child.terminal) {
					size += CodedOutputStream.computeTagSize(OsmandOdb.IndexedStringTable.VAL_FIELD_NUMBER);
					size += 4;
				}
				if (!child.subNodes.isEmpty()) {
					int nestedSize = child.computeSize();
					size += CodedOutputStream.computeTagSize(OsmandOdb.IndexedStringTable.SUBTABLES_FIELD_NUMBER);
					size += CodedOutputStream.computeUInt32SizeNoTag(nestedSize);
					size += nestedSize;
				}
			}
			return size;
		}

		private void writeNode(String prefix, Map<String, BinaryFileReference> res, long init) throws IOException {
			for (Map.Entry<String, IndexedStringTableNode> entry : subNodes.entrySet()) {
				String subKey = entry.getKey();
				IndexedStringTableNode child = entry.getValue();
				if (child == null) {
					continue;
				}
				String fullKey = prefix + subKey;
				codedOutStream.writeString(OsmandOdb.IndexedStringTable.KEY_FIELD_NUMBER, subKey);
				if (child.terminal) {
					codedOutStream.writeTag(OsmandOdb.IndexedStringTable.VAL_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
					codedOutStream.writeFixed32NoTag(0);
					BinaryFileReference ref = BinaryFileReference.createShiftReference(getFilePointer(), init);
					res.put(fullKey, ref);
				}
				if (!child.subNodes.isEmpty()) {
					codedOutStream.writeTag(OsmandOdb.IndexedStringTable.SUBTABLES_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED);
					int subtableSize = child.computeSize();
					codedOutStream.writeUInt32NoTag(subtableSize);
					child.writeNode(fullKey, res, init);
				}
			}
		}
	}


	public void writePoiDataAtom(long id, int x24shift, int y24shift,
								 String type, String subtype, Map<PoiAdditionalType, String> additionalNames,
								 PoiCreatorCategories globalCategories, int limitZip, int precisionXY, List<Integer> tagGroupsIds) throws IOException {
		checkPeekState(POI_DATA);
		TIntArrayList types = globalCategories.buildTypeIds(type, subtype);
		OsmAndPoiBoxDataAtom.Builder builder = OsmandOdb.OsmAndPoiBoxDataAtom.newBuilder();
		builder.setDx(x24shift);
		builder.setDy(y24shift);
		builder.setPrecisionXY(precisionXY);
		for (int i = 0; i < types.size(); i++) {
			int j = types.get(i);
			builder.addCategories(j);
		}

		builder.setId(id);
		for (int tagGroupId : tagGroupsIds) {
			builder.addTagGroups(tagGroupId);
		}

		for (Map.Entry<PoiAdditionalType, String> rt : additionalNames.entrySet()) {
			int targetPoiId = rt.getKey().getTargetId();
			String tg = rt.getKey().getTag();
			if (targetPoiId < 0) {
				throw new IllegalStateException("Illegal target poi id");
			}
			if (!rt.getKey().isText()) {
				builder.addSubcategories(targetPoiId);
			} else {
				builder.addTextCategories(targetPoiId);
				String vl = rt.getValue();
				// bug with opening hours zipping (fixed in 3.9)
				if (vl != null && limitZip != -1 && vl.length() >= limitZip && !"opening_hours".equals(tg)) {
					ByteArrayOutputStream bous = new ByteArrayOutputStream(vl.length());
					GZIPOutputStream gz = new GZIPOutputStream(bous);
					byte[] bts = vl.getBytes("UTF-8");
					gz.write(bts);
					gz.close();
					byte[] res = bous.toByteArray();
					StringBuilder sb = new StringBuilder(res.length);
					sb.append(" gz ");
					for (int i = 0; i < res.length; i++) {
						sb.append((char) ((int) res[i] + 128 + 32));
					}
					vl = sb.toString();
				} else if (vl != null) {
					vl = vl.trim();
				}
				builder.addTextValues(vl);
			}
		}
		codedOutStream.writeMessage(OsmandOdb.OsmAndPoiBoxData.POIDATA_FIELD_NUMBER, builder.build());

	}

	public void startWritePoiData(int zoom, int x, int y, List<BinaryFileReference> fpPoiBox) throws IOException {
		pushState(POI_DATA, POI_INDEX_INIT);
		codedOutStream.writeTag(OsmandOdb.OsmAndPoiIndex.POIDATA_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
		long pointer = getFilePointer();
		preserveInt32Size();
		codedOutStream.flush();
		// write shift to that data
		for (int i = 0; i < fpPoiBox.size(); i++) {
			fpPoiBox.get(i).writeReference(raf, pointer);
		}

		codedOutStream.writeUInt32(OsmandOdb.OsmAndPoiBoxData.ZOOM_FIELD_NUMBER, zoom);
		codedOutStream.writeUInt32(OsmandOdb.OsmAndPoiBoxData.X_FIELD_NUMBER, x);
		codedOutStream.writeUInt32(OsmandOdb.OsmAndPoiBoxData.Y_FIELD_NUMBER, y);

	}

	public void endWritePoiData() throws IOException {
		popState(POI_DATA);
		writeInt32Size();
	}

	public BinaryFileReference startWritePoiBox(int zoom, int tileX, int tileY, long startPoiIndex, boolean end)
			throws IOException {
		checkPeekState(POI_INDEX_INIT, POI_BOX);
		if (state.peek() == POI_INDEX_INIT) {
			codedOutStream.writeTag(OsmandOdb.OsmAndPoiIndex.BOXES_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
		} else {
			codedOutStream.writeTag(OsmandOdb.OsmAndPoiBox.SUBBOXES_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
		}
		state.push(POI_BOX);
		preserveInt32Size();

		Bounds bounds = stackBounds.peek();
		int parentZoom = bounds.rightX;
		int parentTileX = bounds.leftX;
		int parentTileY = bounds.topY;

		int pTileX = parentTileX << (zoom - parentZoom);
		int pTileY = parentTileY << (zoom - parentZoom);
		codedOutStream.writeUInt32(OsmandOdb.OsmAndPoiBox.ZOOM_FIELD_NUMBER, (zoom - parentZoom));
		codedOutStream.writeSInt32(OsmandOdb.OsmAndPoiBox.LEFT_FIELD_NUMBER, tileX - pTileX);
		codedOutStream.writeSInt32(OsmandOdb.OsmAndPoiBox.TOP_FIELD_NUMBER, tileY - pTileY);
		stackBounds.push(new Bounds(tileX, zoom, tileY, 0));

		if (end) {
			codedOutStream.writeTag(OsmandOdb.OsmAndPoiBox.SHIFTTODATA_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
			BinaryFileReference shift = BinaryFileReference.createShiftReference(getFilePointer(), startPoiIndex);
			codedOutStream.writeFixed32NoTag(0);
			return shift;
		}
		return null;
	}

	public void endWritePoiBox() throws IOException {
		popState(POI_BOX);
		writeInt32Size();
		stackBounds.pop();
	}

	private void pushState(int push, int peek) {
		if (state.peek() != peek) {
			throw new IllegalStateException("expected " + peek + " != " + state.peek());
		}
		state.push(push);
	}

	private void checkPeekState(int... states) {
		for (int i = 0; i < states.length; i++) {
			if (states[i] == state.peek()) {
				return;
			}
		}
		throw new IllegalStateException("Note expected state : " + state.peek());
	}

	private void popState(int state) {
		Integer st = this.state.pop();
		if (st != state) {
			throw new IllegalStateException("expected " + state + " != " + st);
		}
	}

	public void flush() throws IOException {
		codedOutStream.flush();
	}

	public void close() throws IOException {
		checkPeekState(OSMAND_STRUCTURE_INIT);
		codedOutStream.writeInt32(OsmandOdb.OsmAndStructure.VERSIONCONFIRM_FIELD_NUMBER, IndexConstants.BINARY_MAP_VERSION);
		codedOutStream.flush();
	}

	public void preclose() throws IOException {
		codedOutStream.writeInt32(OsmandOdb.OsmAndStructure.VERSIONCONFIRM_FIELD_NUMBER, IndexConstants.BINARY_MAP_VERSION);
		codedOutStream.flush();
	}

	public void writeOsmAndOwner(BinaryMapIndexReader.OsmAndOwner owner) throws IOException {
		OsmandOdb.OsmAndOwner.Builder b = OsmandOdb.OsmAndOwner.newBuilder();
		b.setName(owner.getName());
		if (!owner.getResource().isEmpty()) {
			b.setResource(owner.getResource());
		}
		if (!owner.getDescription().isEmpty()) {
			b.setDescription(owner.getDescription());
		}
		if (!owner.getPluginid().isEmpty()) {
			b.setPluginid(owner.getPluginid());
		}
		Message m = b.build();
		codedOutStream.writeMessage(OsmandOdb.OsmAndStructure.OWNER_FIELD_NUMBER, m);
	}

}
