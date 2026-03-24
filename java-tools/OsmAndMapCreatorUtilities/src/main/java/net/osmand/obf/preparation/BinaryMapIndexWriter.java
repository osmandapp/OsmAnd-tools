package net.osmand.obf.preparation;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.Entry;
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
import net.osmand.binary.BloomFilter;
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
import net.osmand.obf.preparation.IndexPoiCreator.PoiDataBlock;
import net.osmand.obf.preparation.IndexPoiCreator.PoiCreatorTagGroups;
import net.osmand.obf.preparation.IndexPoiCreator.LeafMetrics;
import net.osmand.osm.MapRenderingTypes.MapRulType;
import net.osmand.osm.MapRoutingTypes.MapPointName;
import net.osmand.osm.MapRoutingTypes.MapRouteType;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.Node;
import net.osmand.router.HHRouteDataStructure.NetworkDBPoint;
import net.osmand.router.HHRoutingOBFWriter.NetworkDBPointWrite;
import net.osmand.CollatorStringMatcher;
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
	private static final int POI_NAME_SIDECAR_COUNT_THRESHOLD = 256;
	private static final int POI_NAME_SIDECAR_SIZE_BUDGET = 2048;
	private static final int MIN_POSTING_SIZE_FOR_SIDECAR = 3;
	private static final int MAX_SELECTED_KEYS_PER_LEAF = 16;
	private static final double MAX_SPAN_FRAG = 0.50d;
	private static final double SIDE_CAR_BYTE_PENALTY = 0.0d;
	private static final int RESCUE_MIN_POSTING_SIZE_FOR_SIDECAR = 2;
	private static final double RESCUE_MAX_SPAN_FRAG = 0.75d;
	private static final double RESIDUAL_RESCUE_MASS_THRESHOLD = 0.70d;
	private static final double RESIDUAL_RESCUE_COVERAGE_THRESHOLD = 0.30d;
	private static final int POI_NAME_SIDECAR_CONTINUATION_KEY_LENGTH = 2;
	private static final int POI_NAME_SIDECAR_MAX_CONTINUATION_DEPTH = 6;
	private static final int POI_NAME_SIDECAR_BRANCH_HEAVY_THRESHOLD = 64;
	private static final double POI_NAME_SIDECAR_BRANCH_TOP1_MASS_MAX = 0.60d;
	private static final double POI_NAME_SIDECAR_BRANCH_COVERAGE_MIN = 0.70d;
	private static final double POI_NAME_SIDECAR_BRANCH_RESIDUAL_MIN = 0.30d;
	private static final double BROAD_PREFIX_TOP3_MASS_THRESHOLD = 0.35d;
	private static final int BROAD_PREFIX_MIN_ELIGIBLE_CANDIDATES = 3;
	private static final int RESIDUAL_SPARSE_ATOM_THRESHOLD = 512;
	private static final double RESIDUAL_SPARSE_MASS_MIN = 0.85d;
	private static final double RESIDUAL_SPARSE_COVERAGE_MAX = 0.15d;
	private static final int RESIDUAL_SPARSE_MAX_ELIGIBLE = 1;
	private static final double RESIDUAL_SPARSE_TOP1_MAX = 0.10d;
	private static final int MIN_PROBE_ELIGIBLE_KEYS = 2;
	private static final double MIN_PROBE_COVERAGE = 0.15d;
	private static final double MAX_PROBE_RESIDUAL = 0.85d;
	private static final double MIN_PROBE_SPEEDUP = 1.10d;
	private static final double PROBE_IMPROVEMENT_FACTOR = 0.90d;
	private final Map<String, PoiNameSidecarSelectionResult> lastPoiNameSidecarSelections = new HashMap<>();

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

	private static class PoiNameSidecarEntry {
		final String key;
		final LinkedHashSet<Integer> atomIndexes = new LinkedHashSet<>();

		PoiNameSidecarEntry(String key) {
			this.key = key;
		}
	}

	private static class PoiNameSidecarNode {
		final int parentNodeIndex;
		final String keySegment;
		final int continuationDepth;
		final List<Integer> atomIndexes;
		final List<Integer> residualAtomIndexes;
		final List<Integer> childNodeIndexes = new ArrayList<>();
		boolean terminal;

		PoiNameSidecarNode(int parentNodeIndex, String keySegment, int continuationDepth, List<Integer> atomIndexes,
				List<Integer> residualAtomIndexes, boolean terminal) {
			this.parentNodeIndex = parentNodeIndex;
			this.keySegment = keySegment;
			this.continuationDepth = continuationDepth;
			this.atomIndexes = new ArrayList<>(atomIndexes);
			this.residualAtomIndexes = new ArrayList<>(residualAtomIndexes);
			this.terminal = terminal;
		}
	}

	private static class PoiNameSidecarHierarchy {
		final List<PoiNameSidecarNode> nodes;
		final int bytes;
		final int terminalNodeCount;
		final int maxContinuationDepth;

		PoiNameSidecarHierarchy(List<PoiNameSidecarNode> nodes, int bytes, int terminalNodeCount, int maxContinuationDepth) {
			this.nodes = nodes;
			this.bytes = bytes;
			this.terminalNodeCount = terminalNodeCount;
			this.maxContinuationDepth = maxContinuationDepth;
		}

		boolean isEmpty() {
			return Algorithms.isEmpty(nodes);
		}
	}

	private static class PoiNameSidecarProbeDepthResult {
		final int continuationLength;
		final int selectedKeyCount;
		final int eligibleCandidateCount;
		final int coveredAtomCount;
		final double coveredAtomRatio;
		final int residualAtomCount;
		final double residualMass;
		final double top1ContinuationMassRatio;
		final double top3ContinuationMassRatio;
		final double top5ContinuationMassRatio;
		final double avgFrag;
		final double maxFrag;
		final double expectedCostWithSidecar;
		final double estimatedSpeedup;
		final int estimatedSidecarBytes;
		final boolean accepted;

		PoiNameSidecarProbeDepthResult(int continuationLength, int selectedKeyCount, int eligibleCandidateCount,
				int coveredAtomCount, double coveredAtomRatio, int residualAtomCount, double residualMass,
				double top1ContinuationMassRatio, double top3ContinuationMassRatio, double top5ContinuationMassRatio,
				double avgFrag, double maxFrag, double expectedCostWithSidecar, double estimatedSpeedup,
				int estimatedSidecarBytes, boolean accepted) {
			this.continuationLength = continuationLength;
			this.selectedKeyCount = selectedKeyCount;
			this.eligibleCandidateCount = eligibleCandidateCount;
			this.coveredAtomCount = coveredAtomCount;
			this.coveredAtomRatio = coveredAtomRatio;
			this.residualAtomCount = residualAtomCount;
			this.residualMass = residualMass;
			this.top1ContinuationMassRatio = top1ContinuationMassRatio;
			this.top3ContinuationMassRatio = top3ContinuationMassRatio;
			this.top5ContinuationMassRatio = top5ContinuationMassRatio;
			this.avgFrag = avgFrag;
			this.maxFrag = maxFrag;
			this.expectedCostWithSidecar = expectedCostWithSidecar;
			this.estimatedSpeedup = estimatedSpeedup;
			this.estimatedSidecarBytes = estimatedSidecarBytes;
			this.accepted = accepted;
		}
	}

	static class PoiNameSidecarSelectionResult {
		final List<PoiNameSidecarEntry> selectedEntries;
		final List<Integer> residualAtomIndexes;
		final List<PoiNameSidecarNode> sidecarNodes;
		final int selectedKeyCount;
		final int rescueSelectedKeyCount;
		final int residualAtomCount;
		final int coveredAtomCount;
		final double residualMass;
		final double coveredAtomRatio;
		final double avgFrag;
		final double maxFrag;
		final int expectedSearchCostNoSidecar;
		final double expectedSearchCostWithSidecar;
		final double objectiveCostWithSidecar;
		final int sidecarBytes;
		final int sidecarNodeCount;
		final int terminalSidecarNodeCount;
		final int maxContinuationDepth;
		final double avgGainDensity;
		final int overlapFilteredCandidateCount;
		final int coverageEligibleCandidateCount;
		final int budgetRejectedCandidateCount;
		final int eligibleCandidateCount;
		final double top1ContinuationMassRatio;
		final double top3ContinuationMassRatio;
		final double top5ContinuationMassRatio;
		final double selectedGainMassRatio;
		final boolean broadPrefixLowSelectivity;
		final boolean residualFallbackUsed;
		final boolean residualDominatedSparseLeaf;
		final boolean probeTried;
		final int probeBestDepth;
		final int probeAcceptedDepthCount;
		final double probeBaselineCost;
		final double probeBestCost;
		final double probeImprovementRatio;
		final List<PoiNameSidecarProbeDepthResult> probeDepthResults;

		PoiNameSidecarSelectionResult(List<PoiNameSidecarEntry> selectedEntries, List<Integer> residualAtomIndexes,
				List<PoiNameSidecarNode> sidecarNodes, int selectedKeyCount, int rescueSelectedKeyCount, int residualAtomCount, int coveredAtomCount,
				double residualMass, double coveredAtomRatio, double avgFrag, double maxFrag,
				int expectedSearchCostNoSidecar, double expectedSearchCostWithSidecar,
				double objectiveCostWithSidecar, int sidecarBytes, int sidecarNodeCount, int terminalSidecarNodeCount,
				int maxContinuationDepth, double avgGainDensity,
				int overlapFilteredCandidateCount, int coverageEligibleCandidateCount, int budgetRejectedCandidateCount,
				int eligibleCandidateCount,
				double top1ContinuationMassRatio, double top3ContinuationMassRatio, double top5ContinuationMassRatio,
				double selectedGainMassRatio, boolean broadPrefixLowSelectivity, boolean residualFallbackUsed,
				boolean residualDominatedSparseLeaf, boolean probeTried, int probeBestDepth, int probeAcceptedDepthCount,
				double probeBaselineCost, double probeBestCost, double probeImprovementRatio,
				List<PoiNameSidecarProbeDepthResult> probeDepthResults) {
			this.selectedEntries = selectedEntries;
			this.residualAtomIndexes = residualAtomIndexes;
			this.sidecarNodes = sidecarNodes;
			this.selectedKeyCount = selectedKeyCount;
			this.rescueSelectedKeyCount = rescueSelectedKeyCount;
			this.residualAtomCount = residualAtomCount;
			this.coveredAtomCount = coveredAtomCount;
			this.residualMass = residualMass;
			this.coveredAtomRatio = coveredAtomRatio;
			this.avgFrag = avgFrag;
			this.maxFrag = maxFrag;
			this.expectedSearchCostNoSidecar = expectedSearchCostNoSidecar;
			this.expectedSearchCostWithSidecar = expectedSearchCostWithSidecar;
			this.objectiveCostWithSidecar = objectiveCostWithSidecar;
			this.sidecarBytes = sidecarBytes;
			this.sidecarNodeCount = sidecarNodeCount;
			this.terminalSidecarNodeCount = terminalSidecarNodeCount;
			this.maxContinuationDepth = maxContinuationDepth;
			this.avgGainDensity = avgGainDensity;
			this.overlapFilteredCandidateCount = overlapFilteredCandidateCount;
			this.coverageEligibleCandidateCount = coverageEligibleCandidateCount;
			this.budgetRejectedCandidateCount = budgetRejectedCandidateCount;
			this.eligibleCandidateCount = eligibleCandidateCount;
			this.top1ContinuationMassRatio = top1ContinuationMassRatio;
			this.top3ContinuationMassRatio = top3ContinuationMassRatio;
			this.top5ContinuationMassRatio = top5ContinuationMassRatio;
			this.selectedGainMassRatio = selectedGainMassRatio;
			this.broadPrefixLowSelectivity = broadPrefixLowSelectivity;
			this.residualFallbackUsed = residualFallbackUsed;
			this.residualDominatedSparseLeaf = residualDominatedSparseLeaf;
			this.probeTried = probeTried;
			this.probeBestDepth = probeBestDepth;
			this.probeAcceptedDepthCount = probeAcceptedDepthCount;
			this.probeBaselineCost = probeBaselineCost;
			this.probeBestCost = probeBestCost;
			this.probeImprovementRatio = probeImprovementRatio;
			this.probeDepthResults = probeDepthResults;
		}

		boolean isEmpty() {
			return selectedEntries.isEmpty() && residualAtomIndexes.isEmpty();
		}
	}

	private static class PoiNameSidecarEntryMetrics {
		final String key;
		final List<Integer> atomIndexes;
		final int postingSize;
		final int spanCount;
		final double frag;
		final int estimatedBytes;
		final double searchGain;
		final double objectiveGain;
		final double gainDensity;

		PoiNameSidecarEntryMetrics(String key, List<Integer> atomIndexes, int postingSize, int spanCount, double frag,
				int estimatedBytes, double searchGain, double objectiveGain, double gainDensity) {
			this.key = key;
			this.atomIndexes = atomIndexes;
			this.postingSize = postingSize;
			this.spanCount = spanCount;
			this.frag = frag;
			this.estimatedBytes = estimatedBytes;
			this.searchGain = searchGain;
			this.objectiveGain = objectiveGain;
			this.gainDensity = gainDensity;
		}
	}

	private static class PoiNameSidecarSelectionStep {
		final PoiNameSidecarEntryMetrics candidate;
		final List<Integer> selectedAtomIndexes;
		final int estimatedBytes;
		final double objectiveGain;
		final double gainDensity;

		PoiNameSidecarSelectionStep(PoiNameSidecarEntryMetrics candidate, List<Integer> selectedAtomIndexes,
				int estimatedBytes, double objectiveGain, double gainDensity) {
			this.candidate = candidate;
			this.selectedAtomIndexes = selectedAtomIndexes;
			this.estimatedBytes = estimatedBytes;
			this.objectiveGain = objectiveGain;
			this.gainDensity = gainDensity;
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

		Map<String, List<MapObject>> normalizedNamesIndex = normalizeIndex(namesIndex);
		Map<String, BinaryFileReference> res = writeIndexedTable(OsmAndAddressNameIndexData.TABLE_FIELD_NUMBER, normalizedNamesIndex.keySet());
		for (Entry<String, List<MapObject>> entry : normalizedNamesIndex.entrySet()) {
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

	public Map<PoiDataBlock, List<BinaryFileReference>> writePoiNameIndex(Map<String, Set<PoiDataBlock>> namesIndex, long startPoiIndex) throws IOException {
		checkPeekState(POI_INDEX_INIT);
		lastPoiNameSidecarSelections.clear();
		codedOutStream.writeTag(OsmandOdb.OsmAndPoiIndex.NAMEINDEX_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
		preserveInt32Size();
		codedOutStream.writeMessage(OsmandOdb.OsmAndPoiNameIndex.FILTERS_FIELD_NUMBER,
				OsmandOdb.OsmAndBloomFilterAlgorithm.newBuilder().setVersion(BloomFilter.VERSION).build());

		Map<PoiDataBlock, List<BinaryFileReference>> fpToWriteSeeks = new LinkedHashMap<PoiDataBlock, List<BinaryFileReference>>();
		Map<String, BinaryFileReference> indexedTable = writeIndexedTable(OsmandOdb.OsmAndPoiNameIndex.TABLE_FIELD_NUMBER, namesIndex.keySet());
		for (Map.Entry<String, Set<PoiDataBlock>> e : namesIndex.entrySet()) {
			codedOutStream.writeTag(OsmandOdb.OsmAndPoiNameIndex.DATA_FIELD_NUMBER, FieldType.MESSAGE.getWireType());
			BinaryFileReference nameTableRef = indexedTable.get(e.getKey());
			codedOutStream.flush();
			nameTableRef.writeReference(raf, getFilePointer());

			List<PoiDataBlock> poiDataBlocks = new ArrayList<>(e.getValue());
			List<OsmAndPoiNameIndexDataAtom> atoms = new ArrayList<>(poiDataBlocks.size());
			for (PoiDataBlock box : poiDataBlocks) {
				OsmandOdb.OsmAndPoiNameIndexDataAtom.Builder bs = OsmandOdb.OsmAndPoiNameIndexDataAtom.newBuilder();
				bs.setX(box.getX());
				bs.setY(box.getY());
				bs.setZoom(box.getZoom());
				bs.setShiftTo(0);
				bs.addBloomIndex(ByteString.copyFrom(box.getIndexBloom()));
				OsmAndPoiNameIndexDataAtom atom = bs.build();
				if (atom.getBloomIndexCount() != 1) {
					throw new IllegalStateException("POI name index atom must reference exactly one physical data block bloom");
				}
				atoms.add(atom);
			}
			if (atoms.size() != poiDataBlocks.size()) {
				throw new IllegalStateException("POI name index atom count must match PoiDataBlock count");
			}
			int sidecarContributionSize = writePoiNameIndexDataMessage(e.getKey(), poiDataBlocks, atoms);
			long endPointer = getFilePointer();

			// first message
			int accumulateSize = 4 + sidecarContributionSize;
			for (int i = poiDataBlocks.size() - 1; i >= 0; i--) {
				PoiDataBlock box = poiDataBlocks.get(i);
				if (!fpToWriteSeeks.containsKey(box)) {
					fpToWriteSeeks.put(box, new ArrayList<BinaryFileReference>());
				}
				fpToWriteSeeks.get(box).add(net.osmand.obf.preparation.BinaryFileReference.createShiftReference(endPointer - accumulateSize, startPoiIndex));
				accumulateSize += CodedOutputStream.computeMessageSize(OsmAndPoiNameIndex.OsmAndPoiNameIndexData.ATOMS_FIELD_NUMBER,
						atoms.get(i));

			}
		}

		writeInt32Size();
		return fpToWriteSeeks;
	}

	public void writeAtomMetricsReport(List<IndexPoiCreator.LeafMetrics> leafMetrics) {
		System.out.println("=== ATOM_REPORT ===");
		System.out.println("leafKey4,poiCount,atomCount,avg_PoisPerAtom,p95_PoisPerAtom,max_PoisPerAtom,avg_UniqueBloomTokens,p95_UniqueBloomTokens,max_UniqueBloomTokens,avg_AtomKeyCount,p95_AtomKeyCount,uniqKeys,sidecarBuilt,sidecarBytes,sidecarNodeCount,terminalSidecarNodeCount,maxContinuationDepth,selectedKeyCount,rescueSelectedKeyCount,residualAtomCount,coveredAtomCount,residualMass,coveredAtomRatio,avg_Frag,maxFrag,expectedCostNoSidecar,expectedCostWithSidecar,estimatedSpeedup,avgGainDensity,overlapFilteredCandidateCount,eligibleCandidateCount,top1ContinuationMassRatio,top3ContinuationMassRatio,top5ContinuationMassRatio,selectedGainMassRatio,broadPrefixLowSelectivity,residualFallbackUsed,heavyLeaves,heavyLeavesNoEligibleKeys,heavyLeavesNoPositiveGain,heavyLeavesRejectedByCoverage,heavyLeavesRejectedByResidual,heavyLeavesRejectedByBudget,residualDominatedSparseLeaf,probeTried,probeBestDepth,probeAcceptedDepthCount,probeBaselineCost,probeBestCost,probeImprovementRatio,probeCoverage_L2,probeCoverage_L3,probeCoverage_L4,probeCoverage_L5,probeCoverage_L6,probeResidual_L2,probeResidual_L3,probeResidual_L4,probeResidual_L5,probeResidual_L6,probeSpeedup_L2,probeSpeedup_L3,probeSpeedup_L4,probeSpeedup_L5,probeSpeedup_L6,probeEligible_L2,probeEligible_L3,probeEligible_L4,probeEligible_L5,probeEligible_L6");
		if (leafMetrics == null) {
			printSummaryReport(Collections.emptyList());
			printAlertReport(Collections.emptyList());
			printHeavyLeafReport(Collections.emptyList());
			return;
		}
		List<IndexPoiCreator.LeafMetrics> safeLeafMetrics = new ArrayList<>();
		for (IndexPoiCreator.LeafMetrics metrics : leafMetrics) {
			if (metrics == null) {
				continue;
			}
			safeLeafMetrics.add(metrics);
			PoiNameSidecarSelectionResult selectionResult = lastPoiNameSidecarSelections.get(metrics.leafKey4);
			boolean sidecarBuilt = selectionResult != null ? !selectionResult.selectedEntries.isEmpty() : metrics.sidecarBuilt;
			int sidecarBytes = selectionResult != null ? selectionResult.sidecarBytes : metrics.sidecarBytes;
			int selectedKeyCount = selectionResult != null ? selectionResult.selectedKeyCount : metrics.selectedKeyCount;
			int rescueSelectedKeyCount = selectionResult != null ? selectionResult.rescueSelectedKeyCount : 0;
			int sidecarNodeCount = selectionResult != null ? selectionResult.sidecarNodeCount : 0;
			int terminalSidecarNodeCount = selectionResult != null ? selectionResult.terminalSidecarNodeCount : 0;
			int maxContinuationDepth = selectionResult != null ? selectionResult.maxContinuationDepth : POI_NAME_SIDECAR_CONTINUATION_KEY_LENGTH;
			int residualAtomCount = selectionResult != null ? selectionResult.residualAtomCount : metrics.residualAtomCount;
			int coveredAtomCount = selectionResult != null ? selectionResult.coveredAtomCount : Math.max(0, metrics.atomCount - metrics.residualAtomCount);
			double residualMass = selectionResult != null ? selectionResult.residualMass : metrics.residualMass;
			double coveredAtomRatio = selectionResult != null ? selectionResult.coveredAtomRatio : (metrics.atomCount == 0 ? 0d : coveredAtomCount / (double) metrics.atomCount);
			double avgFrag = selectionResult != null ? selectionResult.avgFrag : metrics.avgFrag;
			double maxFrag = selectionResult != null ? selectionResult.maxFrag : metrics.maxFrag;
			int expectedCostNoSidecar = selectionResult != null ? selectionResult.expectedSearchCostNoSidecar : metrics.expectedCostNoSidecar;
			double expectedCostWithSidecar = selectionResult != null ? selectionResult.expectedSearchCostWithSidecar : metrics.expectedCostWithSidecar;
			double estimatedSpeedup = expectedCostWithSidecar == 0d ? 1d : expectedCostNoSidecar / expectedCostWithSidecar;
			double avgGainDensity = selectionResult != null ? selectionResult.avgGainDensity : 0d;
			int overlapFilteredCandidateCount = selectionResult != null ? selectionResult.overlapFilteredCandidateCount : 0;
			int coverageEligibleCandidateCount = selectionResult != null ? selectionResult.coverageEligibleCandidateCount : 0;
			int budgetRejectedCandidateCount = selectionResult != null ? selectionResult.budgetRejectedCandidateCount : 0;
			int eligibleCandidateCount = selectionResult != null ? selectionResult.eligibleCandidateCount : 0;
			double top1ContinuationMassRatio = selectionResult != null ? selectionResult.top1ContinuationMassRatio : 0d;
			double top3ContinuationMassRatio = selectionResult != null ? selectionResult.top3ContinuationMassRatio : 0d;
			double top5ContinuationMassRatio = selectionResult != null ? selectionResult.top5ContinuationMassRatio : 0d;
			double selectedGainMassRatio = selectionResult != null ? selectionResult.selectedGainMassRatio : 0d;
			boolean broadPrefixLowSelectivity = selectionResult != null && selectionResult.broadPrefixLowSelectivity;
			boolean residualFallbackUsed = selectionResult != null && selectionResult.residualFallbackUsed;
			boolean residualDominatedSparseLeaf = selectionResult != null && selectionResult.residualDominatedSparseLeaf;
			boolean probeTried = selectionResult != null && selectionResult.probeTried;
			int probeBestDepth = selectionResult != null ? selectionResult.probeBestDepth : 0;
			int probeAcceptedDepthCount = selectionResult != null ? selectionResult.probeAcceptedDepthCount : 0;
			double probeBaselineCost = selectionResult != null ? selectionResult.probeBaselineCost : 0d;
			double probeBestCost = selectionResult != null ? selectionResult.probeBestCost : 0d;
			double probeImprovementRatio = selectionResult != null ? selectionResult.probeImprovementRatio : 0d;
			int heavyLeaves = isHeavyLeaf(metrics.atomCount) ? 1 : 0;
			int heavyLeavesNoEligibleKeys = heavyLeaves == 1 && coverageEligibleCandidateCount == 0 ? 1 : 0;
			int heavyLeavesNoPositiveGain = heavyLeaves == 1 && coverageEligibleCandidateCount > 0 && eligibleCandidateCount == 0 ? 1 : 0;
			int heavyLeavesRejectedByCoverage = heavyLeaves == 1 && !sidecarBuilt && coveredAtomRatio < RESIDUAL_RESCUE_COVERAGE_THRESHOLD ? 1 : 0;
			int heavyLeavesRejectedByResidual = heavyLeaves == 1 && !sidecarBuilt && residualMass > RESIDUAL_RESCUE_MASS_THRESHOLD ? 1 : 0;
			int heavyLeavesRejectedByBudget = heavyLeaves == 1 && budgetRejectedCandidateCount > 0 ? 1 : 0;
			System.out.println(String.join(",",
					csv(metrics.leafKey4),
					Integer.toString(metrics.poiCount),
					Integer.toString(metrics.atomCount),
					Integer.toString(metrics.avgPoisPerAtom),
					Integer.toString(metrics.p95PoisPerAtom),
					Integer.toString(metrics.maxPoisPerAtom),
					Integer.toString(metrics.avgUniqueBloomTokens),
					Integer.toString(metrics.p95UniqueBloomTokens),
					Integer.toString(metrics.maxUniqueBloomTokens),
					Integer.toString(metrics.avgAtomKeyCount),
					Integer.toString(metrics.p95AtomKeyCount),
					Integer.toString(metrics.uniqKeys),
					Boolean.toString(sidecarBuilt),
					Integer.toString(sidecarBytes),
					Integer.toString(sidecarNodeCount),
					Integer.toString(terminalSidecarNodeCount),
					Integer.toString(maxContinuationDepth),
					Integer.toString(selectedKeyCount),
					Integer.toString(rescueSelectedKeyCount),
					Integer.toString(residualAtomCount),
					Integer.toString(coveredAtomCount),
					formatDouble(residualMass),
					formatDouble(coveredAtomRatio),
					formatDouble(avgFrag),
					formatDouble(maxFrag),
					Integer.toString(expectedCostNoSidecar),
					formatDouble(expectedCostWithSidecar),
					formatDouble(estimatedSpeedup),
					formatDouble(avgGainDensity),
					Integer.toString(overlapFilteredCandidateCount),
					Integer.toString(eligibleCandidateCount),
					formatDouble(top1ContinuationMassRatio),
					formatDouble(top3ContinuationMassRatio),
					formatDouble(top5ContinuationMassRatio),
					formatDouble(selectedGainMassRatio),
					Boolean.toString(broadPrefixLowSelectivity),
					Boolean.toString(residualFallbackUsed),
					Integer.toString(heavyLeaves),
					Integer.toString(heavyLeavesNoEligibleKeys),
					Integer.toString(heavyLeavesNoPositiveGain),
					Integer.toString(heavyLeavesRejectedByCoverage),
					Integer.toString(heavyLeavesRejectedByResidual),
					Integer.toString(heavyLeavesRejectedByBudget),
					Boolean.toString(residualDominatedSparseLeaf),
					Boolean.toString(probeTried),
					Integer.toString(probeBestDepth),
					Integer.toString(probeAcceptedDepthCount),
					formatDouble(probeBaselineCost),
					formatDouble(probeBestCost),
					formatDouble(probeImprovementRatio),
					formatDouble(getProbeCoveredAtomRatio(selectionResult, 2)),
					formatDouble(getProbeCoveredAtomRatio(selectionResult, 3)),
					formatDouble(getProbeCoveredAtomRatio(selectionResult, 4)),
					formatDouble(getProbeCoveredAtomRatio(selectionResult, 5)),
					formatDouble(getProbeCoveredAtomRatio(selectionResult, 6)),
					formatDouble(getProbeResidualMass(selectionResult, 2)),
					formatDouble(getProbeResidualMass(selectionResult, 3)),
					formatDouble(getProbeResidualMass(selectionResult, 4)),
					formatDouble(getProbeResidualMass(selectionResult, 5)),
					formatDouble(getProbeResidualMass(selectionResult, 6)),
					formatDouble(getProbeEstimatedSpeedup(selectionResult, 2)),
					formatDouble(getProbeEstimatedSpeedup(selectionResult, 3)),
					formatDouble(getProbeEstimatedSpeedup(selectionResult, 4)),
					formatDouble(getProbeEstimatedSpeedup(selectionResult, 5)),
					formatDouble(getProbeEstimatedSpeedup(selectionResult, 6)),
					Integer.toString(getProbeEligibleCandidateCount(selectionResult, 2)),
					Integer.toString(getProbeEligibleCandidateCount(selectionResult, 3)),
					Integer.toString(getProbeEligibleCandidateCount(selectionResult, 4)),
					Integer.toString(getProbeEligibleCandidateCount(selectionResult, 5)),
					Integer.toString(getProbeEligibleCandidateCount(selectionResult, 6))));
		}
		printSummaryReport(safeLeafMetrics);
		printAlertReport(safeLeafMetrics);
		printHeavyLeafReport(safeLeafMetrics);
	}

	private void printHeavyLeafReport(List<LeafMetrics> leafMetrics) {
		System.out.println("=== HEAVY-LEAF_REPORT ===");
		System.out.println("leafKey4,atomCount,sidecarBuilt,sidecarBytes,sidecarNodeCount,terminalSidecarNodeCount,maxContinuationDepth,selectedKeyCount,rescueSelectedKeyCount,residualAtomCount,coveredAtomCount,residualMass,coveredAtomRatio,avgFrag,maxFrag,expectedCostNoSidecar,expectedCostWithSidecar,estimatedSpeedup,eligibleCandidateCount,coverageEligibleCandidateCount,budgetRejectedCandidateCount,top1ContinuationMassRatio,top3ContinuationMassRatio,top5ContinuationMassRatio,broadPrefixLowSelectivity,residualFallbackUsed,residualDominatedSparseLeaf,probeTried,probeBestDepth,probeAcceptedDepthCount,probeBaselineCost,probeBestCost,probeImprovementRatio,probeCoverage_L2,probeCoverage_L3,probeCoverage_L4,probeCoverage_L5,probeCoverage_L6,probeResidual_L2,probeResidual_L3,probeResidual_L4,probeResidual_L5,probeResidual_L6,probeSpeedup_L2,probeSpeedup_L3,probeSpeedup_L4,probeSpeedup_L5,probeSpeedup_L6,probeEligible_L2,probeEligible_L3,probeEligible_L4,probeEligible_L5,probeEligible_L6");
		if (Algorithms.isEmpty(leafMetrics)) {
			return;
		}
		for (LeafMetrics metrics : leafMetrics) {
			if (metrics == null || !isHeavyLeaf(metrics.atomCount)) {
				continue;
			}
			PoiNameSidecarSelectionResult selectionResult = lastPoiNameSidecarSelections.get(metrics.leafKey4);
			boolean sidecarBuilt = selectionResult != null ? !selectionResult.selectedEntries.isEmpty() : metrics.sidecarBuilt;
			int sidecarBytes = selectionResult != null ? selectionResult.sidecarBytes : metrics.sidecarBytes;
			int sidecarNodeCount = selectionResult != null ? selectionResult.sidecarNodeCount : 0;
			int terminalSidecarNodeCount = selectionResult != null ? selectionResult.terminalSidecarNodeCount : 0;
			int maxContinuationDepth = selectionResult != null ? selectionResult.maxContinuationDepth : POI_NAME_SIDECAR_CONTINUATION_KEY_LENGTH;
			int selectedKeyCount = selectionResult != null ? selectionResult.selectedKeyCount : metrics.selectedKeyCount;
			int rescueSelectedKeyCount = selectionResult != null ? selectionResult.rescueSelectedKeyCount : 0;
			int residualAtomCount = selectionResult != null ? selectionResult.residualAtomCount : metrics.residualAtomCount;
			int coveredAtomCount = selectionResult != null ? selectionResult.coveredAtomCount : Math.max(0, metrics.atomCount - metrics.residualAtomCount);
			double residualMass = selectionResult != null ? selectionResult.residualMass : metrics.residualMass;
			double coveredAtomRatio = selectionResult != null ? selectionResult.coveredAtomRatio : (metrics.atomCount == 0 ? 0d : coveredAtomCount / (double) metrics.atomCount);
			double avgFrag = selectionResult != null ? selectionResult.avgFrag : metrics.avgFrag;
			double maxFrag = selectionResult != null ? selectionResult.maxFrag : metrics.maxFrag;
			int expectedCostNoSidecar = selectionResult != null ? selectionResult.expectedSearchCostNoSidecar : metrics.expectedCostNoSidecar;
			double expectedCostWithSidecar = selectionResult != null ? selectionResult.expectedSearchCostWithSidecar : metrics.expectedCostWithSidecar;
			double estimatedSpeedup = expectedCostWithSidecar == 0d ? 1d : expectedCostNoSidecar / expectedCostWithSidecar;
			int eligibleCandidateCount = selectionResult != null ? selectionResult.eligibleCandidateCount : 0;
			int coverageEligibleCandidateCount = selectionResult != null ? selectionResult.coverageEligibleCandidateCount : 0;
			int budgetRejectedCandidateCount = selectionResult != null ? selectionResult.budgetRejectedCandidateCount : 0;
			double top1ContinuationMassRatio = selectionResult != null ? selectionResult.top1ContinuationMassRatio : 0d;
			double top3ContinuationMassRatio = selectionResult != null ? selectionResult.top3ContinuationMassRatio : 0d;
			double top5ContinuationMassRatio = selectionResult != null ? selectionResult.top5ContinuationMassRatio : 0d;
			boolean broadPrefixLowSelectivity = selectionResult != null && selectionResult.broadPrefixLowSelectivity;
			boolean residualFallbackUsed = selectionResult != null && selectionResult.residualFallbackUsed;
			boolean residualDominatedSparseLeaf = selectionResult != null && selectionResult.residualDominatedSparseLeaf;
			boolean probeTried = selectionResult != null && selectionResult.probeTried;
			int probeBestDepth = selectionResult != null ? selectionResult.probeBestDepth : 0;
			int probeAcceptedDepthCount = selectionResult != null ? selectionResult.probeAcceptedDepthCount : 0;
			double probeBaselineCost = selectionResult != null ? selectionResult.probeBaselineCost : 0d;
			double probeBestCost = selectionResult != null ? selectionResult.probeBestCost : 0d;
			double probeImprovementRatio = selectionResult != null ? selectionResult.probeImprovementRatio : 0d;
			System.out.println(String.join(",",
					csv(metrics.leafKey4),
					Integer.toString(metrics.atomCount),
					Boolean.toString(sidecarBuilt),
					Integer.toString(sidecarBytes),
					Integer.toString(sidecarNodeCount),
					Integer.toString(terminalSidecarNodeCount),
					Integer.toString(maxContinuationDepth),
					Integer.toString(selectedKeyCount),
					Integer.toString(rescueSelectedKeyCount),
					Integer.toString(residualAtomCount),
					Integer.toString(coveredAtomCount),
					formatDouble(residualMass),
					formatDouble(coveredAtomRatio),
					formatDouble(avgFrag),
					formatDouble(maxFrag),
					Integer.toString(expectedCostNoSidecar),
					formatDouble(expectedCostWithSidecar),
					formatDouble(estimatedSpeedup),
					Integer.toString(eligibleCandidateCount),
					Integer.toString(coverageEligibleCandidateCount),
					Integer.toString(budgetRejectedCandidateCount),
					formatDouble(top1ContinuationMassRatio),
					formatDouble(top3ContinuationMassRatio),
					formatDouble(top5ContinuationMassRatio),
					Boolean.toString(broadPrefixLowSelectivity),
					Boolean.toString(residualFallbackUsed),
					Boolean.toString(residualDominatedSparseLeaf),
					Boolean.toString(probeTried),
					Integer.toString(probeBestDepth),
					Integer.toString(probeAcceptedDepthCount),
					formatDouble(probeBaselineCost),
					formatDouble(probeBestCost),
					formatDouble(probeImprovementRatio),
					formatDouble(getProbeCoveredAtomRatio(selectionResult, 2)),
					formatDouble(getProbeCoveredAtomRatio(selectionResult, 3)),
					formatDouble(getProbeCoveredAtomRatio(selectionResult, 4)),
					formatDouble(getProbeCoveredAtomRatio(selectionResult, 5)),
					formatDouble(getProbeCoveredAtomRatio(selectionResult, 6)),
					formatDouble(getProbeResidualMass(selectionResult, 2)),
					formatDouble(getProbeResidualMass(selectionResult, 3)),
					formatDouble(getProbeResidualMass(selectionResult, 4)),
					formatDouble(getProbeResidualMass(selectionResult, 5)),
					formatDouble(getProbeResidualMass(selectionResult, 6)),
					formatDouble(getProbeEstimatedSpeedup(selectionResult, 2)),
					formatDouble(getProbeEstimatedSpeedup(selectionResult, 3)),
					formatDouble(getProbeEstimatedSpeedup(selectionResult, 4)),
					formatDouble(getProbeEstimatedSpeedup(selectionResult, 5)),
					formatDouble(getProbeEstimatedSpeedup(selectionResult, 6)),
					Integer.toString(getProbeEligibleCandidateCount(selectionResult, 2)),
					Integer.toString(getProbeEligibleCandidateCount(selectionResult, 3)),
					Integer.toString(getProbeEligibleCandidateCount(selectionResult, 4)),
					Integer.toString(getProbeEligibleCandidateCount(selectionResult, 5)),
					Integer.toString(getProbeEligibleCandidateCount(selectionResult, 6))));
		}
	}

	private void printSummaryReport(List<IndexPoiCreator.LeafMetrics> leafMetrics) {
		System.out.println("=== SUMMARY_REPORT ===");
		System.out.println("metric,value");
		if (Algorithms.isEmpty(leafMetrics)) {
			return;
		}
		List<Integer> atomCounts = new ArrayList<>();
		List<Integer> poisPerAtom = new ArrayList<>();
		List<Integer> uniqueBloomTokens = new ArrayList<>();
		List<Integer> sidecarBytes = new ArrayList<>();
		List<Integer> selectedKeyCounts = new ArrayList<>();
		List<Integer> sidecarBytesHeavy = new ArrayList<>();
		List<Integer> selectedKeyCountsHeavy = new ArrayList<>();
		List<Integer> rescueSelectedKeyCountsHeavy = new ArrayList<>();
		List<Double> coveredAtomRatioHeavy = new ArrayList<>();
		List<Double> avgGainDensityHeavy = new ArrayList<>();
		List<Integer> overlapFilteredCandidateCountsHeavy = new ArrayList<>();
		List<Integer> eligibleCandidateCountsHeavy = new ArrayList<>();
		List<Double> top1ContinuationMassRatiosHeavy = new ArrayList<>();
		List<Double> top3ContinuationMassRatiosHeavy = new ArrayList<>();
		List<Double> top5ContinuationMassRatiosHeavy = new ArrayList<>();
		List<Double> selectedGainMassRatiosHeavy = new ArrayList<>();
		List<Double> residualMassHeavy = new ArrayList<>();
		List<Double> fragHeavy = new ArrayList<>();
		List<Double> expectedNoSidecarHeavy = new ArrayList<>();
		List<Double> expectedWithSidecarHeavy = new ArrayList<>();
		List<Double> estimatedSpeedupHeavy = new ArrayList<>();
		double totalExpectedCostNoSidecarHeavy = 0d;
		double totalExpectedCostWithSidecarHeavy = 0d;
		int sidecarBuiltLeaves = 0;
		int broadPrefixLowSelectivityHeavyCount = 0;
		int residualSparseLeafCount = 0;
		int residualSparseLeafProbeTriedCount = 0;
		int residualSparseLeafImprovedCount = 0;
		int residualSparseLeafRejectedAllDepthsCount = 0;
		int residualSparseLeafChosenL3Count = 0;
		int residualSparseLeafChosenL4Count = 0;
		int residualSparseLeafChosenL5Count = 0;
		int residualSparseLeafChosenL6Count = 0;

		for (LeafMetrics metrics : leafMetrics) {
			PoiNameSidecarSelectionResult selectionResult = lastPoiNameSidecarSelections.get(metrics.leafKey4);
			boolean sidecarBuilt = selectionResult != null ? !selectionResult.selectedEntries.isEmpty() : metrics.sidecarBuilt;
			int sidecarBytesValue = selectionResult != null ? selectionResult.sidecarBytes : metrics.sidecarBytes;
			int selectedKeyCountValue = selectionResult != null ? selectionResult.selectedKeyCount : metrics.selectedKeyCount;
			int rescueSelectedKeyCountValue = selectionResult != null ? selectionResult.rescueSelectedKeyCount : 0;
			double residualMassValue = selectionResult != null ? selectionResult.residualMass : metrics.residualMass;
			double avgFragValue = selectionResult != null ? selectionResult.avgFrag : metrics.avgFrag;
			int coverageEligibleCandidateCountValue = selectionResult != null ? selectionResult.coverageEligibleCandidateCount : 0;
			int budgetRejectedCandidateCountValue = selectionResult != null ? selectionResult.budgetRejectedCandidateCount : 0;
			int eligibleCandidateCountValue = selectionResult != null ? selectionResult.eligibleCandidateCount : 0;
			int expectedCostNoSidecarValue = selectionResult != null ? selectionResult.expectedSearchCostNoSidecar : metrics.expectedCostNoSidecar;
			double expectedCostWithSidecarValue = selectionResult != null ? selectionResult.expectedSearchCostWithSidecar : metrics.expectedCostWithSidecar;
			double estimatedSpeedupValue = expectedCostWithSidecarValue == 0d ? 1d : expectedCostNoSidecarValue / expectedCostWithSidecarValue;
			atomCounts.add(metrics.atomCount);
			poisPerAtom.add(metrics.avgPoisPerAtom);
			uniqueBloomTokens.add(metrics.avgUniqueBloomTokens);
			sidecarBytes.add(sidecarBytesValue);
			selectedKeyCounts.add(selectedKeyCountValue);
			if (sidecarBuilt) {
				sidecarBuiltLeaves++;
				sidecarBytesHeavy.add(sidecarBytesValue);
				selectedKeyCountsHeavy.add(selectedKeyCountValue);
				rescueSelectedKeyCountsHeavy.add(rescueSelectedKeyCountValue);
				coveredAtomRatioHeavy.add(selectionResult != null ? selectionResult.coveredAtomRatio : (metrics.atomCount == 0 ? 0d : (metrics.atomCount - metrics.residualAtomCount) / (double) metrics.atomCount));
				avgGainDensityHeavy.add(selectionResult != null ? selectionResult.avgGainDensity : 0d);
				overlapFilteredCandidateCountsHeavy.add(selectionResult != null ? selectionResult.overlapFilteredCandidateCount : 0);
				eligibleCandidateCountsHeavy.add(selectionResult != null ? selectionResult.eligibleCandidateCount : 0);
				top1ContinuationMassRatiosHeavy.add(selectionResult != null ? selectionResult.top1ContinuationMassRatio : 0d);
				top3ContinuationMassRatiosHeavy.add(selectionResult != null ? selectionResult.top3ContinuationMassRatio : 0d);
				top5ContinuationMassRatiosHeavy.add(selectionResult != null ? selectionResult.top5ContinuationMassRatio : 0d);
				selectedGainMassRatiosHeavy.add(selectionResult != null ? selectionResult.selectedGainMassRatio : 0d);
				if (selectionResult != null && selectionResult.broadPrefixLowSelectivity) {
					broadPrefixLowSelectivityHeavyCount++;
				}
				residualMassHeavy.add(residualMassValue);
				fragHeavy.add(avgFragValue);
				expectedNoSidecarHeavy.add((double) expectedCostNoSidecarValue);
				expectedWithSidecarHeavy.add(expectedCostWithSidecarValue);
				estimatedSpeedupHeavy.add(estimatedSpeedupValue);
				totalExpectedCostNoSidecarHeavy += expectedCostNoSidecarValue;
				totalExpectedCostWithSidecarHeavy += expectedCostWithSidecarValue;
			}
			if (selectionResult != null && selectionResult.residualDominatedSparseLeaf) {
				residualSparseLeafCount++;
				if (selectionResult.probeTried) {
					residualSparseLeafProbeTriedCount++;
				}
				if (selectionResult.probeBestDepth > POI_NAME_SIDECAR_CONTINUATION_KEY_LENGTH
						&& selectionResult.probeBestCost > 0d
						&& selectionResult.probeBestCost <= selectionResult.probeBaselineCost * PROBE_IMPROVEMENT_FACTOR) {
					residualSparseLeafImprovedCount++;
					if (selectionResult.probeBestDepth == 3) {
						residualSparseLeafChosenL3Count++;
					} else if (selectionResult.probeBestDepth == 4) {
						residualSparseLeafChosenL4Count++;
					} else if (selectionResult.probeBestDepth == 5) {
						residualSparseLeafChosenL5Count++;
					} else if (selectionResult.probeBestDepth == 6) {
						residualSparseLeafChosenL6Count++;
					}
				} else if (selectionResult.probeTried) {
					residualSparseLeafRejectedAllDepthsCount++;
				}
			}
		}
		print("totalLeaves", leafMetrics.size());
		print("sidecarBuiltLeaves", sidecarBuiltLeaves);
		print("avg_AtomCount", averageInt(atomCounts));
		print("p95_AtomCount", percentileInt(atomCounts, 95));
		print("avg_PoisPerAtom", averageInt(poisPerAtom));
		print("p95_PoisPerAtom", percentileInt(poisPerAtom, 95));
		print("max_PoisPerAtomGlobal", maxInt(poisPerAtom));
		print("avg_UniqueBloomTokens", averageInt(uniqueBloomTokens));
		print("p95_UniqueBloomTokens", percentileInt(uniqueBloomTokens, 95));
		print("max_UniqueBloomTokensGlobal", maxInt(uniqueBloomTokens));
		print("avg_SidecarBytesBuilt", averageInt(sidecarBytes));
		print("p95_SidecarBytesBuilt", percentileInt(sidecarBytes, 95));
		print("avg_SelectedKeyCount", averageInt(selectedKeyCounts));
		print("p95_SelectedKeyCount", percentileInt(selectedKeyCounts, 95));
		print("avg_ResidualMassHeavy", averageDouble(residualMassHeavy));
		print("p95_ResidualMassHeavy", percentileDouble(residualMassHeavy, 95));
		print("avg_FragHeavy", averageDouble(fragHeavy));
		print("p95_FragHeavy", percentileDouble(fragHeavy, 95));
		print("avg_ExpectedCostNoSidecarHeavy", averageDouble(expectedNoSidecarHeavy));
		print("avg_ExpectedCostWithSidecarHeavy", averageDouble(expectedWithSidecarHeavy));
		print("avg_PerLeafEstimatedSpeedupHeavy", averageDouble(estimatedSpeedupHeavy));
		print("global_EstimatedSpeedupHeavy", totalExpectedCostWithSidecarHeavy == 0d ? 1d : totalExpectedCostNoSidecarHeavy / totalExpectedCostWithSidecarHeavy);
		print("avg_SidecarBytesBuiltHeavy", averageInt(sidecarBytesHeavy));
		print("p95_SidecarBytesBuiltHeavy", percentileInt(sidecarBytesHeavy, 95));
		print("avg_SelectedKeyCountHeavy", averageInt(selectedKeyCountsHeavy));
		print("p95_SelectedKeyCountHeavy", percentileInt(selectedKeyCountsHeavy, 95));
		print("avg_RescueSelectedKeyCountHeavy", averageInt(rescueSelectedKeyCountsHeavy));
		print("avg_CoveredAtomRatioHeavy", averageDouble(coveredAtomRatioHeavy));
		print("avg_GainDensityHeavy", averageDouble(avgGainDensityHeavy));
		print("avg_OverlapFilteredCandidateCountHeavy", averageInt(overlapFilteredCandidateCountsHeavy));
		print("avg_EligibleCandidateCountHeavy", averageInt(eligibleCandidateCountsHeavy));
		print("avg_Top1ContinuationMassRatioHeavy", averageDouble(top1ContinuationMassRatiosHeavy));
		print("avg_Top3ContinuationMassRatioHeavy", averageDouble(top3ContinuationMassRatiosHeavy));
		print("avg_Top5ContinuationMassRatioHeavy", averageDouble(top5ContinuationMassRatiosHeavy));
		print("avg_SelectedGainMassRatioHeavy", averageDouble(selectedGainMassRatiosHeavy));
		print("broadPrefixLowSelectivityHeavyCount", broadPrefixLowSelectivityHeavyCount);
		print("residualSparseLeafCount", residualSparseLeafCount);
		print("residualSparseLeafProbeTriedCount", residualSparseLeafProbeTriedCount);
		print("residualSparseLeafImprovedCount", residualSparseLeafImprovedCount);
		print("residualSparseLeafRejectedAllDepthsCount", residualSparseLeafRejectedAllDepthsCount);
		print("residualSparseLeafChosenL3Count", residualSparseLeafChosenL3Count);
		print("residualSparseLeafChosenL4Count", residualSparseLeafChosenL4Count);
		print("residualSparseLeafChosenL5Count", residualSparseLeafChosenL5Count);
		print("residualSparseLeafChosenL6Count", residualSparseLeafChosenL6Count);
	}

	private void print(String metric, int value) {
		System.out.println(metric + "," + value);
	}

	private void print(String metric, double value) {
		System.out.println(metric + "," + formatDouble(value));
	}

	private void printAlertReport(List<LeafMetrics> leafMetrics) {
		System.out.println("=== ALERT_REPORT ===");
		System.out.println("severity,leafKey4,alertType,observedValue,thresholdValue,context");
		if (Algorithms.isEmpty(leafMetrics)) {
			return;
		}

		for (LeafMetrics metrics : leafMetrics) {
			if (metrics == null) {
				continue;
			}
			PoiNameSidecarSelectionResult selectionResult = lastPoiNameSidecarSelections.get(metrics.leafKey4);
			boolean sidecarBuilt = selectionResult != null ? !selectionResult.selectedEntries.isEmpty() : metrics.sidecarBuilt;
			int selectedKeyCount = selectionResult != null ? selectionResult.selectedKeyCount : metrics.selectedKeyCount;
			double avgFrag = selectionResult != null ? selectionResult.avgFrag : metrics.avgFrag;
			int expectedCostNoSidecar = selectionResult != null ? selectionResult.expectedSearchCostNoSidecar : metrics.expectedCostNoSidecar;
			double expectedCostWithSidecar = selectionResult != null ? selectionResult.expectedSearchCostWithSidecar : metrics.expectedCostWithSidecar;
			double coveredAtomRatio = selectionResult != null ? selectionResult.coveredAtomRatio : (metrics.atomCount == 0 ? 0d : (metrics.atomCount - metrics.residualAtomCount) / (double) metrics.atomCount);
			int overlapFilteredCandidateCount = selectionResult != null ? selectionResult.overlapFilteredCandidateCount : 0;
			boolean broadPrefixLowSelectivity = selectionResult != null && selectionResult.broadPrefixLowSelectivity;
			boolean residualFallbackUsed = selectionResult != null && selectionResult.residualFallbackUsed;
			double estimatedSpeedup = expectedCostWithSidecar == 0d ? 1d : expectedCostNoSidecar / expectedCostWithSidecar;
			if (metrics.maxPoisPerAtom > 128) {
				print("ERROR", metrics.leafKey4, "POIS_PER_ATOM_EXCEEDS_BLOCK_SIZE",
						Integer.toString(metrics.maxPoisPerAtom), "128", "maxPoisPerAtom");
			}
			if (sidecarBuilt && selectionResult != null && selectionResult.residualMass > 0.8d) {
				print("WARN", metrics.leafKey4, "RESIDUAL_MASS_HIGH",
						formatDouble(selectionResult.residualMass), "0.80", "heavy leaf residual mass");
			}
			if (sidecarBuilt && estimatedSpeedup < 1.2d) {
				print("WARN", metrics.leafKey4, "SIDECAR_SPEEDUP_LOW",
						formatDouble(estimatedSpeedup), "1.20", "estimated speedup");
			}
			if (sidecarBuilt && selectedKeyCount > MAX_SELECTED_KEYS_PER_LEAF) {
				print("WARN", metrics.leafKey4, "SELECTED_KEY_COUNT_TOO_HIGH",
						Integer.toString(selectedKeyCount), Integer.toString(MAX_SELECTED_KEYS_PER_LEAF), "selected key count cap");
			}
			if (sidecarBuilt && avgFrag >= 0.90d) {
				print("WARN", metrics.leafKey4, "FRAGMENTATION_MAXED",
						formatDouble(avgFrag), "0.90", "span fragmentation");
			}
			if (sidecarBuilt && expectedCostWithSidecar >= expectedCostNoSidecar) {
				print("WARN", metrics.leafKey4, "SIDE_CAR_COST_NOT_BEATING_BASELINE",
						formatDouble(expectedCostWithSidecar), Integer.toString(expectedCostNoSidecar), "search cost comparison");
			}
			if (sidecarBuilt && coveredAtomRatio < 0.25d) {
				print("INFO", metrics.leafKey4, "LOW_SIDE_CAR_COVERAGE",
						formatDouble(coveredAtomRatio), "0.25", "covered atom ratio");
			}
			if (sidecarBuilt && overlapFilteredCandidateCount > 0) {
				print("INFO", metrics.leafKey4, "OVERLAP_FILTERING_ACTIVE",
						Integer.toString(overlapFilteredCandidateCount), "0", "candidates filtered by overlap");
			}
			if (sidecarBuilt && residualFallbackUsed) {
				print("INFO", metrics.leafKey4, "RESIDUAL_FALLBACK_USED",
						Integer.toString(selectionResult.residualAtomCount), "0", "residual retained intentionally");
			}
			if (!sidecarBuilt && metrics.atomCount >= POI_NAME_SIDECAR_COUNT_THRESHOLD && broadPrefixLowSelectivity) {
				print("INFO", metrics.leafKey4, "BROAD_PREFIX_LOW_SELECTIVITY",
						formatDouble(selectionResult != null ? selectionResult.top3ContinuationMassRatio : 0d),
						formatDouble(BROAD_PREFIX_TOP3_MASS_THRESHOLD), "low continuation concentration");
			}
			if (!sidecarBuilt && metrics.atomCount >= POI_NAME_SIDECAR_COUNT_THRESHOLD && !broadPrefixLowSelectivity) {
				print("WARN", metrics.leafKey4, "HEAVY_LEAF_WITHOUT_SIDECAR",
						Integer.toString(metrics.atomCount), Integer.toString(POI_NAME_SIDECAR_COUNT_THRESHOLD), "count threshold reached");
			}

		}
	}

	private void print(String severity, String leafKey4, String alertType, String observedValue, String thresholdValue, String context) {
		System.out.println(String.join(",",
				csv(severity),
				csv(leafKey4),
				csv(alertType),
				csv(observedValue),
				csv(thresholdValue),
				csv(context)));
	}

	private int averageInt(List<Integer> values) {
		if (Algorithms.isEmpty(values)) {
			return 0;
		}
		int sum = 0;
		for (Integer value : values) {
			sum += value;
		}
		return Math.round(sum / (float) values.size());
	}

	private int percentileInt(List<Integer> values, int percentile) {
		if (Algorithms.isEmpty(values)) {
			return 0;
		}
		List<Integer> sorted = new ArrayList<>(values);
		sorted.sort(Integer::compareTo);
		int index = Math.min(sorted.size() - 1, Math.max(0, (int) Math.ceil(sorted.size() * (percentile / 100.0)) - 1));
		return sorted.get(index);
	}

	private int maxInt(List<Integer> values) {
		int max = 0;
		if (values == null) {
			return 0;
		}
		for (Integer value : values) {
			if (value != null && value > max) {
				max = value;
			}
		}
		return max;
	}

	private double averageDouble(List<Double> values) {
		if (Algorithms.isEmpty(values)) {
			return 0d;
		}
		double sum = 0d;
		for (Double value : values) {
			sum += value;
		}
		return sum / values.size();
	}

	private double percentileDouble(List<Double> values, int percentile) {
		if (Algorithms.isEmpty(values)) {
			return 0d;
		}
		List<Double> sorted = new ArrayList<>(values);
		sorted.sort(Double::compareTo);
		int index = Math.min(sorted.size() - 1, Math.max(0, (int) Math.ceil(sorted.size() * (percentile / 100.0)) - 1));
		return sorted.get(index);
	}

	private String csv(String value) {
		if (value == null) {
			return "";
		}
		if (value.indexOf(',') >= 0 || value.indexOf('"') >= 0 || value.indexOf('\n') >= 0 || value.indexOf('\r') >= 0) {
			return '"' + value.replace("\"", "\"\"") + '"';
		}
		return value;
	}

	private String formatDouble(double value) {
		return String.format(Locale.US, "%.6f", value);
	}

	private int writePoiNameIndexDataMessage(String leafTokenPrefix, List<PoiDataBlock> poiDataBlocks,
			List<OsmAndPoiNameIndexDataAtom> atoms) throws IOException {
		ByteArrayOutputStream buffer = new ByteArrayOutputStream();
		CodedOutputStream localOut = CodedOutputStream.newInstance(buffer);
		int atomsContributionSize = 0;
		for (OsmAndPoiNameIndexDataAtom atom : atoms) {
			localOut.writeMessage(OsmandOdb.OsmAndPoiNameIndex.OsmAndPoiNameIndexData.ATOMS_FIELD_NUMBER, atom);
			atomsContributionSize += CodedOutputStream.computeMessageSize(
					OsmandOdb.OsmAndPoiNameIndex.OsmAndPoiNameIndexData.ATOMS_FIELD_NUMBER, atom);
		}
		PoiNameSidecarSelectionResult sidecarSelection = buildMergedPoiNameSidecar(leafTokenPrefix, poiDataBlocks);
		if (sidecarSelection != null && !Algorithms.isEmpty(sidecarSelection.sidecarNodes)) {
			writePoiNameSidecar(localOut, sidecarSelection.sidecarNodes);
		}
		localOut.flush();
		byte[] payload = buffer.toByteArray();
		codedOutStream.writeRawVarint32(payload.length);
		codedOutStream.writeRawBytes(payload);
		return payload.length - atomsContributionSize;
	}

	private PoiNameSidecarSelectionResult buildMergedPoiNameSidecar(String leafTokenPrefix, List<PoiDataBlock> poiDataBlocks) {
		if (Algorithms.isEmpty(leafTokenPrefix) || Algorithms.isEmpty(poiDataBlocks)) {
			lastPoiNameSidecarSelections.remove(leafTokenPrefix);
			return null;
		}
		if (poiDataBlocks.size() < POI_NAME_SIDECAR_COUNT_THRESHOLD) {
			lastPoiNameSidecarSelections.put(leafTokenPrefix, emptySelectionResult(poiDataBlocks.size()));
			return null;
		}
		Map<String, PoiNameSidecarEntry> mergedEntriesByKey = collectMergedEntriesByKey(leafTokenPrefix, poiDataBlocks,
				POI_NAME_SIDECAR_CONTINUATION_KEY_LENGTH);
		PoiNameSidecarSelectionResult selectionResult = chooseBudgetedSidecarEntries(mergedEntriesByKey, poiDataBlocks.size(),
				POI_NAME_SIDECAR_CONTINUATION_KEY_LENGTH);
		selectionResult = maybeApplyResidualSparseDepthProbe(leafTokenPrefix, poiDataBlocks, selectionResult);
		selectionResult = withHierarchy(selectionResult, leafTokenPrefix, poiDataBlocks);
		lastPoiNameSidecarSelections.put(leafTokenPrefix, selectionResult);
		if (selectionResult.isEmpty()) {
			return null;
		}
		return selectionResult;
	}

	private Map<String, PoiNameSidecarEntry> collectMergedEntriesByKey(String leafTokenPrefix, List<PoiDataBlock> poiDataBlocks,
			int continuationLength) {
		Map<String, PoiNameSidecarEntry> mergedEntriesByKey = new LinkedHashMap<>();
		if (Algorithms.isEmpty(leafTokenPrefix) || Algorithms.isEmpty(poiDataBlocks) || continuationLength <= 0) {
			return mergedEntriesByKey;
		}
		for (int atomIndex = 0; atomIndex < poiDataBlocks.size(); atomIndex++) {
			PoiDataBlock poiDataBlock = poiDataBlocks.get(atomIndex);
			for (String normalizedToken : poiDataBlock.bloomTokens) {
				if (Algorithms.isEmpty(normalizedToken) || !normalizedToken.startsWith(leafTokenPrefix)) {
					continue;
				}
				if (normalizedToken.length() >= leafTokenPrefix.length() + continuationLength) {
					String key = encodeContinuationKey(normalizedToken, leafTokenPrefix.length(), continuationLength);
					PoiNameSidecarEntry mergedEntry = mergedEntriesByKey.computeIfAbsent(key, PoiNameSidecarEntry::new);
					mergedEntry.atomIndexes.add(atomIndex);
				}
			}
		}
		return mergedEntriesByKey;
	}

	private PoiNameSidecarSelectionResult chooseBudgetedSidecarEntries(Map<String, PoiNameSidecarEntry> mergedEntriesByKey,
			int atomCount, int continuationLength) {
		List<PoiNameSidecarEntryMetrics> candidates = new ArrayList<>();
		double totalCandidateObjectiveGain = 0d;
		int coverageEligibleCandidateCount = 0;
		for (PoiNameSidecarEntry entry : mergedEntriesByKey.values()) {
			if (entry == null || Algorithms.isEmpty(entry.atomIndexes)) {
				continue;
			}
			List<Integer> atomIndexes = new ArrayList<>(entry.atomIndexes);
			atomIndexes.sort(Integer::compareTo);
			int postingSize = atomIndexes.size();
			int spanCount = countSpans(atomIndexes);
			double frag = postingSize == 0 ? 1d : spanCount / (double) postingSize;
			if (postingSize < MIN_POSTING_SIZE_FOR_SIDECAR || frag > MAX_SPAN_FRAG) {
				continue;
			}
			coverageEligibleCandidateCount++;
			int estimatedBytes = estimateSidecarEntryBytes(entry);
			double searchGain = postingSize - spanCount;
			double objectiveGain = searchGain - (SIDE_CAR_BYTE_PENALTY * estimatedBytes);
			double gainDensity = objectiveGain / Math.max(1d, estimatedBytes);
			if (searchGain <= 0d || objectiveGain <= 0d) {
				continue;
			}
			totalCandidateObjectiveGain += objectiveGain;
			candidates.add(new PoiNameSidecarEntryMetrics(entry.key, atomIndexes, postingSize, spanCount, frag,
					estimatedBytes, searchGain, objectiveGain, gainDensity));
		}
		int eligibleCandidateCount = candidates.size();
		double top1ContinuationMassRatio = computeTopContinuationMassRatio(candidates, atomCount, 1);
		double top3ContinuationMassRatio = computeTopContinuationMassRatio(candidates, atomCount, 3);
		double top5ContinuationMassRatio = computeTopContinuationMassRatio(candidates, atomCount, 5);
		boolean broadPrefixLowSelectivity = isBroadPrefixLowSelectivity(atomCount, eligibleCandidateCount, top3ContinuationMassRatio);
		List<PoiNameSidecarEntry> selectedEntries = new ArrayList<>();
		Set<Integer> selectedAtomIndexSet = new LinkedHashSet<>();
		int selectedBytes = 0;
		double gainDensityAcc = 0d;
		double selectedObjectiveGainAcc = 0d;
		int overlapFilteredCandidateCount = 0;
		int budgetRejectedCandidateCount = 0;
		while (selectedEntries.size() < MAX_SELECTED_KEYS_PER_LEAF) {
			overlapFilteredCandidateCount += countOverlapFilteredCandidates(candidates, selectedEntries, selectedAtomIndexSet);
			budgetRejectedCandidateCount += countBudgetRejectedCandidates(candidates, selectedEntries, selectedAtomIndexSet,
					selectedBytes, MAX_SPAN_FRAG, MIN_POSTING_SIZE_FOR_SIDECAR);
			PoiNameSidecarSelectionStep selectionStep = selectBestSidecarCandidate(candidates, selectedEntries,
					selectedAtomIndexSet, selectedBytes, MAX_SPAN_FRAG, MIN_POSTING_SIZE_FOR_SIDECAR, true);
			if (selectionStep == null) {
				break;
			}
			PoiNameSidecarEntry selectedEntry = new PoiNameSidecarEntry(selectionStep.candidate.key);
			selectedEntry.atomIndexes.addAll(selectionStep.selectedAtomIndexes);
			selectedEntries.add(selectedEntry);
			selectedAtomIndexSet.addAll(selectionStep.selectedAtomIndexes);
			selectedBytes += selectionStep.estimatedBytes;
			gainDensityAcc += selectionStep.gainDensity;
			selectedObjectiveGainAcc += selectionStep.objectiveGain;
		}
		int rescueSelectedKeyCount = 0;
		if (shouldRunResidualRescue(atomCount, selectedAtomIndexSet.size())) {
			while (selectedEntries.size() < MAX_SELECTED_KEYS_PER_LEAF) {
				overlapFilteredCandidateCount += countOverlapFilteredCandidates(candidates, selectedEntries, selectedAtomIndexSet);
				budgetRejectedCandidateCount += countBudgetRejectedCandidates(candidates, selectedEntries, selectedAtomIndexSet,
						selectedBytes, RESCUE_MAX_SPAN_FRAG, RESCUE_MIN_POSTING_SIZE_FOR_SIDECAR);
				PoiNameSidecarSelectionStep rescueStep = selectBestSidecarCandidate(candidates, selectedEntries,
						selectedAtomIndexSet, selectedBytes, RESCUE_MAX_SPAN_FRAG, RESCUE_MIN_POSTING_SIZE_FOR_SIDECAR, false);
				if (rescueStep == null) {
					break;
				}
				PoiNameSidecarEntry rescueEntry = new PoiNameSidecarEntry(rescueStep.candidate.key);
				rescueEntry.atomIndexes.addAll(rescueStep.selectedAtomIndexes);
				selectedEntries.add(rescueEntry);
				selectedAtomIndexSet.addAll(rescueStep.selectedAtomIndexes);
				selectedBytes += rescueStep.estimatedBytes;
				gainDensityAcc += rescueStep.gainDensity;
				selectedObjectiveGainAcc += rescueStep.objectiveGain;
				rescueSelectedKeyCount++;
				if (!shouldRunResidualRescue(atomCount, selectedAtomIndexSet.size())) {
					break;
				}
			}
		}
		List<Integer> selectedAtomIndexes = new ArrayList<>(selectedAtomIndexSet);
		Collections.sort(selectedAtomIndexes);
		List<Integer> residualAtomIndexes = buildResidualAtomIndexes(atomCount, selectedAtomIndexes);
		int selectedKeyCount = selectedEntries.size();
		int residualAtomCount = residualAtomIndexes.size();
		int coveredAtomCount = selectedAtomIndexes.size();
		double residualMass = atomCount == 0 ? 0d : residualAtomCount / (double) atomCount;
		double coveredAtomRatio = atomCount == 0 ? 0d : coveredAtomCount / (double) atomCount;
		double avgFrag = selectedEntries.isEmpty() ? 0d : averageDoubleByCandidate(selectedEntries, mergedEntriesByKey);
		double maxFrag = maxFragByCandidate(selectedEntries);
		int expectedSearchCostNoSidecar = atomCount;
		double expectedSearchCostWithSidecar = computeExpectedSearchCostWithSidecar(atomCount, selectedEntries, residualAtomIndexes);
		double objectiveCostWithSidecar = expectedSearchCostWithSidecar + (SIDE_CAR_BYTE_PENALTY * selectedBytes);
		double avgGainDensity = selectedEntries.isEmpty() ? 0d : gainDensityAcc / selectedEntries.size();
		double selectedGainMassRatio = totalCandidateObjectiveGain <= 0d ? 0d : selectedObjectiveGainAcc / totalCandidateObjectiveGain;
		boolean residualFallbackUsed = !selectedEntries.isEmpty() && residualAtomCount > 0;
		return new PoiNameSidecarSelectionResult(selectedEntries, residualAtomIndexes, Collections.emptyList(), selectedKeyCount,
				rescueSelectedKeyCount, residualAtomCount, coveredAtomCount, residualMass, coveredAtomRatio, avgFrag, maxFrag,
				expectedSearchCostNoSidecar, expectedSearchCostWithSidecar, objectiveCostWithSidecar, selectedBytes, 0, 0,
				continuationLength,
				avgGainDensity, overlapFilteredCandidateCount, coverageEligibleCandidateCount, budgetRejectedCandidateCount,
				eligibleCandidateCount, top1ContinuationMassRatio,
				top3ContinuationMassRatio, top5ContinuationMassRatio, selectedGainMassRatio, broadPrefixLowSelectivity,
				residualFallbackUsed, false, false, 0, 0, 0d, 0d, 0d, Collections.emptyList());
	}

	private PoiNameSidecarSelectionResult maybeApplyResidualSparseDepthProbe(String leafTokenPrefix, List<PoiDataBlock> poiDataBlocks,
			PoiNameSidecarSelectionResult baselineSelectionResult) {
		if (baselineSelectionResult == null) {
			return null;
		}
		boolean residualDominatedSparseLeaf = isResidualDominatedSparseLeaf(poiDataBlocks.size(), baselineSelectionResult)
				&& !baselineSelectionResult.broadPrefixLowSelectivity;
		if (!residualDominatedSparseLeaf) {
			return withProbeMetadata(baselineSelectionResult, false, false, 0, 0,
					baselineSelectionResult.expectedSearchCostWithSidecar, baselineSelectionResult.expectedSearchCostWithSidecar,
					1d, Collections.singletonList(buildProbeDepthResult(baselineSelectionResult)));
		}
		List<PoiNameSidecarProbeDepthResult> probeDepthResults = new ArrayList<>();
		probeDepthResults.add(buildProbeDepthResult(baselineSelectionResult));
		PoiNameSidecarSelectionResult bestAcceptedSelectionResult = null;
		int bestAcceptedDepth = 0;
		int acceptedDepthCount = 0;
		double baselineCost = baselineSelectionResult.expectedSearchCostWithSidecar;
		double materialImprovementCostThreshold = baselineCost * PROBE_IMPROVEMENT_FACTOR;
		for (int continuationLength = POI_NAME_SIDECAR_CONTINUATION_KEY_LENGTH + 1;
				continuationLength <= POI_NAME_SIDECAR_MAX_CONTINUATION_DEPTH; continuationLength++) {
			Map<String, PoiNameSidecarEntry> depthEntries = collectMergedEntriesByKey(leafTokenPrefix, poiDataBlocks, continuationLength);
			PoiNameSidecarSelectionResult depthSelectionResult = chooseBudgetedSidecarEntries(depthEntries, poiDataBlocks.size(), continuationLength);
			boolean accepted = isAcceptedProbeDepth(depthSelectionResult);
			probeDepthResults.add(buildProbeDepthResult(depthSelectionResult, accepted));
			if (accepted) {
				acceptedDepthCount++;
				if (depthSelectionResult.expectedSearchCostWithSidecar <= materialImprovementCostThreshold) {
					if (bestAcceptedSelectionResult == null || continuationLength < bestAcceptedDepth) {
						bestAcceptedSelectionResult = depthSelectionResult;
						bestAcceptedDepth = continuationLength;
					}
				}
			}
		}
		PoiNameSidecarSelectionResult selectedSelectionResult = bestAcceptedSelectionResult != null ? bestAcceptedSelectionResult : baselineSelectionResult;
		double bestCost = bestAcceptedSelectionResult != null ? bestAcceptedSelectionResult.expectedSearchCostWithSidecar : baselineCost;
		double improvementRatio = baselineCost == 0d ? 1d : bestCost / baselineCost;
		return withProbeMetadata(selectedSelectionResult, true, true,
				bestAcceptedSelectionResult != null ? bestAcceptedDepth : POI_NAME_SIDECAR_CONTINUATION_KEY_LENGTH,
				acceptedDepthCount, baselineCost, bestCost, improvementRatio, probeDepthResults);
	}

	private boolean isResidualDominatedSparseLeaf(int atomCount, PoiNameSidecarSelectionResult profile) {
		return profile != null
				&& atomCount >= RESIDUAL_SPARSE_ATOM_THRESHOLD
				&& profile.selectedKeyCount > 0
				&& profile.residualMass >= RESIDUAL_SPARSE_MASS_MIN
				&& profile.coveredAtomRatio <= RESIDUAL_SPARSE_COVERAGE_MAX
				&& profile.eligibleCandidateCount <= RESIDUAL_SPARSE_MAX_ELIGIBLE
				&& profile.top1ContinuationMassRatio <= RESIDUAL_SPARSE_TOP1_MAX;
	}

	private boolean isAcceptedProbeDepth(PoiNameSidecarSelectionResult selectionResult) {
		if (selectionResult == null) {
			return false;
		}
		double estimatedSpeedup = selectionResult.expectedSearchCostWithSidecar == 0d ? 1d
				: selectionResult.expectedSearchCostNoSidecar / selectionResult.expectedSearchCostWithSidecar;
		return selectionResult.selectedKeyCount > 0
				&& selectionResult.eligibleCandidateCount >= MIN_PROBE_ELIGIBLE_KEYS
				&& selectionResult.coveredAtomRatio >= MIN_PROBE_COVERAGE
				&& selectionResult.residualMass <= MAX_PROBE_RESIDUAL
				&& estimatedSpeedup >= MIN_PROBE_SPEEDUP
				&& selectionResult.avgFrag <= MAX_SPAN_FRAG
				&& selectionResult.sidecarBytes <= POI_NAME_SIDECAR_SIZE_BUDGET;
	}

	private PoiNameSidecarSelectionResult withProbeMetadata(PoiNameSidecarSelectionResult selectionResult,
			boolean residualDominatedSparseLeaf, boolean probeTried, int probeBestDepth, int probeAcceptedDepthCount,
			double probeBaselineCost, double probeBestCost, double probeImprovementRatio,
			List<PoiNameSidecarProbeDepthResult> probeDepthResults) {
		if (selectionResult == null) {
			return null;
		}
		return new PoiNameSidecarSelectionResult(selectionResult.selectedEntries, selectionResult.residualAtomIndexes,
				selectionResult.sidecarNodes, selectionResult.selectedKeyCount, selectionResult.rescueSelectedKeyCount,
				selectionResult.residualAtomCount, selectionResult.coveredAtomCount, selectionResult.residualMass,
				selectionResult.coveredAtomRatio, selectionResult.avgFrag, selectionResult.maxFrag,
				selectionResult.expectedSearchCostNoSidecar, selectionResult.expectedSearchCostWithSidecar,
				selectionResult.objectiveCostWithSidecar, selectionResult.sidecarBytes, selectionResult.sidecarNodeCount,
				selectionResult.terminalSidecarNodeCount, selectionResult.maxContinuationDepth,
				selectionResult.avgGainDensity, selectionResult.overlapFilteredCandidateCount,
				selectionResult.coverageEligibleCandidateCount, selectionResult.budgetRejectedCandidateCount,
				selectionResult.eligibleCandidateCount, selectionResult.top1ContinuationMassRatio,
				selectionResult.top3ContinuationMassRatio, selectionResult.top5ContinuationMassRatio,
				selectionResult.selectedGainMassRatio, selectionResult.broadPrefixLowSelectivity,
				selectionResult.residualFallbackUsed, residualDominatedSparseLeaf, probeTried, probeBestDepth,
				probeAcceptedDepthCount, probeBaselineCost, probeBestCost, probeImprovementRatio, probeDepthResults);
	}

	private PoiNameSidecarProbeDepthResult buildProbeDepthResult(PoiNameSidecarSelectionResult selectionResult) {
		return buildProbeDepthResult(selectionResult, false);
	}

	private PoiNameSidecarProbeDepthResult buildProbeDepthResult(PoiNameSidecarSelectionResult selectionResult, boolean accepted) {
		if (selectionResult == null) {
			return new PoiNameSidecarProbeDepthResult(0, 0, 0, 0, 0d, 0, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0d, 0, false);
		}
		double estimatedSpeedup = selectionResult.expectedSearchCostWithSidecar == 0d ? 1d
				: selectionResult.expectedSearchCostNoSidecar / selectionResult.expectedSearchCostWithSidecar;
		return new PoiNameSidecarProbeDepthResult(selectionResult.maxContinuationDepth, selectionResult.selectedKeyCount,
				selectionResult.eligibleCandidateCount, selectionResult.coveredAtomCount, selectionResult.coveredAtomRatio,
				selectionResult.residualAtomCount, selectionResult.residualMass, selectionResult.top1ContinuationMassRatio,
				selectionResult.top3ContinuationMassRatio, selectionResult.top5ContinuationMassRatio,
				selectionResult.avgFrag, selectionResult.maxFrag, selectionResult.expectedSearchCostWithSidecar,
				estimatedSpeedup, selectionResult.sidecarBytes, accepted);
	}

	private PoiNameSidecarSelectionResult emptySelectionResult(int atomCount) {
		return new PoiNameSidecarSelectionResult(Collections.emptyList(), buildResidualAtomIndexes(atomCount, Collections.emptyList()),
				Collections.emptyList(), 0, 0, atomCount, 0, atomCount == 0 ? 0d : 1d, 0d, 0d, 0d, atomCount, atomCount, atomCount, 0, 0,
				0, POI_NAME_SIDECAR_CONTINUATION_KEY_LENGTH, 0d,
				0, 0, 0, 0, 0d, 0d, 0d, 0d, false, false,
				false, false, 0, 0, 0d, 0d, 0d, Collections.emptyList());
	}

	private PoiNameSidecarSelectionResult withHierarchy(PoiNameSidecarSelectionResult selectionResult,
			String leafTokenPrefix, List<PoiDataBlock> poiDataBlocks) {
		if (selectionResult == null || Algorithms.isEmpty(selectionResult.selectedEntries)) {
			return selectionResult;
		}
		PoiNameSidecarHierarchy hierarchy = buildHierarchicalSidecar(leafTokenPrefix, poiDataBlocks, selectionResult);
		return new PoiNameSidecarSelectionResult(selectionResult.selectedEntries, selectionResult.residualAtomIndexes,
				hierarchy.nodes, selectionResult.selectedKeyCount, selectionResult.rescueSelectedKeyCount,
				selectionResult.residualAtomCount, selectionResult.coveredAtomCount, selectionResult.residualMass,
				selectionResult.coveredAtomRatio, selectionResult.avgFrag, selectionResult.maxFrag,
				selectionResult.expectedSearchCostNoSidecar, selectionResult.expectedSearchCostWithSidecar,
				selectionResult.objectiveCostWithSidecar,
				hierarchy.bytes > 0 ? hierarchy.bytes : selectionResult.sidecarBytes,
				hierarchy.nodes.size(), hierarchy.terminalNodeCount, hierarchy.maxContinuationDepth,
				selectionResult.avgGainDensity, selectionResult.overlapFilteredCandidateCount,
				selectionResult.coverageEligibleCandidateCount, selectionResult.budgetRejectedCandidateCount,
				selectionResult.eligibleCandidateCount, selectionResult.top1ContinuationMassRatio,
				selectionResult.top3ContinuationMassRatio, selectionResult.top5ContinuationMassRatio,
				selectionResult.selectedGainMassRatio, selectionResult.broadPrefixLowSelectivity,
				selectionResult.residualFallbackUsed, selectionResult.residualDominatedSparseLeaf,
				selectionResult.probeTried, selectionResult.probeBestDepth, selectionResult.probeAcceptedDepthCount,
				selectionResult.probeBaselineCost, selectionResult.probeBestCost,
				selectionResult.probeImprovementRatio, selectionResult.probeDepthResults);
	}

	private PoiNameSidecarHierarchy buildHierarchicalSidecar(String leafTokenPrefix, List<PoiDataBlock> poiDataBlocks,
			PoiNameSidecarSelectionResult selectionResult) {
		List<PoiNameSidecarNode> nodes = new ArrayList<>();
		if (Algorithms.isEmpty(selectionResult.selectedEntries)) {
			return new PoiNameSidecarHierarchy(nodes, 0, 0, POI_NAME_SIDECAR_CONTINUATION_KEY_LENGTH);
		}
		Set<Integer> globalCoveredAtomIndexes = new LinkedHashSet<>();
		for (PoiNameSidecarEntry selectedEntry : selectionResult.selectedEntries) {
			List<Integer> rootAtomIndexes = new ArrayList<>(selectedEntry.atomIndexes);
			Collections.sort(rootAtomIndexes);
			globalCoveredAtomIndexes.addAll(rootAtomIndexes);
			int nodeIndex = nodes.size();
			int rootContinuationLength = safeLength(selectedEntry.key);
			PoiNameSidecarNode rootNode = new PoiNameSidecarNode(0xFFFFFFFF, selectedEntry.key,
					rootContinuationLength, rootAtomIndexes, Collections.emptyList(), true);
			nodes.add(rootNode);
			refineSidecarNode(nodes, nodeIndex, leafTokenPrefix, selectedEntry.key, rootAtomIndexes, poiDataBlocks,
					rootContinuationLength, 0);
		}
		List<Integer> residualAtomIndexes = buildResidualAtomIndexes(poiDataBlocks.size(), new ArrayList<>(globalCoveredAtomIndexes));
		if (!Algorithms.isEmpty(residualAtomIndexes)) {
			nodes.add(new PoiNameSidecarNode(0xFFFFFFFF, "", 0, Collections.emptyList(), residualAtomIndexes, true));
		}
		int terminalNodeCount = 0;
		int maxContinuationDepth = 0;
		int bytes = 0;
		for (PoiNameSidecarNode node : nodes) {
			if (node.terminal) {
				terminalNodeCount++;
			}
			maxContinuationDepth = Math.max(maxContinuationDepth, node.continuationDepth);
			bytes += estimateSidecarNodeBytes(node);
		}
		return new PoiNameSidecarHierarchy(nodes, bytes, terminalNodeCount, maxContinuationDepth);
	}

	private void refineSidecarNode(List<PoiNameSidecarNode> nodes, int nodeIndex, String leafTokenPrefix,
			String branchKey, List<Integer> branchAtomIndexes, List<PoiDataBlock> poiDataBlocks,
			int rootContinuationLength, int currentDepth) {
		if (rootContinuationLength + currentDepth >= POI_NAME_SIDECAR_MAX_CONTINUATION_DEPTH) {
			return;
		}
		if (!shouldDeepenBranch(leafTokenPrefix, branchKey, branchAtomIndexes, poiDataBlocks)) {
			return;
		}
		Map<String, List<Integer>> childBranches = collectChildBranchAtomIndexes(leafTokenPrefix, branchKey, branchAtomIndexes, poiDataBlocks);
		if (Algorithms.isEmpty(childBranches)) {
			return;
		}
		Set<Integer> coveredByChildren = new LinkedHashSet<>();
		PoiNameSidecarNode parentNode = nodes.get(nodeIndex);
		parentNode.terminal = false;
		for (Map.Entry<String, List<Integer>> entry : childBranches.entrySet()) {
			List<Integer> childAtomIndexes = entry.getValue();
			if (Algorithms.isEmpty(childAtomIndexes)) {
				continue;
			}
			Collections.sort(childAtomIndexes);
			coveredByChildren.addAll(childAtomIndexes);
			int childNodeIndex = nodes.size();
			PoiNameSidecarNode childNode = new PoiNameSidecarNode(nodeIndex, entry.getKey(),
					rootContinuationLength + currentDepth + 1, childAtomIndexes, Collections.emptyList(), true);
			nodes.add(childNode);
			parentNode.childNodeIndexes.add(childNodeIndex);
			refineSidecarNode(nodes, childNodeIndex, leafTokenPrefix, branchKey + entry.getKey(), childAtomIndexes,
					poiDataBlocks, rootContinuationLength, currentDepth + 1);
		}
		parentNode.residualAtomIndexes.clear();
		parentNode.residualAtomIndexes.addAll(buildResidualForBranch(branchAtomIndexes, coveredByChildren));
	}

	private boolean shouldDeepenBranch(String leafTokenPrefix, String branchKey, List<Integer> branchAtomIndexes,
			List<PoiDataBlock> poiDataBlocks) {
		int branchAtomCount = branchAtomIndexes.size();
		if (branchAtomCount < POI_NAME_SIDECAR_BRANCH_HEAVY_THRESHOLD) {
			return false;
		}
		Map<String, List<Integer>> childBranches = collectChildBranchAtomIndexes(leafTokenPrefix, branchKey, branchAtomIndexes, poiDataBlocks);
		Map<String, Integer> childMass = collectChildBranchMass(childBranches);
		if (Algorithms.isEmpty(childMass)) {
			return false;
		}
		List<Integer> masses = new ArrayList<>(childMass.values());
		masses.sort(Collections.reverseOrder());
		double top1MassRatio = masses.get(0) / (double) branchAtomCount;
		Set<Integer> coveredAtomIndexes = new LinkedHashSet<>();
		for (List<Integer> childAtomIndexes : childBranches.values()) {
			coveredAtomIndexes.addAll(childAtomIndexes);
		}
		int coveredAtomCount = coveredAtomIndexes.size();
		double coveredAtomRatio = coveredAtomCount / (double) branchAtomCount;
		double residualMass = 1d - coveredAtomRatio;
		return top1MassRatio <= POI_NAME_SIDECAR_BRANCH_TOP1_MASS_MAX
				&& coveredAtomRatio <= POI_NAME_SIDECAR_BRANCH_COVERAGE_MIN
				&& residualMass >= POI_NAME_SIDECAR_BRANCH_RESIDUAL_MIN;
	}

	private Map<String, Integer> collectChildBranchMass(Map<String, List<Integer>> childBranches) {
		Map<String, Integer> childMass = new LinkedHashMap<>();
		for (Map.Entry<String, List<Integer>> entry : childBranches.entrySet()) {
			childMass.put(entry.getKey(), entry.getValue().size());
		}
		return childMass;
	}

	private Map<String, List<Integer>> collectChildBranchAtomIndexes(String leafTokenPrefix, String branchKey,
			List<Integer> branchAtomIndexes, List<PoiDataBlock> poiDataBlocks) {
		Map<String, LinkedHashSet<Integer>> atomIndexesByChildKey = new LinkedHashMap<>();
		String absolutePrefix = leafTokenPrefix + branchKey;
		for (Integer atomIndex : branchAtomIndexes) {
			PoiDataBlock poiDataBlock = poiDataBlocks.get(atomIndex);
			for (String normalizedToken : poiDataBlock.bloomTokens) {
				if (Algorithms.isEmpty(normalizedToken) || !normalizedToken.startsWith(absolutePrefix)) {
					continue;
				}
				if (normalizedToken.length() <= absolutePrefix.length()) {
					continue;
				}
				String keySegment = normalizedToken.substring(absolutePrefix.length(), absolutePrefix.length() + 1);
				atomIndexesByChildKey.computeIfAbsent(keySegment, k -> new LinkedHashSet<>()).add(atomIndex);
			}
		}
		Map<String, List<Integer>> childBranches = new LinkedHashMap<>();
		for (Map.Entry<String, LinkedHashSet<Integer>> entry : atomIndexesByChildKey.entrySet()) {
			List<Integer> atomIndexes = new ArrayList<>(entry.getValue());
			int spanCount = countSpans(atomIndexes);
			double frag = atomIndexes.isEmpty() ? 1d : spanCount / (double) atomIndexes.size();
			if (atomIndexes.size() >= MIN_POSTING_SIZE_FOR_SIDECAR && frag <= MAX_SPAN_FRAG) {
				childBranches.put(entry.getKey(), atomIndexes);
			}
		}
		return childBranches;
	}

	private List<Integer> buildResidualForBranch(List<Integer> branchAtomIndexes, Set<Integer> coveredAtomIndexes) {
		List<Integer> residualAtomIndexes = new ArrayList<>();
		for (Integer atomIndex : branchAtomIndexes) {
			if (!coveredAtomIndexes.contains(atomIndex)) {
				residualAtomIndexes.add(atomIndex);
			}
		}
		return residualAtomIndexes;
	}

	private PoiNameSidecarSelectionStep selectBestSidecarCandidate(List<PoiNameSidecarEntryMetrics> candidates,
			List<PoiNameSidecarEntry> selectedEntries, Set<Integer> selectedAtomIndexSet, int selectedBytes,
			double maxSpanFrag, int minPostingSize, boolean useGainDensityOrdering) {
		PoiNameSidecarEntryMetrics bestCandidate = null;
		List<Integer> bestUncoveredAtomIndexes = Collections.emptyList();
		int bestEstimatedBytes = 0;
		double bestObjectiveGain = 0d;
		double bestGainDensity = 0d;
		for (PoiNameSidecarEntryMetrics candidate : candidates) {
			if (containsSelectedKey(selectedEntries, candidate.key)) {
				continue;
			}
			List<Integer> uncoveredAtomIndexes = filterUncoveredAtomIndexes(candidate.atomIndexes, selectedAtomIndexSet);
			if (uncoveredAtomIndexes.size() < minPostingSize) {
				continue;
			}
			int uncoveredSpanCount = countSpans(uncoveredAtomIndexes);
			double uncoveredFrag = uncoveredAtomIndexes.isEmpty() ? 1d : uncoveredSpanCount / (double) uncoveredAtomIndexes.size();
			if (uncoveredFrag > maxSpanFrag) {
				continue;
			}
			int estimatedBytes = estimateSidecarEntryBytes(candidate.key, uncoveredAtomIndexes);
			if (selectedBytes + estimatedBytes > POI_NAME_SIDECAR_SIZE_BUDGET) {
				continue;
			}
			double searchGain = uncoveredAtomIndexes.size() - uncoveredSpanCount;
			double objectiveGain = searchGain - (SIDE_CAR_BYTE_PENALTY * estimatedBytes);
			if (searchGain <= 0d || objectiveGain <= 0d) {
				continue;
			}
			double gainDensity = objectiveGain / Math.max(1d, estimatedBytes);
			boolean isBetterCandidate = bestCandidate == null;
			if (!isBetterCandidate && useGainDensityOrdering) {
				isBetterCandidate = gainDensity > bestGainDensity
						|| (gainDensity == bestGainDensity && objectiveGain > bestObjectiveGain)
						|| (gainDensity == bestGainDensity && objectiveGain == bestObjectiveGain && uncoveredAtomIndexes.size() > bestUncoveredAtomIndexes.size());
			} else if (!isBetterCandidate) {
				isBetterCandidate = objectiveGain > bestObjectiveGain
						|| (objectiveGain == bestObjectiveGain && gainDensity > bestGainDensity)
						|| (objectiveGain == bestObjectiveGain && gainDensity == bestGainDensity && uncoveredAtomIndexes.size() > bestUncoveredAtomIndexes.size());
			}
			if (isBetterCandidate) {
				bestCandidate = candidate;
				bestUncoveredAtomIndexes = uncoveredAtomIndexes;
				bestEstimatedBytes = estimatedBytes;
				bestObjectiveGain = objectiveGain;
				bestGainDensity = gainDensity;
			}
		}
		if (bestCandidate == null) {
			return null;
		}
		return new PoiNameSidecarSelectionStep(bestCandidate, bestUncoveredAtomIndexes, bestEstimatedBytes, bestObjectiveGain, bestGainDensity);
	}

	private int countOverlapFilteredCandidates(List<PoiNameSidecarEntryMetrics> candidates, List<PoiNameSidecarEntry> selectedEntries,
			Set<Integer> selectedAtomIndexSet) {
		int overlapFilteredCandidateCount = 0;
		for (PoiNameSidecarEntryMetrics candidate : candidates) {
			if (containsSelectedKey(selectedEntries, candidate.key)) {
				continue;
			}
			List<Integer> uncoveredAtomIndexes = filterUncoveredAtomIndexes(candidate.atomIndexes, selectedAtomIndexSet);
			if (uncoveredAtomIndexes.size() != candidate.atomIndexes.size()) {
				overlapFilteredCandidateCount++;
			}
		}
		return overlapFilteredCandidateCount;
	}

	private int countBudgetRejectedCandidates(List<PoiNameSidecarEntryMetrics> candidates, List<PoiNameSidecarEntry> selectedEntries,
			Set<Integer> selectedAtomIndexSet, int selectedBytes, double maxSpanFrag, int minPostingSize) {
		int budgetRejectedCandidateCount = 0;
		for (PoiNameSidecarEntryMetrics candidate : candidates) {
			if (containsSelectedKey(selectedEntries, candidate.key)) {
				continue;
			}
			List<Integer> uncoveredAtomIndexes = filterUncoveredAtomIndexes(candidate.atomIndexes, selectedAtomIndexSet);
			if (uncoveredAtomIndexes.size() < minPostingSize) {
				continue;
			}
			int uncoveredSpanCount = countSpans(uncoveredAtomIndexes);
			double uncoveredFrag = uncoveredAtomIndexes.isEmpty() ? 1d : uncoveredSpanCount / (double) uncoveredAtomIndexes.size();
			if (uncoveredFrag > maxSpanFrag) {
				continue;
			}
			int estimatedBytes = estimateSidecarEntryBytes(candidate.key, uncoveredAtomIndexes);
			if (selectedBytes + estimatedBytes > POI_NAME_SIDECAR_SIZE_BUDGET) {
				budgetRejectedCandidateCount++;
			}
		}
		return budgetRejectedCandidateCount;
	}

	private boolean isHeavyLeaf(int atomCount) {
		return atomCount >= POI_NAME_SIDECAR_COUNT_THRESHOLD;
	}

	private boolean shouldRunResidualRescue(int atomCount, int coveredAtomCount) {
		if (atomCount < POI_NAME_SIDECAR_COUNT_THRESHOLD || atomCount <= 0) {
			return false;
		}
		double coveredAtomRatio = coveredAtomCount / (double) atomCount;
		double residualMass = 1d - coveredAtomRatio;
		return residualMass > RESIDUAL_RESCUE_MASS_THRESHOLD || coveredAtomRatio < RESIDUAL_RESCUE_COVERAGE_THRESHOLD;
	}

	private double computeTopContinuationMassRatio(List<PoiNameSidecarEntryMetrics> candidates, int atomCount, int topCount) {
		if (atomCount <= 0 || Algorithms.isEmpty(candidates) || topCount <= 0) {
			return 0d;
		}
		List<Integer> postingSizes = new ArrayList<>();
		for (PoiNameSidecarEntryMetrics candidate : candidates) {
			postingSizes.add(candidate.postingSize);
		}
		postingSizes.sort(Collections.reverseOrder());
		int sum = 0;
		for (int i = 0; i < Math.min(topCount, postingSizes.size()); i++) {
			sum += postingSizes.get(i);
		}
		return sum / (double) atomCount;
	}

	private boolean isBroadPrefixLowSelectivity(int atomCount, int eligibleCandidateCount, double top3ContinuationMassRatio) {
		return atomCount >= POI_NAME_SIDECAR_COUNT_THRESHOLD
				&& eligibleCandidateCount >= BROAD_PREFIX_MIN_ELIGIBLE_CANDIDATES
				&& top3ContinuationMassRatio < BROAD_PREFIX_TOP3_MASS_THRESHOLD;
	}

	private double getProbeCoveredAtomRatio(PoiNameSidecarSelectionResult selectionResult, int continuationLength) {
		PoiNameSidecarProbeDepthResult probeDepthResult = findProbeDepthResult(selectionResult, continuationLength);
		return probeDepthResult == null ? 0d : probeDepthResult.coveredAtomRatio;
	}

	private double getProbeResidualMass(PoiNameSidecarSelectionResult selectionResult, int continuationLength) {
		PoiNameSidecarProbeDepthResult probeDepthResult = findProbeDepthResult(selectionResult, continuationLength);
		return probeDepthResult == null ? 0d : probeDepthResult.residualMass;
	}

	private double getProbeEstimatedSpeedup(PoiNameSidecarSelectionResult selectionResult, int continuationLength) {
		PoiNameSidecarProbeDepthResult probeDepthResult = findProbeDepthResult(selectionResult, continuationLength);
		return probeDepthResult == null ? 0d : probeDepthResult.estimatedSpeedup;
	}

	private int getProbeEligibleCandidateCount(PoiNameSidecarSelectionResult selectionResult, int continuationLength) {
		PoiNameSidecarProbeDepthResult probeDepthResult = findProbeDepthResult(selectionResult, continuationLength);
		return probeDepthResult == null ? 0 : probeDepthResult.eligibleCandidateCount;
	}

	private PoiNameSidecarProbeDepthResult findProbeDepthResult(PoiNameSidecarSelectionResult selectionResult, int continuationLength) {
		if (selectionResult == null || Algorithms.isEmpty(selectionResult.probeDepthResults)) {
			return null;
		}
		for (PoiNameSidecarProbeDepthResult probeDepthResult : selectionResult.probeDepthResults) {
			if (probeDepthResult != null && probeDepthResult.continuationLength == continuationLength) {
				return probeDepthResult;
			}
		}
		return null;
	}

	private List<Integer> buildResidualAtomIndexes(int atomCount, List<Integer> selectedAtomIndexes) {
		if (atomCount <= 0) {
			return Collections.emptyList();
		}
		Set<Integer> selected = new HashSet<>(selectedAtomIndexes);
		List<Integer> residualAtomIndexes = new ArrayList<>();
		for (int i = 0; i < atomCount; i++) {
			if (!selected.contains(i)) {
				residualAtomIndexes.add(i);
			}
		}
		return residualAtomIndexes;
	}

	private boolean containsSelectedKey(List<PoiNameSidecarEntry> selectedEntries, String key) {
		for (PoiNameSidecarEntry selectedEntry : selectedEntries) {
			if (selectedEntry.key.equals(key)) {
				return true;
			}
		}
		return false;
	}

	private List<Integer> filterUncoveredAtomIndexes(List<Integer> atomIndexes, Set<Integer> selectedAtomIndexSet) {
		if (Algorithms.isEmpty(atomIndexes)) {
			return Collections.emptyList();
		}
		if (selectedAtomIndexSet.isEmpty()) {
			return new ArrayList<>(atomIndexes);
		}
		List<Integer> uncoveredAtomIndexes = new ArrayList<>();
		for (Integer atomIndex : atomIndexes) {
			if (!selectedAtomIndexSet.contains(atomIndex)) {
				uncoveredAtomIndexes.add(atomIndex);
			}
		}
		return uncoveredAtomIndexes;
	}

	private int countSpans(Collection<Integer> atomIndexes) {
		if (Algorithms.isEmpty(atomIndexes)) {
			return 0;
		}
		int spans = 1;
		Iterator<Integer> iterator = atomIndexes.iterator();
		int prev = iterator.next();
		while (iterator.hasNext()) {
			int current = iterator.next();
			if (current != prev + 1) {
				spans++;
			}
			prev = current;
		}
		return spans;
	}

	private double averageDoubleByCandidate(List<PoiNameSidecarEntry> selectedEntries, Map<String, PoiNameSidecarEntry> mergedEntriesByKey) {
		double sum = 0d;
		int count = 0;
		for (PoiNameSidecarEntry entry : selectedEntries) {
			PoiNameSidecarEntry source = mergedEntriesByKey.get(entry.key);
			if (source == null || Algorithms.isEmpty(source.atomIndexes)) {
				continue;
			}
			int postingSize = source.atomIndexes.size();
			int spanCount = countSpans(source.atomIndexes);
			sum += postingSize == 0 ? 0d : spanCount / (double) postingSize;
			count++;
		}
		return count == 0 ? 0d : sum / count;
	}

	private double maxFragByCandidate(List<PoiNameSidecarEntry> selectedEntries) {
		double maxFrag = 0d;
		for (PoiNameSidecarEntry entry : selectedEntries) {
			int postingSize = entry.atomIndexes.size();
			int spanCount = countSpans(entry.atomIndexes);
			if (postingSize > 0) {
				maxFrag = Math.max(maxFrag, spanCount / (double) postingSize);
			}
		}
		return maxFrag;
	}

	private double computeExpectedSearchCostWithSidecar(int atomCount, List<PoiNameSidecarEntry> selectedEntries,
			List<Integer> residualAtomIndexes) {
		if (atomCount <= 0) {
			return 0d;
		}
		double selectedCost = 0d;
		for (PoiNameSidecarEntry entry : selectedEntries) {
			int postingSize = entry.atomIndexes.size();
			int spanCount = countSpans(entry.atomIndexes);
			selectedCost += postingSize * (spanCount / (double) Math.max(1, postingSize));
		}
		double residualCost = residualAtomIndexes.size();
		return selectedCost + residualCost;
	}

	private int estimateSidecarEntryBytes(PoiNameSidecarEntry entry) {
		return 6 + entry.atomIndexes.size() * 2 + entry.key.length();
	}

	private int estimateSidecarEntryBytes(String key, List<Integer> atomIndexes) {
		return 6 + atomIndexes.size() * 2 + key.length();
	}

	private int safeLength(String value) {
		return value == null ? 0 : value.length();
	}

	private String encodeContinuationKey(String normalizedToken, int prefixLength) {
		return encodeContinuationKey(normalizedToken, prefixLength, POI_NAME_SIDECAR_CONTINUATION_KEY_LENGTH);
	}

	private String encodeContinuationKey(String normalizedToken, int prefixLength, int continuationLength) {
		return normalizedToken.substring(prefixLength, prefixLength + continuationLength);
	}

	private int estimateSidecarNodeBytes(PoiNameSidecarNode node) {
		return 8 + node.keySegment.length() + node.atomIndexes.size() * 2 + node.residualAtomIndexes.size() * 2 + node.childNodeIndexes.size() * 2;
	}

	private void writePoiNameSidecar(CodedOutputStream localOut, List<PoiNameSidecarNode> sidecarNodes) throws IOException {
		for (PoiNameSidecarNode node : sidecarNodes) {
			ByteArrayOutputStream entryBuffer = new ByteArrayOutputStream();
			CodedOutputStream entryOut = CodedOutputStream.newInstance(entryBuffer);
			entryOut.writeUInt32(1, node.parentNodeIndex);
			if (!Algorithms.isEmpty(node.keySegment)) {
				entryOut.writeString(2, node.keySegment);
			}
			entryOut.writeUInt32(3, node.continuationDepth);
			writePackedUInt32(entryOut, 4, node.atomIndexes);
			writePackedUInt32(entryOut, 5, node.residualAtomIndexes);
			writePackedUInt32(entryOut, 6, node.childNodeIndexes);
			if (node.terminal) {
				entryOut.writeBool(7, true);
			}
			entryOut.flush();
			byte[] entryBytes = entryBuffer.toByteArray();
			localOut.writeTag(7, WireFormat.WIRETYPE_LENGTH_DELIMITED);
			localOut.writeRawVarint32(entryBytes.length);
			localOut.writeRawBytes(entryBytes);
		}
	}

	private void writePackedUInt32(CodedOutputStream output, int fieldNumber, List<Integer> values) throws IOException {
		if (Algorithms.isEmpty(values)) {
			return;
		}
		ByteArrayOutputStream packedBuffer = new ByteArrayOutputStream();
		CodedOutputStream packedOut = CodedOutputStream.newInstance(packedBuffer);
		for (Integer value : values) {
			packedOut.writeUInt32NoTag(value);
		}
		packedOut.flush();
		byte[] packedBytes = packedBuffer.toByteArray();
		output.writeTag(fieldNumber, WireFormat.WIRETYPE_LENGTH_DELIMITED);
		output.writeRawVarint32(packedBytes.length);
		output.writeRawBytes(packedBytes);
	}

	private static String normalizeIndexedStringTableKey(String key) {
		if (key == null) {
			return null;
		}
		String normalized = CollatorStringMatcher.alignChars(key);
		normalized = normalized.toLowerCase(Locale.ROOT);
		return normalized;
	}

	private static <V extends Collection> Map<String, V> normalizeIndex(Map<String, V> namesIndex) {
		Map<String, V> normalized = new TreeMap<>();
		for (Entry<String, V> e : namesIndex.entrySet()) {
			String normalizedKey = normalizeIndexedStringTableKey(e.getKey());
			if (normalizedKey == null || normalizedKey.isEmpty()) {
				continue;
			}
			V existing = normalized.get(normalizedKey);
			if (existing == null) {
				normalized.put(normalizedKey, e.getValue());
			} else {
				existing.addAll(e.getValue());
			}
		}
		return normalized;
	}

	private Map<String, BinaryFileReference> writeIndexedTable(int tag, Collection<String> indexedTable) throws IOException {
		codedOutStream.writeTag(tag, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
		preserveInt32Size();

		IndexedStringTableNode root = new IndexedStringTableNode(this);
		for (String key : indexedTable) {
			if (key == null) {
				continue;
			}
			root.addKey(key, 0);
		}

		Map<String, BinaryFileReference> res = new LinkedHashMap<>();
		root.writeNode("", res, getFilePointer());
		writeInt32Size();
		return res;
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
