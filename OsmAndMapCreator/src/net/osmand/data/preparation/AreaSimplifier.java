package net.osmand.data.preparation;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import javax.xml.stream.XMLStreamException;

import static java.lang.Math.cos;
import static java.lang.Math.sin;
import static java.lang.Math.toRadians;

import org.apache.commons.logging.Log;
import org.apache.tools.bzip2.CBZip2InputStream;
import org.xmlpull.v1.XmlPullParserException;

import net.osmand.PlatformUtil;
import net.osmand.data.LatLon;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.osm.io.OsmStorageWriter;
import net.osmand.util.MapUtils;

public class AreaSimplifier {
	
	private static Map<Node, List<Way>> reference;
	private final static Log LOG = PlatformUtil.getLog(AreaSimplifier.class);

	public static void main(String[] args) throws FileNotFoundException, IOException, XmlPullParserException, XMLStreamException {
		if (args.length < 2) {
			System.out.println("Usage: <path_to_osm> <path_to_simplified_osm>");
		}
		File input = new File(args[0]);
		File result = new File(args[1]);
		AreaSimplifier as = new AreaSimplifier();
		as.init(input, result);
	}

	private void init(File input, File result) throws FileNotFoundException, IOException, XmlPullParserException, XMLStreamException {
		OsmBaseStorage data = parseOsmFile(input);
		List<Way> waysToSimplify = new ArrayList<>();
		reference = new HashMap<>();
		Map<EntityId, Entity> registeredEntities = data.getRegisteredEntities();
		for (Entity e : registeredEntities.values()) {
			if (e instanceof Way) {
				Way w = (Way) e;
				waysToSimplify.add(w);
				for (Node n : w.getNodes()) {
					if (reference.get(n) == null) {
						List<Way> ways = new ArrayList<Way>();
						ways.add(w);
						reference.put(n, ways);
					} else {
						reference.get(n).add(w);
					}
				}
			}
		}
		final List<Node> nodesToDelete = new ArrayList<>(); 
		for (final Way way : waysToSimplify) {
			addNodesToDelete(nodesToDelete, way);
		}
		final Map<Node, Integer> nodeCountMap = new HashMap<>();
		for (final Node node : nodesToDelete) {
			Integer count = nodeCountMap.get(node);
			if (count == null) {
				count = 0;
			}
			nodeCountMap.put(node, ++count);
		}

		final Collection<Node> nodesReallyToRemove = new ArrayList<>();

		for (final Entry<Node, Integer> entry : nodeCountMap.entrySet()) {
			final Node node = entry.getKey();
			final Integer count = entry.getValue();
			if (node.getTags().isEmpty() && reference.get(node).size() == count) {
				nodesReallyToRemove.add(node);
			}
		}

		if (!nodesReallyToRemove.isEmpty()) {
			for (final Way way : waysToSimplify) {
				final List<Node> nodes = way.getNodes();
				final boolean closed = nodes.get(0).equals(nodes.get(nodes.size() - 1));
				if (closed) {
					nodes.remove(nodes.size() - 1);
				}

				if (nodes.removeAll(nodesReallyToRemove)) {
					for (Node n : nodesReallyToRemove) {
						registeredEntities.remove(new Entity.EntityId(EntityType.NODE, n.getId()));
					}
					long id = way.getId();
					System.out.println("Way " + id + " simplified.");
					if (closed) {
						nodes.add(nodes.get(0));
					}
					final Way newWay = new Way(id, nodes);
					newWay.replaceTags(way.getTags());
					Entity.EntityId wayId = new Entity.EntityId(EntityType.WAY, id);
					registeredEntities.remove(wayId);
					registeredEntities.put(wayId, newWay);
				}
			}
		}
		
		OsmStorageWriter writer = new OsmStorageWriter();
		LOG.info("Writing file... ");
		writer.saveStorage(new FileOutputStream(result), data, data.getRegisteredEntities().keySet(), true);
		LOG.info("DONE");

	}

	private OsmBaseStorage parseOsmFile(File read) throws FileNotFoundException, IOException, XmlPullParserException {
		OsmBaseStorage storage = new OsmBaseStorage();
		InputStream stream = new BufferedInputStream(new FileInputStream(read), 8192 * 4);
		InputStream streamFile = stream;
		if (read.getName().endsWith(".bz2")) {
			if (stream.read() != 'B' || stream.read() != 'Z') {

			} else {
				stream = new CBZip2InputStream(stream);
			}
		} else if (read.getName().endsWith(".gz")) {
			stream = new GZIPInputStream(stream);
		}
		storage.parseOSM(stream, new ConsoleProgressImplementation(), streamFile, true);
		return storage;
	}

	private static boolean nodeGluesWays(final Node node) {
		Set<Node> referenceNeighbours = null;
		for (final Way way : reference.get(node)) {
			final Set<Node> neighbours = getNeighbours(node, way);
			if (referenceNeighbours == null) {
				referenceNeighbours = neighbours;
			} else if (!referenceNeighbours.containsAll(neighbours)) {
				return true;
			}
		}
		return false;
	}
	
	/**
     * Return nodes adjacent to <code>node</code>
     *
     * @param node the node. May be null.
     * @return Set of nodes adjacent to <code>node</code>
     */
    private static Set<Node> getNeighbours(Node node, Way way) {
        Set<Node> neigh = new HashSet<>();

        if (node == null) return neigh;

        List<Node> wayNodes = way.getNodes();
		Node[] nodes = wayNodes.toArray(new Node[wayNodes.size()]);
        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i].equals(node)) {
                if (i > 0)
                    neigh.add(nodes[i-1]);
                if (i < nodes.length-1)
                    neigh.add(nodes[i+1]);
            }
        }
        return neigh;
    }

	private static void addNodesToDelete(final Collection<Node> nodesToDelete, final Way w) {
		// TODO extract these values as simplification parameters
		final double angleThreshold = 10;
		final double angleFactor = 1.0;
		final double areaThreshold = 5.0;
		final double areaFactor = 1.0;
		final double distanceThreshold = 3;
		final double distanceFactor = 3;

		final List<Node> nodes = new ArrayList<>(w.getNodes());
		final int size = nodes.size();

		if (size == 0) {
			return;
		}

		final boolean closed = nodes.get(0).equals(nodes.get(size - 1));

		if (closed) {
			nodes.remove(size - 1); // remove end node ( = start node)
		}

		// remove nodes within threshold

		final List<Double> weightList = new ArrayList<>(nodes.size()); // weight
																		// cache
		for (int i = 0; i < nodes.size(); i++) {
			weightList.add(null);
		}

		while (true) {
			Node prevNode = null;
			LatLon coord1 = null;
			LatLon coord2 = null;
			int prevIndex = -1;

			double minWeight = Double.POSITIVE_INFINITY;
			Node bestMatch = null;

			final int size2 = nodes.size();

			if (size2 == 0) {
				break;
			}

			for (int i = 0, len = size2 + (closed ? 2 : 1); i < len; i++) {
				final int index = i % size2;

				final Node n = nodes.get(index);
				final LatLon coord3 = n.getLatLon();

				if (coord1 != null) {
					final double weight;

					if (weightList.get(prevIndex) == null) {
						final double angleWeight = computeConvectAngle(coord1, coord2, coord3) / angleThreshold;
						final double areaWeight = computeArea(coord1, coord2, coord3) / areaThreshold;
						final double distanceWeight = Math.abs(crossTrackError(coord1, coord2, coord3))
								/ distanceThreshold;

						weight = !closed && i == len - 1 || // don't remove last node of the not closed way
								nodeGluesWays(prevNode) || angleWeight > 1.0 || areaWeight > 1.0 || distanceWeight > 1.0
										? Double.POSITIVE_INFINITY
										: angleWeight * angleFactor + areaWeight * areaFactor
												+ distanceWeight * distanceFactor;

						weightList.set(prevIndex, weight);
					} else {
						weight = weightList.get(prevIndex);
					}

					if (weight < minWeight) {
						minWeight = weight;
						bestMatch = prevNode;
					}
				}

				coord1 = coord2;
				coord2 = coord3;
				prevNode = n;
				prevIndex = index;
			}

			if (bestMatch == null) {
				break;
			}

			final int index = nodes.indexOf(bestMatch);

			weightList.set((index - 1 + size2) % size2, null);
			weightList.set((index + 1 + size2) % size2, null);
			weightList.remove(index);
			nodes.remove(index);
		}

		final HashSet<Node> delNodes = new HashSet<>(w.getNodes());
		delNodes.removeAll(nodes);

		nodesToDelete.addAll(delNodes);
	}

	public static double computeConvectAngle(final LatLon coord1, final LatLon coord2, final LatLon coord3) {
		final double angle = Math.abs(heading(coord2, coord3) - heading(coord1, coord2));
		return Math.toDegrees(angle < Math.PI ? angle : 2 * Math.PI - angle);
	}

	public static double computeArea(final LatLon coord1, final LatLon coord2, final LatLon coord3) {
		final double a = MapUtils.getDistance(coord1, coord2);
		final double b = MapUtils.getDistance(coord2, coord3);
		final double c = MapUtils.getDistance(coord3, coord1);

		final double p = (a + b + c) / 2.0;

		final double q = p * (p - a) * (p - b) * (p - c);
		return q < 0.0 ? 0.0 : Math.sqrt(q);
	}

	public static double R = 6378135;

	public static double crossTrackError(final LatLon l1, final LatLon l2, final LatLon l3) {
		return R * Math.asin(sin(MapUtils.getDistance(l1, l2) / R) * sin(heading(l1, l2) - heading(l1, l3)));
	}

	public static double heading(final LatLon a, final LatLon b) {
		double hd = Math.atan2(sin(toRadians(a.getLongitude() - b.getLongitude())) * cos(toRadians(b.getLatitude())),
				cos(toRadians(a.getLatitude())) * sin(toRadians(b.getLatitude())) - sin(toRadians(a.getLatitude()))
						* cos(toRadians(b.getLatitude())) * cos(toRadians(a.getLongitude() - b.getLongitude())));
		hd %= 2 * Math.PI;
		if (hd < 0) {
			hd += 2 * Math.PI;
		}
		return hd;
	}
}
