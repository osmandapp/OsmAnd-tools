package net.osmand.data;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.SortedSet;
import java.util.TreeSet;

import net.osmand.osm.edit.Way;

import org.apache.commons.logging.Log;

/**
 * The idea of multipolygon:
 * - we treat each outer way as closed polygon
 * - multipolygon is always closed!
 * - each way we try to assign to existing way and form 
 *   so a more complex polygon
 * - number of outer ways, is number of polygons
 * 
 * @author Pavol Zibrita
 */
public class MultipolygonBuilder {

	/* package */List<Way> outerWays = new ArrayList<Way>();
	/* package */List<Way> innerWays = new ArrayList<Way>();

	long id;

	/**
	 * Create a multipolygon with initialized outer and inner ways
	 * 
	 * @param outers
	 *            a list of outer ways
	 * @param inners
	 *            a list of inner ways
	 */
	public MultipolygonBuilder(List<Way> outers, List<Way> inners) {
		this();
		outerWays.addAll(outers);
		innerWays.addAll(inners);
	}

	public MultipolygonBuilder() {
		id = -1L;
	}

	public void setId(long newId) {
		id = newId;
	}

	public long getId() {
		return id;
	}

	public MultipolygonBuilder addInnerWay(Way w) {
		innerWays.add(w);
		return this;
	}
	
	public List<Way> getOuterWays() {
		return outerWays;
	}
	
	public List<Way> getInnerWays() {
		return innerWays;
	}

	public MultipolygonBuilder addOuterWay(Way w) {
		outerWays.add(w);
		return this;
	}

	/**
	 * Split this multipolygon in several separate multipolygons with one outer ring each
	 * 
	 * @param log
	 *            the stream to log problems to, if log = null, nothing will be logged
	 * @return a list with multipolygons which have exactly one outer ring
	 */
	public List<Multipolygon> splitPerOuterRing(Log log) {
		SortedSet<Ring> inners = new TreeSet<Ring>(combineToRings(innerWays));
		ArrayList<Ring> outers = combineToRings(outerWays);
		ArrayList<Multipolygon> multipolygons = new ArrayList<Multipolygon>();
		// loop; start with the smallest outer ring
		for (Ring outer : outers) {
			ArrayList<Ring> innersInsideOuter = new ArrayList<Ring>();
			Iterator<Ring> innerIt = inners.iterator();
			while (innerIt.hasNext()) {
				Ring inner = innerIt.next();
				if (inner.isIn(outer)) {
					innersInsideOuter.add(inner);
					innerIt.remove();
				}
			}
			multipolygons.add(new Multipolygon(outer, innersInsideOuter, id));
		}

		if (!inners.isEmpty() && log != null) {
			log.warn("Multipolygon " + getId() + " has a mismatch in outer and inner rings");
		}

		return multipolygons;
	}

	public Multipolygon build() {
		return new Multipolygon(combineToRings(outerWays), combineToRings(innerWays), id);
	}

	public ArrayList<Ring> combineToRings(List<Way> ways) {
		// make a list of multiLines (connecter pieces of way)
		ArrayList<Way> multiLines = new ArrayList<Way>();
		for (Way toAdd : ways) {
			if (toAdd.getNodeIds().size() < 2) {
				continue;
			}
			// iterate over the multiLines, and add the way to the correct one
			Way changedWay = toAdd;
			Way newWay;
			do {
				newWay = null;
				if(changedWay != null) {
					ListIterator<Way> it = multiLines.listIterator();
					while(it.hasNext()){
						Way w = it.next();
						newWay = combineTwoWaysIfHasPoints(changedWay, w);
						if(newWay != null) {
							changedWay = newWay;
							it.remove();
							break;
						}
					}
				}
			} while(newWay != null);
			multiLines.add(changedWay);
			
		}
		ArrayList<Ring> result = new ArrayList<Ring>();
		for (Way multiLine : multiLines) {
			Ring r = new Ring(multiLine);
			result.add(r);
		}
		return result;
	}

	/**
	 * make a new Way with the nodes from two other ways
	 * 
	 * @param w1
	 *            the first way
	 * @param w2
	 *            the second way
	 * @return null if it is not possible
	 */
	private Way combineTwoWaysIfHasPoints(Way w1, Way w2) {
		boolean combine = true;
		boolean firstReverse = false;
		boolean secondReverse = false;
		if (w1.getFirstNodeId() == w2.getFirstNodeId()) {
			firstReverse = true;
			secondReverse = false;
		} else if (w1.getLastNodeId() == w2.getFirstNodeId()) {
			firstReverse = false;
			secondReverse = false;
		} else if (w1.getLastNodeId() == w2.getLastNodeId()) {
			firstReverse = false;
			secondReverse = true;
		} else if (w1.getFirstNodeId() == w2.getLastNodeId()) {
			firstReverse = true;
			secondReverse = true;
		} else {
			combine = false;
		}
		if (combine) {
			Way newWay = new Way(nextRandId());
			boolean nodePresent = w1.getNodes() != null || w1.getNodes().size() != 0;
			int w1size = nodePresent ? w1.getNodes().size() : w1.getNodeIds().size();
			for (int i = 0; i < w1size; i++) {
				int ind = firstReverse ? (w1size - 1 - i) : i;
				if (nodePresent) {
					newWay.addNode(w1.getNodes().get(ind));
				} else {
					newWay.addNode(w1.getNodeIds().get(ind));
				}
			}
			int w2size = nodePresent ? w2.getNodes().size() : w2.getNodeIds().size();
			for (int i = 1; i < w2size; i++) {
				int ind = secondReverse ? (w2size - 1 - i) : i;
				if (nodePresent) {
					newWay.addNode(w2.getNodes().get(ind));
				} else {
					newWay.addNode(w2.getNodeIds().get(ind));
				}
			}
			return newWay;
		}
		return null;

	}

	private static long initialValue = -1000;
	private final static long randomInterval = 5000;

	/**
	 * get a random long number
	 * 
	 * @return
	 */
	private static long nextRandId() {
		// exclude duplicates in one session (!) and be quazirandom every run
		long val = initialValue - Math.round(Math.random() * randomInterval);
		initialValue = val;
		return val;
	}

}
