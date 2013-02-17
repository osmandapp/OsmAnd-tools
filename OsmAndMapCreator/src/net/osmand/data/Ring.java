package net.osmand.data;


import java.util.ArrayList;
import java.util.List;

import net.osmand.osm.Node;
import net.osmand.osm.Way;
import net.osmand.util.MapAlgorithms;

/**
 * A ring is a list of CONTINUOS ways that form a simple boundary or an area. <p />
 * @author sander
 */
public class Ring implements Comparable<Ring> {
	/**
	 * This is a list of the ways added by the user
	 * The order can be changed with methods from this class
	 */
//	private final ArrayList<Way> ways;
	
	/**
	 * a concatenation of the ways to form the border
	 * this is NOT necessarily a CLOSED way
	 * The id is random, so this may never leave the Ring object
	 */
	private Way border;
	
	/**
	 * area can be asked a lot of times when comparing rings, cache it
	 */
	private double area = -1;
	

	/**
	 * Construct a Ring with a list of ways
	 * @param ways the ways that make up the Ring
	 */
	Ring(Way w) {
		border = w;
	}

	/**
	 * check if this ring is closed by nature
	 * @return true if this ring is closed, false otherwise
	 */
	public boolean isClosed() {
		return border.getFirstNodeId() == border.getLastNodeId();
	}
	
	public List<Node> getBorder() {
		return border.getNodes();
	}

	
	/**
	 * check if this Ring contains the node
	 * @param n the Node to check
	 * @return yes if the node is inside the ring
	 */
	public boolean containsNode(Node n) {
		return  containsPoint(n.getLatitude(), n.getLongitude());
	}
	
	/**
	 * check if this Ring contains the point
	 * @param latitude lat of the point
	 * @param longitude lon of the point
	 * @return yes if the point is inside the ring
	 */
	public boolean containsPoint(double latitude, double longitude){
		return  countIntersections(latitude, longitude) % 2 == 1;
	}
	
	/**
	 * count the intersections when going from lat, lon to outside the ring
	 */
	private int countIntersections(double latitude, double longitude) {
		int intersections = 0;
		
		List<Node> polyNodes = getBorder();
		if (polyNodes.size() == 0) return 0;
		for (int i = 0; i < polyNodes.size() - 1; i++) {
			if (MapAlgorithms.ray_intersect_lon(polyNodes.get(i),
					polyNodes.get(i + 1), latitude, longitude) != -360d) {
				intersections++;
			}
		}
		// special handling, also count first and last, might not be closed, but
		// we want this!
		if (MapAlgorithms.ray_intersect_lon(polyNodes.get(0),
				polyNodes.get(polyNodes.size() - 1), latitude, longitude) != -360d) {
			intersections++;
		}
		return intersections;
	}
	
	
	/**
	 * Check if this is in Ring r
	 * @param r the ring to check
	 * @return true if this Ring is inside Ring r
	 */
	public boolean isIn(Ring r) {
		/*
		 * bi-directional check is needed because some concave rings can intersect
		 * and would only fail on one of the checks
		 */
		List<Node> points = this.getBorder();
		
		// r should contain all nodes of this
		for(Node n : points) {
			if (!r.containsNode(n)) {
				return false;
			}
		}
		
		points = r.getBorder();
		
		// this should not contain a node from r
		for(Node n : points) {
			if (this.containsNode(n)) {
				return false;
			}
		}
		
		return true;
		
	}

	
	/**
	 * If this Ring is not complete 
	 * (some ways are not initialized 
	 * because they are not included in the OSM file) <p />
	 * 
	 * We are trying to close this Ring by using the other Ring.<p />
	 * 
	 * The other Ring must be complete, and the part of this Ring 
	 * inside the other Ring must also be complete.
	 * @param other the other Ring (which is complete) used to close this one
	 */
	public void closeWithOtherRing(Ring other) {
		List<Node> thisBorder = getBorder();
		List<Integer> thisSwitchPoints = new ArrayList<Integer>();
		
		boolean insideOther = other.containsNode(thisBorder.get(0));
		
		// Search the node pairs for which the ring goes inside or out the other
		for (int i = 0; i<thisBorder.size(); i++) {
			Node n = thisBorder.get(i);
			if (other.containsNode(n) != insideOther) {
				// we are getting out or in the boundary now.
				// toggle switch
				insideOther = !insideOther;
				
				thisSwitchPoints.add(i);
			}
		}
		
		List<Integer> otherSwitchPoints = new ArrayList<Integer>();
		
		// Search the according node pairs in the other ring
		for (int i : thisSwitchPoints) {
			LatLon a = thisBorder.get(i-1).getLatLon();
			LatLon b = thisBorder.get(i).getLatLon();
			otherSwitchPoints.add(getTheSegmentRingIntersectsSegment(a, b));
		}
		
		
		
		/*
		 * TODO:
		 * 
		 * * Split the other Ring into ways from splitPoint to splitPoint
		 * 
		 * * Split this ring into ways from splitPoint to splitPoint
		 * 
		 * * Filter out the parts of way from this that are inside the other Ring
		 * 		Use the insideOther var and the switchPoints list for this.
		 * 
		 * * For each two parts of way from this, search a part of way connecting the two. 
		 * 		If there are two, take the shortest.
		 */
	}
	
	/**
	 * Get the segment of the Ring that intersects a segment 
	 * going from point a to point b
	 * 
	 * @param a the begin point of the segment
 	 * @param b the end point of the segment
	 * @return an integer i which is the index so that the segment 
	 * 		from getBorder().get(i-1) to getBorder().get(i) intersects with 
	 * 		the segment from parameters a to b. <p />
	 * 
	 * 		0 if the segment from a to b doesn't intersect with the Ring. 
	 */
	private int getTheSegmentRingIntersectsSegment(LatLon a, LatLon b) {
		List<Node> border = getBorder();
		for (int i = 1; i<border.size(); i++) {
			LatLon c = border.get(i-1).getLatLon();
			LatLon d = border.get(i).getLatLon();
			if (MapAlgorithms.linesIntersect(
					a.getLatitude(), a.getLongitude(), 
					b.getLatitude(), b.getLongitude(), 
					c.getLatitude(), c.getLongitude(), 
					d.getLatitude(), d.getLongitude())) {
				return i;
			}
		}
		return 0;
		
	}
	
	public double getArea() {
		if (area == -1) {
			//cache the area
			area = MapAlgorithms.getArea(getBorder());
		}
		return area;
	}
	
	


	/**
	 * Use area size as comparable metric
	 */
	@Override
	public int compareTo(Ring r) {
		return Double.compare(getArea(), r.getArea());
	}
}