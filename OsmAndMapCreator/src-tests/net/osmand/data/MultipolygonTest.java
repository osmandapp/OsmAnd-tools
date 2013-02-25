package net.osmand.data;

import static org.junit.Assert.*;


import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;

import org.junit.Before;
import org.junit.Test;

public class MultipolygonTest {

	private Way poly1_1_of_2;
	private Way poly1_2_of_2;
	private int wayid;
	private Way poly2;
	private Way openedBaseCircle;
	private Way closedBaseCircle;

	@Before
	public void setUp() 
	{
		poly1_1_of_2 = polygon(n(0,0),n(1,0),n(1,1),n(1,2));
		poly1_2_of_2 = polygon(n(1,2),n(0,2),n(-1,2),n(0,0));
		poly2 = polygon(n(4,4), n(4,5), n(3,5), n(4,4));
		openedBaseCircle = polygon(n(1,-1), n(1,1), n(-1,1), n(-1,-1));
		closedBaseCircle = polygon(n(1,-1), n(1,1), n(-1,1), n(-1,-1), n(1,-1));
	}
	
	public Way polygon(Node... n) {
		Way way = new Way(wayid++);
		for (Node nn : n) {
			way.addNode(nn);
		}
		return way;
	}

	public Way scale(int i, Way w) {
		Way way = new Way(wayid++);
		for (Node nn : w.getNodes()) {
			way.addNode(n(i*(int)nn.getLatitude(),i*(int)nn.getLongitude()));
		}
		return way;
	}

	public Way move(int i, int j, Way w) {
		Way way = new Way(wayid++);
		for (Node nn : w.getNodes()) {
			way.addNode(n(i+(int)nn.getLatitude(),j+(int)nn.getLongitude()));
		}
		return way;
	}

	public Node n(int i, int j) {
		return new Node(i, j, i*i + j*j + i*j + i + j); //Node has ID derived from i,j
	}

	@Test
	public void test_twoWayPolygon() {
		Multipolygon testee = new MultipolygonBuilder().addOuterWay(poly1_1_of_2).
				addOuterWay(poly1_2_of_2).build();
		assertEquals(1, testee.countOuterPolygons());
		assertFalse(testee.hasOpenedPolygons());
	}
	
	@Test
	public void test_ringArea(){
		Way w = new Way(0L);
		
		w.addNode(new Node(0.0, 0.0, 1));
		w.addNode(new Node(1.0, 0.0, 2));
		w.addNode(new Node(1.0, 0.5, 3));
		w.addNode(new Node(1.5, 0.5, 4));
		w.addNode(new Node(1.0, 1.0, 5));
		
		Multipolygon m = new MultipolygonBuilder().addOuterWay(w).build();
		
		Ring r = m.getOuterRings().get(0);
		// calculated with JOSM measurement tool
		double expected = 7716818755.73;
		// allow 1% deviation because of rounding errors and alternative projections
		assertTrue(expected/r.getArea() > 0.99);
		assertTrue(expected/r.getArea() < 1.01);
	}

	@Test
	public void test_oneWayPolygon() {
		Multipolygon testee = new MultipolygonBuilder().addOuterWay(poly2)
				.build();
		assertEquals(1, testee.countOuterPolygons());
		assertFalse(testee.hasOpenedPolygons());
	}

	@Test
	public void test_containsPoint()
	{
		Multipolygon testee = new MultipolygonBuilder().addOuterWay(scale(4,poly2))
				.build();
		LatLon center = testee.getCenterPoint();
		assertTrue(testee.containsPoint(center));
	}
	
	@Test
	public void test_containsPointOpenedCircle()
	{
		Multipolygon testee = new MultipolygonBuilder().addOuterWay(scale(4,openedBaseCircle))
				.build();
		LatLon center = testee.getCenterPoint();
		assertTrue(testee.containsPoint(center));
	}
	
	@Test
	public void test_containsPointClosedCircle()
	{
		Multipolygon testee = new MultipolygonBuilder().addOuterWay(scale(4,closedBaseCircle))
				.build();
		LatLon center = testee.getCenterPoint();
		assertTrue(testee.containsPoint(center));
	}
	
	@Test
	public void test_oneInnerRingOneOuterRingOpenedCircle()
	{
		test_oneInnerRingOneOuterRing(openedBaseCircle);
	}

	@Test
	public void test_oneInnerRingOneOuterRingClosedCircle()
	{
		test_oneInnerRingOneOuterRing(closedBaseCircle);
	}

	public void test_oneInnerRingOneOuterRing(Way polygon)
	{
		MultipolygonBuilder bld = new MultipolygonBuilder();
		bld.addOuterWay(scale(4,polygon));
		Multipolygon testee = bld.build();
		LatLon center = testee.getCenterPoint();
		assertTrue(testee.containsPoint(center));

		MultipolygonBuilder mpoly2 = new MultipolygonBuilder();
		mpoly2.addOuterWay(polygon);
		
		assertTrue(testee.containsPoint(mpoly2.build().getCenterPoint()));
		
		bld.addInnerWay(polygon);
		testee = bld.build();
		assertFalse(testee.containsPoint(mpoly2.build().getCenterPoint()));
	}

	@Test
	public void test_twoInnerRingsOneOuterRingOpenedCircle()
	{
		test_twoInnerRingsOneOuterRing(openedBaseCircle);
	}
	
	@Test
	public void test_twoInnerRingsOneOuterRingClosedCircle()
	{
		test_twoInnerRingsOneOuterRing(closedBaseCircle);
	}
	
	public void test_twoInnerRingsOneOuterRing(Way polygon)
	{
		MultipolygonBuilder bld = new MultipolygonBuilder();
		bld.addOuterWay(scale(40,polygon));
		Multipolygon testee = bld.build();
		LatLon center = testee.getCenterPoint();
		assertTrue(testee.containsPoint(center));
		
		MultipolygonBuilder mpoly2 = new MultipolygonBuilder();
		mpoly2.addOuterWay(polygon);
		MultipolygonBuilder movepoly2 = new MultipolygonBuilder();
		movepoly2.addOuterWay(move(10,10,polygon));

		assertTrue(testee.containsPoint(mpoly2.build().getCenterPoint()));
		assertTrue(testee.containsPoint(movepoly2.build().getCenterPoint()));

		bld.addInnerWay(polygon);
		bld.addInnerWay(move(10,10,polygon));
		testee = bld.build();

		assertFalse(testee.containsPoint(mpoly2.build().getCenterPoint()));
		assertFalse(testee.containsPoint(movepoly2.build().getCenterPoint()));
	}

	@Test
	public void test_multipolygon1twoWay2oneWay()
	{
		MultipolygonBuilder bld = new MultipolygonBuilder();
		bld.addOuterWay(poly1_1_of_2).addOuterWay(poly1_2_of_2).addOuterWay(poly2);
		Multipolygon testee = bld.build();
		assertEquals(2, testee.countOuterPolygons());
		assertFalse(testee.hasOpenedPolygons());
	}

	@Test
	public void test_firstEmptyWayThanOpenedWay()
	{
		MultipolygonBuilder bld = new MultipolygonBuilder();
		bld.addOuterWay(new Way(111)).addOuterWay(poly1_1_of_2);
		Multipolygon testee = bld.build();
		assertEquals(1, testee.countOuterPolygons());
		assertTrue(testee.hasOpenedPolygons());
	}

	@Test
	public void test_mergingExistingPolygons()
	{
		MultipolygonBuilder bld = new MultipolygonBuilder();
		Way part1 = polygon(n(1,1),n(1,2),n(1,3));
		Way part2 = polygon(n(1,3),n(1,4),n(1,5));
		Way part3 = polygon(n(1,5),n(1,6),n(1,2));
		bld.addOuterWay(part1);
		bld.addOuterWay(part3);
		bld.addOuterWay(part2);
		Multipolygon testee = bld.build();
		assertEquals(1, testee.countOuterPolygons());
		assertTrue(testee.hasOpenedPolygons());
	}

	@Test
	public void test_mergingExistingPolygonsReversed()
	{
		MultipolygonBuilder bld = new MultipolygonBuilder();
		Way part1 = polygon(n(1,3),n(1,2),n(1,1));
		Way part2 = polygon(n(1,3),n(1,4),n(1,5));
		Way part3 = polygon(n(1,5),n(1,6),n(1,2));
		bld.addOuterWay(part1);
		bld.addOuterWay(part3);
		bld.addOuterWay(part2);
		Multipolygon testee = bld.build();
		assertEquals(1, testee.countOuterPolygons());
		assertTrue(testee.hasOpenedPolygons());
	}
	

}
