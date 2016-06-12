package net.osmand.data;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map.Entry;

import net.osmand.IProgress;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Way;
import net.osmand.osm.io.OsmBaseStorage;

import org.junit.Test;
import org.xmlpull.v1.XmlPullParserException;

public class MultipolygonFileTest {


	@Test
	public void testDifferentOrientationMultipolygon() throws IOException, XmlPullParserException {
		String file = "multipolygon.osm";
		OsmBaseStorage st = parse(file);
		Multipolygon polygon = buildPolygon(st, 1184817L);
		assertTrue(polygon.areRingsComplete());
		assertEquals(1, polygon.getOuterRings().size());
		Ring rng = polygon.getOuterRings().get(0);
		assertTrue(rng.containsPoint(53.17573, 8.26));
		assertTrue(rng.containsPoint(53.18901289819956, 8.296700487828224));
		assertFalse(rng.containsPoint(53.1863199155393, 8.309607569738336));
		assertFalse(rng.containsPoint(53.13992097340422, 8.280586804995954));
	}
	
	
	private OsmBaseStorage parse(String file) throws IOException, XmlPullParserException {
		InputStream is = MultipolygonFileTest.class.getResourceAsStream(file);
		OsmBaseStorage st = new OsmBaseStorage();
		st.parseOSM(is, IProgress.EMPTY_PROGRESS);
		return st;
	}

	private Multipolygon buildPolygon(OsmBaseStorage st, long id) {
		Relation r = (Relation) st.getRegisteredEntities().get(new EntityId(EntityType.RELATION, id));
		Iterator<Entry<Entity, String>> it = r.getMemberEntities().entrySet().iterator();
		MultipolygonBuilder bld = new MultipolygonBuilder();
		while (it.hasNext()) {
			Entry<Entity, String> e = it.next();
			if (e.getValue().equals("outer")) {
				bld.addOuterWay((Way) e.getKey());
			} else if (e.getValue().equals("inner")) {
				bld.addInnerWay((Way) e.getKey());
			}
		}
		Multipolygon polygon = bld.build();
		return polygon;
	}
}
