package net.osmand.obf;

import org.junit.Ignore;
import org.junit.Test;

import java.io.*;

import static org.junit.Assert.assertTrue;

/**
 * Tests icons order for different zoom level
 * by default Synthetic_test_rendering.obf should be in the /resources/test-resources folder
 */

public class IconVisibilityTest {

	@Test
	@Ignore
	public void testVisibility() throws IOException {
		IconVisibility iconComparator = new IconVisibility();
		String res = iconComparator.compare("src/test/resources/Synthetic_test_rendering.obf", "default.render.xml");
		assertTrue(res, res.isEmpty());
	}

}
