package net.osmand.router;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import net.osmand.NativeJavaRendering;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.swing.DataExtractionSettings;
import net.osmand.util.Algorithms;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class JUnitRouteTest  {

	static BinaryMapIndexReader[]  rs;
	static NativeJavaRendering lib;


	@Before
	public void setupFiles() throws IOException {
		if(rs != null){
			return;
		}
		// test without native because it is not present on the server
//		lib = NativeSwingRendering.getDefaultFromSettings();
		RouteResultPreparation.PRINT_TO_CONSOLE_ROUTE_INFORMATION_TO_TEST = true;
		String obfdir = System.getenv("OBF_DIR");
		if(Algorithms.isEmpty(obfdir)){
			obfdir = DataExtractionSettings.getSettings().getBinaryFilesDir();
		}

		List<File> files = new ArrayList<File>();
		for (File f : new File(obfdir).listFiles()) {
			if (f.getName().endsWith(".obf")) {
				files.add(f);
			}
		}
		rs = new BinaryMapIndexReader[files.size()];
		int it = 0;
		for (File f : files) {
			RandomAccessFile raf = new RandomAccessFile(f, "r"); //$NON-NLS-1$ //$NON-NLS-2$
			rs[it++] = new BinaryMapIndexReader(raf, f);
		}
	}

	@Test
	public void runCZ() throws Exception {
		TestRouting.test(lib, getClass().getResourceAsStream("cz.test.xml"), rs, RoutingConfiguration.getDefault());
	}

	@Test
	public void runUk() throws Exception {
		TestRouting.test(lib, getClass().getResourceAsStream("uk.test.xml"), rs, RoutingConfiguration.getDefault());
	}

	@Test
	@Ignore
	public void runNL() throws Exception {
		TestRouting.test(lib, getClass().getResourceAsStream("nl.test.xml"), rs, RoutingConfiguration.getDefault());
	}

	@Test
	@Ignore
	public void runNL2() throws Exception {
		TestRouting.test(lib, getClass().getResourceAsStream("nl2.test.xml"), rs, RoutingConfiguration.getDefault());
	}

	@Test
	@Ignore
	public void runNLLeid() throws Exception {
		TestRouting.test(lib, getClass().getResourceAsStream("nl_leid.test.xml"), rs, RoutingConfiguration.getDefault());
	}

	@Test
	public void runBLR() throws Exception {
		TestRouting.test(lib, getClass().getResourceAsStream("blr.test.xml"), rs, RoutingConfiguration.getDefault());
	}

}
