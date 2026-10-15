package net.osmand.server.api.services;

import static net.osmand.router.RouteExporter.OSMAND_ROUTER_V2;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.Test;

import net.osmand.shared.gpx.GpxFile;

// temp files of a renamed or saved track carry its name: a long name must still fit the 255-byte file name limit
public class TmpFileNameTest {

	private static final int NAME_MAX_BYTES = 255;
	// buildFromFileAndDelete writes "<name>gpx.gz" next to "<name>.gpx"
	private static final int GZ_EXTRA_BYTES = 2;

	@Test
	public void longAsciiNameIsZipped() throws IOException {
		File tmp = new GpxService().createTmpFileByGpxFile(new GpxFile(OSMAND_ROUTER_V2), "a".repeat(240) + ".gpx");
		File gz = new File(tmp.getParent(), tmp.getName().replace(".gpx", "gpx.gz"));
		try (StorageService.InternalZipFile zip = StorageService.InternalZipFile.buildFromFileAndDelete(tmp)) {
			assertTrue(zip.getSize() > 0);
			assertTrue(gz.exists());
		}
		assertFalse(gz.exists());
	}

	@Test
	public void longCyrillicNameFitsInBytes() throws IOException {
		File tmp = new GpxService().createTmpFileByGpxFile(new GpxFile(OSMAND_ROUTER_V2), "я".repeat(130) + ".gpx");
		tmp.deleteOnExit();
		String name = tmp.getName();
		assertTrue(name.getBytes(StandardCharsets.UTF_8).length + GZ_EXTRA_BYTES <= NAME_MAX_BYTES);
		assertFalse(name.contains("�"));
	}
}
