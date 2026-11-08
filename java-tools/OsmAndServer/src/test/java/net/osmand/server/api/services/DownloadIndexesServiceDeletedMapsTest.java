package net.osmand.server.api.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import net.osmand.server.api.services.DownloadIndexesService.DownloadType;
import net.osmand.server.api.services.DownloadIndexesService.ServerCommonFile;

public class DownloadIndexesServiceDeletedMapsTest {

	private DownloadIndexesService service;

	@Before
	public void setUp() throws IOException {
		DownloadIndexDocument doc = new DownloadIndexDocument();
		doc.setOutdatedMaps();
		service = spy(new DownloadIndexesService());
		doReturn(doc).when(service).getIndexesDocument(false, false);
	}

	@Test
	public void deletedMapIsRecognizedAsServerFile() throws IOException {
		assertDeletedMap("Hungary_europe.obf");
		assertDeletedMap("Hungary_europe.road.obf");
	}

	@Test
	public void unknownMapIsUserFile() throws IOException {
		assertNull(service.getServerGlobalFile("My_own_map.obf"));
	}

	private void assertDeletedMap(String deviceFileName) throws IOException {
		ServerCommonFile scf = service.getServerGlobalFile(deviceFileName);
		assertNotNull(deviceFileName + " must not be stored in the cloud as a user file", scf);
		assertEquals(DownloadType.DELETED_MAP, scf.di.getDownloadType());
		assertNull(scf.file);
		assertNull(scf.url);
	}
}
