package net.osmand.server.api.services;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.junit.Before;
import org.junit.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.multipart.MultipartFile;

import com.google.gson.JsonObject;

import net.osmand.server.api.repo.CloudUserDevicesRepository.CloudUserDevice;
import net.osmand.server.api.repo.CloudUserFilesRepository;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFile;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFileNoData;
import net.osmand.server.controllers.pub.UserdataController;
import net.osmand.server.controllers.pub.UserdataController.UserFilesResults;

public class UserdataServiceDeletedMapsTest {

	private UserdataService service;
	private DownloadIndexesService downloads;

	@Before
	public void setUp() throws Exception {
		DownloadIndexDocument indexes = new DownloadIndexDocument();
		indexes.setOutdatedMaps();
		downloads = mock(DownloadIndexesService.class);
		when(downloads.getIndexesDocument(false, false)).thenReturn(indexes);
		service = new UserdataService();
		service.downloadService = downloads;
		service.filesRepository = mock(CloudUserFilesRepository.class);
		service.storageService = mock(StorageService.class);
	}

	@Test
	public void deletedMapsAreReportedAsPresentWithoutCheckingDownloadServers() throws Exception {
		UserdataController controller = new UserdataController();
		ReflectionTestUtils.setField(controller, "userdataService", service);
		for (String name : List.of("Hungary_europe.obf", "Hungary_europe.road.obf", "maps/Hungary_europe.road.obf")) {
			String response = controller.checkFileOnServer(name, "file").getBody();
			assertEquals("present", service.gson.fromJson(response, JsonObject.class).get("status").getAsString());
		}
		verify(downloads, never()).getServerGlobalFile(anyString());
	}

	@Test
	public void unrelatedFilesAreNotDeletedMaps() throws Exception {
		assertFalse(service.isDeletedMapFile("My_own_map.obf", "FILE"));
		assertFalse(service.isDeletedMapFile("Hungary_europe.obf", "GPX"));
		assertFalse(service.isDeletedMapFile("Hungary_europe.obf.bak", "FILE"));
		assertFalse(service.isDeletedMapFile("obf", "FILE"));
	}

	@Test
	public void repeatedUploadsOfDeletedMapsAndTheirInfoDoNotReadOrStoreContent() throws Exception {
		MultipartFile body = mock(MultipartFile.class);
		for (String name : List.of("Hungary_europe.obf", "Hungary_europe.road.obf", "Hungary_europe.road.obf.info")) {
			for (int attempt = 0; attempt < 2; attempt++) {
				var response = service.uploadMultipartFile(body, new CloudUserDevice(), name, "FILE", 123L);
				assertEquals(200, response.getStatusCode().value());
				JsonObject result = service.gson.fromJson(response.getBody(), JsonObject.class);
				assertEquals("ok", result.get("status").getAsString());
				assertTrue(result.get("updatetime").getAsLong() > 0);
				assertEquals(0L, result.get("filesize").getAsLong());
			}
		}
		verifyNoInteractions(body, service.filesRepository, service.storageService);
	}

	@Test
	public void cloudListsHideAllDeletedMapVersionsAndInfoWithoutChangingQuota() throws Exception {
		UserFileNoData custom = file("My_own_map.obf", "FILE", 100);
		UserFileNoData unrelated = file("Hungary_europe.obf", "GPX", 100);
		UserFilesResults files = new UserFilesResults();
		files.uniqueFiles = new ArrayList<>(List.of(custom, unrelated,
				file("Hungary_europe.obf", "FILE", 100),
				file("maps/Hungary_europe.road.obf", "file", 0),
				file("Hungary_europe.obf.info", "FILE", 100)));
		files.allFiles = new ArrayList<>(files.uniqueFiles);
		files.allFiles.add(file("Hungary_europe.road.obf", "FILE", -1));
		files.totalZipSize = 1234;

		service.hideDeletedMaps(files);

		assertEquals(List.of(custom, unrelated), files.uniqueFiles);
		assertEquals(List.of(custom, unrelated), files.allFiles);
		assertEquals(1234L, files.totalZipSize);
		verify(downloads).getIndexesDocument(false, false);
		files.allFiles = null;
		service.hideDeletedMaps(files);
		assertEquals(List.of(custom, unrelated), files.uniqueFiles);
	}

	@Test
	public void existingFullCopyCanStillBeDownloadedDirectly() throws Exception {
		byte[] content = new byte[] {1, 2, 3};
		ByteArrayOutputStream stored = new ByteArrayOutputStream();
		try (GZIPOutputStream gzip = new GZIPOutputStream(stored)) {
			gzip.write(content);
		}
		UserFile file = new UserFile();
		file.name = "Hungary_europe.road.obf";
		file.data = stored.toByteArray();
		MockHttpServletRequest request = new MockHttpServletRequest();
		request.addHeader("Accept-Encoding", "gzip");
		MockHttpServletResponse response = new MockHttpServletResponse();

		service.getFile(file, response, request, file.name, "FILE", new CloudUserDevice(), false);

		try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(response.getContentAsByteArray()))) {
			assertArrayEquals(content, gzip.readAllBytes());
		}
	}

	private UserFileNoData file(String name, String type, long size) {
		return new UserFileNoData(1, 1, 1, type, name, new Date(), null, size, size, "local", null);
	}
}
