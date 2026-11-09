package net.osmand.server.api.services;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import javax.xml.bind.JAXBContext;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.util.ReflectionTestUtils;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import net.osmand.server.api.repo.CloudUserDevicesRepository;
import net.osmand.server.api.repo.CloudUserDevicesRepository.CloudUserDevice;
import net.osmand.server.api.repo.CloudUserFilesRepository;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFile;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFileNoData;
import net.osmand.server.api.repo.CloudUsersRepository;
import net.osmand.server.api.repo.CloudUsersRepository.CloudUser;
import net.osmand.server.api.services.DownloadIndexesService.DownloadType;
import net.osmand.server.controllers.pub.UserdataController;
import net.osmand.server.utils.exception.OsmAndPublicApiException;

public class UserdataServiceDeletedMapsTest {

	private static final String MAP = "Germany_nordrhein-westfalen_europe.obf";
	private static final String ROAD_MAP = "roads/Canada_nunavut_northamerica.road.obf";
	private static final String ACTIVE_MAP = "Germany_berlin_europe.obf";
	private static final String TYPE = "FILE";
	private static final long MODIFIED_TIME = 1000;

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	private final List<UserFile> database = new ArrayList<>();
	private final UserdataService service = new UserdataService();
	private final UserdataController controller = new UserdataController();
	private final CloudUserDevice device = new CloudUserDevice();
	private DownloadIndexesService downloads;

	@Before
	public void setUp() throws IOException {
		DownloadIndexDocument indexes = new DownloadIndexDocument();
		indexes.setOutdatedMaps();
		DownloadIndex active = new DownloadIndex();
		active.setName("Germany_berlin_europe_2.obf.zip");
		active.setType(DownloadType.MAP);
		indexes.getMaps().add(active);
		folder.newFile(active.getName());
		downloads = spy(new DownloadIndexesService());
		doReturn(indexes).when(downloads).getIndexesDocument(false, false);
		ReflectionTestUtils.setField(downloads, "pathToDownloadFiles", folder.getRoot().getAbsolutePath());
		service.downloadService = downloads;
		service.storageService = mock(StorageService.class);
		service.filesRepository = mock(CloudUserFilesRepository.class);
		service.deletedMapsService = new DeletedMapsService(downloads, service.filesRepository);
		service.usersRepository = mock(CloudUsersRepository.class);

		device.id = 10;
		device.userid = 7;
		device.accesstoken = "token";
		CloudUser user = new CloudUser();
		user.id = device.userid;
		when(service.usersRepository.findById(device.userid)).thenReturn(user);
		when(service.filesRepository.listFilesByUserid(device.userid, null, null)).thenAnswer(invocation -> {
			List<UserFileNoData> files = new ArrayList<>();
			for (UserFile file : database) {
				files.add(new UserFileNoData(file));
			}
			return files;
		});
		when(service.filesRepository.saveAndFlush(any(UserFile.class))).thenAnswer(invocation -> {
			UserFile file = invocation.getArgument(0);
			file.id = database.size() + 1;
			database.add(file);
			return file;
		});
		when(service.filesRepository.findTopByUseridAndNameAndTypeOrderByUpdatetimeDesc(
				anyInt(), anyString(), anyString())).thenAnswer(invocation -> {
			UserFile latest = null;
			for (UserFile file : database) {
				if (file.name.equals(invocation.getArgument(1)) && file.type.equals(invocation.getArgument(2))
						&& (latest == null || file.updatetime.after(latest.updatetime))) {
					latest = file;
				}
			}
			return latest;
		});

		CloudUserDevicesRepository devices = mock(CloudUserDevicesRepository.class);
		when(devices.findById(device.id)).thenReturn(device);
		ReflectionTestUtils.setField(controller, "devicesRepository", devices);
		ReflectionTestUtils.setField(controller, "userdataService", service);
		ReflectionTestUtils.setField(controller, "deletedMapsService", service.deletedMapsService);
	}

	@Test
	public void deletedMapsSurviveDownloadCatalogXmlRoundTrip() throws Exception {
		JAXBContext context = JAXBContext.newInstance(DownloadIndexDocument.class);
		StringWriter xml = new StringWriter();
		context.createMarshaller().marshal(downloads.getIndexesDocument(false, false), xml);
		DownloadIndexDocument restored = (DownloadIndexDocument) context.createUnmarshaller()
				.unmarshal(new StringReader(xml.toString()));
		doReturn(restored).when(downloads).getIndexesDocument(false, false);
		assertFalse(restored.getDeletedMaps().isEmpty());
		assertTrue(service.deletedMapsService.isDeletedMapFile(MAP, TYPE));
		assertTrue(service.deletedMapsService.isDeletedMapFile(ROAD_MAP, TYPE));
		assertFalse(service.deletedMapsService.isDeletedMapFile(ACTIVE_MAP, TYPE));
	}

	@Test
	public void firstEmptyUploadStopsUploadsOnSubsequentSyncs() throws Exception {
		AppBackup app = new AppBackup(MAP, true, 0);
		assertEquals(EnumSet.of(Change.UPLOAD), app.plan());

		app.sync();
		assertEquals(1, app.mapUploads);
		assertEquals(0, app.uploadedMapBytes);
		assertTrue(app.uploadTime >= MODIFIED_TIME);
		assertEquals(1, database.size()); // The .info upload adds no Cloud entry.
		assertEquals(Long.valueOf(-1), database.get(0).filesize);
		assertEquals(Long.valueOf(0), database.get(0).zipfilesize);
		assertNull(database.get(0).data);
		assertNull(database.get(0).storage);

		for (int sync = 0; sync < 3; sync++) {
			assertTrue("The unchanged map must leave the sync plan", app.plan().isEmpty());
			app.sync();
		}
		assertEquals(1, app.mapUploads);
		assertTrue(new AppBackup(MAP, false, 0).plan().isEmpty());
		assertEquals(0, listFiles(true).get("totalZipSize").getAsLong());
		verify(service.filesRepository, times(1)).saveAndFlush(any(UserFile.class));
		verifyNoInteractions(service.storageService);
	}

	@Test
	public void oldCloudCopiesBecomeOneMarkerWithoutDownloadOrLocalDeletion() throws Exception {
		for (String name : new String[] {MAP, ROAD_MAP}) {
			addCloudFile(name, TYPE, 2000);
			addCloudFile(name, TYPE, 3000);
			addCloudFile(name + UserdataService.INFO_EXT, TYPE, 3000);
		}
		JsonObject result = listFiles(true);
		assertEquals(0, result.getAsJsonArray("uniqueFiles").size());
		assertEquals(2, result.getAsJsonArray("allFiles").size());
		for (JsonElement element : result.getAsJsonArray("allFiles")) {
			JsonObject marker = element.getAsJsonObject();
			assertEquals(TYPE, marker.get("type").getAsString());
			assertEquals(-1, marker.get("filesize").getAsLong());
			assertEquals(0, marker.get("zipSize").getAsLong());
			assertEquals(0, marker.get("updatetimems").getAsLong());
			assertFalse(marker.get("name").getAsString().endsWith(UserdataService.INFO_EXT));
		}
		for (String name : new String[] {MAP, ROAD_MAP}) {
			assertTrue(new AppBackup(name, false, 0).plan().isEmpty());
			assertTrue(new AppBackup(name, true, 1500).plan().isEmpty());
		}
		assertEquals(0, listFiles(false).getAsJsonArray("uniqueFiles").size());
		assertEquals(Long.valueOf(10000), database.get(0).filesize); // Cleanup remains separate.
	}

	@Test
	public void deviceWithNoSyncHistoryStopsUploadsEvenWhenVersionHistoryIsDisabled() throws Exception {
		addCloudFile(ROAD_MAP, TYPE, 2000);
		AppBackup app = new AppBackup(ROAD_MAP, true, 0);
		app.keepVersionHistory = false;
		app.sync();
		app.sync();
		assertEquals(1, app.mapUploads);
		assertEquals(0, app.uploadedMapBytes);
		assertTrue(app.plan().isEmpty());
		assertTrue(new AppBackup(ROAD_MAP, false, 0).plan().isEmpty());
		assertEquals(2, database.size()); // Old full copy and the marker survive version cleanup.
		verify(service.filesRepository, never()).delete(any(UserFile.class));
		verifyNoInteractions(service.storageService);
	}

	@Test
	public void acknowledgementAlsoStopsRepeatsWhenDeviceClockIsAhead() throws Exception {
		AppBackup app = new AppBackup(MAP, true, 0);
		app.modifiedTime = System.currentTimeMillis() + 60000;
		app.sync();
		assertTrue(app.uploadTime >= app.modifiedTime);
		app.sync();
		assertEquals(1, app.mapUploads);
	}

	@Test
	public void activeAndCustomMapsKeepTheirNormalCloudEntries() throws Exception {
		String custom = "My_map.obf";
		addCloudFile(ACTIVE_MAP, TYPE, 2000);
		addCloudFile(custom, TYPE, 2000);
		addCloudFile(MAP, "GPX", 2000);
		assertEquals(3, listFiles(true).getAsJsonArray("uniqueFiles").size());
		assertEquals(EnumSet.of(Change.DOWNLOAD), new AppBackup(custom, false, 0).plan());
		assertEquals("present", json(controller.checkFileOnServer(ACTIVE_MAP, TYPE)).get("status").getAsString());
		assertEquals("not-present", json(controller.checkFileOnServer(custom, TYPE)).get("status").getAsString());
		assertEquals(folder.getRoot().toPath().resolve("Germany_berlin_europe_2.obf.zip").toFile(),
				downloads.getServerGlobalFile(ACTIVE_MAP).file);
		assertNull(downloads.getServerGlobalFile(MAP));
	}

	@Test
	public void directDownloadOfObsoleteMapFailsBeforeReadingCloudContent() throws Exception {
		UserFile file = addCloudFile(MAP, TYPE, 2000);
		try {
			service.getFile(file, new MockHttpServletResponse(), new MockHttpServletRequest(), MAP, TYPE, device, false);
			fail("Obsolete map content must not be downloaded");
		} catch (OsmAndPublicApiException error) {
			assertEquals(UserdataService.ERROR_CODE_FILE_NOT_AVAILABLE, error.getErrorCode());
		}
		verifyNoInteractions(service.storageService);
	}

	private UserFile addCloudFile(String name, String type, long updateTime) {
		UserFile file = new UserFile();
		file.userid = device.userid;
		file.name = name;
		file.type = type;
		file.updatetime = new Date(updateTime);
		file.clienttime = new Date(MODIFIED_TIME);
		file.filesize = 10000L;
		file.zipfilesize = 1000L;
		database.add(file);
		return file;
	}

	private JsonObject listFiles(boolean allVersions) throws Exception {
		return json(controller.listFiles(device.id, device.accesstoken, null, null, allVersions));
	}

	private static JsonObject json(ResponseEntity<String> response) {
		assertEquals(200, response.getStatusCode().value());
		return new JsonParser().parse(response.getBody()).getAsJsonObject();
	}

	private static MockMultipartFile gzip(String name, String content) throws IOException {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		try (GZIPOutputStream gzip = new GZIPOutputStream(bytes)) {
			gzip.write(content.getBytes(StandardCharsets.UTF_8));
		}
		return new MockMultipartFile("file", name, "application/gzip", bytes.toByteArray());
	}

	private enum Change {UPLOAD, DOWNLOAD, DELETE_LOCAL, MERGE}

	private class AppBackup {
		final String name;
		final boolean hasLocalMap;
		long modifiedTime = MODIFIED_TIME;
		long uploadTime;
		boolean keepVersionHistory = true;
		int mapUploads;
		int uploadedMapBytes;

		AppBackup(String name, boolean hasLocalMap, long uploadTime) {
			this.name = name;
			this.hasLocalMap = hasLocalMap;
			this.uploadTime = uploadTime;
		}

		JsonObject remoteFile() throws Exception {
			JsonArray files = listFiles(true).getAsJsonArray("allFiles");
			for (JsonElement element : files) {
				JsonObject file = element.getAsJsonObject();
				if (TYPE.equals(file.get("type").getAsString()) && name.equals(file.get("name").getAsString())) {
					return file;
				}
			}
			return null;
		}

		EnumSet<Change> plan() throws Exception {
			JsonObject remote = remoteFile();
			// Single-map flow from Android GenerateBackupInfoTask and iOS OAGenerateBackupInfoTask.
			EnumSet<Change> changes = EnumSet.noneOf(Change.class);
			if (remote != null) {
				boolean deleted = remote.get("filesize").getAsLong() < 0;
				if (hasLocalMap) {
					boolean changedLocally = modifiedTime > uploadTime;
					boolean changedRemotely = remote.get("updatetimems").getAsLong() > uploadTime;
					if (changedLocally && changedRemotely) {
						changes.add(Change.MERGE);
					} else if (changedLocally) {
						changes.add(Change.UPLOAD);
					} else if (changedRemotely) {
						changes.add(deleted ? Change.DELETE_LOCAL : Change.DOWNLOAD);
					}
				} else if (!deleted) {
					changes.add(Change.DOWNLOAD);
				}
			} else if (hasLocalMap) {
				// A missing remote entry causes upload even after a successful acknowledgement.
				changes.add(Change.UPLOAD);
			}
			return changes;
		}

		void sync() throws Exception {
			EnumSet<Change> changes = plan();
			assertFalse(changes.contains(Change.DOWNLOAD));
			assertFalse(changes.contains(Change.DELETE_LOCAL));
			assertFalse(changes.contains(Change.MERGE));
			if (changes.contains(Change.UPLOAD)) {
				JsonObject previous = remoteFile();
				String status = json(controller.checkFileOnServer(name, TYPE)).get("status").getAsString();
				String content = "present".equals(status) ? "" : "full map content";
				JsonObject response = json(controller.upload(gzip(name, content), name, TYPE,
						device.id, device.accesstoken, modifiedTime));
				assertEquals("ok", response.get("status").getAsString());
				uploadTime = response.get("updatetime").getAsLong();
				mapUploads++;
				uploadedMapBytes += content.length();
				String info = name + UserdataService.INFO_EXT;
				assertEquals("ok", json(controller.upload(gzip(info, "{}"), info, TYPE,
						device.id, device.accesstoken, modifiedTime)).get("status").getAsString());
				if (!keepVersionHistory && previous != null) {
					assertEquals("ok", json(controller.deleteFile(new MockHttpServletResponse(),
							new MockHttpServletRequest(), name, TYPE, previous.get("updatetimems").getAsLong(),
							device.id, device.accesstoken)).get("status").getAsString());
				}
			}
		}
	}
}
