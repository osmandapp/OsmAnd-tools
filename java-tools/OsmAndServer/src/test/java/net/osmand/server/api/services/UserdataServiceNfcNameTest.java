package net.osmand.server.api.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Date;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.PlatformTransactionManager;

import net.osmand.server.api.repo.CloudUserDevicesRepository.CloudUserDevice;
import net.osmand.server.api.repo.CloudUserFilesRepository;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFile;
import net.osmand.server.api.services.StorageService.InternalZipFile;

public class UserdataServiceNfcNameTest {

	private static final String NFC_NAME = "favorites-Köln-Test.gpx";
	private static final String NFD_NAME = "favorites-Köln-Test.gpx";
	private static final String TYPE = UserdataService.FILE_TYPE_FAVOURITES;

	private UserdataService service;
	private CloudUserFilesRepository filesRepository;
	private StorageService storageService;
	private ShareFileService shareFileService;

	@Before
	public void setUp() {
		service = new UserdataService();
		filesRepository = mock(CloudUserFilesRepository.class);
		storageService = mock(StorageService.class);
		shareFileService = mock(ShareFileService.class);
		ReflectionTestUtils.setField(service, "filesRepository", filesRepository);
		ReflectionTestUtils.setField(service, "storageService", storageService);
		ReflectionTestUtils.setField(service, "shareFileService", shareFileService);
		ReflectionTestUtils.setField(service, "transactionManager", mock(PlatformTransactionManager.class));
	}

	@Test
	public void uploadStoresNfcName() throws IOException {
		InternalZipFile zip = mock(InternalZipFile.class);
		when(storageService.save(anyString(), anyString(), any())).thenReturn("s3");

		service.uploadFile(zip, device(), NFD_NAME, TYPE, null);

		ArgumentCaptor<UserFile> saved = ArgumentCaptor.forClass(UserFile.class);
		verify(filesRepository).saveAndFlush(saved.capture());
		assertEquals(NFC_NAME, saved.getValue().name);
		ArgumentCaptor<String> storageKey = ArgumentCaptor.forClass(String.class);
		verify(storageService).save(anyString(), storageKey.capture(), eq(zip));
		assertEquals(service.storageFileName(saved.getValue()), storageKey.getValue());
		assertTrue(storageKey.getValue().contains(NFC_NAME));
	}

	@Test
	public void deleteStoresNfcName() {
		service.deleteFile(NFD_NAME, TYPE, null, null, device());

		ArgumentCaptor<UserFile> saved = ArgumentCaptor.forClass(UserFile.class);
		verify(filesRepository).saveAndFlush(saved.capture());
		assertEquals(NFC_NAME, saved.getValue().name);
		verify(shareFileService).deleteShareFile(NFC_NAME, 1);
	}

	@Test
	public void renameKeepsStorageKeyOfOldName() {
		UserFile file = version(591L);
		String oldKey = service.storageFileName(file);

		service.renameFileVersion(file, NFC_NAME);

		assertEquals(NFC_NAME, file.name);
		assertEquals(oldKey, file.storagename);
		assertEquals(oldKey, service.storageFileName(file));
		verify(filesRepository).save(file);
	}

	@Test
	public void renameOfDeletedVersionLeavesStorageKeyEmpty() {
		UserFile file = version(-1L);

		service.renameFileVersion(file, NFC_NAME);

		assertEquals(NFC_NAME, file.name);
		assertNull(file.storagename);
	}

	private static UserFile version(long filesize) {
		UserFile file = new UserFile();
		file.userid = 1;
		file.type = TYPE;
		file.name = NFD_NAME;
		file.updatetime = new Date(1_794_486_051_979L);
		file.filesize = filesize;
		return file;
	}

	private static CloudUserDevice device() {
		CloudUserDevice dev = new CloudUserDevice();
		dev.id = 7;
		dev.userid = 1;
		return dev;
	}
}
