package net.osmand.server.api.services;

import net.osmand.server.api.repo.ShareFileRepository;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EmailSenderServiceShareSettingsPathTest {

	@Test
	public void trackOpensTracksShareMenu() {
		assertEquals("mydata/tracks/info/c2hhcmUtNjQ4LXJlcXVlc3QuZ3B4/share/",
				EmailSenderService.getShareSettingsPath(file("share-648-request.gpx", "GPX")));
	}

	@Test
	public void favoritesOpenFavoritesShareMenuWithEscapedId() {
		assertEquals("mydata/favorites/info/ZmF2b3JpdGVzLdCH0LbQsCA%2FPi5ncHg%3D/share/",
				EmailSenderService.getShareSettingsPath(file("favorites-Їжа ?>.gpx", UserdataService.FILE_TYPE_FAVOURITES)));
	}

	private static ShareFileRepository.ShareFile file(String filepath, String type) {
		ShareFileRepository.ShareFile file = new ShareFileRepository.ShareFile();
		file.filepath = filepath;
		file.type = type;
		return file;
	}
}
