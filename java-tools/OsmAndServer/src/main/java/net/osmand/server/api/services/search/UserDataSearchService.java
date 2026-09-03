package net.osmand.server.api.services.search;

import static net.osmand.IndexConstants.GPX_FILE_EXT;
import static net.osmand.server.api.services.UserdataService.FILE_TYPE_FAVOURITES;
import static net.osmand.server.api.services.UserdataService.FILE_TYPE_GPX;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import net.osmand.CollatorStringMatcher;
import net.osmand.search.core.spatial.StringPrefixTree;
import net.osmand.server.api.repo.CloudUserDevicesRepository.CloudUserDevice;
import net.osmand.server.api.repo.CloudUserFilesRepository;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFile;
import net.osmand.server.api.repo.CloudUserFilesRepository.UserFileNoData;
import net.osmand.server.api.services.ShareFileService;
import net.osmand.server.api.services.UserdataService;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.primitives.WptPt;
import net.osmand.util.SearchAlgorithms;

@Service
public class UserDataSearchService {

	private static final Log LOG = LogFactory.getLog(UserDataSearchService.class);
	private static final int RESULTS_LIMIT = 8;

	@Autowired
	UserdataService userdataService;

	@Autowired
	ShareFileService shareFileService;

	@Autowired
	CloudUserFilesRepository filesRepository;

	// In-memory index per user (single server instance), dropped after a day without access
	private final Cache<Integer, UserIndex> indexByUser = CacheBuilder.newBuilder()
			.expireAfterAccess(24, TimeUnit.HOURS).build();

	public record UserDataItem(String file, boolean shared, String name) {
	}

	public record OpenedTrack(String file, boolean shared) {
	}

	public record SearchResult(List<UserDataItem> tracks, List<UserDataItem> favorites, List<UserDataItem> wpts) {
	}

	private record FileVersion(String name, long updatetimems, boolean shared) {
		String key() {
			return sharedFileKey(name, shared);
		}
	}

	private static class NamesIndex {
		final List<UserDataItem> items = new ArrayList<>();
		final StringPrefixTree<Integer> tree = new StringPrefixTree<>();
		long updatetimems;

		void add(UserDataItem item) {
			tree.put(SearchAlgorithms.alignChars(item.name()), items.size());
			items.add(item);
		}
	}

	private static class UserIndex {
		NamesIndex tracks = new NamesIndex();
		Map<String, Long> trackVersions = Map.of();
		Date tracksVersion;
		Date favoritesVersion;
		final Map<String, NamesIndex> favoritesByFile = new ConcurrentHashMap<>();
		final Map<String, NamesIndex> sharedFavoritesByFile = new ConcurrentHashMap<>();
		final Map<String, NamesIndex> wptsByTrack = new ConcurrentHashMap<>();
	}

	private record Match(UserDataItem item, int matchedTokens) {
	}

	// Called from get-shared-with-me for favorites
	public void updateSharedFavorites(List<UserFileNoData> files, CloudUserDevice dev) {
		syncFiles(getUserIndex(dev).sharedFavoritesByFile, fileVersions(files, true), dev, FILE_TYPE_FAVOURITES);
	}

	public void removeSharedFavorites(String fileName, CloudUserDevice dev) {
		getUserIndex(dev).sharedFavoritesByFile.remove(sharedFileKey(fileName, true));
	}

	private static String sharedFileKey(String fileName, boolean shared) {
		return shared ? "shared:" + fileName : fileName;
	}

	public SearchResult search(String query, List<OpenedTrack> openedTracks, CloudUserDevice dev) {
		UserIndex index = getUserIndex(dev);
		refreshIndex(index, dev);
		syncOpenedTracks(index, openedTracks == null ? List.of() : openedTracks, dev);
		List<String> tokens = SearchAlgorithms.splitAndNormalize(SearchAlgorithms.alignChars(query), true);
		return new SearchResult(
				searchIndexes(tokens, List.of(index.tracks)),
				searchIndexes(tokens, index.favoritesByFile.values(), index.sharedFavoritesByFile.values()),
				searchIndexes(tokens, index.wptsByTrack.values()));
	}

	@SafeVarargs
	private List<UserDataItem> searchIndexes(List<String> tokens, Collection<NamesIndex>... indexGroups) {
		List<Match> matches = new ArrayList<>();
		for (Collection<NamesIndex> indexes : indexGroups) {
			indexes.forEach(index -> collectMatches(index, tokens, matches));
		}
		return matches.stream()
				.sorted(Comparator.comparingInt(Match::matchedTokens).reversed()
						.thenComparing(match -> match.item().name()))
				.limit(RESULTS_LIMIT).map(Match::item).toList();
	}

	private UserIndex getUserIndex(CloudUserDevice dev) {
		return indexByUser.asMap().computeIfAbsent(dev.userid, userid -> new UserIndex());
	}

	// Any file change (upload, rename, delete, sync) adds a row with a new updatetime, so max(updatetime) is the index version
	private void refreshIndex(UserIndex index, CloudUserDevice dev) {
		Date tracksVersion = filesRepository.maxUpdatetime(dev.userid, FILE_TYPE_GPX);
		if (!Objects.equals(tracksVersion, index.tracksVersion)) {
			NamesIndex tracks = new NamesIndex();
			Map<String, Long> trackVersions = new HashMap<>();
			for (UserFileNoData file : listFiles(dev, FILE_TYPE_GPX)) {
				if (file.name.toLowerCase().endsWith(GPX_FILE_EXT)) {
					tracks.add(new UserDataItem(file.name, false, trackDisplayName(file.name)));
					trackVersions.put(file.name, file.updatetimems);
				}
			}
			index.tracks = tracks;
			index.trackVersions = trackVersions;
			index.tracksVersion = tracksVersion;
		}
		Date favoritesVersion = filesRepository.maxUpdatetime(dev.userid, FILE_TYPE_FAVOURITES);
		if (!Objects.equals(favoritesVersion, index.favoritesVersion)
				&& syncFiles(index.favoritesByFile, fileVersions(listFiles(dev, FILE_TYPE_FAVOURITES), false), dev, FILE_TYPE_FAVOURITES)) {
			index.favoritesVersion = favoritesVersion;
		}
	}

	private void syncOpenedTracks(UserIndex index, List<OpenedTrack> openedTracks, CloudUserDevice dev) {
		List<FileVersion> files = new ArrayList<>();
		for (OpenedTrack track : openedTracks) {
			if (track.shared()) {
				UserFile file = shareFileService.getSharedWithMeFile(track.file(), FILE_TYPE_GPX, dev);
				if (file != null) {
					files.add(new FileVersion(track.file(), file.updatetime.getTime(), true));
				}
			} else if (index.trackVersions.containsKey(track.file())) {
				files.add(new FileVersion(track.file(), index.trackVersions.get(track.file()), false));
			}
		}
		syncFiles(index.wptsByTrack, files, dev, FILE_TYPE_GPX);
	}

	private List<FileVersion> fileVersions(List<UserFileNoData> files, boolean shared) {
		return files.stream().map(f -> new FileVersion(f.name, f.updatetimems, shared)).toList();
	}

	private List<UserFileNoData> listFiles(CloudUserDevice dev, String type) {
		return userdataService.generateFiles(dev.userid, null, false, false, Set.of(type)).uniqueFiles;
	}

	private String trackDisplayName(String fileName) {
		String name = fileName.substring(fileName.lastIndexOf('/') + 1);
		return name.substring(0, name.length() - GPX_FILE_EXT.length());
	}

	// Keeps only listed files, re-reads a file when it is new or its updatetime changed.
	private boolean syncFiles(Map<String, NamesIndex> byFile, List<FileVersion> files, CloudUserDevice dev, String type) {
		byFile.keySet().retainAll(files.stream().map(FileVersion::key).toList());
		boolean allLoaded = true;
		for (FileVersion file : files) {
			NamesIndex current = byFile.get(file.key());
			if (current == null || current.updatetimems != file.updatetimems()) {
				GpxFile gpxFile = loadGpx(file.name(), file.shared(), type, dev);
				if (gpxFile == null) {
					allLoaded = false;
				} else {
					byFile.put(file.key(), buildPointsIndex(file, gpxFile));
				}
			}
		}

		return allLoaded;
	}

	private GpxFile loadGpx(String fileName, boolean shared, String type, CloudUserDevice dev) {
		try {
			UserFile file = shared ? shareFileService.getSharedWithMeFile(fileName, type, dev)
					: userdataService.getUserFile(fileName, type, null, dev);
			return shareFileService.getFile(file);
		} catch (Exception e) {
			LOG.warn(String.format("User data search index failed userid=%d %s: %s", dev.userid, fileName, e.getMessage()));
			return null;
		}
	}

	private NamesIndex buildPointsIndex(FileVersion file, GpxFile gpxFile) {
		NamesIndex index = new NamesIndex();
		index.updatetimems = file.updatetimems();
		for (WptPt point : gpxFile.getPointsList()) {
			if (point.getName() != null && !point.getName().isEmpty()) {
				index.add(new UserDataItem(file.name(), file.shared(), point.getName()));
			}
		}
		return index;
	}

	// Count query tokens matched by prefix in item name tokens
	private void collectMatches(NamesIndex index, List<String> tokens, List<Match> matches) {
		int[] matchedTokens = new int[index.items.size()];
		for (String token : tokens) {
			for (int position : new HashSet<>(index.tree.simpleGet(token + CollatorStringMatcher.INCOMPLETE_DOT))) {
				matchedTokens[position]++;
			}
		}
		for (int position = 0; position < matchedTokens.length; position++) {
			if (matchedTokens[position] > 0) {
				matches.add(new Match(index.items.get(position), matchedTokens[position]));
			}
		}
	}

}
