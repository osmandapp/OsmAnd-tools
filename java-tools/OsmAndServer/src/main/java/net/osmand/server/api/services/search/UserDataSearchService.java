package net.osmand.server.api.services.search;

import static net.osmand.IndexConstants.GPX_FILE_EXT;
import static net.osmand.server.api.services.UserdataService.FILE_TYPE_FAVOURITES;
import static net.osmand.server.api.services.UserdataService.FILE_TYPE_GPX;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

	public record SearchResult(List<UserDataItem> tracks, List<UserDataItem> favorites) {
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
		Date tracksVersion;
		Date favoritesVersion;
		final Map<String, NamesIndex> favoritesByFile = new ConcurrentHashMap<>();
		final Map<String, NamesIndex> sharedFavoritesByFile = new ConcurrentHashMap<>();
	}

	private record Match(UserDataItem item, int matchedTokens) {
	}

	// Called from get-shared-with-me for favorites
	public void updateSharedFavorites(List<UserFileNoData> files, CloudUserDevice dev) {
		syncFavorites(getUserIndex(dev).sharedFavoritesByFile, files, dev, true);
	}

	public void removeSharedFavorites(String fileName, CloudUserDevice dev) {
		getUserIndex(dev).sharedFavoritesByFile.remove(fileName);
	}

	public SearchResult search(String query, CloudUserDevice dev) {
		UserIndex index = getUserIndex(dev);
		refreshIndex(index, dev);
		List<String> tokens = SearchAlgorithms.splitAndNormalize(SearchAlgorithms.alignChars(query), true);

		List<Match> tracks = new ArrayList<>();
		collectMatches(index.tracks, tokens, tracks);

		List<Match> favorites = new ArrayList<>();
		index.favoritesByFile.values().forEach(groupIndex -> collectMatches(groupIndex, tokens, favorites));
		index.sharedFavoritesByFile.values().forEach(groupIndex -> collectMatches(groupIndex, tokens, favorites));

		return new SearchResult(topResults(tracks), topResults(favorites));
	}

	private UserIndex getUserIndex(CloudUserDevice dev) {
		return indexByUser.asMap().computeIfAbsent(dev.userid, userid -> new UserIndex());
	}

	// Any file change (upload, rename, delete, sync) adds a row with a new updatetime, so max(updatetime) is the index version
	private void refreshIndex(UserIndex index, CloudUserDevice dev) {
		Date tracksVersion = filesRepository.maxUpdatetime(dev.userid, FILE_TYPE_GPX);
		if (tracksVersion != null && !tracksVersion.equals(index.tracksVersion)) {
			NamesIndex tracks = new NamesIndex();
			for (UserFileNoData file : listFiles(dev, FILE_TYPE_GPX)) {
				if (file.name.toLowerCase().endsWith(GPX_FILE_EXT)) {
					tracks.add(new UserDataItem(file.name, false, trackDisplayName(file.name)));
				}
			}
			index.tracks = tracks;
			index.tracksVersion = tracksVersion;
		}
		Date favoritesVersion = filesRepository.maxUpdatetime(dev.userid, FILE_TYPE_FAVOURITES);
		if (favoritesVersion != null && !favoritesVersion.equals(index.favoritesVersion)) {
			syncFavorites(index.favoritesByFile, listFiles(dev, FILE_TYPE_FAVOURITES), dev, false);
			index.favoritesVersion = favoritesVersion;
		}
	}

	private List<UserFileNoData> listFiles(CloudUserDevice dev, String type) {
		return userdataService.generateFiles(dev.userid, null, false, false, Set.of(type)).uniqueFiles;
	}

	private String trackDisplayName(String fileName) {
		String name = fileName.substring(fileName.lastIndexOf('/') + 1);
		return name.substring(0, name.length() - GPX_FILE_EXT.length());
	}

	// Keeps only listed files, re-reads a file when its updatetime changed
	private void syncFavorites(Map<String, NamesIndex> byFile, List<UserFileNoData> files, CloudUserDevice dev, boolean shared) {
		byFile.keySet().retainAll(files.stream().map(f -> f.name).toList());
		for (UserFileNoData file : files) {
			NamesIndex current = byFile.get(file.name);
			if (current == null || current.updatetimems != file.updatetimems) {
				byFile.put(file.name, buildFavoritesIndex(file, shared, loadFavorites(file.name, shared, dev)));
			}
		}
	}

	private GpxFile loadFavorites(String fileName, boolean shared, CloudUserDevice dev) {
		try {
			UserFile file = shared ? shareFileService.getSharedWithMeFile(fileName, FILE_TYPE_FAVOURITES, dev)
					: userdataService.getLastFileVersion(dev.userid, fileName, FILE_TYPE_FAVOURITES);
			return shareFileService.getFile(file);
		} catch (Exception e) {
			LOG.warn(String.format("Favorites search index failed userid=%d %s: %s", dev.userid, fileName, e.getMessage()));
			return null;
		}
	}

	private NamesIndex buildFavoritesIndex(UserFileNoData file, boolean shared, GpxFile gpxFile) {
		NamesIndex index = new NamesIndex();
		index.updatetimems = file.updatetimems;
		if (gpxFile != null) {
			for (WptPt point : gpxFile.getPointsList()) {
				if (point.getName() != null && !point.getName().isEmpty()) {
					index.add(new UserDataItem(file.name, shared, point.getName()));
				}
			}
		}
		return index;
	}

	// Count query tokens matched by prefix in item name tokens; ties keep index order (stable sort)
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

	private List<UserDataItem> topResults(List<Match> matches) {
		return matches.stream()
				.sorted(Comparator.comparingInt(Match::matchedTokens).reversed())
				.limit(RESULTS_LIMIT).map(Match::item).toList();
	}
}
