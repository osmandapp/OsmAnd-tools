package net.osmand.server.tileManager;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TileMemoryCache<T extends TileCacheProvider> {
	private final Map<String, T> cacheMap = new ConcurrentHashMap<>();
	private final Map<String, Object> lockMap = new ConcurrentHashMap<>();
	private final AtomicInteger cacheTouch = new AtomicInteger(0);

	private static final int MAX_RUNTIME_IMAGE_CACHE_SIZE = 80;
	private static final int MAX_RUNTIME_TILES_CACHE_SIZE = 10000;

	public T getTile(String key, TileProvider<T> provider) {
		return cacheMap.computeIfAbsent(key, provider::createTile);
	}

	public void put(String key, T tile) {
		cacheMap.put(key, tile);
	}

	public T get(String key) {
		return cacheMap.get(key);
	}

	public Object getLock(String tileId) {
		return lockMap.computeIfAbsent(tileId, k -> new Object());
	}

	public void removeLock(String tileId) {
		lockMap.remove(tileId);
	}

	public void cleanupCache() {
		int version = cacheTouch.incrementAndGet();
		// so with atomic only 1 thread will get % X == 0
		if (version % MAX_RUNTIME_IMAGE_CACHE_SIZE == 0 && version > 0) {
			cacheTouch.set(0);
			SortedSet<T> sortedCache = Collections.synchronizedSortedSet(new TreeSet<>(cacheMap.values()));
			List<T> imageTiles = new ArrayList<>();
			for (T tile : sortedCache) {
				if (tile.getImg() != null) {
					imageTiles.add(tile);
				}
			}
			if (imageTiles.size() >= MAX_RUNTIME_IMAGE_CACHE_SIZE) {
				for (int i = 0; i < MAX_RUNTIME_IMAGE_CACHE_SIZE / 2; i++) {
					T tile = imageTiles.get(i);
					tile.setImg(null);
				}
			}
			if (cacheMap.size() >= MAX_RUNTIME_TILES_CACHE_SIZE) {
				Iterator<T> it = sortedCache.iterator();
				while (cacheMap.size() >= MAX_RUNTIME_TILES_CACHE_SIZE / 2 && it.hasNext()) {
					T tile = it.next();
					cacheMap.remove(tile.getTileId());
				}
			}
		}
	}

	public interface TileProvider<T> {
		T createTile(String key);
	}
}
