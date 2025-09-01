package net.osmand.server.api.services;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLEncoder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import jakarta.servlet.AsyncContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpHeaders;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.gson.Gson;
import com.vividsolutions.jts.geom.Geometry;

import net.osmand.Location;
import net.osmand.binary.BinaryVectorTileReader;
import net.osmand.data.GeometryTile;
import net.osmand.data.LatLon;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

@Service
public class PlacesService {

	private static final int TIMEOUT = 20000;
	private static final int THREADS_FOR_PROC = 30;

	private static final Log LOGGER = LogFactory.getLog(PlacesService.class);

	private static final String WIKIMEDIA = "wikimedia.org";
	private static final String WIKIPEDIA = "wikipedia.org";
	private static final String WIKI_FILE_PREFIX = "File:";
	private static final String TEMP_MAPILLARY_FOLDER = "mapillary_cache";
	
	private static final String FILE_MAPILLARY_PREFIX = "mapillary_";
	private static final long MAPILLARY_CACHE_TIMEOUT = TimeUnit.HOURS.toMillis(6);
	private static final long MAPILLARY_GC_TIMEOUT = TimeUnit.MINUTES.toMillis(15);
	private static final double MAPILLARY_RADIUS = 40.0;
	private static final int MAPILLARY_IMAGES_LIMIT = 20;
	
	private final RestTemplate restTemplate;
	
	private long lastMapillaryGCTimestamp = 0; 
	

	@Value("${mapillary.accesstoken}")
	private String mapillaryAccessToken;

	private ThreadPoolTaskExecutor executor;

	@Autowired
	public PlacesService(RestTemplateBuilder builder) {
		this.restTemplate = builder.requestFactory(HttpComponentsClientHttpRequestFactory.class)
				.defaultHeader(HttpHeaders.USER_AGENT, WikiService.USER_AGENT)
				.setConnectTimeout(Duration.ofMillis(TIMEOUT))
				.setReadTimeout(Duration.ofMillis(TIMEOUT))
				.build();

		this.executor = new ThreadPoolTaskExecutor();
		executor.setThreadNamePrefix("ImageService");
		executor.setCorePoolSize(THREADS_FOR_PROC);
		executor.setKeepAliveSeconds(60);
		executor.setAllowCoreThreadTimeOut(true);
		executor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
		executor.initialize();
	}

	public void processPlacesAround(HttpHeaders headers, HttpServletRequest request, HttpServletResponse response,
			Gson gson, double lat, double lon) {
		AsyncContext asyncCtx = request.startAsync(request, response);
		long start = System.currentTimeMillis();
		executor.submit(() -> {
			try {
				if (System.currentTimeMillis() - start > TIMEOUT * 3) {
					// waited too long (speedup queue processing)
					response.getWriter().println("{'features':[]}");
					asyncCtx.complete();
					return;
				}
				// request.getParameter("mloc")
				// request.getParameter("app")
				// request.getParameter("lang")
				String osmImage = request.getParameter("osm_image");
				String osmMapillaryKey = request.getParameter("osm_mapillary_key");

				InetSocketAddress inetAddress = headers.getHost();
				String host = inetAddress.getHostName();
				String proto = request.getScheme();
				String forwardedHost = headers.getFirst("X-Forwarded-Host");
				String forwardedProto = headers.getFirst("X-Forwarded-Proto");
				if (forwardedHost != null) {
					host = forwardedHost;
				}
				if (forwardedProto != null) {
					proto = forwardedProto;
				}
				if (host == null) {
					LOGGER.error("Bad request. Host is null");
					response.getWriter().println("{'features':[]}");
					asyncCtx.complete();
					return;
				}

				// TODO Replacew ith database
				CameraPlace wikimediaPrimaryCameraPlace = processWikimediaData(lat, lon, osmImage);
				List<CameraPlace> visibile = processMapillaryData(lat, lon, osmMapillaryKey, host, proto);
				if (!visibile.isEmpty()) {
					visibile.add(createEmptyCameraPlaceWithTypeOnly("mapillary-contribute"));
				}
				if (wikimediaPrimaryCameraPlace != null) {
					visibile.add(0, wikimediaPrimaryCameraPlace);
				}
				response.setCharacterEncoding("UTF-8");
				response.getWriter().println(gson.toJson(Collections.singletonMap("features", visibile)));
				asyncCtx.complete();
			} catch (Exception e) {
				LOGGER.error("Error processing places: " + e.getMessage());
				LOGGER.warn(e);
			}
		});
	}

	private List<CameraPlace> sortByDistance(List<CameraPlace> arr) {
		return arr.stream().sorted(Comparator.comparing(CameraPlace::getDistance)).collect(Collectors.toList());
	}

	private CameraPlace createEmptyCameraPlaceWithTypeOnly(String type) {
		CameraPlace p = new CameraPlace();
		p.setType(type);
		return p;
	}

	private double computeInitialBearing(double cameraLat, double cameraLon, double targetLat, double targetLon) {
		Location cameraLocation = new Location("mapillary");
		cameraLocation.setLatitude(cameraLat);
		cameraLocation.setLongitude(cameraLon);
		Location targetLocation = new Location("target");
		targetLocation.setLatitude(targetLat);
		targetLocation.setLongitude(targetLon);
		return cameraLocation.bearingTo(targetLocation);
	}

	private double computeDistance(double cameraLat, double cameraLon, double targetLat, double targetLon) {
		Location cameraLocation = new Location("mapillary");
		cameraLocation.setLatitude(cameraLat);
		cameraLocation.setLongitude(cameraLon);
		Location targetLocation = new Location("target");
		targetLocation.setLatitude(targetLat);
		targetLocation.setLongitude(targetLon);
		return cameraLocation.distanceTo(targetLocation);
	}

	private double parseCameraAngle(Object cameraAngle) {
		return Double.parseDouble(String.valueOf(cameraAngle));
	}


	private String buildUrl(String path, boolean hires, String photoIdKey, String host, String proto) {
		UriComponentsBuilder uriBuilder = UriComponentsBuilder.newInstance();
		uriBuilder.scheme(proto);
		uriBuilder.host(host);
		uriBuilder.path(path);
		if (hires) {
			uriBuilder.queryParam(OsmandPhotoApiConstants.OSMAND_PARAM_HIRES, true);
		}
		uriBuilder.queryParam(OsmandPhotoApiConstants.OSMAND_PARAM_PHOTO_ID, photoIdKey);
		return uriBuilder.build().toString();
	}

	private String buildOsmandImageUrl(boolean hires, String photoIdKey, String host, String proto) {
		return buildUrl(OsmandPhotoApiConstants.OSMAND_GET_IMAGE_ROOT_PATH, hires, photoIdKey, host, proto);
	}

	private String buildOsmandPhotoViewerUrl(String photoIdKey, String host, String proto) {
		return buildUrl(OsmandPhotoApiConstants.OSMAND_PHOTO_VIEWER_ROOT_PATH, false, photoIdKey, host, proto);
	}

	private boolean angleDiff(double angle, double diff) {
		if (angle > 360.0) {
			angle -= 360.0;
		}
		if (angle < -360.0) {
			angle += 360.0;
		}
		return Math.abs(angle) < diff;
	}

	private void splitCameraPlaceByAngle(CameraPlace cp, List<CameraPlace> main, List<CameraPlace> rest) {
		double ca = cp.getCa();
		double bearing = cp.getBearing();
		if (cp.is360()) {
			main.add(cp);
		} else if (ca > 0d && angleDiff(bearing - ca, 30.0)) {
			main.add(cp);
		} else if (!(ca > 0d && !angleDiff(bearing - ca, 60.0))) {
			// exclude all with camera angle and angle more than 60 (keep w/o camera and angle < 60)
			rest.add(cp);
		}
	}

	private boolean isPrimaryCameraPlace(CameraPlace cp, String primaryImageKey) {
		return !isEmpty(primaryImageKey) && cp != null && cp.getKey() != null && cp.getKey().equals(primaryImageKey);
	}

	private boolean isEmpty(String str) {
		return Algorithms.isEmpty(str);
	}
	
	
	public static void main(String[] args) throws IOException {
		GeometryTile gt = BinaryVectorTileReader.readTile(new File("/Users/victorshcherb/Desktop/mly1_public-2-14-13304-4104.pbf"));
		System.out.println(gt);
	}
	
	
	
	//https://graph.mapillary.com/images?bbox=4.86933,52.33994,4.87507,52.34188&fields=id,geometry,compass_angle,captured_at,camera_type,creator,thumb_256_url,thumb_1024_url&limit=20&access_token=
	public List<CameraPlace> parseMapillaryPlacesTiles(double lat, double lon, String host, String proto) {
			List<CameraPlace> lst = new ArrayList<>();
			if (Algorithms.isEmpty(mapillaryAccessToken)) {
				return lst;
			}
			String url = "";
			try {
				double x = MapUtils.getTileNumberX(MapillaryApiConstants.ZOOM_QUERY, lon);
				double y = MapUtils.getTileNumberY(MapillaryApiConstants.ZOOM_QUERY, lat);
				double dist = MapUtils.getTileDistanceWidth(MapillaryApiConstants.ZOOM_QUERY);
				double cr = MAPILLARY_RADIUS / dist;
				LatLon topLeft = new LatLon(
						MapUtils.getLatitudeFromTile(MapillaryApiConstants.ZOOM_QUERY, y - cr),
						MapUtils.getLongitudeFromTile(MapillaryApiConstants.ZOOM_QUERY, x - cr));
				LatLon bottomRight = new LatLon(
						MapUtils.getLatitudeFromTile(MapillaryApiConstants.ZOOM_QUERY, y + cr),
						MapUtils.getLongitudeFromTile(MapillaryApiConstants.ZOOM_QUERY, x + cr));
				
				UriComponentsBuilder uriBuilder = UriComponentsBuilder
						.fromHttpUrl(MapillaryApiConstants.GRAPH_URL_IMAGES);
				uriBuilder.queryParam("limit", MAPILLARY_IMAGES_LIMIT);
				uriBuilder.queryParam("bbox", ((float)topLeft.getLongitude())+","+ 
						((float)bottomRight.getLatitude()) +","+
						((float)bottomRight.getLongitude()) +","+
						((float)topLeft.getLatitude()));
				uriBuilder.queryParam(MapillaryApiConstants.MAPILLARY_PARAM_ACCESS_TOKEN, mapillaryAccessToken);
				uriBuilder.queryParam("fields", "id,geometry,compass_angle,captured_at,camera_type,creator,thumb_256_url,thumb_1024_url");
				url = uriBuilder.build().toString();
				MapillaryData dt = restTemplate.getForObject(url, MapillaryData.class);
				for (MapillaryImage g : dt.data) {
					if (g.geometry == null || !"Point".equals(g.geometry.type) || g.geometry.coordinates == null) {
						continue;
					}
					CameraPlace cameraPlace = new CameraPlace();
					double clat = g.geometry.coordinates[1];
					double clon = g.geometry.coordinates[0];
					cameraPlace.setLat(clat);
					cameraPlace.setLon(clon);
					cameraPlace.setType("mapillary-photo");
					cameraPlace.setTimestamp(String.valueOf(g.captured_at));
					String key = g.id.toString();
					cameraPlace.setKey(key);
					cameraPlace.setCa(parseCameraAngle(g.compass_angle));
					cameraPlace.setImageUrl(g.thumb_256_url);
					cameraPlace.setImageHiresUrl(g.thumb_1024_url);
					cameraPlace.setUrl(buildOsmandPhotoViewerUrl(key, host, proto));
					cameraPlace.setExternalLink(false);
					// cameraPlace.setUsername((String) data.get("username"));
					// equirectangular perspective spherical
					cameraPlace.setIs360("equirectangular".equals(g.camera_type) || 
								"spherical".equals(g.camera_type));
					cameraPlace.setTopIcon("ic_logo_mapillary");
					double bearing = computeInitialBearing(clat, clon, lat, lon);
					cameraPlace.setBearing(bearing);
					cameraPlace.setDistance(computeDistance(clat, clon, lat, lon));
					lst.add(cameraPlace);
				}
			} catch (RuntimeException ex) {
				LOGGER.error("Error Mappillary api (" + url + "): " + ex.getMessage());
			}
			return lst;
		}

	@SuppressWarnings("unchecked")
	public List<CameraPlace> parseMapillaryPlacesApi(double lat, double lon, String host, String proto) {
		List<CameraPlace> lst = new ArrayList<>();
		if (Algorithms.isEmpty(mapillaryAccessToken)) {
			return lst;
		}
		String url = "";
		try {
			int x = (int) MapUtils.getTileNumberX(MapillaryApiConstants.ZOOM_QUERY, lon);
			int y = (int) MapUtils.getTileNumberY(MapillaryApiConstants.ZOOM_QUERY, lat);
			File f = new File(TEMP_MAPILLARY_FOLDER, FILE_MAPILLARY_PREFIX + x + "_" + y + ".mvt");
			gcMapillaryCache();
			if (!f.exists() || System.currentTimeMillis() - f.lastModified() > MAPILLARY_CACHE_TIMEOUT) {
				String accessTokenParam = MapillaryApiConstants.MAPILLARY_PARAM_ACCESS_TOKEN + "="
						+ URLEncoder.encode(mapillaryAccessToken, "UTF-8");
				url = MapillaryApiConstants.MAPILLARY_VECTOR_TILE_URL + "/" + MapillaryApiConstants.ZOOM_QUERY + "/" + x
						+ "/" + y + "?" + accessTokenParam;
				InputStream is = new URL(url).openConnection().getInputStream();
				FileOutputStream fous = new FileOutputStream(f);
				Algorithms.streamCopy(is, fous);
				fous.close();
				is.close();
			}
			
			GeometryTile tl = BinaryVectorTileReader.readTile(f);
			for (Geometry g : tl.getData()) {
				if (!"Point".equals(g.getGeometryType())) {
					continue;
				}
				// compass_angle, captured_at, is_pano, sequence_id, id (image_id)
				Map<String, Object> data = (Map<String, Object>) g.getUserData();
				int cx = (int) g.getCentroid().getX();
				int cy = (int) g.getCentroid().getY();
				double clat = MapUtils.getLatitudeFromTile(MapillaryApiConstants.ZOOM_POINT_MAX,
						cy + (y << MapillaryApiConstants.ZOOM_SHIFT));
				double clon = MapUtils.getLongitudeFromTile(MapillaryApiConstants.ZOOM_POINT_MAX,
						cx + (x << MapillaryApiConstants.ZOOM_SHIFT));
				CameraPlace cameraPlace = new CameraPlace();
				cameraPlace.setType("mapillary-photo");
				cameraPlace.setTimestamp(String.valueOf(data.get("captured_at")));
				String key = data.get("id").toString();
				cameraPlace.setKey(key);
				cameraPlace.setCa(parseCameraAngle(data.get("compass_angle")));
				cameraPlace.setImageUrl(buildOsmandImageUrl(false, key, host, proto));
				cameraPlace.setImageHiresUrl(buildOsmandImageUrl(true, key, host, proto));
				cameraPlace.setUrl(buildOsmandPhotoViewerUrl(key, host, proto));
				cameraPlace.setExternalLink(false);
				cameraPlace.setUsername((String) data.get("username"));
				if (data.get("is_pano") instanceof Boolean) {
					cameraPlace.setIs360((Boolean) data.get("is_pano"));
				}
				cameraPlace.setLat(clat);
				cameraPlace.setLon(clon);
				cameraPlace.setTopIcon("ic_logo_mapillary");
				double bearing = computeInitialBearing(clat, clon, lat, lon);
				cameraPlace.setBearing(bearing);
				cameraPlace.setDistance(computeDistance(clat, clon, lat, lon));
				lst.add(cameraPlace);
			}
		} catch (IOException ex) {
			LOGGER.error("Error Mappillary api (" + url + "): " + ex.getMessage());
		}
		return lst;
	}
	
	private void gcMapillaryCache() {
		long tm = System.currentTimeMillis();
		if (tm - lastMapillaryGCTimestamp > MAPILLARY_GC_TIMEOUT) {
			lastMapillaryGCTimestamp = tm;
			File fld = new File(TEMP_MAPILLARY_FOLDER);
			fld.mkdirs();
			File[] lf = fld.listFiles();
			if (lf != null) {
				for (File f : lf) {
					if (f.getName().startsWith(FILE_MAPILLARY_PREFIX)
							&& tm - f.lastModified() > MAPILLARY_CACHE_TIMEOUT) {
						f.delete();
					}
				}
			}
		}
	}

	public void initMapillaryImageUrl(CameraPlace cp) {
		// String url = MapillaryApiConstants.GRAPH_URL + cp.getKey() + "?"
		// + MapillaryApiConstants.MAPILLARY_PARAM_ACCESS_TOKEN + "="
		// + URLEncoder.encode(mapillaryAccessToken, "UTF-8") + "&fields=id,computed_geometry,thumb_1024_url";
		if (Algorithms.isEmpty(mapillaryAccessToken)) {
			return;
		}
		UriComponentsBuilder uriBuilder = UriComponentsBuilder
				.fromHttpUrl(MapillaryApiConstants.GRAPH_URL + cp.getKey());
		uriBuilder.queryParam(MapillaryApiConstants.MAPILLARY_PARAM_ACCESS_TOKEN, mapillaryAccessToken);
		uriBuilder.queryParam("fields", "id,computed_geometry,thumb_256_url,thumb_1024_url");
		MapillaryImage img = restTemplate.getForObject(uriBuilder.build().toString(), MapillaryImage.class);
		if (img != null) {
			cp.setImageUrl(img.thumb_256_url);
			cp.setImageHiresUrl(img.thumb_1024_url);
		}
	}

	public List<CameraPlace> processMapillaryData(double lat, double lon, String primaryImageKey, String host, String proto) {
		
		CameraPlace primaryPlace = null;
		List<CameraPlace> result = new ArrayList<CameraPlace>();
		List<CameraPlace> rest = new ArrayList<CameraPlace>();
		List<CameraPlace> places = parseMapillaryPlacesTiles(lat, lon, host, proto);
		for (CameraPlace cp : places) {
			if(cp.getDistance() == null || cp.getDistance() > MAPILLARY_RADIUS) {
				continue;
			}
			if (isPrimaryCameraPlace(cp, primaryImageKey)) {
				primaryPlace = cp;
				continue;
			}
			splitCameraPlaceByAngle(cp, result, rest);
		}
		if (result.isEmpty()) {
			result.addAll(rest);
		}
		result = sortByDistance(result);
		if (primaryPlace != null) {
			result.add(0, primaryPlace);
		}
		if (result.size() > MAPILLARY_IMAGES_LIMIT) {
			result = result.subList(0, MAPILLARY_IMAGES_LIMIT);
		}
		for(CameraPlace cp : result) {
			initMapillaryImageUrl(cp);
		}
		return result;
	}

	

	private String getFilename(String osmImage) {
		if (osmImage.startsWith(WIKI_FILE_PREFIX)) {
			return osmImage;
		}
		return osmImage.substring(osmImage.indexOf(WIKI_FILE_PREFIX));
	}

	private boolean isWikimediaUrl(String osmImage) {
		return osmImage != null && (osmImage.startsWith(WIKI_FILE_PREFIX)
				|| ((osmImage.contains(WIKIMEDIA) || osmImage.contains(WIKIPEDIA))
						&& osmImage.contains(WIKI_FILE_PREFIX)));
	}

	private String parseTitle(WikiPage page) {
		String title = page.getTitle();
		if (title.contains(WIKI_FILE_PREFIX)) {
			title = title.substring(title.indexOf(WIKI_FILE_PREFIX) + 5);
		}
		return title;
	}

	private CameraPlace parseWikimediaImage(double targetLat, double targetLon, String filename, String title,
			ImageInfo imageInfo) {
		CameraPlace builder = new CameraPlace();
		builder.setType("wikimedia-photo");
		builder.setTimestamp(imageInfo.timestamp);
		builder.setKey(filename);
		builder.setTitle(title);
		builder.setImageUrl(imageInfo.thumburl);
		builder.setImageHiresUrl(imageInfo.url);
		builder.setUrl(imageInfo.descriptionurl);
		builder.setExternalLink(false);
		builder.setUsername(imageInfo.user);
		builder.setLat(targetLat);
		builder.setLon(targetLon);
		return builder;
	}

	public CameraPlace processWikimediaData(double lat, double lon, String osmImage) {
		if (isEmpty(osmImage)) {
			return null;
		}
		CameraPlace primaryImage;
		if (isWikimediaUrl(osmImage)) {
			String filename = getFilename(osmImage);
			if (filename.isEmpty()) {
				return null;
			}
			UriComponentsBuilder uriBuilder = UriComponentsBuilder.fromHttpUrl(WikimediaApiConstants.WIKIMEDIA_API_URL);
			uriBuilder.queryParam(WikimediaApiConstants.WIKIMEDIA_PARAM_TITLES, filename);
			WikiBatch batch = restTemplate.getForObject(uriBuilder.build().toString(), WikiBatch.class);
			if (batch == null || batch.getQuery().getPages().isEmpty()) {
				return null;
			}
			WikiPage page = batch.getQuery().getPages().get(0);
			if (page.getMissing()) {
				return null;
			}
			if (page.getImageinfo() == null || page.getImageinfo().isEmpty() || page.getImageinfo().get(0) == null) {
				return null;
			}
			primaryImage = parseWikimediaImage(lat, lon, filename, parseTitle(page), page.getImageinfo().get(0));
		} else {
			CameraPlace builder = new CameraPlace();
			builder.setType("url-photo");
			builder.setImageUrl(osmImage);
			builder.setUrl(osmImage);
			builder.setLat(lat);
			builder.setLon(lon);
			primaryImage = builder;
		}
		return primaryImage;
	}

	private static class MapillaryApiConstants {
		static final String GRAPH_URL = "https://graph.mapillary.com/";
		static final String GRAPH_URL_IMAGES = "https://graph.mapillary.com/images";
		static final String MAPILLARY_VECTOR_TILE_URL = "https://tiles.mapillary.com/maps/vtp/mly1_public/2";
		static final String MAPILLARY_PARAM_ACCESS_TOKEN = "access_token";
		static final int ZOOM_QUERY = 14;
		static final int ZOOM_SHIFT = 12;
		static final int ZOOM_POINT_MAX = ZOOM_QUERY + ZOOM_SHIFT; // 4096 max 
	}

	private static class WikimediaApiConstants {
		static final String WIKIMEDIA_API_URL = "https://commons.wikimedia.org/w/api.php?format=json&formatversion=2&action=query&prop=imageinfo"
				+ "&iiprop=timestamp|user|url&iiurlwidth=576";
		static final String WIKIMEDIA_PARAM_TITLES = "titles";
	}

	private static class OsmandPhotoApiConstants {
		static final String OSMAND_GET_IMAGE_ROOT_PATH = "/api/mapillary/get_photo";
		static final String OSMAND_PHOTO_VIEWER_ROOT_PATH = "/api/mapillary/photo-viewer";
		static final String OSMAND_PARAM_PHOTO_ID = "photo_id";
		static final String OSMAND_PARAM_HIRES = "hires";
	}

	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	public static class ImageInfo {
		public String timestamp;
		public String user;
		public String thumburl;
		public String url;
		public String descriptionurl;

	}

	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	public static class Query {

		private List<WikiPage> pages;

		public List<WikiPage> getPages() {
			return pages;
		}

		public void setPages(List<WikiPage> pages) {
			this.pages = pages;
		}
	}

	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	public static class WikiBatch {

		private Boolean batchcomplete;
		private Query query;

		public Boolean getBatchcomplete() {
			return batchcomplete;
		}

		public void setBatchcomplete(Boolean batchcomplete) {
			this.batchcomplete = batchcomplete;
		}

		public Query getQuery() {
			return query;
		}

		public void setQuery(Query query) {
			this.query = query;
		}
	}
	
	
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	public static class MapillaryData {

		private List<MapillaryImage> data = new ArrayList<PlacesService.MapillaryImage>();

		public List<MapillaryImage> getData() {
			return data;
		}

	}
	
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	public static class MapillaryGeometry {

		private double[] coordinates;
		private String type;
		public double[] getCoordinates() {
			return coordinates;
		}
		public void setCoordinates(double[] coordinates) {
			this.coordinates = coordinates;
		}
		public String getType() {
			return type;
		}
		public void setType(String type) {
			this.type = type;
		}
		
	}
	
	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	public static class MapillaryImage {

		private String id;
		private String thumb_1024_url;
		private String thumb_256_url;
		private double compass_angle;
		private String camera_type;
		private long captured_at;
		private MapillaryGeometry geometry;
		private MapillaryGeometry computed_geometry;
		
		public double getCompass_angle() {
			return compass_angle;
		}
		public void setCompass_angle(double compass_angle) {
			this.compass_angle = compass_angle;
		}
		public String getCamera_type() {
			return camera_type;
		}
		public void setCamera_type(String camera_type) {
			this.camera_type = camera_type;
		}
		public long getCaptured_at() {
			return captured_at;
		}
		public void setCaptured_at(long captured_at) {
			this.captured_at = captured_at;
		}
		public MapillaryGeometry getGeometry() {
			return geometry;
		}
		public void setGeometry(MapillaryGeometry geometry) {
			this.geometry = geometry;
		}
		public MapillaryGeometry getComputed_geometry() {
			return computed_geometry;
		}
		public void setComputed_geometry(MapillaryGeometry computed_geometry) {
			this.computed_geometry = computed_geometry;
		}
		public String getThumb_256_url() {
			return thumb_256_url;
		}
		public void setThumb_256_url(String thumb_256_url) {
			this.thumb_256_url = thumb_256_url;
		}
		public String getId() {
			return id;
		}
		public void setId(String id) {
			this.id = id;
		}
		public String getThumb_1024_url() {
			return thumb_1024_url;
		}
		public void setThumb_1024_url(String thumb_1024_url) {
			this.thumb_1024_url = thumb_1024_url;
		}
		
	}

	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	public static class WikiPage {
		private String title;
		private boolean missing;
		private List<ImageInfo> imageinfo;

		public String getTitle() {
			return title;
		}

		public List<ImageInfo> getImageinfo() {
			return imageinfo;
		}

		public boolean getMissing() {
			return missing;
		}

	}

}
