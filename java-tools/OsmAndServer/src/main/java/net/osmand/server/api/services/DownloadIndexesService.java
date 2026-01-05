package net.osmand.server.api.services;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.util.SubnetUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;

import net.osmand.IndexConstants;
import net.osmand.util.Algorithms;

@Service
public class DownloadIndexesService  {
	
	private static final Log LOGGER = LogFactory.getLog(DownloadIndexesService.class);

	private static final String INDEX_FILE = "indexes.xml";
	private static final String DOWNLOAD_SETTINGS = "api/download_settings.json";
	private static final String INDEX_FILE_EXTERNAL_URL = "index-source.info";
    private static final String EXTERNAL_URL = "public-api-indexes/";
	
	@Value("${osmand.files.location}")
    private String pathToDownloadFiles;
	
	@Value("${osmand.gen.location}")
	private String pathToGenFiles;
	
	@Value("${osmand.web.location}")
    private String websiteLocation;

	private DownloadServerLoadBalancer settings;

	private Gson gson;

	private Map<String, Double> mapSizesCache;
	
	public DownloadIndexesService() {
		gson = new Gson();
		mapSizesCache = new HashMap<>();
	}
	
	public DownloadServerLoadBalancer getSettings() {
		if(settings == null) {
			reloadConfig(new ArrayList<String>());
		}
		return settings;
	}
	
	public boolean reloadConfig(List<String> errors) {
    	try {
    		DownloadServerLoadBalancer s = gson.fromJson(new FileReader(new File(websiteLocation, DOWNLOAD_SETTINGS)), DownloadServerLoadBalancer.class);
    		s.prepare();
    		settings = s;
    	} catch (IOException ex) {
    		if(errors != null) {
    			errors.add(DOWNLOAD_SETTINGS + " is invalid: " + ex.getMessage());
    		}
            LOGGER.warn(ex.getMessage(), ex);
            return false;
    	}
        return true;
    }
	
	

	// 15 minutes
	@Scheduled(fixedDelay = 1000 * 60 * 15)
	public void checkOsmAndLiveStatus() {
		generateStandardIndexFile();
	}
	
	public DownloadIndexDocument loadDownloadIndexes() {
		DownloadIndexDocument doc = new DownloadIndexDocument();
		File rootFolder = new File(pathToDownloadFiles);
		loadIndexesFromDir(doc.getMaps(), rootFolder, DownloadType.MAP);
		loadIndexesFromDir(doc.getVoices(), rootFolder, DownloadType.VOICE);
		loadIndexesFromDir(doc.getFonts(), rootFolder, DownloadType.FONTS);
		loadIndexesFromDir(doc.getInapps(), rootFolder, DownloadType.DEPTH);
		loadIndexesFromDir(doc.getDepths(), rootFolder, DownloadType.DEPTHMAP);
		loadIndexesFromDir(doc.getStarmap(), rootFolder, DownloadType.STARMAP);
		loadIndexesFromDir(doc.getWikimaps(), rootFolder, DownloadType.WIKIMAP);
		loadIndexesFromDir(doc.getTravelGuides(), rootFolder, DownloadType.TRAVEL);
		loadIndexesFromDir(doc.getRoadMaps(), rootFolder, DownloadType.ROAD_MAP);
		loadIndexesFromDir(doc.getSrtmMaps(), rootFolder, DownloadType.SRTM_MAP.getPath(), DownloadType.SRTM_MAP, IndexConstants.BINARY_SRTM_MAP_INDEX_EXT);
		loadIndexesFromDir(doc.getSrtmFeetMaps(), rootFolder, DownloadType.SRTM_MAP.getPath(), DownloadType.SRTM_MAP, IndexConstants.BINARY_SRTM_FEET_MAP_INDEX_EXT);
		loadIndexesFromDir(doc.getHillshade(), rootFolder, DownloadType.HILLSHADE);
		loadIndexesFromDir(doc.getSlope(), rootFolder, DownloadType.SLOPE);
		loadIndexesFromDir(doc.getHeightmap(), rootFolder, DownloadType.HEIGHTMAP);
		loadIndexesFromDir(doc.getWeather(), rootFolder, DownloadType.WEATHER);
		loadIndexesFromDir(doc.getHeightmap(), rootFolder, DownloadType.GEOTIFF);
		DownloadFreeMapsConfig free = getSettings().freemaps;
		for (DownloadIndex di : doc.getAllMaps()) {
			mapSizesCache.put(di.getName(), di.getSize());
			for (String pattern : free.namepatterns) {
				if (di.getName().startsWith(pattern)) {
					di.setFree(true);
					di.setFreeMessage(free.message);
				}
			}
		}
		return doc;
	}
	
	public static class ServerCommonFile {

		public final File file;
		public final URL url;
		public final DownloadIndex di;

		public ServerCommonFile(File file, DownloadIndex di) {
			this.file = file;
			this.di = di;
			this.url = null;
		}
		
		public ServerCommonFile(URL url, DownloadIndex di) {
			this.url = url;
			this.file = null;
			this.di = di;
		}
		
		
		public InputStream getInputStream() throws IOException {
			return url != null ? url.openStream() : new FileInputStream(file);
		}
	}
	
	public ServerCommonFile getServerGlobalFile(String name) throws IOException {
		DownloadIndexDocument doc = getIndexesDocument(false, false);
		// ignore folders for srtm / hillshade / slope
		if (name.lastIndexOf('/') != -1) {
			name = name.substring(name.lastIndexOf('/') + 1);
		}
		String dwName;
		if (name.endsWith("obf")) {
			// add _2 for obf files 
			int ind = name.indexOf('.');
			dwName = name.substring(0, ind) + "_2" + name.substring(ind);
		} else {
			// replace ' ' as it could be done on device 
			dwName = name.replace(' ', '_');
		}

		for (DownloadIndex di : doc.getAllMaps()) {
			if (di.getName().equals(dwName) || di.getName().equals(dwName + ".zip")) {
				File file = new File(pathToDownloadFiles, dwName + ".zip");
				if (!file.exists()) {
					file = new File(pathToDownloadFiles, di.getDownloadType().getPath() + "/" + dwName + ".zip");
				}
				if (!file.exists()) {
					file = new File(pathToDownloadFiles, dwName);
				}
				if (!file.exists()) {
					file = new File(pathToDownloadFiles, di.getDownloadType().getPath() + "/" + dwName);
				}
				if (file.exists()) {
					return new ServerCommonFile(file, di);
				}
				DownloadServerLoadBalancer servers = getSettings();
				PredefinedServerSpecialty sp = PredefinedServerSpecialty.getSpecialtyByDownloadType(di.getDownloadType());
				if (sp != null) {
					String host = servers.getServer(servers.getServerType(sp, ""), null);
					if (!Algorithms.isEmpty(host)) {
						try {
							String pm = "";
							if (di.getDownloadType().getHeaders().length > 0) {
								pm = "&" + di.getDownloadType().getHeaders()[0] + "=yes";
							}
							String urlRaw = "https://" + host + "/download?file=" + di.getName() + pm;
							URL url = new URL(urlRaw);
							HttpURLConnection con = (HttpURLConnection) url.openConnection();
							con.setRequestMethod("HEAD");
							con.setDoOutput(false);
							int code = con.getResponseCode();
							con.disconnect();
							if (code >= 200 && code < 400) {
								return new ServerCommonFile(url, di);
							}
						} catch (IOException e) {
							LOGGER.error("Error checking existing index: " + e.getMessage(), e);
						}
						return null;
					}
				}
			}
		}
		return null;
	}
	
	public File getIndexesXml(boolean upd, boolean gzip) {
		File target = getStandardFilePath(gzip);
		if (!target.exists() || upd) {
			generateStandardIndexFile();
		}
		return target;
	}
	
	public DownloadIndexDocument getIndexesDocument(boolean upd, boolean gzip) throws IOException {
		File target = getIndexesXml(upd, gzip);
		return unmarshallIndexes(target);
	}


	private File getStandardFilePath(boolean gzip) {
		return new File(pathToGenFiles, gzip ? INDEX_FILE + ".gz" : INDEX_FILE);
	}
	
	private synchronized void generateStandardIndexFile() {
		long start = System.currentTimeMillis();
		DownloadIndexDocument di = loadDownloadIndexes();
		File target = getStandardFilePath(false);
		generateIndexesFile(di, target, start);
		File gzip = getStandardFilePath(true);
		gzipFile(target, gzip);
		LOGGER.info(String.format("Regenerate indexes.xml in %.1f seconds",
				((System.currentTimeMillis() - start) / 1000.0)));
	}

	private void gzipFile(File target, File gzip) {
		try {
			if (!target.getParentFile().exists()) {
				target.getParentFile().mkdirs();
			}
			FileInputStream is = new FileInputStream(target);
			GZIPOutputStream out = new GZIPOutputStream(new FileOutputStream(gzip));
			Algorithms.streamCopy(is, out);
			is.close();
			out.close();
		} catch (IOException e) {
			LOGGER.error("Gzip file " + target.getName(), e);
			e.printStackTrace();
		}
	}

	private void loadIndexesFromDir(List<DownloadIndex> list, File rootFolder, DownloadType type) {
		if(type == DownloadType.MAP) {
			loadIndexesFromDir(list, rootFolder, ".", type, null);
		}
		loadIndexesFromDir(list, rootFolder, type.getPath(), type, null);
	}
	
	public long getGzipUncompressedSize(File file) {
	    try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
	        raf.seek(raf.length() - 4);
	        byte[] b = new byte[4];
	        raf.readFully(b);
	        // Gzip stores size in Little Endian
	        return ((long) (b[3] & 0xFF) << 24) |
	               ((long) (b[2] & 0xFF) << 16) |
	               ((long) (b[1] & 0xFF) << 8) |
	               ((long) (b[0] & 0xFF));
	    } catch (IOException e) {
	        return file.length(); // Fallback to compressed size on error
	    }
	}
	
	private void loadIndexesFromDir(List<DownloadIndex> list, File rootFolder, String subPath, DownloadType type, String filterFiles) {
		File subFolder = new File(rootFolder, subPath);
		File[] files = subFolder.listFiles();
		if(files == null || files.length == 0) {
			return;
		}
		if (files.length > 0 && files[0].getName().equals(INDEX_FILE_EXTERNAL_URL)) {
            try {
                String host;
                BufferedReader bufferreader = new BufferedReader(new FileReader(files[0]));
                while ((host = bufferreader.readLine()) != null) {
                    URL url = new URL("https://" + host + "/" + EXTERNAL_URL + subPath);
                    InputStreamReader reader = new InputStreamReader(url.openStream());
                    ExternalSource [] externalSources = gson.fromJson(reader, ExternalSource[].class);
                    if (externalSources.length > 0) {
                        boolean areFilesAdded = false;
                        for (ExternalSource source : externalSources) {
                            // do not read external zip files, otherwise it will be too long by remote connection
                            if (source.type.equals("file") && type.acceptFileName(source.name) && !isZip(source.name)) {
                            	
                                DownloadIndex di = new DownloadIndex();
                                di.setType(type);
                                String name = source.name;
                                long actualContentSize = source.size;
                                int extInd = name.indexOf('.');
                                String ext = name.substring(extInd + 1);
                                formatName(name, extInd);
                                di.setName(name);
                                di.setSize(source.size);
                                di.setContainerSize(source.size);
                                di.setTimestamp(source.getTimestamp());
                                di.setDate(source.getTimestamp());
                                di.setContentSize(actualContentSize);
                                di.setTargetsize(actualContentSize);
                                di.setDescription(type.getDefaultTitle(name, ext));
                                list.add(di);
                                areFilesAdded = true;
                            }
                        }
                        if (areFilesAdded) {
                            break;
                        }
                    }
                    // will continue if was not find any files in this host (server)
                }
                bufferreader.close();
            } catch (IOException e) {
                LOGGER.error("LOAD EXTERNAL INDEXES: " + e.getMessage(), e.getCause());
            }
            return;
        }
		for (File lf : files) {
			if (filterFiles != null && !lf.getName().contains(filterFiles)) {
				continue;
			} else if (type.acceptFileName(lf.getName())) {
				String name = lf.getName();
                int extInd = name.indexOf('.');
                String ext = name.substring(extInd + 1);
				formatName(name, extInd);
				DownloadIndex di = new DownloadIndex();
				di.setType(type);
				di.setName(lf.getName());
				di.setSize(lf.length());
				di.setContainerSize(lf.length());
				if (isZip(lf)) {
					try {
						ZipFile zipFile = new ZipFile(lf);
						long contentSize = zipFile.stream().mapToLong(ZipEntry::getSize).sum();
						di.setContentSize(contentSize);
						di.setTargetsize(contentSize);
						Enumeration<? extends ZipEntry> entries = zipFile.entries();
						if (entries.hasMoreElements()) {
							ZipEntry entry = entries.nextElement();
							long mtime = entry.getLastModifiedTime().to(TimeUnit.MILLISECONDS);
							di.setTimestamp(mtime);
							di.setDate(mtime);
							String description = entry.getComment();
							if (description != null) {
								di.setDescription(description);
							} else {
								di.setDescription(type.getDefaultTitle(name, ext));
							}
						}
						list.add(di);
						zipFile.close();
					} catch (Exception e) {
						LOGGER.error(lf.getName() + ": " + e.getMessage(), e);
						e.printStackTrace();
					}
				} else {
					long actualContentSize = lf.length();
					if (name.toLowerCase().endsWith(".gz")) {
                        actualContentSize = getGzipUncompressedSize(lf);
                    }
					di.setTimestamp(lf.lastModified());
					di.setDate(lf.lastModified());
					di.setContentSize(actualContentSize);
					di.setTargetsize(actualContentSize);
					di.setDescription(type.getDefaultTitle(name, ext));
					list.add(di);
				}
			}
		}
	}

	protected boolean isZipValid(File file) {
		boolean isValid = true;
		if (isZip(file)) {
			try {
				ZipFile fl = new ZipFile(file);
				fl.close();
			} catch (IOException ex) {
				isValid = false;
			}
		}
		return isValid;
	}

	private boolean isZip(File file) {
		return file.getName().endsWith(".zip");
	}

    private boolean isZip(String fileName) {
        return fileName.endsWith(".zip");
    }

	private void generateIndexesFile(DownloadIndexDocument doc, File file, long start) {
		try {
			JAXBContext jc = JAXBContext.newInstance(DownloadIndexDocument.class);
			Marshaller marshaller = jc.createMarshaller();
			marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
			doc.setMapVersion(1);
			doc.setTimestamp(new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(new Date()));
			doc.setOutdatedMaps();
			doc.setGentime(String.format("%.1f",
					((System.currentTimeMillis() - start) / 1000.0)));
			marshaller.marshal(doc, file);
		} catch (JAXBException ex) {
			LOGGER.error(ex.getMessage(), ex);
			ex.printStackTrace();
		}
	}
	
	public enum DownloadType {
	    MAP("indexes"),
	    OSMLIVE("aosmc", "aosmc", "osmc"),
	    VOICE("indexes") ,
	    DEPTH("indexes/inapp/depth", "depth"), // Deprecated
	    DEPTHMAP("depth", "depth") ,
	    STARMAP("indexes") ,
	    FONTS("indexes/fonts", "fonts") ,
	    WIKIMAP("wiki", "wiki") ,
	    TRAVEL("travel", "wikivoyage", "travel") ,
	    ROAD_MAP("road-indexes", "road"),
	    HILLSHADE("hillshade", "hillshade"),
	    HEIGHTMAP("heightmap", "heightmap"), // Deprecated
	    GEOTIFF("heightmap", "heightmap"),
	    SLOPE("slope", "slope") ,
	    SRTM_MAP("srtm-countries", "srtmcountry"),
	    WEATHER("weather/regions", "weather"),
	    
	    DELETED_MAP("indexes", "");


		private final String path;
		private final String[] headers;

		DownloadType(String path, String... headers) {
			this.path = path;
			this.headers = headers;
		}
		
		public String[] getHeaders() {
			if (headers == null) {
				return new String[0];
			}
			return headers;
		}
		
		public String getPath() {
			return path;
		}


        public boolean acceptFileName(String fileName) {
            switch (this) {
                case HEIGHTMAP:
                    return fileName.endsWith(".sqlite");
                case WEATHER:
                    return fileName.endsWith(".tifsqlite.zip");
                case GEOTIFF:
                    return fileName.endsWith(".tif");
                case TRAVEL:
                    return fileName.endsWith(".travel.obf.zip") || fileName.endsWith(".travel.obf");
                case OSMLIVE:
                	return fileName.endsWith(".obf.gz");
                case STARMAP:
                	return fileName.endsWith(".stardb.gz");
                case MAP:
                case ROAD_MAP:
                case WIKIMAP:
                case DEPTH:
                case DEPTHMAP:
                case SRTM_MAP:
                    return fileName.endsWith(".obf.zip") || fileName.endsWith(".obf") || fileName.endsWith(".extra.zip");
                case HILLSHADE:
                case SLOPE:
                    return fileName.endsWith(".sqlitedb");
                case FONTS:
                    return fileName.endsWith(".otf.zip");
                case VOICE:
                    return fileName.endsWith(".voice.zip");
            }
            return false;
        }


		public String getDefaultTitle(String regionName, String ext) {
			switch (this) {
			case MAP:
				return String.format("Map, Roads, POI, Transport, Address data for %s", regionName);
			case ROAD_MAP:
				return String.format("Roads, POI, Address data for %s", regionName);
			case WIKIMAP:
				return String.format("Wikipedia POI data for %s", regionName);
			case STARMAP:
				return String.format("Starmap for %s", regionName);
			case DEPTH:
			case DEPTHMAP:
				return String.format("Depth maps for %s", regionName);
			case SRTM_MAP:
				String suf = ext.contains("srtmf") ? "feet" : "meters";
				return String.format("Contour lines (%s) for %s", suf, regionName);
			case TRAVEL:
				return String.format("Travel for %s", regionName);
			case OSMLIVE:
			case HEIGHTMAP:
			case WEATHER:
			case HILLSHADE:
			case GEOTIFF:
			case SLOPE:
				return String.format("%s", regionName);
			case FONTS:
				return String.format("Fonts %s", regionName);
			case VOICE:
				return String.format("Voice package: %s", regionName);
			}
			return "";
		}

	    public String getType() {
	    	return name().toLowerCase();
	    }
	}
	
	
	public static void main(String[] args) throws JsonSyntaxException, JsonIOException, FileNotFoundException {
		// small test
		Gson gson = new Gson();
		DownloadServerLoadBalancer dp = gson.fromJson(new FileReader(new File(
				System.getProperty("repo.location") + "/web-server-config/", DOWNLOAD_SETTINGS)), DownloadServerLoadBalancer.class);
		dp.prepare();
		DownloadServerType type = dp.getServerType(PredefinedServerSpecialty.MAIN, "");
		for(String serverName : dp.getServerNames()) {
			System.out.println(serverName + " " + dp.getGlobalPercent(type, serverName)+"%");
		}			
		Map<String, Integer> cnts = new TreeMap<String, Integer>();
		for (String s : dp.getServerNames()) {
			cnts.put(s, 0);
		}
		for(int i = 0; i < 1000; i ++) {
			String s = dp.getServer(type, "83.85.1.232"); // nl ip
//			String s = dp.getServer(type, "217.85.4.23"); // german ip
			cnts.put(s, cnts.get(s) + 1);
		}
		for (String serverName : dp.getServerNames()) {
			System.out.println(serverName + " " + dp.getDownloadCounts(type, serverName));
		}
		for (DownloadServerRegion reg : dp.getRegions()) {
			for (String serverName : dp.getServerNames()) {
				System.out.println(reg.getName() + " " + reg.getDownloadCounts(
						type , serverName));
			}
		}
		System.out.println(cnts);
	}
	
	private static class DownloadServerCategory {
	
		List<String> serverNames;
		int sum;
		int[] bounds;
		int[] counts;
		int[] percents;
	}
	
	public static class DownloadServerType {
		public PredefinedServerSpecialty type;
		public String key;
	}
		
	public enum PredefinedServerSpecialty {
		MAIN(DownloadType.VOICE, DownloadType.STARMAP, DownloadType.FONTS, DownloadType.MAP),
		SRTM(DownloadType.SRTM_MAP),
		HILLSHADE(DownloadType.HILLSHADE),
		SLOPE(DownloadType.SLOPE),
		HEIGHTMAP(DownloadType.HEIGHTMAP, DownloadType.GEOTIFF),
		OSMLIVE(DownloadType.OSMLIVE),
		DEPTH(DownloadType.DEPTH, DownloadType.DEPTHMAP),
		ROADS(DownloadType.ROAD_MAP),
		WIKI(DownloadType.WIKIMAP, DownloadType.TRAVEL),
		WEATHER(DownloadType.WEATHER);
		
		public final DownloadType[] types;

		PredefinedServerSpecialty(DownloadType... tp) {
			this.types = tp;
		}
		
		
		public static PredefinedServerSpecialty getSpecialtyByDownloadType(DownloadType c) {
			for (PredefinedServerSpecialty s : values()) {
				if (s.types == null) {
					continue;
				}
				for (DownloadType t : s.types) {
					if (t == c) {
						return s;
					}
				}
			}
			return null;
		}
		
	}
	
	public static class DownloadServerRegion {
		String name = "Global";
		List<String> servers = new ArrayList<String>();
		List<String> zones = new ArrayList<String>();
		List<SubnetUtils> cidrZones = new ArrayList<>();
//		DownloadServerCategory[] specialties = new DownloadServerCategory[DownloadServerSpecialty.values().length];
		Map<String, DownloadServerCategory> specialties = new TreeMap<>();
		long ips = 0;
		
		public void prepare() {
			for (String zone : zones) {
				try {
					SubnetUtils ut = new SubnetUtils(zone);
					ips += ut.getInfo().getAddressCountLong();
					cidrZones.add(ut);
				} catch (Exception e) {
					LOGGER.error("Incorrect CIDR " + e.getMessage(), e);
				}
			}
		}
		
		public int asInteger(String ip) {
			if (cidrZones.isEmpty()) {
				return 0;
			}
			return cidrZones.get(0).getInfo().asInteger(ip);
		}
		
		public boolean matchesIp(int ip) {
			// array is sorted so it could be faster
			for (SubnetUtils ut : cidrZones) {
				if (ut.getInfo().isInRange(ip)) {
					return true;
				}
			}
			return false;
		}
		
		public Collection<String> getServers() {
			return servers;
		}
		
		@Override
		public String toString() {
			return getName() + " " + cidrZones.size();
		}

		private void prepare(DownloadServerType tp, Map<String, Integer> mp) {
			DownloadServerCategory cat = new DownloadServerCategory();
			specialties.put(tp.key, cat);

			cat.serverNames = new ArrayList<String>();
			for (String serverName : mp.keySet()) {
				if (servers.contains(serverName)) {
					cat.serverNames.add(serverName);
					cat.sum += mp.get(serverName);
				}
			}
			if (cat.sum > 0) {
				cat.bounds = new int[cat.serverNames.size()];
				cat.counts = new int[cat.serverNames.size()];
				cat.percents = new int[cat.serverNames.size()];
				int ind = 0;
				for (String server : cat.serverNames) {
					cat.bounds[ind] = mp.get(server);
					cat.percents[ind] = 100 * mp.get(server) / cat.sum;
					ind++;
				}
			} else {
				cat.bounds = new int[0];
				cat.counts = new int[0];
				cat.percents = new int[0];
				cat.serverNames = new ArrayList<>();
			}
		}

		private int getServerIndex(DownloadServerType type, String serverName) {
			DownloadServerCategory s = specialties.get(type.key);
			if (s != null) {
				return s.serverNames.indexOf(serverName);
			}
			return -1;
		}

		public int getDownloadCounts(DownloadServerType type, String serverName) {
			int ind = getServerIndex(type, serverName);
			if (ind >= 0) {
				return specialties.get(type.key).counts[ind];
			}
			return 0;
		}
		
		public int getDownloadCounts(String serverName) {
			int sum = 0;
			for (DownloadServerCategory s : specialties.values()) {
				if (s != null) {
					int i = s.serverNames.indexOf(serverName);
					if (i >= 0) {
						sum += s.counts[i];
					}
				}
			}
			return sum;
		}

		public int getPercent(DownloadServerType type, String serverName) {
			int ind = getServerIndex(type, serverName);
			if (ind >= 0 && specialties.get(type.key) != null) {
				return specialties.get(type.key).percents[ind];
			}
			return 0;
		}

		public String getName() {
			return name;
		}
		
	}
	
	public static class DownloadFreeMapsConfig {
		
		public String message;
		public List<String> namepatterns = new ArrayList<String>();
	}
	
	public static class DownloadServerFilterByHeaders {
		String specialty;
		List<String> userAgents = new ArrayList<String>();
		// after init
		DownloadServerType mainType;
	}
	
	public static class DownloadServerLoadBalancer {
		public final static String SELF = "self";
				
		List<DownloadServerType> serverTypes = new ArrayList<DownloadServerType>();
		
		// provided from settings.json
		Map<String, DownloadServerFilterByHeaders> filterTypes = new TreeMap<>();
		Map<String, Map<String, Integer>> servers = new TreeMap<>();
		List<DownloadServerRegion> regions = new ArrayList<>();
		
		DownloadFreeMapsConfig freemaps = new DownloadFreeMapsConfig();
		DownloadServerRegion globalRegion = new DownloadServerRegion();

		
		public DownloadFreeMapsConfig getFreemaps() {
			return freemaps;
		}
		
		public DownloadServerType getServerType(PredefinedServerSpecialty type, String userAgent) {
			for (DownloadServerFilterByHeaders filter : filterTypes.values()) {
				if (filter.mainType.type == type) {
					for (String us : filter.userAgents) {
						if (userAgent.contains(us)) {
							return filter.mainType;
						}
					}
				}
			}
			for (DownloadServerType c : serverTypes) {
				if (c.type == type) {
					return c;
				}
			}
			throw new IllegalStateException();
		}

		public void prepare() {
			for (DownloadServerRegion region : regions) {
				region.prepare();
			}
			serverTypes.clear();
			for(PredefinedServerSpecialty p : PredefinedServerSpecialty.values()) {
				DownloadServerType serverType = new DownloadServerType();
				serverType.type = p;
				serverType.key = p.name().toLowerCase();
				serverTypes.add(serverType);
			}
			Iterator<Entry<String, DownloadServerFilterByHeaders>> it = filterTypes.entrySet().iterator();
			while(it.hasNext()) {
				Entry<String, DownloadServerFilterByHeaders> e = it.next();
				DownloadServerFilterByHeaders filter  = e.getValue();
				PredefinedServerSpecialty ps = PredefinedServerSpecialty.valueOf(filter.specialty.toUpperCase());
				DownloadServerType serverType = new DownloadServerType();
				serverType.type = ps;
				serverType.key = e.getKey();
				filter.mainType = serverType;
				serverTypes.add(serverType);
				
			}
			for (DownloadServerType serverType : serverTypes) {
				Map<String, Integer> numbers = servers.get(serverType.key);
				if (numbers == null) {
					numbers = Collections.emptyMap();
				}
				for (String serverName : numbers.keySet()) {
					if (!globalRegion.servers.contains(serverName)) {
						globalRegion.servers.add(serverName);
					}
				}
				globalRegion.prepare(serverType, numbers);
				for (DownloadServerRegion region : regions) {
					region.prepare(serverType, numbers);
				}
			}
			
		}
		
		public List<DownloadServerType> getServerTypes() {
			return serverTypes;
		}
		
		public List<DownloadServerRegion> getRegions() {
			return regions;
		}
		
		public Collection<String> getServerNames() {
			return globalRegion.servers;
		}
		
		public DownloadServerRegion getGlobalRegion() {
			return globalRegion;
		}
		
		public int getDownloadCounts(DownloadServerType type, String serverName) {
			int sum = globalRegion.getDownloadCounts(type, serverName);
			for (DownloadServerRegion r : regions) {
				sum += r.getDownloadCounts(type, serverName);
			}
			return sum;
		}

		public int getGlobalPercent(DownloadServerType type, String serverName) {
			return globalRegion.getPercent(type, serverName);
			
		}

		public String getServer(DownloadServerType type, String remoteAddr) {
			DownloadServerRegion region = globalRegion;
			// avoid IPv6 ~/:/ as incompatible with matchesIp()
			if (remoteAddr != null && !remoteAddr.contains(":")) {
				for (DownloadServerRegion r : regions) {
					if (r.matchesIp(r.asInteger(remoteAddr))) {
						region = r;
						break;
					}
				}
			}
			DownloadServerCategory cat = region.specialties.get(type.key);
			if (cat != null && cat.sum > 0) {
				ThreadLocalRandom tlr = ThreadLocalRandom.current();
				int val = tlr.nextInt(cat.sum);
				for (int i = 0; i < cat.bounds.length; i++) {
					if (val >= cat.bounds[i]) {
						val -= cat.bounds[i];
					} else {
						String serverName = cat.serverNames.get(i);
						cat.counts[i]++;
						if (SELF.equals(serverName)) {
							return null;
						}
						return serverName;
					}
				}
			}
			return null;
		}

	}
	
	private DownloadIndexDocument unmarshallIndexes(File fl) throws IOException {
		try {
			JAXBContext jc = JAXBContext.newInstance(DownloadIndexDocument.class);
			Unmarshaller unmarshaller = jc.createUnmarshaller();
			DownloadIndexDocument did = (DownloadIndexDocument) unmarshaller.unmarshal(fl);
			did.prepareMaps();
			return did;
		} catch (JAXBException ex) {
			LOGGER.error(ex.getMessage(), ex);
			throw new IOException(ex);
		}
	}

	private void formatName(String name, int extInd) {
        name = name.substring(0, extInd);
        if (name.endsWith("_ext_2")) {
            name = name.replace("_ext_2", "");
        }
        if (name.endsWith("_2")) {
            name = name.replace("_2", "");
        }
        name = name.replace('_', ' ');
    }

    public static class ExternalSource {
	    private String name;
	    private String type;
	    private String mtime;
	    private long size;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getMtime() {
            return mtime;
        }

        public void setMtime(String mtime) {
            this.mtime = mtime;
        }

        public long getSize() {
            return size;
        }

        public void setSize(long size) {
            this.size = size;
        }

        public long getTimestamp() {
            //example Wed, 31 Aug 2022 11:53:18 GMT
            DateFormat format = new SimpleDateFormat("EEE, d MMM yyyy hh:mm:ss zzz");
            try {
                Date date = format.parse(mtime);
                return date.getTime();
            } catch (ParseException e) {
                LOGGER.error("LOAD EXTERNAL INDEXES, problem parse of date: \"" + mtime + "\"" + e.getMessage(), e.getCause());
            }
            return -1;
        }
    }

	public Map<String, Double> getMapSizesCache() {
		if (mapSizesCache.size() == 0) {
			loadDownloadIndexes();
		}
		return mapSizesCache;
	}
}
