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
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
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
		loadIndexesFromDir(doc.getWikimaps(), rootFolder, DownloadType.WIKIMAP);
		loadIndexesFromDir(doc.getTravelGuides(), rootFolder, DownloadType.TRAVEL);
		loadIndexesFromDir(doc.getRoadMaps(), rootFolder, DownloadType.ROAD_MAP);
		loadIndexesFromDir(doc.getSrtmMaps(), rootFolder, DownloadType.SRTM_MAP.getPath(), DownloadType.SRTM_MAP, IndexConstants.BINARY_SRTM_MAP_INDEX_EXT);
		loadIndexesFromDir(doc.getSrtmFeetMaps(), rootFolder, DownloadType.SRTM_MAP.getPath(), DownloadType.SRTM_MAP, IndexConstants.BINARY_SRTM_FEET_MAP_INDEX_EXT);
		loadIndexesFromDir(doc.getHillshade(), rootFolder, DownloadType.HILLSHADE);
		loadIndexesFromDir(doc.getSlope(), rootFolder, DownloadType.SLOPE);
		loadIndexesFromDir(doc.getHeightmap(), rootFolder, DownloadType.HEIGHTMAP);
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
				DownloadServerSpecialty sp = DownloadServerSpecialty.getSpecialtyByDownloadType(di.getDownloadType());
				if (sp != null) {
					String host = servers.getServer(sp, null);
					if (!Algorithms.isEmpty(host)) {
						try {
							String pm = "";
							if (sp.httpParams.length > 0) {
								pm = "&" + sp.httpParams[0] + "=yes";
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
                                int extInd = name.indexOf('.');
                                String ext = name.substring(extInd + 1);
                                formatName(name, extInd);
                                di.setName(name);
                                di.setSize(source.size);
                                di.setContainerSize(source.size);
                                di.setTimestamp(source.getTimestamp());
                                di.setDate(source.getTimestamp());
                                di.setContentSize(source.size);
                                di.setTargetsize(source.size);
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
					di.setTimestamp(lf.lastModified());
					di.setDate(lf.lastModified());
					di.setContentSize(lf.length());
					di.setTargetsize(lf.length());
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
	    VOICE("indexes") ,
	    DEPTH("indexes/inapp/depth") ,
	    DEPTHMAP("depth") ,
	    FONTS("indexes/fonts") ,
	    WIKIMAP("wiki") ,
	    TRAVEL("travel") ,
	    ROAD_MAP("road-indexes") ,
	    HILLSHADE("hillshade"),
	    HEIGHTMAP("heightmap"),
	    GEOTIFF("heightmap"),
	    SLOPE("slope") ,
	    SRTM_MAP("srtm-countries") ;


		private final String path;

		DownloadType(String path) {
			this.path = path;
		}
		
		public String getPath() {
			return path;
		}


        public boolean acceptFileName(String fileName) {
            switch (this) {
                case HEIGHTMAP:
                    return fileName.endsWith(".sqlite");
                case GEOTIFF:
                    return fileName.endsWith(".tif");
                case TRAVEL:
                    return fileName.endsWith(".travel.obf.zip") || fileName.endsWith(".travel.obf");
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
			case DEPTH:
			case DEPTHMAP:
				return String.format("Depth maps for %s", regionName);
			case SRTM_MAP:
				String suf = ext.contains("srtmf") ? "feet" : "meters";
				return String.format("Contour lines (%s) for %s", suf, regionName);
			case TRAVEL:
				return String.format("Travel for %s", regionName);
			case HEIGHTMAP:
				return String.format("%s", regionName);
			case GEOTIFF:
				return String.format("%s", regionName);
			case HILLSHADE:
				return String.format("%s", regionName);
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
		DownloadServerSpecialty type = DownloadServerSpecialty.MAIN;
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
				System.out.println(reg.getName() + " " + reg.getDownloadCounts(type, serverName));
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
	
	public enum DownloadServerSpecialty {
		MAIN(new String[0], DownloadType.VOICE, DownloadType.FONTS, DownloadType.MAP),
		SRTM("srtmcountry", DownloadType.SRTM_MAP),
		HILLSHADE("hillshade", DownloadType.HILLSHADE),
		SLOPE("slope", DownloadType.SLOPE),
		HEIGHTMAP("heightmap", DownloadType.HEIGHTMAP, DownloadType.GEOTIFF),
		OSMLIVE(new String[] {"aosmc", "osmc"}, DownloadType.MAP),
		DEPTH("depth", DownloadType.DEPTH, DownloadType.DEPTHMAP),
		WIKI(new String[] {"wikivoyage", "wiki", "travel"}, DownloadType.WIKIMAP, DownloadType.TRAVEL),
		ROADS("road", DownloadType.ROAD_MAP);
		
		public final DownloadType[] types;
		public final String[] httpParams;

		DownloadServerSpecialty(String httpParam, DownloadType... tp) {
			this.httpParams = new String[] {httpParam};
			this.types = tp;
		}
		
		DownloadServerSpecialty(String[] httpParams, DownloadType... tp) {
			this.httpParams = httpParams;
			this.types = tp;
		}
		
		public static DownloadServerSpecialty getSpecialtyByDownloadType(DownloadType c) {
			for(DownloadServerSpecialty s : values())  {
				if(s.types == null) {
					continue;
				}
				for(DownloadType t : s.types) {
					if(t == c) {
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
		DownloadServerCategory[] specialties = new DownloadServerCategory[DownloadServerSpecialty.values().length];
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

		private void prepare(DownloadServerSpecialty tp, Map<String, Integer> mp) {
			DownloadServerCategory cat = new DownloadServerCategory();
			specialties[tp.ordinal()] = cat;

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

		private int getServerIndex(DownloadServerSpecialty type, String serverName) {
			DownloadServerCategory s = specialties[type.ordinal()];
			if (s != null) {
				return s.serverNames.indexOf(serverName);
			}
			return -1;
		}

		public int getDownloadCounts(DownloadServerSpecialty type, String serverName) {
			int ind = getServerIndex(type, serverName);
			if (ind >= 0) {
				return specialties[type.ordinal()].counts[ind];
			}
			return 0;
		}
		
		public int getDownloadCounts(String serverName) {
			int sum = 0;
			for (DownloadServerCategory s : specialties) {
				if (s != null) {
					int i = s.serverNames.indexOf(serverName);
					if (i >= 0) {
						sum += s.counts[i];
					}
				}
			}
			return sum;
		}

		public int getPercent(DownloadServerSpecialty type, String serverName) {
			int ind = getServerIndex(type, serverName);
			if (ind >= 0 && specialties[type.ordinal()] != null) {
				return specialties[type.ordinal()].percents[ind];
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
	
	public static class DownloadServerLoadBalancer {
		public final static String SELF = "self";
				
		// provided from settings.json
		Map<String, Map<String, Integer>> servers = new TreeMap<>();
		List<DownloadServerRegion> regions = new ArrayList<>();
		DownloadFreeMapsConfig freemaps = new DownloadFreeMapsConfig();
		DownloadServerRegion globalRegion = new DownloadServerRegion();

		
		
		public DownloadFreeMapsConfig getFreemaps() {
			return freemaps;
		}
		
		public void prepare() {
			for (DownloadServerRegion region : regions) {
				region.prepare();
			}
			for (DownloadServerSpecialty s : DownloadServerSpecialty.values()) {
				Map<String, Integer> mp = servers.get(s.name().toLowerCase());
				for (String serverName : mp.keySet()) {
					if (!globalRegion.servers.contains(serverName)) {
						globalRegion.servers.add(serverName);
					}
				}
				globalRegion.prepare(s, mp);
				for (DownloadServerRegion region : regions) {
					region.prepare(s, mp);
				}
			}
			
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
		
		public int getDownloadCounts(DownloadServerSpecialty type, String serverName) {
			int sum = globalRegion.getDownloadCounts(type, serverName);
			for (DownloadServerRegion r : regions) {
				sum += r.getDownloadCounts(type, serverName);
			}
			return sum;
		}

		public int getGlobalPercent(DownloadServerSpecialty type, String serverName) {
			return globalRegion.getPercent(type, serverName);
			
		}
		
		
		public String getServer(DownloadServerSpecialty type, String remoteAddr) {
			DownloadServerRegion region = globalRegion;
			if (remoteAddr != null) {
				for (DownloadServerRegion r : regions) {
					if (r.matchesIp(r.asInteger(remoteAddr))) {
						region = r;
						break;
					}
				}
			}
			DownloadServerCategory cat = region.specialties[type.ordinal()];
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
