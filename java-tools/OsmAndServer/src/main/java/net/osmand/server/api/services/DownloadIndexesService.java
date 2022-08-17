package net.osmand.server.api.services;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;

import net.osmand.IndexConstants;
import net.osmand.util.Algorithms;

@Service
public class DownloadIndexesService  {
	
	private static final Log LOGGER = LogFactory.getLog(DownloadIndexesService.class);

	private static final String INDEX_FILE = "indexes.xml";
	private static final String DOWNLOAD_SETTINGS = "api/settings.json";
	
	@Value("${osmand.files.location}")
    private String pathToDownloadFiles;
	
	@Value("${osmand.gen.location}")
	private String pathToGenFiles;
	
	@Value("${osmand.web.location}")
    private String websiteLocation;

	private DownloadProperties settings;

	private Gson gson;
	
	public DownloadIndexesService() {
		gson = new Gson();
	}
	
	public DownloadProperties getSettings() {
		if(settings == null) {
			reloadConfig(new ArrayList<String>());
		}
		return settings;
	}
	
	public boolean reloadConfig(List<String> errors) {
    	try {
    		DownloadProperties s = gson.fromJson(new FileReader(new File(websiteLocation, DOWNLOAD_SETTINGS)), DownloadProperties.class);
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
		loadIndexesFromDir(doc.getWikimaps(), rootFolder, DownloadType.WIKIMAP);
		loadIndexesFromDir(doc.getTravelGuides(), rootFolder, DownloadType.TRAVEL);
		loadIndexesFromDir(doc.getWikivoyages(), rootFolder, DownloadType.WIKIVOYAGE);
		loadIndexesFromDir(doc.getRoadMaps(), rootFolder, DownloadType.ROAD_MAP);
		loadIndexesFromDir(doc.getSrtmMaps(), rootFolder, DownloadType.SRTM_MAP.getPath(), DownloadType.SRTM_MAP, IndexConstants.BINARY_SRTM_MAP_INDEX_EXT);
		loadIndexesFromDir(doc.getSrtmFeetMaps(), rootFolder, DownloadType.SRTM_MAP.getPath(), DownloadType.SRTM_MAP, IndexConstants.BINARY_SRTM_FEET_MAP_INDEX_EXT);
		loadIndexesFromDir(doc.getHillshade(), rootFolder, DownloadType.HILLSHADE);
		loadIndexesFromDir(doc.getSlope(), rootFolder, DownloadType.SLOPE);
		loadIndexesFromDir(doc.getHeightmap(), rootFolder, DownloadType.HEIGHTMAP);
		return doc;
	}
	
	public File getFilePath(String name) throws IOException {
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
		File file = null;
		for (DownloadIndex di : doc.getAllMaps()) {
			if (di.getName().equals(dwName) || di.getName().equals(dwName + ".zip")) {
				file = new File(pathToDownloadFiles, dwName + ".zip");
				if (!file.exists()) {
					file = new File(pathToDownloadFiles, di.getDownloadType().getPath() + "/" + dwName + ".zip");
				}
				if (!file.exists()) {
					file = new File(pathToDownloadFiles, dwName);
				}
				if (!file.exists()) {
					file = new File(pathToDownloadFiles, di.getDownloadType().getPath() + "/" + dwName);
				}
				break;
			}
		}
		if (file != null && file.exists()) {
			return file;
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
		for (File lf : files) {
			if (filterFiles != null && !lf.getName().contains(filterFiles)) {
				continue;
			} else if (type.acceptFile(lf)) {
				String name = lf.getName();
				int extInd = name.indexOf('.');
				String ext = name.substring(extInd + 1);
				name = name.substring(0, extInd);
				if (name.endsWith("_ext_2")) {
					name = name.replace("_ext_2", "");
				}
				if (name.endsWith("_2")) {
					name = name.replace("_2", "");
				}
				name = name.replace('_', ' ');
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
	    FONTS("indexes/fonts") ,
	    WIKIMAP("wiki") ,
	    WIKIVOYAGE("wikivoyage") ,
	    TRAVEL("travel") ,
	    ROAD_MAP("road-indexes") ,
	    HILLSHADE("hillshade"),
	    HEIGHTMAP("heightmap"),
	    SLOPE("slope") ,
	    SRTM_MAP("srtm-countries") ;


		private final String path;

		DownloadType(String path) {
			this.path = path;
		}
		
		public String getPath() {
			return path;
		}


		public boolean acceptFile(File f) {
			switch (this) {
			case WIKIVOYAGE:
			case HEIGHTMAP:
				return f.getName().endsWith(".sqlite");
			case TRAVEL:
				return f.getName().endsWith(".travel.obf.zip") || f.getName().endsWith(".travel.obf");
			case MAP:
			case ROAD_MAP:
			case WIKIMAP:
			case DEPTH:
			case SRTM_MAP:
				return f.getName().endsWith(".obf.zip") || f.getName().endsWith(".obf") || f.getName().endsWith(".extra.zip");
			case HILLSHADE:
			case SLOPE:
				return f.getName().endsWith(".sqlitedb");
			case FONTS:
				return f.getName().endsWith(".otf.zip");
			case VOICE:
				return f.getName().endsWith(".voice.zip");
				
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
				return String.format("Depth contours for %s", regionName);
			case SRTM_MAP:
				String suf = ext.contains("srtmf") ? "feet" : "meters";
				return String.format("Contour lines (%s) for %s", suf, regionName);
			case TRAVEL:
				return String.format("Travel for %s", regionName);
			case WIKIVOYAGE:
				return String.format("Wikivoyage for %s", regionName);
			case HEIGHTMAP:
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
	
	
	public static void main(String[] args) {
		// small test
		DownloadProperties dp = new DownloadProperties();
		String key = DownloadServerSpecialty.OSMLIVE.toString().toLowerCase();
		dp.servers.put(key, new HashMap<>());
		dp.servers.get(key).put("dl1", 1);
		dp.servers.get(key).put("dl2", 1);
		dp.servers.get(key).put("dl3", 3);
		dp.prepare();
		System.out.println(dp.getPercent(DownloadServerSpecialty.OSMLIVE, "dl1"));
		System.out.println(dp.getPercent(DownloadServerSpecialty.OSMLIVE, "dl2"));
		System.out.println(dp.getPercent(DownloadServerSpecialty.OSMLIVE, "dl3"));
		Map<String, Integer> cnts = new TreeMap<String, Integer>();
		for(String s : dp.serverNames) {
			cnts.put(s, 0);
		}
		for(int i = 0; i < 1000; i ++) {
			String s = dp.getServer(DownloadServerSpecialty.OSMLIVE);
			cnts.put(s, cnts.get(s) + 1);
		}
		System.out.println(cnts);
	}
	
	private static class DownloadServerCategory {
	
		Map<String, Integer> percents = new TreeMap<>();
		int sum;
		String[] serverNames;
		int[] bounds;
	}
	
	public enum DownloadServerSpecialty {
		SRTM,
		HILLSHADE,
		SLOPE,
		HEIGHTMAP,
		OSMLIVE,
		MAIN,
		WIKI,
		ROADS
		
	}
	
	public static class DownloadProperties {
		public final static String SELF = "self";
		
		Set<String> serverNames = new TreeSet<String>();
		DownloadServerCategory[] cats = new DownloadServerCategory[DownloadServerSpecialty.values().length];
		Map<String, Map<String, Integer>> servers = new TreeMap<>();
		
		public void prepare() {
			for(DownloadServerSpecialty s : DownloadServerSpecialty.values()) {
				Map<String, Integer> mp = servers.get(s.name().toLowerCase());
				prepare(s, mp == null ? Collections.emptyMap() : mp);
			}
		}
		
		public Set<String> getServers() {
			return serverNames;
		}
		
		private void prepare(DownloadServerSpecialty tp, Map<String, Integer> mp) {
			serverNames.addAll(mp.keySet());
			DownloadServerCategory cat = new DownloadServerCategory();
			cats[tp.ordinal()] = cat;
			for(Integer i : mp.values()) {
				cat.sum += i;
			}
			if(cat.sum > 0) {
				int ind = 0;
				cat.bounds = new int[mp.size()];
				cat.serverNames = new String[mp.size()];
				for(String server : mp.keySet()) {
					cat.serverNames[ind] = SELF.equals(server) ? null : server;
					cat.bounds[ind] = mp.get(server);
					cat.percents.put(server, 100 * mp.get(server) / cat.sum);
					ind++;
				}
			} else {
				cat.bounds = new int[0];
				cat.serverNames = new String[0];
			}
			
		}

		public int getPercent(DownloadServerSpecialty type, String s) {
			Integer p = cats[type.ordinal()].percents.get(s);
			if(p == null) {
				return 0;
			}
			return p.intValue();
		}
		
		
		public String getServer(DownloadServerSpecialty type) {
			DownloadServerCategory cat = cats[type.ordinal()];
			if (cat.sum > 0) {
				ThreadLocalRandom tlr = ThreadLocalRandom.current();
				int val = tlr.nextInt(cat.sum);
				for(int i = 0; i < cat.bounds.length; i++) {
					if(val >= cat.bounds[i]) {
						val -= cat.bounds[i];
					} else {
						return cat.serverNames[i];
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
}
