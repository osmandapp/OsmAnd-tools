package net.osmand.server.api.services;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javax.annotation.PostConstruct;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import net.osmand.util.Algorithms;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

@Service
public class DownloadIndexesService  {
	
	private static final Log LOGGER = LogFactory.getLog(DownloadIndexesService.class);

	private static final String INDEX_FILE = "indexes.xml";
	private static final String DOWNLOAD_SETTINGS = "api/settings.json";
	
	@Value("${files.location}")
    private String pathToDownloadFiles;
	
	@Value("${gen.location}")
	private String pathToGenFiles;
	
	@Value("${web.location}")
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
    		settings = gson.fromJson(new FileReader(new File(websiteLocation, DOWNLOAD_SETTINGS)), DownloadProperties.class);
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
		loadIndexesFromDir(doc.getMaps(), rootFolder, "indexes", DownloadType.MAP);
		loadIndexesFromDir(doc.getMaps(), rootFolder, ".", DownloadType.MAP);
		loadIndexesFromDir(doc.getVoices(), rootFolder, "indexes", DownloadType.VOICE);
		loadIndexesFromDir(doc.getFonts(), rootFolder, "indexes/fonts", DownloadType.FONTS);
		loadIndexesFromDir(doc.getInapps(), rootFolder, "indexes/inapp/depth", DownloadType.DEPTH);
		loadIndexesFromDir(doc.getWikimaps(), rootFolder, "wiki", DownloadType.WIKIMAP);
		loadIndexesFromDir(doc.getWikivoyages(), rootFolder, "wikivoyage", DownloadType.WIKIVOYAGE);
		loadIndexesFromDir(doc.getRoadMaps(), rootFolder, "road-indexes", DownloadType.ROAD_MAP);
		loadIndexesFromDir(doc.getSrtmMaps(), rootFolder, "srtm-countries", DownloadType.SRTM_MAP);
		loadIndexesFromDir(doc.getHillshade(), rootFolder, "hillshade", DownloadType.HILLSHADE);
		return doc;
	}
	
	public File getIndexesXml(boolean upd, boolean gzip) {
		File target = getStandardFilePath(gzip);
		if(!target.exists() || upd) {
			generateStandardIndexFile();
		}
		return target;
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

	private void loadIndexesFromDir(List<DownloadIndex> list, File rootFolder, String subPath, DownloadType tp) {
		File subFolder = new File(rootFolder, subPath);
		File[] files = subFolder.listFiles();
		if(files == null || files.length == 0) {
			return;
		}
		for(File lf : files) {
			if(tp.acceptFile(lf)) {
				String name = lf.getName();
				name = name.substring(0, name.indexOf('.'));
				if (name.endsWith("_ext_2")) {
					name = name.replace("_ext_2", "");
				}
				if (name.endsWith("_2")) {
					name = name.replace("_2", "");
				}
				name = name.replace('_', ' ');
				DownloadIndex di = new DownloadIndex();
				di.setType(tp);
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
								di.setDescription(tp.getDefaultTitle(name));
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
					di.setDescription(tp.getDefaultTitle(name));
					list.add(di);
				}
			}
		}
	}

	private boolean isValid(File file) {
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
	    MAP ("region"),
	    VOICE ("region"),
	    DEPTH ("inapp"),
	    FONTS ("fonts"),
	    WIKIMAP ("wiki"),
	    WIKIVOYAGE ("wikivoyage"),
	    ROAD_MAP ("road_region"),
	    HILLSHADE ("hillshade"),
	    SRTM_MAP ("srtmcountry");

	    private final String xmlTag;

		private DownloadType(String xmlTag) {
			this.xmlTag = xmlTag;
	    }
		
		public String getXmlTag() {
			return xmlTag;
		}

		public boolean acceptFile(File f) {
			switch (this) {
			case MAP:
			case ROAD_MAP:
			case WIKIMAP:
			case DEPTH:
			case SRTM_MAP:
				return f.getName().endsWith(".obf.zip") || f.getName().endsWith(".obf") || f.getName().endsWith(".extra.zip");
			case WIKIVOYAGE:
				return f.getName().endsWith(".sqlite");
			case HILLSHADE:
				return f.getName().endsWith(".sqlitedb");
			case FONTS:
				return f.getName().endsWith(".otf.zip");
			case VOICE:
				return f.getName().endsWith(".voice.zip");
				
			}
			return false;
		}
	    
		
		public String getDefaultTitle(String regionName) {
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
				return String.format("Contour lines for %s", regionName);
			case WIKIVOYAGE:
				return String.format("Wikivoyage for %s", regionName);
			case HILLSHADE:
				return String.format("Hillshade for %s", regionName);
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
	
	public static class DownloadProperties {
		public List<String> osmlive = new ArrayList<>();
		public List<String> srtm = new ArrayList<>();
		public List<String> main = new ArrayList<>();
		

	}
}
