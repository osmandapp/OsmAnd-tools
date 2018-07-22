package net.osmand.server.index;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import net.osmand.util.Algorithms;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class DownloadIndexesService  {
	
	private static final Log LOGGER = LogFactory.getLog(DownloadIndexesService.class);

	private static final String INDEX_FILE = "new_indexes.xml";
	
	private String getEnvVar(String key, String df) {
		if(!Algorithms.isEmpty(System.getenv(key))) {
			return System.getenv(key);
		}
		return df;
	}
	
	//@Value("${download.indexes}")
    private String pathToDownloadFiles = getEnvVar("MAPS_DIR", "/var/www-download/");

	// 15 minutes
	@Scheduled(fixedDelay = 1000 * 60 * 15)
	public void checkOsmAndLiveStatus() {
		generateStandardIndexFile();
	}
	
	public DownloadIndexDocument loadDownloadIndexes() {
		DownloadIndexDocument doc = new DownloadIndexDocument();
		List<DownloadIndex> list = new ArrayList<>();
		File rootFolder = new File(pathToDownloadFiles);

		loadIndexesFromDir(list, rootFolder, "indexes", DownloadType.MAP);
		doc.getMaps().addAll(list);
		list.clear();

		loadIndexesFromDir(list, rootFolder, ".", DownloadType.MAP);
		doc.getMaps().addAll(list);
		list.clear();

		loadIndexesFromDir(list, rootFolder, "indexes", DownloadType.VOICE);
		doc.getVoices().addAll(list);
		list.clear();

		loadIndexesFromDir(list, rootFolder, "indexes/fonts", DownloadType.FONTS);
		doc.getFonts().addAll(list);
		list.clear();

		loadIndexesFromDir(list, rootFolder, "indexes/inapp/depth", DownloadType.DEPTH);
		doc.getInapps().addAll(list);
		list.clear();

		loadIndexesFromDir(list, rootFolder, "wiki", DownloadType.WIKIMAP);
		doc.getWikimaps().addAll(list);
		list.clear();

		loadIndexesFromDir(list, rootFolder, "wikivoyage", DownloadType.WIKIVOYAGE);
		doc.getWikivoyages().addAll(list);
		list.clear();

		loadIndexesFromDir(list, rootFolder, "road-indexes", DownloadType.ROAD_MAP);
		doc.getRoadMaps().addAll(list);
		list.clear();

		loadIndexesFromDir(list, rootFolder, "srtm-countries", DownloadType.SRTM_MAP);
		doc.getSrtmMaps().addAll(list);
		list.clear();

		loadIndexesFromDir(list, rootFolder, "hillshade", DownloadType.HILLSHADE);
		doc.getHillshade().addAll(list);
		list.clear();
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
		return new File(pathToDownloadFiles, gzip ? INDEX_FILE + ".gz" : INDEX_FILE);
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
				di.setTimestamp(lf.lastModified());
				di.setDate(lf.lastModified());
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
							String description = entry.getComment();
							if (description != null) {
								di.setDescription(description);
							} else {
								di.setDescription(tp.getDefaultTitle(name));
							}
						}
						zipFile.close();
					} catch (Exception e) {
						LOGGER.error(lf.getName() + ": " + e.getMessage(), e);
						e.printStackTrace();
					}
				} else {
					di.setContentSize(lf.length());
					di.setTargetsize(lf.length());
					di.setDescription(tp.getDefaultTitle(name));
				}
				if(isValid(lf)) {
					list.add(di);
				}
			}
		}
	}

	private boolean isValid(File file) {
		boolean isValid = true;
		if (isZip(file)) {
			try {
				new ZipFile(file);
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
				return f.getName().endsWith(".obf.zip") || f.getName().endsWith(".obf");
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
}
