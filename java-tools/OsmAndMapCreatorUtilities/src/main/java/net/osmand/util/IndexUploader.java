package net.osmand.util;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import org.apache.commons.logging.Log;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.osmand.IndexConstants;
import net.osmand.PlatformUtil;
import net.osmand.ResultMatcher;
import net.osmand.binary.BinaryIndexPart;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.binary.BinaryMapIndexReader;
import net.osmand.binary.BinaryMapIndexReader.MapIndex;
import net.osmand.binary.BinaryMapIndexReader.MapRoot;
import net.osmand.binary.BinaryMapIndexReader.SearchFilter;
import net.osmand.binary.BinaryMapIndexReader.SearchRequest;
import net.osmand.binary.BinaryMapIndexReader.TagValuePair;
import net.osmand.binary.OsmandOdb;
import net.osmand.obf.BinaryMerger;
import net.osmand.obf.preparation.AbstractIndexPartCreator;
import net.osmand.obf.preparation.BinaryFileReference;
import net.osmand.obf.preparation.BinaryMapIndexWriter;
import net.osmand.obf.preparation.IndexVectorMapCreator;
import rtree.LeafElement;
import rtree.RTree;
import rtree.RTreeException;
import rtree.Rect;

/**
 * This helper will find obf and zip files, create description for them, and zip them, or update the description. This
 * helper also can upload files through ssh,ftp or to googlecode.
 *
 * IndexUploader dir targetDir [--ff=file] [--fp=ptns] [--ep=ptns] [--dp=ptns] [-ssh|-ftp|-google]
 * [--password=|--user=|--url=|--path=|--gpassword=|--privKey=|--knownHosts=] --ff file with names of files to be
 * uploaded from the dir, supporting regexp --fp comma separated names of files to be uploaded from the dir, supporting
 * regexp --ep comma separated names of files to be excluded from upload, supporting regexp --dp comma separated names
 * of files to be delete on remote system, supporting regexp One of: -ssh to upload to ssh site -fpt to upload to ftp
 * site -google to upload to googlecode of osmand Additional params: --user user to use for ftp,ssh,google --password
 * password to use for ftp,ssh,google (gmail password to retrieve tokens for deleting files) --url url to use for
 * ssh,ftp --path path in url to use for ssh,ftp --gpassword googlecode password to use for google --privKey priv key
 * for ssh --knownHosts known hosts for ssh
 *
 * @author Pavol Zibrita <pavol.zibrita@gmail.com>
 */
public class IndexUploader {

	protected static final Log log = PlatformUtil.getLog(IndexUploader.class);
	private final static double MIN_SIZE_TO_UPLOAD = 0.001d;

	private final static int BUFFER_SIZE = 1 << 15;
	private final static int MB = 1 << 20;

	/**
	 * Something bad have happened
	 */
	public static class IndexUploadException extends Exception {

		private static final long serialVersionUID = 2343219168909577070L;

		public IndexUploadException(String message) {
			super(message);
		}

		public IndexUploadException(String message, Throwable e) {
			super(message, e);
		}

	}

	/**
	 * Processing of one file failed, but other files could continue
	 */
	public static class OneFileException extends Exception {

		private static final long serialVersionUID = 6463200194419498979L;

		public OneFileException(String message) {
			super(message);
		}

		public OneFileException(String message, Exception e) {
			super(message, e);
		}

	}

	private File directory;
	private File targetDirectory;
	private UploadCredentials uploadCredentials = new DummyCredentials();
	private FileFilter fileFilter = new FileFilter();
	private FileFilter deleteFileFilter = null;
	
	private boolean roadProcess;
	private boolean wikiProcess;
	private boolean srtmProcess;
	private boolean travelProcess;
	private boolean depthProcess;
	private boolean mapsProcess;
	private int numberOfThreads = 2;

	public IndexUploader(String path, String targetPath) throws IndexUploadException {
		directory = new File(path);
		if (!directory.isDirectory()) {
			throw new IndexUploadException("Not a directory:" + path);
		}
		targetDirectory = new File(targetPath);
		if (!targetDirectory.isDirectory()) {
			throw new IndexUploadException("Not a directory:" + targetPath);
		}
	}

	public static void main(String[] args) throws IOException, IndexUploadException, RTreeException {
//		if (true) {
//			File src = new File("/Users/victorshcherb/osmand/temp/Luxembourg_europe_2.obf");
//			File dest = new File("/Users/victorshcherb/osmand/maps/Luxembourg_europe_2.road.obf");
//			IndexUploader.extractRoadOnlyFile(src, dest);
//			return;
//		}
		try {
			String srcPath = extractDirectory(args, 0);
			String targetPath = srcPath;
			if (args.length > 1) {
				targetPath = extractDirectory(args, 1);
			}
			IndexUploader indexUploader = new IndexUploader(srcPath, targetPath);
			if (args.length > 2) {
				indexUploader.parseParameters(args, 2);
			}
			indexUploader.run();
		} catch (IndexUploadException e) {
			log.error(e.getMessage());
		}
	}

	private void parseParameters(String[] args, int start) throws IndexUploadException {
		int idx = parseFilter(args, start);
		if (args.length > idx) {
			parseUploadCredentials(args, idx);
		}
	}

	private int parseFilter(String[] args, int start) throws IndexUploadException {
		FileFilter fileFilter = null;
		int p;
		do {
			p = start;
			if (args[start].startsWith("--ff=")) {
				try {
					if (fileFilter == null) {
						fileFilter = new FileFilter();
					}
					fileFilter.parseFile(args[start].substring("--ff=".length()));
				} catch (IOException e) {
					throw new IndexUploadException(e.getMessage());
				}
				start++;
			} else if (args[start].startsWith("--fp=")) {
				if (fileFilter == null) {
					fileFilter = new FileFilter();
				}
				fileFilter.parseCSV(args[start].substring("--fp=".length()));
				start++;
			} else if (args[start].startsWith("--ep=")) {
				if (fileFilter == null) {
					throw new NullPointerException();
				}
				fileFilter.parseExcludeCSV(args[start].substring("--ep=".length()));
				start++;
			} else if (args[start].startsWith("--dp=")) {
				deleteFileFilter = new FileFilter();
				deleteFileFilter.parseCSV(args[start].substring("--dp=".length()));
				start++;
			} else if (args[start].startsWith("--roads")) {
				roadProcess = true;
				start++;
			} else if (args[start].startsWith("--nt=")) {
				numberOfThreads = Integer.parseInt(args[start].substring("--nt=".length()));
				start++;
			} else if (args[start].startsWith("--wiki")) {
				wikiProcess = true;
				start++;
			} else if (args[start].startsWith("--srtm")) {
				srtmProcess = true;
				start++;
			} else if (args[start].startsWith("--travel")) {
				travelProcess = true;
				start++;
			} else if (args[start].startsWith("--depth")) {
				depthProcess = true;
				start++;
			} else if (args[start].startsWith("--maps")) {
				mapsProcess = true;
				start++;
			}
		} while (p != start);
		if (fileFilter != null) {
			this.fileFilter = fileFilter;
		}
		return start;
	}

	protected void parseUploadCredentials(String[] args, int start) throws IndexUploadException {
		if ("-ssh".equals(args[start])) {
			uploadCredentials = new UploadSSHCredentials();
		} else {
			return;
		}
		for (int i = start + 1; i < args.length; i++) {
			uploadCredentials.parseParameter(args[i]);
		}
	}

	public void setUploadCredentials(UploadCredentials uploadCredentials) {
		this.uploadCredentials = uploadCredentials;
	}

	protected static class FileFilter {

		private List<Pattern> matchers = null;
		private List<Pattern> exculdeMatchers = null;

		public void parseCSV(String patterns) {
			String[] s = patterns.split(",");
			if (matchers == null) {
				matchers = new ArrayList<Pattern>();
			}
			for (String p : s) {
				matchers.add(Pattern.compile(p.trim()));
			}
		}

		public void parseExcludeCSV(String patterns) {
			String[] s = patterns.split(",");
			if (exculdeMatchers == null) {
				exculdeMatchers = new ArrayList<Pattern>();
			}
			for (String p : s) {
				exculdeMatchers.add(Pattern.compile(p.trim()));
			}
		}

		public void parseFile(String file) throws IOException {
			File f = new File(file);
			if (f.exists()) {
				readPatterns(f);
			}
		}

		private void readPatterns(File f) throws IOException {
			BufferedReader reader = new BufferedReader(new FileReader(f));
			try {
				String line;
				if (matchers == null) {
					matchers = new ArrayList<Pattern>();
				}
				while ((line = reader.readLine()) != null) {
					matchers.add(Pattern.compile(line));
				}
			} finally {
				Algorithms.closeStream(reader);
			}
		}

		public boolean patternMatches(String name) {
			return internalPatternMatches(name, matchers);
		}

		public boolean internalPatternMatches(String name, List<Pattern> matchers) {
			if (matchers == null) {
				return true;
			}
			for (Pattern p : matchers) {
				if (p.matcher(name).matches()) {
					return true;
				}
			}
			return false;
		}

		protected boolean fileCanBeUploaded(File f) {
			if (!f.isFile() || f.getName().endsWith(IndexBatchCreator.GEN_LOG_EXT)) {
				return false;
			}
			boolean matches = internalPatternMatches(f.getName(), matchers);
			if (matches && exculdeMatchers != null && internalPatternMatches(f.getName(), exculdeMatchers)) {
				return false;
			}
			return matches;
		}
	}

	public void run() throws IndexUploadException, IOException, RTreeException {
		// take files before whole upload process
		try {
			uploadCredentials.connect();
			File[] listFiles = directory.listFiles();
			ExecutorService service = Executors.newFixedThreadPool(numberOfThreads);
			for (File f : listFiles) {
				if (checkFileNeedToBeUploaded(f)) {
					service.submit(new Runnable() {
						@Override
						public void run() {
							processFile(f);
						}
					});
				}
			}
			log.error("Stopping the tasks queue...");
			service.shutdown();
			log.error("Waiting termination all tasks...");
			service.awaitTermination(24, TimeUnit.HOURS);
			if (deleteFileFilter != null) {
				log.error("Delete file filter is not supported with this credentions (method) " + uploadCredentials);
			}
			log.error("All tasks has finished.");
		} catch (InterruptedException e) {
			log.error("Await failed: " + e.getMessage(), e);
		} finally {
			uploadCredentials.disconnect();
		}
	}

	private void processFile(File f) {
		try {
			String fileName = f.getName();
			File unzippedFolder = unzip(f);
			File mainFile = unzippedFolder;
			if (unzippedFolder.isDirectory()) {
				for (File tf : unzippedFolder.listFiles()) {
					if (tf.getName().endsWith(IndexConstants.BINARY_MAP_INDEX_EXT)) {
						mainFile = tf;
						break;
					}
				}
			}
			boolean skip = false;
			try {
				if (mapsProcess) {
					boolean worldFile = fileName.toLowerCase().contains("basemap") || fileName.toLowerCase().contains("world");
					if (!worldFile && !fileName.contains("_ext_")) {
						extractRoadOnlyFile(mainFile, new File(directory, fileName.replace(IndexConstants.BINARY_MAP_INDEX_EXT,
								IndexConstants.BINARY_ROAD_MAP_INDEX_EXT)));
					}
				}
				String description = checkfileAndGetDescription(mainFile);
				long timestampCreated = mainFile.lastModified();
				if (description == null) {
					log.info("Skip file empty description " + f.getName());
					skip = true;
				} else {
					File zFile = new File(f.getParentFile(), unzippedFolder.getName() + ".zip");
					zip(unzippedFolder, zFile, description, timestampCreated);
					uploadIndex(f, zFile, description, uploadCredentials);
				}
			} finally {
				if (!skip) {
					if (!f.getName().equals(unzippedFolder.getName())
							|| (targetDirectory != null && !targetDirectory.equals(directory))) {
						Algorithms.removeAllFiles(unzippedFolder);
					}
				}
			}
		} catch (OneFileException | IOException | RuntimeException | RTreeException e) {
			log.error(f.getName() + ": " + e.getMessage(), e);
		}
	}

	private boolean checkFileNeedToBeUploaded(File f) {
		if (!fileFilter.fileCanBeUploaded(f)) {
			return false;
		}
		long timestampCreated = f.lastModified();
		if (!uploadCredentials.checkIfUploadNeededByTimestamp(f.getName(), timestampCreated)) {
			log.info("File skipped because timestamp was not changed " + f.getName());
			return false;
		}
		String fileName = f.getName();
		log.info("Process file " + f.getName());
		boolean skip = false;
		if ((fileName.contains(".srtm") || fileName.contains(".srtmf")) != this.srtmProcess) {
			skip = true;
		} else if (fileName.contains(".road") != this.roadProcess) {
			skip = true;
		} else if (fileName.contains(".wiki") != this.wikiProcess) {
			skip = true;
		} else if (fileName.contains(".travel") != this.travelProcess) {
			skip = true;
		} else if (fileName.contains(".depth") != this.depthProcess) {
			skip = true;
		}
		if (skip) {
			log.info("Skip file: " + f.getName());
			return false;
		}
		return true;
	}

	public static File zip(File folder, File zFile, String description, long lastModifiedTime) throws OneFileException {
		try {
			ZipOutputStream zout = new ZipOutputStream(new FileOutputStream(zFile));
			zout.setLevel(9);
			Collection<File> lfs = folder.isFile() ? Collections.singleton(folder) : Arrays.asList(folder.listFiles());
			for (File f : lfs) {
				log.info("Zipping to file:" + zFile.getName() + " with desc:" + description);
				putZipEntry(description, "", lastModifiedTime, zout, f);
			}
			Algorithms.closeStream(zout);
			zFile.setLastModified(lastModifiedTime);
		} catch (IOException e) {
			throw new OneFileException("cannot zip file:" + e.getMessage());
		}
		return zFile;
	}

	private static void putZipEntry(String description, String parentEntry, long lastModifiedTime,
			ZipOutputStream zout, File f) throws IOException, FileNotFoundException {
		if (f.isDirectory()) {
			for (File lf : f.listFiles()) {
				putZipEntry(description, parentEntry + f.getName() + "/", lastModifiedTime, zout, lf);
			}
		} else {
			log.info("Zipping file:" + f.getName() + " with desc:" + description);
			ZipEntry zEntry = new ZipEntry(parentEntry + f.getName());
			zEntry.setSize(f.length());
			zEntry.setComment(description);
			zEntry.setTime(lastModifiedTime / 1000 * 1000); // ZIP does not support milliseconds (format ID 0x5455)
			zout.putNextEntry(zEntry);
			FileInputStream is = new FileInputStream(f);
			Algorithms.streamCopy(is, zout);
			Algorithms.closeStream(is);
		}
	}

	private String checkfileAndGetDescription(File mainFile) throws OneFileException, IOException, RTreeException {
		String fileName = mainFile.getName();
		
		if (fileName.endsWith(IndexConstants.BINARY_MAP_INDEX_EXT)) {
			RandomAccessFile raf = null;
			try {
				if (mainFile.length() > Integer.MAX_VALUE) {
					throw new OneFileException(String.format("File size exceeds maximum supported - %s %d MB", mainFile.getName(),
							mainFile.length() >> 20));
				}
				raf = new RandomAccessFile(mainFile, "r");
				BinaryMapIndexReader reader = new BinaryMapIndexReader(raf, mainFile);
				if (reader.getVersion() != IndexConstants.BINARY_MAP_VERSION) {
					throw new OneFileException("Uploader version is not compatible " + reader.getVersion()
							+ " to current " + IndexConstants.BINARY_MAP_VERSION);
				}
				String summary = getDescription(reader, fileName);
				reader.close();
				boolean setLastModified = mainFile.setLastModified(reader.getDateCreated());
				if (!setLastModified) {
					// it's possible we can't update timestamp due to permission missing
					File copy = new File(mainFile.getAbsolutePath() + ".cp");
					Algorithms.fileCopy(mainFile, copy);
					mainFile.delete();
					copy.renameTo(mainFile);
					setLastModified = mainFile.setLastModified(reader.getDateCreated());
				}
				return summary;
			} catch (IOException e) {
				if (raf != null) {
					try {
						raf.close();
					} catch (IOException e1) {
					}
				}
				throw new OneFileException("Reader could not read the index: " + e.getMessage());
			}
		} else {
			throw new OneFileException("Not supported file format " + fileName);
		}
	}

	// synchronized is not needed as R-tree now works fine in multithread 
	public static void extractRoadOnlyFile(File mainFile, File roadOnlyFile) throws IOException, RTreeException {
		RandomAccessFile raf = new RandomAccessFile(mainFile, "r");
		BinaryMapIndexReader index = new BinaryMapIndexReader(raf, mainFile);
		final RandomAccessFile routf = new RandomAccessFile(roadOnlyFile, "rw");
		routf.setLength(0);
		CodedOutputStream ous = CodedOutputStream.newInstance(new OutputStream() {
			@Override
			public void write(int b) throws IOException {
				routf.write(b);
			}

			@Override
			public void write(byte[] b) throws IOException {
				routf.write(b);
			}

			@Override
			public void write(byte[] b, int off, int len) throws IOException {
				routf.write(b, off, len);
			}

		});
		byte[] BUFFER_TO_READ = new byte[BUFFER_SIZE];
		ous.writeInt32(OsmandOdb.OsmAndStructure.VERSION_FIELD_NUMBER, index.getVersion());
		ous.writeInt64(OsmandOdb.OsmAndStructure.DATECREATED_FIELD_NUMBER, index.getDateCreated());

		for (int i = 0; i < index.getIndexes().size(); i++) {
			BinaryIndexPart part = index.getIndexes().get(i);
			if (part instanceof MapIndex) {
				// copy only part of map index
				copyMapIndex(roadOnlyFile, (MapIndex) part, index, ous, raf, routf);
				continue;
			} 
			ous.writeTag(part.getFieldNumber(), WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
			BinaryMerger.writeInt(ous, part.getLength());
			BinaryMerger.copyBinaryPart(ous, BUFFER_TO_READ, raf, part.getFilePointer(), part.getLength());
		}

		ous.writeInt32(OsmandOdb.OsmAndStructure.VERSIONCONFIRM_FIELD_NUMBER, index.getVersion());
		ous.flush();
		routf.close();
		raf.close();
	}

	private static void copyMapIndex(File roadOnlyFile, MapIndex part, BinaryMapIndexReader index,
			CodedOutputStream ous, RandomAccessFile raf, RandomAccessFile routf) throws IOException, RTreeException {
		final List<MapRoot> rts = part.getRoots();
		BinaryMapIndexWriter writer = new BinaryMapIndexWriter(routf, ous);
		writer.startWriteMapIndex(part.getName());
		boolean first = true;
		for (MapRoot r : rts) {
			if(r.getMaxZoom() <= 10) {
				if(first) {
					throw new UnsupportedOperationException("Can't write top level zoom");
				}
				ous.writeTag(OsmandOdb.OsmAndMapIndex.LEVELS_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
				BinaryMerger.writeInt(ous, r.getLength());
				BinaryMerger.copyBinaryPart(ous, new byte[BUFFER_SIZE], raf, r.getFilePointer(), r.getLength());
				continue;
			}
			final TLongObjectHashMap<BinaryMapDataObject> objects = new TLongObjectHashMap<BinaryMapDataObject>();
			File nonpackRtree = new File(roadOnlyFile.getParentFile(), "nonpack" + r.getMinZoom() + "."
					+ roadOnlyFile.getName() + ".rtree");
			File packRtree = new File(roadOnlyFile.getParentFile(), "pack" + r.getMinZoom() + "."
					+ roadOnlyFile.getName() + ".rtree");
			RTree rtree = null;
			try {
				rtree = new RTree(nonpackRtree.getAbsolutePath());
				final SearchRequest<BinaryMapDataObject> req = buildSearchRequest(r, objects, rtree);
				index.searchMapIndex(req, part);
				if(first) {
					first = false;
					writer.writeMapEncodingRules(part.decodingRules);
				}
				rtree = AbstractIndexPartCreator.packRtreeFile(rtree, nonpackRtree.getAbsolutePath(),
						packRtree.getAbsolutePath());
				TLongObjectHashMap<BinaryFileReference> treeHeader = new TLongObjectHashMap<BinaryFileReference>();

				long rootIndex = rtree.getFileHdr().getRootIndex();
				rtree.Node root = rtree.getReadNode(rootIndex);
				Rect rootBounds = IndexVectorMapCreator.calcBounds(root);
				if (rootBounds != null) {
					writer.startWriteMapLevelIndex(r.getMinZoom(), r.getMaxZoom(), rootBounds.getMinX(),
							rootBounds.getMaxX(), rootBounds.getMinY(), rootBounds.getMaxY());
					IndexVectorMapCreator.writeBinaryMapTree(root, rootBounds, rtree, writer, treeHeader);
					IndexVectorMapCreator.writeBinaryMapBlock(root, rootBounds, rtree, writer, treeHeader, objects, r.getMapZoom(), false);

					writer.endWriteMapLevelIndex();
				}
			} finally {
				if (rtree != null) {
					RandomAccessFile file = rtree.getFileHdr().getFile();
					file.close();
				}
				nonpackRtree.delete();
				packRtree.delete();
			}
		}
		writer.endWriteMapIndex();
	}

	private static double polygonArea(BinaryMapDataObject obj) {
		int zoom = 16;
		float mult = (float) (1. / MapUtils.getPowZoom(Math.max(31 - (zoom + 8), 0)));
		double area = 0.;
		int j = obj.getPointsLength() - 1;
		for (int i = 0; i < obj.getPointsLength(); i++) {
			int px = obj.getPoint31XTile(i);
			int py = obj.getPoint31YTile(i);
			int sx = obj.getPoint31XTile(j);
			int sy = obj.getPoint31YTile(j);
			area += (sx + ((float) px)) * (sy - ((float) py));
			j = i;
		}
		return Math.abs(area) * mult * mult * .5;
	}

	private static SearchRequest<BinaryMapDataObject> buildSearchRequest(MapRoot r,
			final TLongObjectHashMap<BinaryMapDataObject> objects, final RTree urTree) {
		final SearchRequest<BinaryMapDataObject> req = BinaryMapIndexReader.buildSearchRequest(0,
				Integer.MAX_VALUE, 0, Integer.MAX_VALUE, r.getMinZoom(), new SearchFilter() {
					@Override
					public boolean accept(TIntArrayList types, MapIndex index) {
						return true;
					}
				}, new ResultMatcher<BinaryMapDataObject>() {
					@Override
					public boolean publish(BinaryMapDataObject obj) {
						int minX = obj.getPoint31XTile(0);
						int maxX = obj.getPoint31XTile(0);
						int maxY = obj.getPoint31YTile(0);
						int minY = obj.getPoint31YTile(0);
						for (int i = 1; i < obj.getPointsLength(); i++) {
							minX = Math.min(minX, obj.getPoint31XTile(i));
							minY = Math.min(minY, obj.getPoint31YTile(i));
							maxX = Math.max(maxX, obj.getPoint31XTile(i));
							maxY = Math.max(maxY, obj.getPoint31YTile(i));
						}

						if (accept(obj, minX, maxX, minY, maxY)) {
							objects.put(obj.getId(), obj);
							try {
								urTree.insert(new LeafElement(new Rect(minX, minY, maxX, maxY), obj.getId()));
							} catch (Exception e) {
								throw new RuntimeException(e);
							}
						}
						return false;
					}

					@Override
					public boolean isCancelled() {
						return false;
					}
				});
		return req;
	}


	protected static boolean accept(BinaryMapDataObject obj, int minX, int maxX, int minY, int maxY) {
//		double l = MapUtils.convert31XToMeters(minX, maxX);
//		double h = MapUtils.convert31YToMeters(minY, maxY);
//		if(obj.isArea() && (l >= 100 || h >= 100)) {
		if(obj.isArea()) {
			// pixels on 16 zoom
			if(polygonArea(obj) > 1000){
				return true;
			}
		}
		boolean point = obj.getPointsLength() == 1;
			for(int i = 0; i < obj.getTypes().length; i++) {
				TagValuePair tv = obj.getMapIndex().decodeType(obj.getTypes()[i]);
				if(tv.tag.equals("waterway") ) {
					return true;
				}
				if(tv.tag.equals("place") && point) {
					return true;
				} else if(tv.tag.equals("highway") && point) {
					return true;
				} else if(tv.tag.equals("crossing") && point) {
					return true;
				} else if(tv.tag.equals("public_transport") && point) {
					return true;
				}
			}
		return false;
	}

	private String getDescription(BinaryMapIndexReader reader, String fileName) {
		String summary = " data for ";
		boolean fir = true;
		if (fileName.contains(".srtm")) {
			summary = "Contour lines " + (fileName.contains(".srtmf") ? "feet " : "meters ");
		} else if (fileName.contains(".travel")){
			summary = "Travel guide" + summary;
		} else if (fileName.contains(".depth")){
			summary = "Depth" + summary;
		} else if (fileName.contains("_wiki.")) {
			summary = "Wikipedia" + summary;
		} else {
			if (reader.containsAddressData()) {
				summary = "Address" + (fir ? "" : ", ") + summary;
				fir = false;
			}
			if (reader.hasTransportData()) {
				summary = "Transport" + (fir ? "" : ", ") + summary;
				fir = false;
			}
			if (reader.containsPoiData()) {
				summary = "POI" + (fir ? "" : ", ") + summary;
				fir = false;
			}
			if (reader.containsRouteData()) {
				summary = "Roads" + (fir ? "" : ", ") + summary;
				fir = false;
			}
			if (reader.containsMapData()) {
				summary = "Map" + (fir ? "" : ", ") + summary;
				fir = false;
			}
		}
		int last = fileName.lastIndexOf('_', fileName.indexOf('.'));
		if (last == -1) {
			last = fileName.indexOf('.');
		}
		String regionName = fileName.substring(0, last);
		summary += regionName;
		summary = summary.replace('_', ' ');
		return summary;
	}

	private File unzip(File f) throws OneFileException {
		ZipFile zipFile = null;
		try {
			if (!Algorithms.isZipFile(f)) {
				return f;
			}
			log.info("Unzipping file: " + f.getName());
			zipFile = new ZipFile(f);
			Enumeration<? extends ZipEntry> entries = zipFile.entries();
			long slastModified = f.lastModified();
			String folderName = f.getName().substring(0, f.getName().length() - 4);
			File unzipFolder = new File(f.getParentFile(), folderName);
			unzipFolder.mkdirs();
			while (entries.hasMoreElements()) {
				ZipEntry entry = entries.nextElement();
				long lastModified = slastModified;
				if (entry.isDirectory()) {
					continue;
				}
				if (entry.getTime() < lastModified) {
					lastModified = entry.getTime();
				}
				File tempFile = new File(unzipFolder, entry.getName());
				tempFile.getParentFile().mkdirs();
				InputStream zin = zipFile.getInputStream(entry);
				FileOutputStream out = new FileOutputStream(tempFile);
				Algorithms.streamCopy(zin, out);
				Algorithms.closeStream(zin);
				Algorithms.closeStream(out);
				tempFile.setLastModified(lastModified);
			}
			return unzipFolder;
		} catch (ZipException e) {
			throw new OneFileException("cannot unzip:" + e.getMessage(), e);
		} catch (IOException e) {
			throw new OneFileException("cannot unzip:" + e.getMessage(), e);
		} finally {
			if (zipFile != null) {
				try {
					zipFile.close();
				} catch (IOException e) {
					throw new OneFileException("cannot unzip:" + e.getMessage(), e);
				}
			}
		}
	}

	private void uploadIndex(File srcFile, File zipFile, String summary, UploadCredentials uc) {
		double mbLengh = (double) zipFile.length() / MB;
		File toUpload = zipFile;
		String fileName = toUpload.getName();
		String logFileName = fileName.substring(0, fileName.lastIndexOf('.')) + IndexConstants.GEN_LOG_EXT;
		if (mbLengh < MIN_SIZE_TO_UPLOAD) {
			log.info("Skip uploading index due to size " + fileName);
			// do not upload small files
		}
		try {
			log.info("Uploading index " + fileName + " (log file: " + logFileName + ")" );
			boolean uploaded = uploadFileToServer(toUpload, summary, uc);
			if (!uploaded) {
				log.info("Upload failed");
			}
			// remove source file if file was split
			if (uploaded && targetDirectory != null && !targetDirectory.equals(directory)) {
				File toBackup = new File(targetDirectory, fileName);
				File logFile = new File(zipFile.getParentFile(), logFileName);
				File logFileUploaded = new File(targetDirectory, logFileName);
				if (toBackup.exists()) {
					toBackup.delete();
				}
				
				if (logFile.exists()) {
					logFileUploaded.delete();
					logFile.renameTo(logFileUploaded);
				}
				log.info("Upload succesful, saving backup to " + toBackup.getAbsolutePath());
				if (!toUpload.renameTo(toBackup)) {
					FileOutputStream fout = new FileOutputStream(toBackup);
					FileInputStream fin = new FileInputStream(toUpload);
					Algorithms.streamCopy(fin, fout);
					fin.close();
					fout.close();
					toUpload.delete();
				}
				toBackup.setLastModified(System.currentTimeMillis()); // keep upload timestamp to compare
				if (srcFile.equals(zipFile)) {
					srcFile.delete();
				}
			}
		} catch (IOException e) {
			log.error("Input/output exception uploading " + fileName, e);
		}
	}

	private static String extractDirectory(String[] args, int ind) throws IndexUploadException {
		if (args.length > ind) {
			if ("-h".equals(args[0])) {
				throw new IndexUploadException(
						"Usage: IndexZipper [directory] (if not specified, the current one will be taken)");
			} else {
				return args[ind];
			}
		}
		return ".";
	}

	public abstract static class UploadCredentials {
		String password;
		String user;
		String url;
		String path;

		protected void parseParameter(String param) throws IndexUploadException {
			if (param.startsWith("--url=")) {
				url = param.substring("--url=".length());
			} else if (param.startsWith("--password=")) {
				password = param.substring("--password=".length());
			} else if (param.startsWith("--user=")) {
				user = param.substring("--user=".length());
			} else if (param.startsWith("--path=")) {
				path = param.substring("--path=".length());
			}
		}

		public boolean checkIfUploadNeededByTimestamp(String filename, long time) {
			return true;
		}

		public void disconnect() {
			// if the uploading needs to close a session
		}

		public void connect() throws IndexUploadException {
			// if the uploading needs to open a session
		}

		public abstract void upload(IndexUploader uploader, File toUpload, String summary, String size, String date)
				throws IOException, JSchException;
	}

	public static class DummyCredentials extends UploadCredentials {
		@Override
		public void upload(IndexUploader uploader, File toUpload, String summary, String size, String date)
				throws IOException, JSchException {
			// do nothing
			log.info("Uploading dummy file " + toUpload.getName() + " " + size + " MB " + date + " of ");
		}
	}

	

	public static class UploadSSHCredentials extends UploadCredentials {
		String privateKey;
		String knownHosts;

		@Override
		protected void parseParameter(String param) throws IndexUploadException {
			super.parseParameter(param);

			if (param.startsWith("--privKey=")) {
				privateKey = param.substring("--privKey=".length());
			} else if (param.startsWith("--knownHosts=")) {
				knownHosts = param.substring("--knownHosts=".length());
			}
		}

		@Override
		public void upload(IndexUploader uploader, File toUpload, String summary, String size, String date)
				throws IOException, JSchException {
			uploader.uploadToSSH(toUpload, summary, size, date, this);
		}
	}

	public boolean uploadFileToServer(File toUpload, String summary, UploadCredentials credentials) throws IOException {
		double originalLength = (double) toUpload.length() / MB;
		MessageFormat dateFormat = new MessageFormat("{0,date,dd.MM.yyyy}", Locale.US);
		MessageFormat numberFormat = new MessageFormat("{0,number,##.#}", Locale.US);
		String size = numberFormat.format(new Object[] { originalLength });
		String date = dateFormat.format(new Object[] { new Date(toUpload.lastModified()) });
		try {
			credentials.upload(this, toUpload, summary, size, date);
		} catch (IOException e) {
			log.error("Input/output exception uploading " + toUpload.getName(), e);
			return false;
		} catch (JSchException e) {
			log.error("Input/output exception uploading " + toUpload.getName(), e);
			return false;
		}
		return true;
	}


	public void uploadToSSH(File f, String description, String size, String date, UploadSSHCredentials cred)
			throws IOException, JSchException {
		log.info("Uploading file " + f.getName() + " " + size + " MB " + date + " of " + description);
		// Upload to ftp
		JSch jSch = new JSch();
		boolean knownHosts = false;
		if (cred.knownHosts != null) {
			jSch.setKnownHosts(cred.knownHosts);
			knownHosts = true;
		}
		if (cred.privateKey != null) {
			jSch.addIdentity(cred.privateKey);
		}
		String serverName = cred.url;
		if (serverName.startsWith("ssh://")) {
			serverName = serverName.substring("ssh://".length());
		}
		Session session = jSch.getSession(cred.user, serverName);
		if (cred.password != null) {
			session.setPassword(cred.password);
		}
		if (!knownHosts) {
			java.util.Properties config = new java.util.Properties();
			config.put("StrictHostKeyChecking", "no");
			session.setConfig(config);
		}
		String rfile = cred.path + "/" + f.getName();
		String lfile = f.getAbsolutePath();
		session.connect();

		// exec 'scp -t rfile' remotely
		String command = "scp -p -t \"" + rfile + "\"";
		Channel channel = session.openChannel("exec");
		((ChannelExec) channel).setCommand(command);

		// get I/O streams for remote scp
		OutputStream out = channel.getOutputStream();
		InputStream in = channel.getInputStream();

		channel.connect();

		if (checkAck(in) != 0) {
			channel.disconnect();
			session.disconnect();
			return;
		}

		// send "C0644 filesize filename", where filename should not include '/'
		long filesize = (new File(lfile)).length();
		command = "C0644 " + filesize + " ";
		if (lfile.lastIndexOf('/') > 0) {
			command += lfile.substring(lfile.lastIndexOf('/') + 1);
		} else {
			command += lfile;
		}
		command += "\n";
		out.write(command.getBytes());
		out.flush();
		if (checkAck(in) != 0) {
			channel.disconnect();
			session.disconnect();
			return;
		}

		// send a content of lfile
		FileInputStream fis = new FileInputStream(lfile);
		byte[] buf = new byte[1024];
		try {
			int len;
			while ((len = fis.read(buf, 0, buf.length)) > 0) {
				out.write(buf, 0, len); // out.flush();
			}
		} finally {
			fis.close();
		}
		fis = null;
		// send '\0'
		buf[0] = 0;
		out.write(buf, 0, 1);
		out.flush();
		if (checkAck(in) != 0) {
			channel.disconnect();
			session.disconnect();
			return;
		}
		out.close();

		channel.disconnect();
		session.disconnect();
		log.info("Finish uploading file index");
	}

	static int checkAck(InputStream in) throws IOException {
		int b = in.read();
		// b may be 0 for success,
		// 1 for error,
		// 2 for fatal error,
		// -1
		if (b == 0)
			return b;
		if (b == -1)
			return b;

		if (b == 1 || b == 2) {
			StringBuilder sb = new StringBuilder();
			int c;
			do {
				c = in.read();
				sb.append((char) c);
			} while (c != '\n');
			if (b == 1) { // error
				System.out.print(sb.toString());
			}
			if (b == 2) { // fatal error
				System.out.print(sb.toString());
			}
		}
		return b;
	}

}
