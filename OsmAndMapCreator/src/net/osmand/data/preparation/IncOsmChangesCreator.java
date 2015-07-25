package net.osmand.data.preparation;

import gnu.trove.iterator.TLongIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLStreamException;

import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.EntityInfo;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Way;
import net.osmand.osm.io.IOsmStorageFilter;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.osm.io.OsmBaseStoragePbf;
import net.osmand.osm.io.OsmStorageWriter;
import net.osmand.regions.CountryOcbfGeneration;
import net.osmand.regions.CountryOcbfGeneration.CountryRegion;
import net.osmand.util.Algorithms;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class IncOsmChangesCreator {
	private static final Log log = LogFactory.getLog(IncOsmChangesCreator.class);
	private static final int OSC_FILES_TO_COMBINE = 300;
	private static final long INTERVAL_TO_UPDATE_PBF = 1000 * 60 * 60 * 2;
	private static final long MB = 1024 * 1024;
	
	private void process(String location, String repo, String binaryFolder) throws Exception {
		CountryOcbfGeneration ocbfGeneration = new CountryOcbfGeneration();
		CountryRegion regionStructure = ocbfGeneration.parseRegionStructure(repo);
		Map<String, File> polygons = ocbfGeneration.getPolygons(repo);
		Iterator<CountryRegion> it = regionStructure.iterator();
		while(it.hasNext()) {
			CountryRegion reg = it.next();
			if (reg.map) {
				File countryFolder = new File(location, reg.getDownloadName());
				if (!countryFolder.exists()) {
					continue;
				}
				File polygonFile = getPolygonFile(polygons, reg, countryFolder);
				if (polygonFile == null) {
					System.err.println("Boundary doesn't exist " + reg.getDownloadName());
					continue;
				}
				File pbfFile = new File(countryFolder, reg.getDownloadName() + ".pbf");
				if(!pbfFile.exists()) {
					extractPbf(pbfFile, reg, binaryFolder, polygonFile);
				}
				if(pbfFile.exists()) {
					updatePbfFile(countryFolder, pbfFile, polygonFile, binaryFolder);
				}
			}
		}
	}
	
	private void extractPbf(File pbfFile, CountryRegion reg, String binaryFolder, File polygonFile) {
		if(!Algorithms.isEmpty(reg.getPolyExtract())) {
			File fromExtract = new File(pbfFile.getParentFile().getParentFile(), reg.getPolyExtract() +".pbf");
			if(fromExtract.exists()) {
				List<String> args = new ArrayList<String>();
				args.add(fromExtract.getAbsolutePath());
				args.add("-B=" + polygonFile.getName());
				args.add("--complex-ways");
				args.add("--complete-ways");
				args.add("--drop-author");
				args.add("-o=" + pbfFile.getName());
				exec(pbfFile.getParentFile(), "osmconvert", args);
			} else {
				System.err.println("Extract file doesn't exist " + fromExtract.getName());
			}
		}
	}

	private void updatePbfFile(File countryFolder, File pbfFile, File polygonFile, String binaryFolder) throws Exception {
		List<File> oscFiles = new ArrayList<File>();
		List<File> oscTxtFiles = new ArrayList<File>();
		List<File> oscFilesIds = new ArrayList<File>();
		long minTimestamp = Long.MAX_VALUE;
		for(File oscFile : getSortedFiles(countryFolder)) {
			if(oscFile.getName().endsWith("osc.gz")) {
				String baseFile = oscFile.getName().substring(0, oscFile.getName().length() - "osc.gz".length());
				File oscFileTxt = new File(oscFile.getParentFile(), baseFile + "txt");
				File oscFileIdsTxt = new File(oscFile.getParentFile(), baseFile + "ids.txt");
				if(!oscFileIdsTxt.exists()) {
					break;
				}
				oscFiles.add(oscFile);
				oscTxtFiles.add(oscFileTxt);
				oscFilesIds.add(oscFileIdsTxt);
				minTimestamp = Math.min(oscFile.lastModified(), minTimestamp);
			}
			if(oscFiles.size() > OSC_FILES_TO_COMBINE) {
				process(binaryFolder, pbfFile, polygonFile, oscFiles, oscTxtFiles, oscFilesIds);
				oscFiles.clear();
				oscTxtFiles.clear();
				oscFilesIds.clear();
			}
		}	
		if(oscFiles.size() > 0 && System.currentTimeMillis() - INTERVAL_TO_UPDATE_PBF > minTimestamp) {
			process(binaryFolder, pbfFile, polygonFile, oscFiles, oscTxtFiles, oscFilesIds);
		}
	}
	
	private void iterateEntity(final TLongSet ids, final TLongObjectHashMap<Entity> found,
			final boolean changes, final TLongHashSet search, Entity entity, TLongObjectHashMap<Entity> cache) {
		long key = IndexIdByBbox.convertId(EntityId.valueOf(entity));
		if(entity instanceof Relation) {
			boolean loadRelation = search.contains(key);
			// 1. if relation changed, we need to load all members (multipolygon case)
			// 2. if any member of relation changed, we need to load all members (multipolygon case)
			Relation r = (Relation) entity;
			if (changes) {
				Iterator<EntityId> it = r.getMemberIds().iterator();
				while(it.hasNext()) {
					// load next iteration other nodes
					long toLoadNode = IndexIdByBbox.convertId(it.next());
					if (search.contains(toLoadNode)) {
						loadRelation = true;
					}
				}
			}
			if(loadRelation) {
				found.put(key, entity);
				ids.remove(key);
				Iterator<EntityId> it = r.getMemberIds().iterator();
				while(it.hasNext()) {
					// load next iteration other nodes
					long toLoadNode = IndexIdByBbox.convertId(it.next());
					if (!found.contains(toLoadNode)) {
						ids.add(toLoadNode);
					}
				}
			}
		} else if(entity instanceof Node){
			if(search.contains(key)) {
//					System.out.println("F" + (key >> 2));
				found.put(key, entity);
				ids.remove(key);
			}
		} else if (entity instanceof Way) {
			boolean loadWay = search.contains(key);
			Way w = (Way) entity;
			if (changes) {
				for (int i = 0; i < w.getNodeIds().size(); i++) {
					// load next iteration other nodes
					long toLoadNode = IndexIdByBbox.nodeId(w.getNodeIds().get(i));
					if (search.contains(toLoadNode)) {
						loadWay = true;
					}
				}
			}
			if (loadWay) {
				// always load all nodes for way if changed or needed
				found.put(key, entity);
				ids.remove(key);
				for (int i = 0; i < w.getNodeIds().size(); i++) {
					// load next iteration other nodes
					long toLoadNode = IndexIdByBbox.nodeId(w.getNodeIds().get(i));
					if (!found.contains(toLoadNode)) {
						ids.add(toLoadNode);
					}
				}
			}
		}
	}
	
	private IOsmStorageFilter iteratePbf(final TLongSet ids, final TLongObjectHashMap<Entity> found, final boolean changes, 
			final TLongObjectHashMap<Entity> cache) {
		final TLongHashSet search = new TLongHashSet(ids);
		ids.clear();
		TLongIterator it = search.iterator();
		while(it.hasNext()) {
			long toSearch = it.next();
			if(found.containsKey(toSearch)) {
				throw new IllegalStateException("?");
			}
//			System.out.println("S" + (toSearch >> 2));
			found.put(toSearch, null);
		}
		if(cache != null && !cache.isEmpty()) {
			for(Entity e : cache.valueCollection()) {
				iterateEntity(ids, found, changes, search, e, cache);
			}
			return null;
		}
		return new IOsmStorageFilter() {
			@Override
			public boolean acceptEntityToLoad(OsmBaseStorage storage, EntityId entityId, Entity entity) {
				try {
					iterateEntity(ids, found, changes, search, entity, cache);
					long key = IndexIdByBbox.convertId(entityId);
					if(cache != null && !cache.contains(key)) {
						cache.put(key, entity);
					}
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				return false;
			}

			
		};
	}
	
	private void process(String binaryFolder, File pbfFile, File polygonFile, List<File> oscFiles,
			List<File> oscTxtFiles, List<File> oscFilesIds) throws Exception {
		File outPbf = new File(pbfFile.getParentFile(), pbfFile.getName() + ".o.pbf");
		List<String> args = new ArrayList<String>();
		args.add(pbfFile.getName());
		args.add("-B=" + polygonFile.getName());
		args.add("--complex-ways");
		for (File f : oscFiles) {
			args.add(f.getName());
		}
		args.add("-o=" + outPbf.getName());
		boolean res = exec(pbfFile.getParentFile(), binaryFolder + "osmconvert", args);
		if (!res) {
			throw new IllegalStateException(pbfFile.getName() + " convert failed");
		}
	
		TLongObjectHashMap<Entity> found = new TLongObjectHashMap<Entity>();
		TLongObjectHashMap<Entity> cache = outPbf.length() > 100 * MB ? null : new TLongObjectHashMap<Entity>();
		TLongHashSet toFind = getIds(oscFilesIds);
		// doesn't give any reasonable performance
//		final byte[] allBytes = Files.readAllBytes(Paths.get(outPbf.getAbsolutePath()));
		boolean changes = true;
		int iteration = 0;
		while (!toFind.isEmpty()) {
			iteration++;
			log.info("Iterate pbf to find " + toFind.size() + " ids (" + iteration + ")");
			IOsmStorageFilter filter = iteratePbf(toFind, found, changes, cache);
			if (filter != null) {
				OsmBaseStoragePbf pbfReader = new OsmBaseStoragePbf();
				InputStream fis = new FileInputStream(outPbf);
//				InputStream fis = new ByteArrayInputStream(allBytes);
				pbfReader.getFilters().add(filter);
				pbfReader.parseOSMPbf(fis, null, false);
				fis.close();
			}
			changes = false;
			if(iteration > 30) {
				 throw new RuntimeException("Too many iterations");
			}
		}
		System.gc();
		if(cache != null) {
			cache.clear();
		}
		writeOsmFile(pbfFile.getParentFile(), found);
		if(true) {
			// delete files
			outPbf.renameTo(pbfFile);
			for(File f : oscFilesIds) {
				f.delete();
			}
			for(File f : oscFiles) {
				f.delete();
			}
			for(File f : oscTxtFiles) {
				f.delete();
			}
		}
	}

	private void writeOsmFile(File parentFile, TLongObjectHashMap<Entity> found) throws FactoryConfigurationError, XMLStreamException, IOException {
		
		int ind = 1;
		File f = getOutputOsmFile(parentFile, ind);
		while(f.exists()) {
			ind++;
			f = getOutputOsmFile(parentFile, ind);
		}
		
		OsmStorageWriter writer = new OsmStorageWriter();
		FileOutputStream fous = new FileOutputStream(f);
		GZIPOutputStream gzout = new GZIPOutputStream (fous);
		List<Node> nodes = new ArrayList<Node>();
		List<Way> ways = new ArrayList<Way>();
		List<Relation> relations= new ArrayList<Relation>();
		TLongObjectIterator<Entity> it = found.iterator();
		HashMap<EntityId, EntityInfo> mp = new HashMap<Entity.EntityId, EntityInfo>();
		while(it.hasNext()) {
			it.advance();
			if(it.value() == null) {
				long id = (it.key() >> 2);
				Entity e ;
				if(it.key() % 4 == 0) {
					e = new Node(0, 0, id);
					nodes.add((Node) e);
				} else if(it.key() % 4 == 1) {
					e = new Way(id);
					ways.add((Way) e);
				} else {
					e = new Relation(id);
					relations.add((Relation) e);
				}
				EntityInfo i = new EntityInfo();
				i.setAction("delete");
				mp.put(EntityId.valueOf(e), i);
			} else {
				if(it.value() instanceof Node) {
					nodes.add((Node) it.value());
				} else if(it.value() instanceof Way) {
					ways.add((Way) it.value());
				} else if(it.value() instanceof Relation) {
					relations.add((Relation) it.value());
				}
			}
		}
		writer.writeOSM(gzout, mp, nodes, ways, relations);
		gzout.close();
		fous.close();
	}

	private File getOutputOsmFile(File parentFile, int ind) {
		SimpleDateFormat sdf = new SimpleDateFormat("yy_MM_dd");
		String nd = ind < 10 ? "0"+ind : ind+"";
		File f = new File(parentFile, sdf.format(new Date()) + "_" + nd + ".osm.gz");
		return f;
	}

	private TLongHashSet getIds(List<File> oscFilesIds) throws IOException {
		TLongHashSet ids = new TLongHashSet();
		for(File f : oscFilesIds) {
			BufferedReader br = new BufferedReader(new FileReader(f));
			String l;
			while ((l = br.readLine()) != null) {
				String[] lns = l.split(" ");
				if(l.charAt(0) == 'W') {
					ids.add(IndexIdByBbox.wayId(Long.parseLong(lns[1])));
				} else if(l.charAt(0) == 'N') {
					ids.add(IndexIdByBbox.nodeId(Long.parseLong(lns[1])));
				} else if(l.charAt(0) == 'R') {
					ids.add(IndexIdByBbox.relationId(Long.parseLong(lns[1])));
				}
			}
			br.close();
		}
		return ids;
	}

	protected File[] getSortedFiles(File dir){
		File[] listFiles = dir.listFiles();
		Arrays.sort(listFiles, new Comparator<File>(){
			@Override
			public int compare(File o1, File o2) {
				return o1.getName().compareTo(o2.getName());
			}
		});
		return listFiles;
	}

	private boolean exec(File directory, String cmd, List<String> args) {
		BufferedReader output = null;
		OutputStream wgetInput = null;
		Process proc = null;
		try {
			log.info("Executing " + cmd + " " + args); //$NON-NLS-1$//$NON-NLS-2$ $NON-NLS-3$
			ArrayList<String> argsList = new ArrayList<String>();
			argsList.add(cmd);
			argsList.addAll(args);
			ProcessBuilder exec = new ProcessBuilder(argsList);
			exec.directory(directory);
			exec.redirectErrorStream(true);
			proc = exec.start();
			output = new BufferedReader(new InputStreamReader(proc.getInputStream()));
			String line;
			while ((line = output.readLine()) != null) {
				log.info("output:" + line); //$NON-NLS-1$
			}
			int exitValue = proc.waitFor();
			proc = null;
			if (exitValue != 0) {
				log.error("Exited with error code: " + exitValue); //$NON-NLS-1$
			} else {
				return true;
			}
		} catch (IOException e) {
			log.error("Input/output exception " + " ", e); //$NON-NLS-1$ //$NON-NLS-2$ $NON-NLS-3$
		} catch (InterruptedException e) {
			log.error("Interrupted exception " + " ", e); //$NON-NLS-1$ //$NON-NLS-2$ $NON-NLS-3$
		} finally {
			safeClose(output, ""); //$NON-NLS-1$
			safeClose(wgetInput, ""); //$NON-NLS-1$
			if (proc != null) {
				proc.destroy();
			}
		}
		return false;
	}
	
	private static void safeClose(Closeable ostream, String message) {
		if (ostream != null) {
			try {
				ostream.close();
			} catch (Exception e) {
				log.error(message, e); //$NON-NLS-1$ //$NON-NLS-2$
			}
		}
	}


	private File getPolygonFile(Map<String, File> polygons, CountryRegion reg, File countryFolder) throws IOException {
		File file = polygons.get(reg.boundary);
		if(file != null) {
			File polygonFile = new File(countryFolder, file.getName());
			if(!polygonFile.exists() || polygonFile.length() != file.length()) {
				Algorithms.fileCopy(file, polygonFile);
			}
			return polygonFile;
		}
		return null;
	}
	
	
	public static void main(String[] args) throws Exception {
		String location = "/Users/victorshcherb/osmand/temp/osmc/";
		if (args.length > 0) {
			location = args[0];
		}
		
		String repo = "/Users/victorshcherb/osmand/repos/";
		if (args.length > 1) {
			repo = args[1];
		}
		
		String binaryFolder = "/Users/victorshcherb/bin/";
		if (args.length > 2) {
			binaryFolder = args[2];
		}
		new IncOsmChangesCreator().process(location, repo, binaryFolder);
	}

	
}
