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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLStreamException;

import net.osmand.IProgress;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Entity.EntityType;
import net.osmand.osm.edit.EntityInfo;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.OSMSettings.OSMTagKey;
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
	private static final int OSC_FILES_TO_COMBINE_OSMCONVERT = 400;
	private static final long INTERVAL_TO_UPDATE_PBF = 1000 * 60 * 60 * 4;
//	private static final long INTERVAL_TO_UPDATE_PBF = 1000 ;
	private static final long INTERVAL_TO_UPDATE_PBF_GENERIC = 1000 * 60 * 60 * 7;
	private static final long MB = 1024 * 1024;
	private static final long LIMIT_TO_LOAD_IN_MEMORY = 200 * MB;


	private void process(String location, String repo, String binaryFolder) throws Exception {
		CountryOcbfGeneration ocbfGeneration = new CountryOcbfGeneration();
		CountryRegion regionStructure = ocbfGeneration.parseRegionStructure(repo);
		Map<String, File> polygons = ocbfGeneration.getPolygons(repo);
		List<CountryRegion> rt = new ArrayList<CountryRegion>();
		Iterator<CountryRegion> it = regionStructure.iterator();
		while(it.hasNext()) {
			CountryRegion reg = it.next();
			if (reg.map) {
				File countryFolder = new File(location, reg.getDownloadName());
				reg.timestampToUpdate = getMinTimestamp(countryFolder, "osc.gz");
				if(reg.timestampToUpdate == Long.MAX_VALUE) {
					System.out.println("Skip " + countryFolder.getName() + " because no changes");
				} else {
					rt.add(reg);
				}
			}
		}
		Collections.sort(rt, new Comparator<CountryRegion>() {

			@Override
			public int compare(CountryRegion o1, CountryRegion o2) {
				return Long.compare(o1.timestampToUpdate, o2.timestampToUpdate);
			}
		});
		for (CountryRegion reg : rt) {
			File countryFolder = new File(location, reg.getDownloadName());
			if (!countryFolder.exists()) {
				System.out.println("Country pbf doesn't exist " + reg.getDownloadName());
				continue;
			}
			File polygonFile = getPolygonFile(polygons, reg, countryFolder);
			if (polygonFile == null) {
				System.err.println("Boundary doesn't exist " + reg.getDownloadName());
				continue;
			}
			File pbfFile = new File(countryFolder, reg.getDownloadName() + ".pbf");
			if (!pbfFile.exists()) {
				extractPbf(pbfFile, reg, polygonFile);
			}
			if (pbfFile.exists()) {
				updatePbfFile(reg, countryFolder, pbfFile, polygonFile, binaryFolder);
			}
		}
	}

	private void extractPbf(File pbfFile, CountryRegion reg,  File polygonFile) {
		if(!Algorithms.isEmpty(reg.getPolyExtract())) {
			File fromExtract ;
			if (reg.getParent() != null && reg.getParent().map) {
				fromExtract = new File(pbfFile.getParentFile().getParentFile(), reg.getParent().getDownloadName() + "/"
						+ reg.getParent().getDownloadName() + ".pbf");
			} else {
				fromExtract = new File(pbfFile.getParentFile().getParentFile(), reg.getPolyExtract() +".pbf");
			}
			if(!fromExtract.exists()) {
				fromExtract = new File(fromExtract.getParentFile(), fromExtract.getName().replace(".pbf", ".o5m"));
			}
			if(fromExtract.exists()) {
				if(fromExtract.lastModified() < getMinTimestamp(pbfFile.getParentFile(), "osc.gz")) {
					SimpleDateFormat sdf = new SimpleDateFormat();
					System.err.println("!! Extract file is older than any osc.gz change available that means that extract file should be updated: " + fromExtract.getName() +
							" " + sdf.format(new Date(fromExtract.lastModified())) +
							" polygon " + sdf.format(new Date(getMinTimestamp(pbfFile.getParentFile(), "osc.gz"))));

					return;
				}
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

	private long getMinTimestamp(File parentFile, String ext) {
		long l = Long.MAX_VALUE;
		File[] fls = parentFile.listFiles();
		if (fls != null) {
			for (File f : fls) {
				if (f.getName().endsWith(ext)) {
					l = Math.min(l, f.lastModified());
				}
			}
		}
		return l;
	}

	private void updatePbfFile(CountryRegion reg,
			File countryFolder, File pbfFile, File polygonFile, String binaryFolder) throws Exception {
		List<File> oscFiles = new ArrayList<File>();
		List<File> oscTxtFiles = new ArrayList<File>();
		List<File> oscFilesIds = new ArrayList<File>();
		long minTimestamp = getMinTimestamp(countryFolder, "osc.gz");
		for(File oscFile : getSortedFiles(countryFolder)) {
			if(oscFile.getName().endsWith("osc.gz") && !oscFile.getName().contains(".pbf")) {
				String baseFile = oscFile.getName().substring(0, oscFile.getName().length() - "osc.gz".length());
				File oscFileTxt = new File(oscFile.getParentFile(), baseFile + "txt");
				File oscFileIdsTxt = new File(oscFile.getParentFile(), baseFile + "ids.txt");
				if(!oscFileIdsTxt.exists()) {
					break;
				}
				oscFiles.add(oscFile);
				oscTxtFiles.add(oscFileTxt);
				oscFilesIds.add(oscFileIdsTxt);
			}
		}
		long waited = System.currentTimeMillis() - minTimestamp;
		boolean hasMapSubRegions = false;
		Iterator<CountryRegion> it = reg.iterator();
		while(it.hasNext()) {
			if(it.next().map) {
				hasMapSubRegions = true;
				break;
			}
		}
		if (oscFiles.size() > 0) {
			log.info("Region " + countryFolder.getName() + " waiting time " + (waited / 60000) + " minutes");
			if (waited > INTERVAL_TO_UPDATE_PBF_GENERIC || (!hasMapSubRegions && waited > INTERVAL_TO_UPDATE_PBF)) {
				process(binaryFolder, pbfFile, polygonFile, oscFiles, oscTxtFiles, oscFilesIds);
			}
		}
	}

	private void iterateOsmEntity(final TLongSet ids, final TLongObjectHashMap<Entity> found,
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
				boolean loadFullRelation = changes
						|| entity.getTag("restriction") != null
						|| entity.getTag("area") != null
						|| ("multipolygon".equals(entity.getTag(OSMTagKey.TYPE)) && entity
								.getTag(OSMTagKey.ADMIN_LEVEL) == null);
				if (loadFullRelation) {
					Iterator<EntityId> it = r.getMemberIds().iterator();
					while (it.hasNext()) {
						// load next iteration other nodes
						long toLoadNode = IndexIdByBbox.convertId(it.next());
						if (!found.contains(toLoadNode)) {
							ids.add(toLoadNode);
						}
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

	private void iteratePbf(final TLongSet ids, final TLongObjectHashMap<Entity> found, final boolean changes,
			OsmDbAccessor acessor, final TLongObjectHashMap<Entity> cache, File outPbf) throws SQLException, InterruptedException, IOException {
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
				iterateOsmEntity(ids, found, changes, search, e, cache);
			}
		} else if (acessor != null){
			OsmDbAccessor.OsmDbVisitor vis = new OsmDbAccessor.OsmDbVisitor() {

				@Override
				public void iterateEntity(Entity e, OsmDbAccessorContext ctx) throws SQLException {
					try {
						iterateOsmEntity(ids, found, changes, search, e, cache);
						long key = IndexIdByBbox.convertId(EntityId.valueOf(e));
						if(cache != null && !cache.contains(key)) {
							cache.put(key, e);
						}
					} catch (Exception es) {
						throw new RuntimeException(es);
					}
				}
			};
			acessor.iterateOverEntities(IProgress.EMPTY_PROGRESS, EntityType.NODE, vis);
			acessor.iterateOverEntities(IProgress.EMPTY_PROGRESS, EntityType.WAY, vis);
			acessor.iterateOverEntities(IProgress.EMPTY_PROGRESS, EntityType.RELATION, vis);
		} else {
			OsmBaseStoragePbf pbfReader = new OsmBaseStoragePbf();
			InputStream fis = new FileInputStream(outPbf);
			// InputStream fis = new ByteArrayInputStream(allBytes);
			pbfReader.getFilters().add(new IOsmStorageFilter() {

				@Override
				public boolean acceptEntityToLoad(OsmBaseStorage storage, EntityId entityId, Entity e) {
					try {
						iterateOsmEntity(ids, found, changes, search, e, cache);
						long key = IndexIdByBbox.convertId(EntityId.valueOf(e));
						if(cache != null && !cache.contains(key)) {
							cache.put(key, e);
						}
					} catch (Exception es) {
						throw new RuntimeException(es);
					}
					return false;
				}
			});
			pbfReader.parseOSMPbf(fis, null, false);
			fis.close();
		}
	}

	private void process(String binaryFolder, File pbfFile, File polygonFile, List<File> oscFiles,
			List<File> oscTxtFiles, List<File> oscFilesIds) throws Exception {
		File outPbf = new File(pbfFile.getParentFile(), pbfFile.getName() + ".o.pbf");
		List<String> args = new ArrayList<String>();
		List<File> additionalOsc = new ArrayList<File>();
		args.add(pbfFile.getName());
		args.add("-B=" + polygonFile.getName());
		args.add("--complex-ways");
//		args.add("--merge-versions"); // doesn't finish in reasonable time

		int currentOsc = 0;
		int currentOscInd = 1;
		File osc = new File(pbfFile.getParentFile(), pbfFile.getName() +"."+currentOscInd++ + ".osc");
		additionalOsc.add(osc);
		List<File> currentList = new ArrayList<File>();
		for (File f : oscFiles) {
			currentOsc++;
			currentList.add(f);
			if (currentOsc > OSC_FILES_TO_COMBINE_OSMCONVERT) {
				combineOscs(pbfFile.getParentFile(), binaryFolder, polygonFile, osc, currentList);
				args.add(osc.getName());
				// start over
				currentOsc = 0;
				currentList.clear();
				osc = new File(pbfFile.getParentFile(), pbfFile.getName() + "." + currentOscInd++ + ".osc");
				additionalOsc.add(osc);
			}
		}
		combineOscs(pbfFile.getParentFile(), binaryFolder, polygonFile, osc, currentList);
		args.add(osc.getName());
		args.add(osc.getName());
		args.add("-o=" + outPbf.getName());
		boolean res = exec(pbfFile.getParentFile(), binaryFolder + "osmconvert", args);
		if (!res) {
			throw new IllegalStateException(pbfFile.getName() + " convert failed");
		}
		try {
			TLongObjectHashMap<Entity> found = new TLongObjectHashMap<Entity>();
			TLongObjectHashMap<Entity> cache = outPbf.length() > LIMIT_TO_LOAD_IN_MEMORY ? null
					: new TLongObjectHashMap<Entity>();
			TLongHashSet toFind = getIds(oscFilesIds, found);

			if (!toFind.isEmpty()) {
				// doesn't give any reasonable performance
				// final byte[] allBytes = Files.readAllBytes(Paths.get(outPbf.getAbsolutePath()));
				boolean changes = true;
				int iteration = 0;
				File dbFile = new File(outPbf.getParentFile(), outPbf.getName() + ".db");
				DBDialect dlct = DBDialect.SQLITE;
				// OsmDbAccessor accessor = createDbAcessor(outPbf, dbFile, dlct);
				// not fast enough
				OsmDbAccessor accessor = null;

				while (!toFind.isEmpty()) {
					iteration++;
					log.info("Iterate pbf to find " + toFind.size() + " ids (" + iteration + ") " + stat(toFind) );
					iteratePbf(toFind, found, changes, accessor, cache, outPbf);
					changes = false;
					if (iteration > 30) {
						throw new RuntimeException("Too many iterations");
					}
				}
				if (accessor != null) {
					dlct.closeDatabase(accessor.getDbConn());
					dlct.removeDatabase(dbFile);
				}
			}
			System.gc();
			if (cache != null) {
				cache.clear();
			}
			writeOsmFile(pbfFile.getParentFile(), found);

			// delete files
			outPbf.renameTo(pbfFile);
			for (File f : oscFilesIds) {
				f.delete();
			}
			for (File f : oscFiles) {
				f.delete();
			}
			for (File f : oscTxtFiles) {
				f.delete();
			}
		} finally {
			for (File f : additionalOsc) {
				f.delete();
			}
		}
	}

	private String stat(TLongHashSet toFind) {
		int relations = 0;
		int ways = 0;
		int nodes = 0;
		for (long l : toFind.toArray()) {
			if (l % 4 == 0) {
				nodes++;
			} else if (l % 4 == 1) {
				ways++;
			} else {
				relations++;
			}
		}
		return " " + nodes + " nodes, " + ways + " ways, " + relations + " relations";
	}

	protected void combineOscs(File parentFile, String binaryFolder, File polygonFile, File osc, List<File> currentList) {
		List<String> args = new ArrayList<String>();
		for (File f : currentList) {
			args.add(f.getName());
		}
//		args.add("-B=" + polygonFile.getName());
//		args.add("--complex-ways");
		args.add("--merge-versions");
		args.add("-o=" + osc.getName());

		boolean res = exec(parentFile, binaryFolder + "osmconvert", args);
		if (!res) {
			throw new IllegalStateException(osc.getName() + " convert failed");
		}
	}

	protected OsmDbAccessor createDbAcessor(File outPbf, File dbFile, DBDialect dlct) throws SQLException,
			FileNotFoundException, IOException {
		OsmDbAccessor accessor = new OsmDbAccessor();
		if (dlct.databaseFileExists(dbFile)) {
			dlct.removeDatabase(dbFile);
		}
		log.info("Load pbf into sqlite " + dbFile.getAbsolutePath());
		Object dbConn = dlct.getDatabaseConnection(dbFile.getAbsolutePath(), log);
		accessor.setDbConn(dbConn, dlct);
		OsmDbCreator dbCreator = new OsmDbCreator();
		dbCreator.initDatabase(dlct, dbConn, true);
		OsmBaseStoragePbf pbfReader = new OsmBaseStoragePbf();
		InputStream fis = new FileInputStream(outPbf);
		// InputStream fis = new ByteArrayInputStream(allBytes);
		pbfReader.getFilters().add(dbCreator);
		pbfReader.parseOSMPbf(fis, null, false);
		fis.close();
		dbCreator.finishLoading();
		dlct.commitDatabase(dbConn);
		accessor.initDatabase(dbCreator);
		return accessor;
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

	private TLongHashSet getIds(List<File> oscFilesIds, TLongObjectHashMap<Entity> found) throws IOException {
		TLongHashSet ids = new TLongHashSet();
		for(File f : oscFilesIds) {
			BufferedReader br = new BufferedReader(new FileReader(f));
			String l;
			while ((l = br.readLine()) != null) {
				String[] lns = l.split(" ");
				if(l.charAt(0) == 'W') {
					long oid = Long.parseLong(lns[1]);
					long wayId = IndexIdByBbox.wayId(oid);
					if (l.charAt(1) == 'D') {
						Way wd = new Way(oid);
						long baseForNodeIds = ((oid << 4) + 5) << 8;
						Node tln = new Node(Double.parseDouble(lns[2]), Double.parseDouble(lns[3]), -(baseForNodeIds++));
						Node brn = new Node(Double.parseDouble(lns[3]), Double.parseDouble(lns[4]), -(baseForNodeIds++));
						wd.addNode(tln);
						wd.addNode(brn);
						wd.putTag("osmand_change", "delete");
						found.put(wayId, wd);
						found.put(IndexIdByBbox.nodeId(tln.getId()), tln);
						found.put(IndexIdByBbox.nodeId(brn.getId()), brn);
						ids.remove(wayId);
					} else {
						ids.add(wayId);
						found.remove(wayId);
					}
				} else if(l.charAt(0) == 'N') {
					long oid = Long.parseLong(lns[1]);
					long nodeId = IndexIdByBbox.nodeId(oid);
					if (l.charAt(1) == 'D') {
						Node node = new Node(Double.parseDouble(lns[2]), Double.parseDouble(lns[3]), oid);
						node.putTag("osmand_change", "delete");
						found.put(nodeId, node);
						ids.remove(nodeId);
					} else {
						ids.add(nodeId);
						found.remove(nodeId);
					}
				} else if(l.charAt(0) == 'R') {
					long relationId = IndexIdByBbox.relationId(Long.parseLong(lns[1]));
					if (l.charAt(1) == 'D') {
						ids.remove(relationId);
						found.put(relationId, null);
					} else {
						ids.add(relationId);
						found.remove(relationId);
					}
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
