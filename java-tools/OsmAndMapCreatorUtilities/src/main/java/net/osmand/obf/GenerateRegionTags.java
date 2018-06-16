package net.osmand.obf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.xml.stream.XMLStreamException;

import net.osmand.PlatformUtil;
import net.osmand.binary.BinaryMapDataObject;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.map.OsmandRegions;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.Node;
import net.osmand.osm.edit.Way;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.osm.io.OsmStorageWriter;
import net.osmand.util.Algorithms;
import net.osmand.util.MapUtils;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.logging.Log;
import org.xmlpull.v1.XmlPullParserException;

public class GenerateRegionTags {

	private static final Log LOG = PlatformUtil.getLog(GenerateRegionTags.class);
	public static void main(String[] args) {
		try {
			if (args.length == 0) {
				args = new String[] { "/Users/victorshcherb/osmand/temp/proc_line_motorway_out.osm.bz2",
						"/Users/victorshcherb/osmand/temp/region_proc_line_motorway_out.osm.bz2" };
			}

			File inputFile = new File(args[0]);
			File targetFile = new File(args[1]);
			OsmandRegions or = new OsmandRegions();
			or.prepareFile();
			or.cacheAllCountries();
			process(inputFile, targetFile, or);
		} catch (Throwable e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	private static void process(File inputFile, File targetFile, OsmandRegions or) throws IOException, XmlPullParserException, XMLStreamException {
		InputStream fis = new FileInputStream(inputFile);
		if (inputFile.getName().endsWith(".gz")) {
			fis = new GZIPInputStream(fis);
		} else if (inputFile.getName().endsWith(".bz2")) {
			fis = new BZip2CompressorInputStream(fis);
		}
		OsmBaseStorage bs = new OsmBaseStorage();
		bs.parseOSM(fis, new ConsoleProgressImplementation());
		LOG.info("File was read");
		iterateOverEntities(bs.getRegisteredEntities(), or);
		
		OsmStorageWriter w = new OsmStorageWriter();
		OutputStream output = new FileOutputStream(targetFile);
		if(targetFile.getName().endsWith(".gz")) {
			output = new GZIPOutputStream(output);
		} else if(targetFile.getName().endsWith(".bz2")) {
			output = new BZip2CompressorOutputStream(output);
		}
		LOG.info("Entities processed. About to save the file.");
		w.saveStorage(output, bs, null, true);
		output.close();
		fis.close();
	}

	private static void iterateOverEntities(Map<EntityId, Entity> ids, OsmandRegions or) throws IOException {
		Map<EntityId, TreeSet<String>> mp = new LinkedHashMap<Entity.EntityId, TreeSet<String>>();
		LOG.info("About to process " + ids.size() + " entities") ;
		LOG.info("Processing nodes...");
		long i = 0;
		for (Entity e : ids.values()) {
			if (e instanceof Node) {
				i++;
				printProgress(i, ids.size());
				int y = MapUtils.get31TileNumberY(((Node) e).getLatitude());
				int x = MapUtils.get31TileNumberX(((Node) e).getLongitude());
				List<BinaryMapDataObject> l = or.query(x, y);
				EntityId id = EntityId.valueOf(e);
				TreeSet<String> lst = new TreeSet<String>();
				mp.put(id, lst);
				for (BinaryMapDataObject b : l) {
					if(or.contain(b, x, y)) {
						String dw = or.getDownloadName(b);
						if (!Algorithms.isEmpty(dw) && or.isDownloadOfType(b, OsmandRegions.MAP_TYPE)) {
							lst.add(dw);
						}
					}
				}
				if(!e.getTags().isEmpty()) {
					e.putTag(MapRenderingTypesEncoder.OSMAND_REGION_NAME_TAG, serialize(lst));
				}
			}
		}
		LOG.info("Processing ways...");
		for(Entity e : ids.values()) {
			if (e instanceof Way) {
				i++;
				printProgress(i, ids.size());
				Way w = (Way) e;
				TreeSet<String> lst = new TreeSet<String>();
				for(EntityId id : w.getEntityIds()) {
					TreeSet<String> ls = mp.get(id);
					if(ls != null) {
						lst.addAll(ls);
					}
				}
				if(!e.getTags().isEmpty()) {
					e.putTag(MapRenderingTypesEncoder.OSMAND_REGION_NAME_TAG, serialize(lst));
				}
			}
		}
		
	}

	private static void printProgress(long i, int size) {
		if( i % 10000 == 0) {
			LOG.info("Processing objects " + i +"...");
		}
	}

	private static String serialize(TreeSet<String> lst) {
		StringBuilder bld = new StringBuilder();
		Iterator<String> it = lst.iterator();
		while(it.hasNext()) {
			String next = it.next();
			if(bld.length() > 0) {
				bld.append(",");
			}
			bld.append(next);
		}
		return bld.toString();
	}
}
