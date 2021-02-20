package net.osmand.osm;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.xml.stream.XMLStreamException;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.logging.Log;
import org.xmlpull.v1.XmlPullParserException;

import com.google.gson.Gson;

import net.osmand.PlatformUtil;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.io.IOsmStorageFilter;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.osm.io.OsmStorageWriter;
import net.osmand.util.Algorithms;

public class FilterOsmByTags {

	private static final Log LOG = PlatformUtil.getLog(FilterOsmByTags.class);
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws IOException, XmlPullParserException, XMLStreamException {
//		if (args == null || args.length == 0) {
//			args = new String[] { "wikivoyage.osm.gz",
//					"default_wikivoyage.osm",
//					"[{\"route_id\": [\"Q9259\",\"Q46\",\"Q49\",\"Q538\",\"Q15\",\"Q48\",\"Q18\"] }]" };
//		}
		if(args.length < 3) {
			System.out.println("Synopsis: <input_osm_file> <output_osm_file> <json_filter>");
			System.exit(1);
			return;
		}
		
		File input = new File(args[0]);
		File output = new File(args[1]);
		Gson gson = new Gson();
		List<Map<String, Object>> arrayConditions = gson.fromJson(args[2], List.class);
		process(input, output, arrayConditions);
	}

	private static void process(File inputFile, File targetFile, List<Map<String, Object>> arrayConditions) throws IOException, XmlPullParserException, XMLStreamException {
		FileInputStream original = new FileInputStream(inputFile);
		InputStream fis = original ;
		if (inputFile.getName().endsWith(".gz")) {
			fis = new GZIPInputStream(fis);
		} else if (inputFile.getName().endsWith(".bz2")) {
			fis = new BZip2CompressorInputStream(fis);
		}
		OsmBaseStorage bs = new OsmBaseStorage();
		bs.getFilters().add(new IOsmStorageFilter() {
			@Override
			public boolean acceptEntityToLoad(OsmBaseStorage storage, EntityId entityId, Entity entity) {
				for (Map<String, Object> orCondition : arrayConditions) {
					boolean test = testCondition(orCondition, entityId, entity);
					if (test) {
						return true;
					}
				}
				return false;
			}
		});
		bs.parseOSM(fis, new ConsoleProgressImplementation(), original, true);
		LOG.info("File was read");
		OsmStorageWriter w = new OsmStorageWriter();
		OutputStream outputStream = new FileOutputStream(targetFile);
		if(targetFile.getName().endsWith(".gz")) {
			outputStream = new GZIPOutputStream(outputStream);
		} else if(targetFile.getName().endsWith(".bz2")) {
			outputStream = new BZip2CompressorOutputStream(outputStream);
		}
		LOG.info("Entities processed. Saving file.");
		w.saveStorage(outputStream, bs, null, true);
		outputStream.close();
		fis.close();
		LOG.info("Completed.");
		
	}

	@SuppressWarnings("unchecked")
	protected static boolean testCondition(Map<String, Object> orCondition, EntityId entityId, Entity entity) {
		boolean allMet = true;
		for (String tag : orCondition.keySet()) {
			boolean t = false;
			Object o = orCondition.get(tag);
			String value = entity.getTag(tag);
			// one of values from array
			if (o instanceof List) {
				for (Object obj : ((List<Object>) o)) {
					t = Algorithms.objectEquals(value, obj);
					if (t) {
						break;
					}
				}
			} else {
				t = Algorithms.objectEquals(value, o);
			}
			allMet = allMet && t;
		}
		return allMet;
	}
}
