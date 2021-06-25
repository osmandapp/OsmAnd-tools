package net.osmand.travel;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLStreamException;

import org.xmlpull.v1.XmlPullParserException;

import net.osmand.IProgress;
import net.osmand.binary.MapZooms;
import net.osmand.data.TransportRoute;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.obf.preparation.IndexCreator;
import net.osmand.obf.preparation.IndexCreatorSettings;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.RouteActivityType;
import net.osmand.osm.edit.Entity;
import net.osmand.osm.edit.Entity.EntityId;
import net.osmand.osm.edit.OSMSettings.OSMTagKey;
import net.osmand.osm.edit.Relation;
import net.osmand.osm.edit.Relation.RelationMember;
import net.osmand.osm.edit.Way;
import net.osmand.osm.io.OsmBaseStorage;
import net.osmand.osm.io.OsmStorageWriter;
import net.osmand.util.Algorithms;
import rtree.RTree;

public class TravelGuideOSMRelations {

	public static void main(String[] args) throws Exception {

//		File inputFile = new File("/Users/victorshcherb/osmand/temp/map.osm");
		File inputFile = new File("/Users/victorshcherb/osmand/maps/routes/Sweden_routes.osm.gz");
		
		File folder = inputFile.getParentFile();
		String basename = inputFile.getName().substring(0, inputFile.getName().indexOf('.'));
		File intFile = new File(folder, basename + "-int.osm");
		
		parseOSMRelationFile(inputFile, intFile);
		generateFile(new File(folder, "tmp-" + basename), intFile, basename, new File(folder, basename + ".travel.obf"));
	}

	private static void generateFile(File tmpFolder, File intFile, String basename, File targetObf)
			throws IOException, SQLException, InterruptedException, XmlPullParserException {
		IndexCreatorSettings settings = new IndexCreatorSettings();
		settings.indexMap = true;
		settings.indexAddress = false;
		settings.indexPOI = true;
		settings.indexTransport = false;
		settings.indexRouting = false;
		RTree.clearCache();
		try {
			tmpFolder.mkdirs();
			IndexCreator ic = new IndexCreator(tmpFolder, settings);
			MapRenderingTypesEncoder types = new MapRenderingTypesEncoder(null, basename);
			ic.setMapFileName(basename);
			// IProgress.EMPTY_PROGRESS
			IProgress prog = IProgress.EMPTY_PROGRESS;
			// prog = new ConsoleProgressImplementation();
			ic.generateIndexes(intFile, prog, null, MapZooms.getDefault(), types, null);
			new File(tmpFolder, ic.getMapFileName()).renameTo(targetObf);
		} finally {
			Algorithms.removeAllFiles(tmpFolder);
		}
	}

	private static void parseOSMRelationFile(File inputFile, File intFile) throws IOException, XmlPullParserException, FactoryConfigurationError, XMLStreamException {
		InputStream fis;
		if (inputFile.getName().endsWith(".gz")) {
			fis = new GZIPInputStream(new FileInputStream(inputFile));
		} else {
			fis = new FileInputStream(inputFile);
		}
		OsmBaseStorage st = new OsmBaseStorage();
		st.parseOSM(fis, new ConsoleProgressImplementation());
		fis.close();
		List<Relation> routes = new ArrayList<>();
		Map<EntityId, Entity> parsedEntities = st.getRegisteredEntities();
		for (Entity e : parsedEntities.values()) {
			if (e instanceof Relation && e.getTag(OSMTagKey.ROUTE) != null) {
				e.initializeLinks(parsedEntities);
				routes.add((Relation) e);
			}
		}
		long id = -1;
		List<EntityId> saveEntities = new ArrayList<>();
		for (Relation r : routes) {
			List<Way> ways = new ArrayList<Way>();
			List<RelationMember> ms = r.getMembers();
			for (RelationMember rm : ms) {
				if (rm.getEntity() instanceof Way) {
					Way w = (Way) rm.getEntity();
					// TODO is correct id?
					Way newWay = new Way(id--, w.getNodes());
					// TODO better tags
					newWay.copyTags(r);
					newWay.putTag("route", "segment");
					newWay.putTag("route_type", "track");
					RouteActivityType activityType = RouteActivityType.getTypeFromOSMTags(r.getTags()); 
					if (activityType != null) {
						// red, blue, green, orange, yellow
						// gpxTrackTags.put("gpx_icon", "");
						newWay.putTag("gpx_bg", activityType.getColor() + "_hexagon_3_road_shield");
						if(newWay.getTag("color") == null && newWay.getTag("coloru") == null ) {
							newWay.putTag("color", activityType.getColor());
						}
						// TODO better parsing activity type
						newWay.putTag("route_activity_type", activityType.getName().toLowerCase());
					}
					ways.add(newWay);
				}
			}
			TransportRoute.mergeRouteWays(ways);
			for (Way w : ways) {
				saveEntities.add(EntityId.valueOf(w));
				parsedEntities.put(EntityId.valueOf(w), w);
			}
		}
		FileOutputStream fout = new FileOutputStream(intFile);
		new OsmStorageWriter().saveStorage(fout, parsedEntities, null, saveEntities, true);
		fout.close();
	}

}
