package net.osmand.router;


import java.io.File;

import org.apache.commons.logging.Log;

import gnu.trove.map.hash.TLongObjectHashMap;
import net.osmand.PlatformUtil;
import net.osmand.data.LatLon;
import net.osmand.obf.preparation.DBDialect;
import net.osmand.osm.edit.Entity;
import net.osmand.router.HHRouteDataStructure.*;

public class TestHHRouting {
	

	final static Log LOG = PlatformUtil.getLog(TestHHRouting.class);
	static LatLon PROCESS_START = null;
	static LatLon PROCESS_END = null;
	


	private static File testData() {
		String name = "Montenegro_europe_2.road.obf";
		name = "Netherlands_europe";
//		name = "Ukraine_europe";
//		name = "Germany";
		
		// "Netherlands_europe_2.road.obf"
		PROCESS_START = new LatLon(52.34800, 4.86206); // AMS
////		PROCESS_END = new LatLon(51.57803, 4.79922); // Breda
		PROCESS_END = new LatLon(51.35076, 5.45141); // ~Eindhoven
		
		// "Montenegro_europe_2.road.obf"
//		PROCESS_START = new LatLon(43.15274, 19.55169); 
//		PROCESS_END= new LatLon(42.45166, 18.54425);
		
		// Ukraine
//		PROCESS_START = new LatLon(50.43539, 30.48234); // Kyiv
//		PROCESS_START = new LatLon(50.01689, 36.23278); // Kharkiv
//		PROCESS_END = new LatLon(46.45597, 30.75604);   // Odessa
//		PROCESS_END = new LatLon(48.43824, 22.705723); // Mukachevo
//		
		
//		PROCESS_START = new LatLon(50.30487, 31.29761); // 
//		PROCESS_END = new LatLon(50.30573, 28.51402); //
		
		// Germany
//		PROCESS_START = new LatLon(53.06264, 8.79675); // Bremen 
//		PROCESS_END = new LatLon(48.08556, 11.50811); // Munich
		
		return new File(System.getProperty("maps.dir"), name);
	}
	
	public static void main(String[] args) throws Exception {
		File obfFile = args.length == 0 ? testData() : new File(args[0]);
		HHRoutingConfig c = null;
		String ROUTING_PROFILE = "car";
		for (String a : args) {
			if (a.startsWith("--start=")) {
				String[] latLons = a.substring("--start=".length()).split(",");
				PROCESS_START = new LatLon(Double.parseDouble(latLons[0]), Double.parseDouble(latLons[1]));
			} else if (a.startsWith("--end=")) {
				String[] latLons = a.substring("--end=".length()).split(",");
				PROCESS_END = new LatLon(Double.parseDouble(latLons[0]), Double.parseDouble(latLons[1]));
			} else if (a.startsWith("--heuristic=")) {
				if (c == null) {
					c = new HHRoutingConfig();
				}
				c.HEURISTIC_COEFFICIENT = (float) Double.parseDouble(a.substring("--heuristic=".length()));
			} else if (a.startsWith("--direction=")) {
				if (c == null) {
					c = new HHRoutingConfig();
				}
				c.DIJKSTRA_DIRECTION = (float) Double.parseDouble(a.substring("--direction=".length()));
			} else if (a.startsWith("--profile")) {
				ROUTING_PROFILE = a.substring("--profile=".length());
			} else if (a.startsWith("--ch")) {
				c = HHRoutingConfig.ch();
			} else if (a.startsWith("--preload")) {
				c.preloadSegments();
			} else if (a.startsWith("--midpoint=")) {
				String[] s = a.substring("--midpoint=".length()).split(":");
				if (c == null) {
					c = new HHRoutingConfig();
				}
				c.MIDPOINT_MAX_DEPTH = Integer.parseInt(s[0]);
				c.MIDPOINT_ERROR = Integer.parseInt(s[1]);
			}
		}
		if (PROCESS_START == null || PROCESS_END == null) {
			System.err.println("Start / end point is not specified");
			return;
		}
		File folder = obfFile.isDirectory() ? obfFile : obfFile.getParentFile();
		String name = obfFile.getCanonicalFile().getName() + "_" + ROUTING_PROFILE;
		File dbFile = new File(folder, name + HHRoutingDB.EXT);
		HHRoutePlanner<?> planner = HHRoutePlanner.create(HHRoutePlanner.prepareContext(ROUTING_PROFILE),
				new HHRoutingDB(dbFile, DBDialect.SQLITE.getDatabaseConnection(dbFile.getAbsolutePath(), LOG)));
		HHNetworkRouteRes route = planner.runRouting(PROCESS_START, PROCESS_END, c);
		TLongObjectHashMap<Entity> entities = new TLongObjectHashMap<Entity>();
		for (HHNetworkSegmentRes r : route.segments) {
			if (r.list != null) {
//					for (RouteSegment rs : r.list) {
//						HHRoutingUtilities.addWay(entities, rs, "highway", "secondary");
//					}
			} else if (r.segment != null) {
				HHRoutingUtilities.addWay(entities, r.segment, "highway", "primary");
			}
		}
		HHRoutingUtilities.saveOsmFile(entities.valueCollection(), new File(folder, name + "-rt.osm"));
		planner.close();
	}


}
