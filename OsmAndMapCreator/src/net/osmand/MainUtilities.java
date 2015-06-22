package net.osmand;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.xml.sax.SAXException;

import net.osmand.data.preparation.IndexCreator;
import net.osmand.data.preparation.MapZooms;
import net.osmand.data.preparation.OceanTilesCreator;
import net.osmand.impl.ConsoleProgressImplementation;
import net.osmand.osm.MapRenderingTypes;
import net.osmand.osm.MapRenderingTypesEncoder;
import net.osmand.osm.util.ResourceDeleter;
import net.osmand.regions.CountryOcbfGeneration;

public class MainUtilities {
	private static Log log = PlatformUtil.getLog(MainUtilities.class);

	
	public static void main(String[] args) throws Exception {
		if(args.length == 0) {
			printSynopsys();
		} else {
			String utl = args[0];
			List<String> subArgs = new  ArrayList<String>(Arrays.asList(args).subList(1, args.length));
			String[] subArgsArray = subArgs.toArray(new String[args.length -1]);
			if(utl.equals("check-ocean-tile")) {
				OceanTilesCreator.checkOceanTile(subArgsArray);
			} else if (utl.equals("generate-ocean-tile")) {
				OceanTilesCreator.createTilesFile(subArgsArray[0], subArgsArray.length > 1 ? args[1] : null);
			} else if(utl.equals("test-routing")) {
				net.osmand.router.TestRouting.main(subArgsArray);
			} else if(utl.equals("generate-ocbf")) {
				CountryOcbfGeneration.main(subArgsArray);
			} else if(utl.equals("generate-obf")) {
				IndexCreator ic = new IndexCreator(new File("."));
				ic.setIndexMap(true);
				ic.setIndexPOI(true);
				ic.setIndexRouting(true);
				ic.setIndexAddress(true);
				generateObf(subArgsArray, ic);
			} else if(utl.equals("generate-map")) {
				IndexCreator ic = new IndexCreator(new File("."));
				ic.setIndexMap(true);
				generateObf(subArgsArray, ic);
			} else if(utl.equals("generate-address")) {
				IndexCreator ic = new IndexCreator(new File("."));
				ic.setIndexAddress(true);
				generateObf(subArgsArray, ic);
			} else if(utl.equals("generate-poi")) {
				IndexCreator ic = new IndexCreator(new File("."));
				ic.setIndexPOI(true);
				generateObf(subArgsArray, ic);
				
			} else if(utl.equals("delete-unused-strings")) {
				ResourceDeleter.main(subArgsArray);
			} else if(utl.equals("generate-roads")) {
				IndexCreator ic = new IndexCreator(new File("."));
				ic.setIndexRouting(true);
				generateObf(subArgsArray, ic);
			} else {
				printSynopsys();
			}
		}
	}

	private static void generateObf(String[] subArgsArray, IndexCreator ic) throws IOException, SAXException,
			SQLException, InterruptedException {
		ic.generateIndexes(new File(subArgsArray[1]),
				new ConsoleProgressImplementation(), null, MapZooms.getDefault(), MapRenderingTypesEncoder.getDefault(),
				log);
	}

	private static void printSynopsys() {
		System.out.println("This utility provides access to all other console utilities of OsmAnd,");
		System.out.println("each utility has own argument list and own synopsys. Here is the list:");
		System.out.println("\t\t check-ocean-tile <lat> <lon> <zoom=11>: checks ocean or land tile is in bz2 list");
		System.out.println("\t\t generate-ocean-tile <coastline osm file> <optional output file>: creates ocean tiles 12 zoom");
		System.out.println("\t\t test-routing <own list of parameters>: helps to run routing test for specific locations");
		System.out.println("\t\t generate-ocbf <path to osmand/repos/ repository>: generates regions.ocbf file, this path should contain folders 'misc', 'tools', 'resources'");
		System.out.println("\t\t delete-unused-strings <path to repos/android/OsmAnd/res>: deletes unused translation in git repository (transforms all strings.xml)");
		System.out.println("\t\t generate-ocean-tile-osm <optional path to osm file to write> <optional zoom>: generates ocean tiles osm file to check in JOSM ");
		System.out.println("\t\t generate-obf <path to osm file>: simple way to generate obf file in place. "
				+ "\t\t\t	Another supported options generate-map, generate-address, generate-poi, generate-roads (generate obf partially)");
	}
}
