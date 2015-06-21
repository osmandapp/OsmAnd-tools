package net.osmand;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.xml.sax.SAXException;

import net.osmand.data.preparation.OceanTilesCreator;

public class MainUtilities {

	
	public static void main(String[] args) throws IOException, SAXException {
		if(args.length == 0) {
			System.out.println("This utility provides access to all other console utilities of OsmAnd,");
			System.out.println("each utility has own argument list and own synopsys. Here is the list:");
			System.out.println("\t\t check-ocean-tile <lat> <lon> <zoom=11>: checks ocean or land tile is in bz2 list");
			System.out.println("\t\t generate-ocean-tile <coastline osm file> <optional output file> : creates ocean tiles 12 zoom");
		} else {
			String utl = args[0];
			List<String> subArgs = new  ArrayList<String>(Arrays.asList(args).subList(1, args.length));
			String[] subArgsArray = subArgs.toArray(new String[args.length -1]);
			if(utl.equals("check-ocean-tile")) {
				OceanTilesCreator.checkOceanTile(subArgsArray);
			} else if (utl.equals("generate-ocean-tile")) {
				OceanTilesCreator.createTilesFile(subArgsArray[0], subArgsArray.length > 1 ? args[1] : null);
			}
		}
	}
}
