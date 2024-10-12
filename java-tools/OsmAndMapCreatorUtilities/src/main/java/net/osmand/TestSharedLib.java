package net.osmand;

import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxUtilities;
import net.osmand.shared.io.KFile;

public class TestSharedLib {

	
	public static void main(String[] args) {
		String fileName = args[0];
		GpxUtilities gpxUtilities = GpxUtilities.INSTANCE;
		GpxFile file = gpxUtilities.loadGpxFile(new KFile(fileName));
		System.out.println(fileName + " "  + file.getTracksCount() + " " + file.getAllPoints().size());
	}
}
 