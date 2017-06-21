package net.osmand.data.diff;


/**
 * Generates obf from overpass data.
 * @author paul
 *
 */
public class OSMLiveObfCreator {
	
	private static final String PATH_TO_OVERPASS = System.getProperty("user.home") + "/2017_06_18-10_30.osm";
	private static final String PATH_TO_WORKING_DIR = "/home/paul/osmand/test/fiction/2017_06_16";
	private static final String PATH_TO_REGIONS = System.getProperty("user.home") + "/OsmAndMapCreator/regions.ocbf";
	
	public static void main(String[] args) {
		
		if (args.length < 3) {
			System.out.println("Usage: PATH_TO_OVERPASS PATH_TO_WORKING_DIR PATH_TO_REGIONS");
			return;
		}
		
		
		String[] argsToGenerateOsm = new String[] {
				args[0],
				args[1],
				args[2]
		};
		
		AugmentedDiffsInspector.main(argsToGenerateOsm);
		
		String[] argsToGenerateObf = new String[] {
				args[1]
		};
		
		GenerateDailyObf.main(argsToGenerateObf);
		
	}

}
