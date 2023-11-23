package net.osmand.router;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.logging.Log;

import net.osmand.PlatformUtil;
import net.osmand.obf.preparation.BinaryMapIndexWriter;

public class HHRoutingOBFWriter {
	final static Log LOG = PlatformUtil.getLog(HHRoutingOBFWriter.class);
	
	
	public static void main(String[] args) throws IOException, SQLException {
		File f ;
		String profile = "car";
		if(args.length == 0) {
			f = new File(System.getProperty("maps.dir"), "Netherlands_europe_car.chdb");
			
		} else {
			f  = new File(args[0]);
			if(args.length > 1) {
				profile = args[1];
			}
		}
		new HHRoutingOBFWriter().writeFile(profile, f);
	}


	public void writeFile(String profile, File dbFile) throws IOException, SQLException {
		HHRoutingPreparationDB db = new HHRoutingPreparationDB(dbFile);
		File outFile = new File(dbFile.getParentFile(),
				dbFile.getName().substring(0, dbFile.getName().lastIndexOf('.')) + ".obf");
		long timestamp = System.currentTimeMillis();
		BinaryMapIndexWriter bmiw = new BinaryMapIndexWriter(new RandomAccessFile(outFile, "rw"), timestamp);
		bmiw.startHHRoutingIndex(timestamp, profile);
		bmiw.endHHRoutingIndex();
		bmiw.close();
	}
		

}