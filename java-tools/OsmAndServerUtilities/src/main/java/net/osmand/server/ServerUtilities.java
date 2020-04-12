package net.osmand.server;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.osmand.data.changeset.CalculateOsmChangesets;
import net.osmand.data.changeset.OsmAndLiveReports;
import net.osmand.live.subscriptions.UpdateSubscription;
import net.osmand.mailsender.EmailSenderMain;
import net.osmand.server.osmgpx.DownloadOsmGPX;

public class ServerUtilities {

	public static void main(String[] args) throws Exception {
		String utl = args[0];
		List<String> subArgs = new ArrayList<String>(Arrays.asList(args).subList(1, args.length));
		String[] subArgsArray = subArgs.toArray(new String[args.length - 1]);
		if (utl.equals("generate-report-underpaid")) {
			UpdateSubscription.main(subArgsArray);
		} else if (utl.equals("send-email")) {
			EmailSenderMain.main(subArgsArray);
		} else if (utl.equals("update-countries-for-changeset")) {
			CalculateOsmChangesets.calculateCountries();
		} else if (utl.equals("download-changeset")) {
			CalculateOsmChangesets.downloadChangesets();
		} else if (utl.equals("download-osm-gpx")) {
			DownloadOsmGPX.main(subArgsArray);
		} else if (utl.equals("generate-reports")) {
			OsmAndLiveReports.main(subArgsArray);
		} else {
			System.err.println("Uknown command");
			System.exit(1);
		}
	}
}
