package net.osmand.server;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.osmand.data.changeset.CalculateOsmChangesets;
import net.osmand.data.changeset.OsmAndLiveReports;
import net.osmand.exceptionanalyzer.ExceptionAnalyzerMain;
import net.osmand.purchases.UpdateInAppPurchase;
import net.osmand.purchases.UpdateSubscription;
import net.osmand.mailsender.EmailSenderMain;
import net.osmand.server.osmgpx.DownloadOsmGPX;
import net.osmand.server.utilities.GenerateWebTranslations;
import net.osmand.server.utilities.StyleDataExtractor;
import net.osmand.server.utilities.WikidataUtilities;

public class ServerUtilities {

	public static void main(String[] args) throws Exception {
		String utl = args[0];
		List<String> subArgs = new ArrayList<String>(Arrays.asList(args).subList(1, args.length));
		String[] subArgsArray = subArgs.toArray(new String[args.length - 1]);
		if (utl.equals("update-subscriptions")) {
			UpdateSubscription.main(subArgsArray);
        } else if (utl.equals("update-inapps")) {
            UpdateInAppPurchase.main(subArgsArray);
		} else if (utl.equals("upload-apk")) {
			ApkPublisher.main(subArgsArray);
		} else if (utl.equals("send-email")) {
			EmailSenderMain.main(subArgsArray);
		} else if (utl.equals("analyze-exceptions")) {
			ExceptionAnalyzerMain.main(subArgsArray);
		} else if (utl.equals("update-countries-for-changeset")) {
			CalculateOsmChangesets.calculateCountries();
		} else if (utl.equals("download-changeset")) {
			CalculateOsmChangesets.downloadChangesets();
		} else if (utl.equals("download-osm-gpx")) {
			DownloadOsmGPX.main(subArgsArray);
		} else if (utl.equals("generate-translations")) {
			GenerateWebTranslations.generateTranslations(subArgsArray[0], subArgsArray[1]);
		} else if (utl.equals("parse-styles")) {
			StyleDataExtractor.parseStylesXml(subArgsArray[0], subArgsArray[1]);
		} else if (utl.equals("parse-poi-styles")) {
			StyleDataExtractor.parsePoiStylesXml(subArgsArray[0], subArgsArray[1]);
		} else if (utl.equals("parse-wikidata-licenses")) {
			WikidataUtilities.parseWikidataLicenses(subArgsArray[0]);
		} else if (utl.equals("generate-reports")) {
			OsmAndLiveReports.main(subArgsArray);
		} else {
			System.err.println("Unknown command");
			System.exit(1);
		}
	}
}
