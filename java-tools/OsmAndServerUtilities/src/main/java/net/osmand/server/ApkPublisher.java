package net.osmand.server;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONException;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;

import com.google.api.client.http.FileContent;
import com.google.api.services.androidpublisher.AndroidPublisher;
import com.google.api.services.androidpublisher.AndroidPublisher.Edits;
import com.google.api.services.androidpublisher.AndroidPublisher.Edits.Bundles.Upload;
import com.google.api.services.androidpublisher.AndroidPublisher.Edits.Commit;
import com.google.api.services.androidpublisher.AndroidPublisher.Edits.Insert;
import com.google.api.services.androidpublisher.AndroidPublisher.Edits.Tracks.Update;
import com.google.api.services.androidpublisher.model.AppEdit;
import com.google.api.services.androidpublisher.model.Bundle;
import com.google.api.services.androidpublisher.model.LocalizedText;
import com.google.api.services.androidpublisher.model.Track;
import com.google.api.services.androidpublisher.model.TrackRelease;

import net.osmand.PlatformUtil;
import net.osmand.purchases.UpdateSubscription;

public class ApkPublisher {
	private static final Log log = LogFactory.getLog(ApkPublisher.class);
	// init one time
	public static final String GOOGLE_PRODUCT_NAME = "OsmAnd+";
	public static final String GOOGLE_PRODUCT_NAME_FREE = "OsmAnd";

	public static final String GOOGLE_PACKAGE_NAME = "net.osmand.plus";
	public static final String GOOGLE_PACKAGE_NAME_FREE = "net.osmand";

	protected static final String TRACK_ALPHA = "alpha";
	protected static final String TRACK_BETA = "beta";

	protected static String DEFAULT_PATH = "android/OsmAnd/res/";

	protected static String RELEASE_KEY = "release";
	protected static int MAX_RELEASE_SYMBOLS = 490;

	protected static String[][] LOCALES = new String[][] {
		{"en-US", ""},
		{"ar", "ar"},
		{"bg", "bg"},
		{"cs-CZ", "cs"},
		{"da-DK", "da"},
		{"de-DE", "de"},
		{"el-GR", "el"},
		{"es-ES", "es"},
		{"fr-FR", "fr"},
		{"hu-HU", "hu"},
		{"id", "in"},
		{"it-IT", "it"},
		{"iw-IL", "iw"},
		{"ja-JP", "ja"},
		{"ko-KR", "ko"},
		{"nl-NL", "nl"},
		{"no-NO", "nn"},
		{"pl-PL", "pl"},
		{"pt-PT", "pt"},
		{"ro", "ro"},
		{"ru-RU", "ru"},
		{"sv-SE", "sv"},
		{"tr-TR", "tr"},
		{"uk", "uk"},
	};



	public static void main2(String[] args) {
		gatherReleaseNotes(new File(DEFAULT_PATH), "4305");
	}
//
	public static List<LocalizedText> gatherReleaseNotes(File file, String version) {
		List<LocalizedText> releaseNotes = new ArrayList<LocalizedText>();
		for(int i = 0; i < LOCALES.length; i++) {
			String fld = LOCALES[i][1].length() == 0 ? "" : ("-"+ LOCALES[i][1]);
			File fl = new File(file, "values"+fld+"/strings.xml");
			String releaseNote = null; //"Release " + version;
			String key = RELEASE_KEY + "_" + version.charAt(0) + "_"+version.charAt(1);
			if (fl.exists()) {
				try {
					XmlPullParser parser = PlatformUtil.newXMLPullParser();
					FileReader fis = new FileReader(fl);
					parser.setInput(fis);
					int tok;
					boolean found = false;
					StringBuilder sb = new StringBuilder();
					while ((tok = parser.next()) != XmlPullParser.END_DOCUMENT) {
						if (tok == XmlPullParser.START_TAG && parser.getName().equals("string")) {
							found = key.equals(parser.getAttributeValue("", "name"));
							releaseNote = "";
						} else if (tok == XmlPullParser.END_TAG && parser.getName().equals("string") && found) {
							int ind = 0;
							while ((ind = sb.indexOf("\\n")) != -1) {
								sb.replace(ind, ind + 2, "\n");
							}
							ind = 0;
							while ((ind = sb.indexOf("\\\"")) != -1) {
								sb.replace(ind, ind + 2, "\"");
							}
							String[] lines = sb.toString().split("\n");
							StringBuilder res = new StringBuilder();
							for (String line : lines) {
								line = line.trim();
								if (line.length() == 0) {
									continue;
								}
								if (line.startsWith("-")) {
									line = "•" + line.substring(1);
								}
								if (res.length() + line.length() > MAX_RELEASE_SYMBOLS) {
									break;
								}
								res.append(line).append("\n");
							}
							releaseNote = res.toString().trim();
							if (releaseNote.length() > MAX_RELEASE_SYMBOLS) {
								releaseNote = releaseNote.substring(0, MAX_RELEASE_SYMBOLS).trim() + "...";
							}
							System.out.println("-------- " + LOCALES[i][0] + " ----------");
							System.out.println(releaseNote);
							break;
						} else if (tok == XmlPullParser.TEXT && found) {
							sb.append(parser.getText());
						}

					}
					fis.close();
				} catch (IOException | XmlPullParserException e) {
					log.warn("Error reading strings " + LOCALES[i][1] + ": " + e.getMessage(), e);
				}
			}
			if (releaseNote != null) {
				releaseNotes.add(new LocalizedText().setLanguage(LOCALES[i][0]).setText(releaseNote));
			}
		}
		return releaseNotes;
//		System.out.println(releaseNotes);
	}

	public static void main(String[] args) throws JSONException, IOException, GeneralSecurityException {
		String androidClientSecretFile = "";
		String path = "";
		String apkNumber = "";
		String pack = "";
		String track = null;
		for (int i = 0; i < args.length; i++) {
			if (args[i].startsWith("--androidclientsecret=")) {
				androidClientSecretFile = args[i].substring("--androidclientsecret=".length());
			} else if (args[i].startsWith("--path=")) {
				path = args[i].substring("--path=".length());
			} else if (args[i].startsWith("--version=")) {
				apkNumber = args[i].substring("--version=".length());
			} else if (args[i].startsWith("--package=")) {
				pack = args[i].substring("--package=".length());
			} else if (args[i].startsWith("--track=")) {
				track = args[i].substring("--track=".length());
			}
		}

//		List<LocalizedText> releaseNotes = new ArrayList<LocalizedText>();
//		releaseNotes.add(new LocalizedText().setLanguage("en-US").setText("Release " + version));
		List<LocalizedText> releaseNotes = gatherReleaseNotes(new File(DEFAULT_PATH), apkNumber);
		if (track == null || track.isEmpty()) {
			System.out.println("Nothing to upload - track is not selected");
			return;
		}

		AndroidPublisher publisher = UpdateSubscription.getPublisherApi(androidClientSecretFile);
		String v1 = apkNumber.charAt(0) + "";
		String v2 = apkNumber.charAt(1) + "";
		String v3 = apkNumber.substring(2, 4);
		if (v3.startsWith("0")) {
			v3 = v3.substring(1);
		}
		String version = v1 + "." + v2 + "." + v3;
		String name = pack + "-" + version + "-" + apkNumber + ".aab";
		String appName = pack.contains("plus") ? "OsmAnd+" : "OsmAnd";
		//
		FileContent aabFile = new FileContent("application/octet-stream", new File(path, name));
		final Edits edits = publisher.edits();
		// Create a new edit to make changes to your listing.
		Insert editRequest = edits.insert(pack, null);
		AppEdit edit = editRequest.execute();
		final String editId = edit.getId();
		log.info(String.format("Created edit with id: %s", editId));

		Upload uploadRequest = edits.bundles().upload(pack, editId, aabFile);
		Bundle bundle = uploadRequest.execute();
		log.info(String.format("Version code %d has been uploaded", bundle.getVersionCode()));
		List<Long> versionCode = Collections.singletonList(Long.valueOf(bundle.getVersionCode()));

		Update updateTrackRequest = edits.tracks().update(pack, editId, track,
				new Track().setReleases(Collections.singletonList(new TrackRelease().setName(appName + " " + version)
						.setVersionCodes(versionCode).setStatus("completed").setReleaseNotes(releaseNotes))));
		Track updatedTrack = updateTrackRequest.execute();
		log.info(String.format("Track %s has been updated.", updatedTrack.getTrack()));

		// Commit changes for edit.
		Commit commitRequest = edits.commit(pack, editId);
		AppEdit appEdit = commitRequest.execute();
		log.info(String.format("App edit with id %s has been comitted", appEdit.getId()));

	}
}
