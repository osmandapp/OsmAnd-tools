package net.osmand.server;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONException;

import com.google.api.client.http.FileContent;
import com.google.api.services.androidpublisher.AndroidPublisher;
import com.google.api.services.androidpublisher.AndroidPublisher.Edits;
import com.google.api.services.androidpublisher.AndroidPublisher.Edits.Bundles.Upload;
import com.google.api.services.androidpublisher.AndroidPublisher.Edits.Commit;
import com.google.api.services.androidpublisher.AndroidPublisher.Edits.Insert;
import com.google.api.services.androidpublisher.AndroidPublisher.Edits.Tracks.Update;
import com.google.api.services.androidpublisher.AndroidPublisher.Internalappsharingartifacts.Uploadbundle;
import com.google.api.services.androidpublisher.model.AppEdit;
import com.google.api.services.androidpublisher.model.Bundle;
import com.google.api.services.androidpublisher.model.InternalAppSharingArtifact;
import com.google.api.services.androidpublisher.model.LocalizedText;
import com.google.api.services.androidpublisher.model.Track;
import com.google.api.services.androidpublisher.model.TrackRelease;

import net.osmand.live.subscriptions.UpdateSubscription;

public class ApkPublisher {
	private static final Log log = LogFactory.getLog(ApkPublisher.class);
	// init one time
	public static final String GOOGLE_PRODUCT_NAME = "OsmAnd+";
	public static final String GOOGLE_PRODUCT_NAME_FREE = "OsmAnd";

	public static final String GOOGLE_PACKAGE_NAME = "net.osmand.plus";
	public static final String GOOGLE_PACKAGE_NAME_FREE = "net.osmand";

	protected static final String TRACK_ALPHA = "alpha";
	protected static final String TRACK_BETA = "beta";

	public static void main(String[] args) throws JSONException, IOException, GeneralSecurityException {
		String androidClientSecretFile = "";
		String path = "";
		String apkNumber = "";
		String pack = "";
		String track = TRACK_ALPHA;
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
		List<LocalizedText> releaseNotes = new ArrayList<LocalizedText>();
		releaseNotes.add(new LocalizedText().setLanguage("en-US").setText("Release " + version));
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
