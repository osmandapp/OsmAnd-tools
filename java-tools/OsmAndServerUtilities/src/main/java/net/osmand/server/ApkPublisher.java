package net.osmand.server;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;

import org.json.JSONException;

import com.google.api.client.http.FileContent;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.services.androidpublisher.AndroidPublisher;
import com.google.api.services.androidpublisher.AndroidPublisher.Internalappsharingartifacts.Uploadbundle;
import com.google.api.services.androidpublisher.model.InternalAppSharingArtifact;

import net.osmand.live.subscriptions.UpdateSubscription;

public class ApkPublisher {

	// init one time
	public static final String GOOGLE_PRODUCT_NAME = "OsmAnd+";
	public static final String GOOGLE_PRODUCT_NAME_FREE = "OsmAnd";

	public static final String GOOGLE_PACKAGE_NAME = "net.osmand.plus";
	public static final String GOOGLE_PACKAGE_NAME_FREE = "net.osmand";

	private static HttpRequestInitializer setHttpTimeout(final HttpRequestInitializer requestInitializer) {
		  return new HttpRequestInitializer() {
		    @Override
		    public void initialize(HttpRequest httpRequest) throws IOException {
		      requestInitializer.initialize(httpRequest);
		      httpRequest.setConnectTimeout(3 * 60000);  // 3 minutes connect timeout
		      httpRequest.setReadTimeout(3 * 60000);  // 3 minutes read timeout
		    }
		  };
	}
	
	public static void main(String[] args) throws JSONException, IOException, GeneralSecurityException {
		String androidClientSecretFile = "";
		String path = "";
		String version = "";
		String pack = "";
		for (int i = 0; i < args.length; i++) {
			if (args[i].startsWith("--androidclientsecret=")) {
				androidClientSecretFile = args[i].substring("--androidclientsecret=".length());
			} else if (args[i].startsWith("--path=")) {
				path = args[i].substring("--path=".length());
			} else if (args[i].startsWith("--version=")) {
				version = args[i].substring("--version=".length());
			} else if (args[i].startsWith("--package=")) {
				pack = args[i].substring("--package=".length());
			}
		}

		AndroidPublisher publisher = UpdateSubscription.getPublisherApi(androidClientSecretFile);
		String v1 = version.charAt(0) + "";
		String v2 = version.charAt(1) + "";
		String v3 = version.substring(2, 4);
		if (v3.startsWith("0")) {
			v3 = v3.substring(1);
		}
		String name = pack + "-" + v1 + "." + v2 + "." + v3 + "-" + version + ".aab";
		// 
		
//		setHttpTimeout(publisher.getGoogleClientRequestInitializer());
		Uploadbundle bundle = publisher.internalappsharingartifacts().uploadbundle(pack,
				new FileContent("application/octet-stream", new File(path, name)));
		InternalAppSharingArtifact artifact = bundle.execute();
		System.out.println(String.format("Release %s - uploaded fingerprint %s, url - ", name,
				artifact.getCertificateFingerprint(), artifact.getDownloadUrl()));
	}

}
