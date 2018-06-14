package net.osmand;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;

public class TestImageUrl {

	private static final String IMAGE_ROOT_URL = "https://upload.wikimedia.org/wikipedia/commons/";
	private static final String THUMB_PREFIX = "320px-";
	private static final String REGULAR_PREFIX = "800px-";
	
	public static void main(String[] args) {
		System.out.println(getImageUrl("Kiev_banner_Maidan_Nezalezhnosti_%28Independence_Square%29.jpg", false));	
	}
	
	public static String getImageUrl(String imageTitle, boolean thumbnail) {
		try {
			imageTitle = URLDecoder.decode(imageTitle, "UTF-8");
		} catch (IOException e) {
		}
		String[] hash = getHash(imageTitle);
		String prefix = thumbnail ? THUMB_PREFIX : REGULAR_PREFIX;
		return IMAGE_ROOT_URL + "thumb/" + hash[0] + "/" + hash[1] + "/" + imageTitle + "/" + prefix + imageTitle;
	}
	private static String[] getHash(String s) {
		String md5 = new String(Hex.encodeHex(DigestUtils.md5(s.replace(" ", "_"))));
		return new String[]{md5.substring(0, 1), md5.substring(0, 2)};
	}
}
