package net.osmand.server.index;

import javax.xml.bind.annotation.adapters.XmlAdapter;

public class DownloadIndexTypeAdapter extends XmlAdapter<String, DownloadIndexesService.DownloadType> {

	@Override
	public DownloadIndexesService.DownloadType unmarshal(String v) throws Exception {
		DownloadIndexesService.DownloadType dt = null;
		if (v.equals("map")) {
			dt = DownloadIndexesService.DownloadType.MAP;
		} else if (v.equals("voice")) {
			dt = DownloadIndexesService.DownloadType.VOICE;
		} else if (v.equals("fonts")) {
			dt = DownloadIndexesService.DownloadType.FONTS;
		} else if (v.equals("depth")) {
			dt = DownloadIndexesService.DownloadType.DEPTH;
		} else if (v.equals("wikivoyage")) {
			dt = DownloadIndexesService.DownloadType.WIKIVOYAGE;
		} else if (v.equals("wikimap")) {
			dt = DownloadIndexesService.DownloadType.WIKIMAP;
		} else if (v.equals("srtm_map")) {
			dt = DownloadIndexesService.DownloadType.SRTM_MAP;
		} else if (v.equals("hillshade")) {
			dt = DownloadIndexesService.DownloadType.HILLSHADE;
		} else if (v.equals("road_map")) {
			dt = DownloadIndexesService.DownloadType.ROAD_MAP;
		}
		return dt;
	}

	@Override
	public String marshal(DownloadIndexesService.DownloadType v) throws Exception {
		return v.getType();
	}
}
