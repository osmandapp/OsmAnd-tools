package net.osmand.server.services.index;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

@XmlRootElement(name = "osmand_regions")
public class DownloadIndexDocument {

	@XmlAttribute(name = "mapversion")
	private int mapVersion = 1;

	private String timestamp;

	private String gentime;

	public void setMapVersion(int mapVersion) {
		this.mapVersion = mapVersion;
	}

	@XmlElement(name = "region")
	private List<DownloadIndex> maps = new ArrayList<>();

	@XmlElement(name = "inapp")
	private List<DownloadIndex> inapps = new ArrayList<>();

	@XmlElement(name = "fonts")
	private List<DownloadIndex> fonts = new ArrayList<>();

	@XmlElement(name = "region")
	private List<DownloadIndex> voices = new ArrayList<>();

	@XmlElement(name = "inapp")
	private List<DownloadIndex> depths = new ArrayList<>();

	@XmlElement(name = "wiki")
	private List<DownloadIndex> wikimaps = new ArrayList<>();

	@XmlElement(name = "wikivoyage")
	private List<DownloadIndex> wikivoyages = new ArrayList<>();

	@XmlElement(name = "road_region")
	private List<DownloadIndex> roadMaps = new ArrayList<>();

	@XmlElement(name = "srtmcountry")
	private List<DownloadIndex> srtmMaps = new ArrayList<>();

	@XmlElement(name = "hillshade")
	private List<DownloadIndex> hillshade = new ArrayList<>();

	public List<DownloadIndex> getMaps() {
		return maps;
	}

	public List<DownloadIndex> getInapps() {
		return inapps;
	}

	public List<DownloadIndex> getFonts() {
		return fonts;
	}

	public List<DownloadIndex> getVoices() {
		return voices;
	}

	public List<DownloadIndex> getDepths() {
		return depths;
	}

	public List<DownloadIndex> getWikimaps() {
		return wikimaps;
	}

	public List<DownloadIndex> getWikivoyages() {
		return wikivoyages;
	}

	public List<DownloadIndex> getRoadMaps() {
		return roadMaps;
	}

	public List<DownloadIndex> getSrtmMaps() {
		return srtmMaps;
	}

	public List<DownloadIndex> getHillshade() {
		return hillshade;
	}

	@XmlAttribute
	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	@XmlAttribute
	public String getGentime() {
		return gentime;
	}

	public void setGentime(String gentime) {
		this.gentime = gentime;
	}
}
