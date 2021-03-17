package net.osmand.server.api.services;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

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
	
	@XmlElement(name = "travel")
	private List<DownloadIndex> travel = new ArrayList<>();

	@XmlElement(name = "road_region")
	private List<DownloadIndex> roadMaps = new ArrayList<>();

	@XmlElement(name = "srtmcountry")
	private List<DownloadIndex> srtmMaps = new ArrayList<>();
	
	@XmlElement(name = "srtmfeetcountry")
	private List<DownloadIndex> srtmFeetMaps = new ArrayList<>();

	@XmlElement(name = "hillshade")
	private List<DownloadIndex> hillshade = new ArrayList<>();
	
	@XmlElement(name = "slope")
	private List<DownloadIndex> slope = new ArrayList<>();
	
	public void prepareMaps() {
		sortMaps(maps);
		sortMaps(roadMaps);
		sortMaps(srtmMaps);
		sortMaps(srtmFeetMaps);
		sortMaps(slope);
		sortMaps(hillshade);
		sortMaps(inapps);
		sortMaps(voices);
		sortMaps(fonts);
		sortMaps(depths);
		sortMaps(wikimaps);
		sortMaps(wikivoyages);
		sortMaps(travel);
	}

	public void sortMaps(List<DownloadIndex> l) {
		Comparator<DownloadIndex> cmp = new Comparator<DownloadIndex>() {

			@Override
			public int compare(DownloadIndex o1, DownloadIndex o2) {
				return o1.getName().compareTo(o2.getName());
			}
		};
		Collections.sort(l, cmp);
	}

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
	
	public List<DownloadIndex> getTravelGuides() {
		return travel;
	}

	public List<DownloadIndex> getRoadMaps() {
		return roadMaps;
	}

	public List<DownloadIndex> getSrtmMaps() {
		return srtmMaps;
	}
	
	public List<DownloadIndex> getSrtmFeetMaps() {
		return srtmFeetMaps;
	}

	public List<DownloadIndex> getHillshade() {
		return hillshade;
	}
	
	public List<DownloadIndex> getSlope() {
		return slope;
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
