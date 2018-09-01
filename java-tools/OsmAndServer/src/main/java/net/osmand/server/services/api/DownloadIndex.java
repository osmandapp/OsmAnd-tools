package net.osmand.server.services.api;

import net.osmand.server.services.api.DownloadIndexesService.DownloadType;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.adapters.XmlAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class DownloadIndex {

	@XmlJavaTypeAdapter(DownloadIndexTypeAdapter.class)
	@XmlAttribute(name = "type")
	private DownloadType type;

	private long containerSize;

    private long contentSize;

    private long timestamp;

	@XmlAttribute(name = "date")
    private String date;

    @XmlJavaTypeAdapter(DownloadIndexSizeAdapter.class)
	@XmlAttribute(name = "size")
    private Double size;

	@XmlJavaTypeAdapter(DownloadIndexSizeAdapter.class)
	@XmlAttribute(name = "targetsize")
    private Double targetsize;

	private String name;

    private String description;
    
    private SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("dd.MM.yyyy");

    public void setType(DownloadType type) {
		this.type = type;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public void setDate(long timestamp) {
		this.date = DATE_FORMAT.format(new Date(timestamp));
	}

	public void setSize(double size) {
		this.size = size;
	}

	public void setTargetsize(double targetsize) {
		this.targetsize = targetsize;
	}

	public void setContentSize(long contentSize) {
		this.contentSize = contentSize;
	}

	public void setContainerSize(long containerSize) {
		this.containerSize = containerSize;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type.getType();
	}

	@XmlAttribute(name = "containerSize")
	public long getContainerSize() {
		return containerSize;
	}

	@XmlAttribute(name = "contentSize")
	public long getContentSize() {
		return contentSize;
	}

	@XmlAttribute(name = "timestamp")
	public long getTimestamp() {
		return timestamp;
	}

	public String getDate() {
		return date;
	}

	public Double getSize() {
		return size;
	}

	public Double getTargetsize() {
		return targetsize;
	}

	@XmlAttribute(name = "name")
	public String getName() {
		return name;
	}

	@XmlAttribute(name = "description")
	public String getDescription() {
		return description;
	}

    public void setDescription(String description) {
		this.description = description;
	}

	@Override
	public String toString() {
		return "DownloadIndex{" +
				"type=" + type +
				", containerSize=" + containerSize +
				", contentSize=" + contentSize +
				", timestamp=" + timestamp +
				", date='" + date + '\'' +
				", size=" + size +
				", targetSize=" + targetsize +
				", name='" + name + '\'' +
				", description='" + description + '\'' +
				'}';
	}
	
	public static class DownloadIndexSizeAdapter extends XmlAdapter<String, Double> {

		private static final double MB =  1 << 20;

		@Override
		public Double unmarshal(String v) throws Exception {
			return Double.parseDouble(v);
		}

		@Override
		public String marshal(Double v) throws Exception {
			return String.format(Locale.US, "%.1f", v / MB);
		}
	}
	
	public static class DownloadIndexTypeAdapter extends XmlAdapter<String, DownloadIndexesService.DownloadType> {

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
}