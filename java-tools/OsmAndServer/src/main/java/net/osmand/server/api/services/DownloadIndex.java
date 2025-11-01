package net.osmand.server.api.services;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.adapters.XmlAdapter;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import net.osmand.server.api.services.DownloadIndexesService.DownloadType;

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
    
    private boolean free;
    
    private String freeMessage;
    
    private SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("dd.MM.yyyy");
    
    private SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("HH:mm:ss");

    
    @XmlAttribute(name = "free")
	public boolean isFree() {
		return free;
	}

	public void setFree(boolean free) {
		this.free = free;
	}

	@XmlAttribute(name = "freeMessage")
	public String getFreeMessage() {
		return freeMessage;
	}

	public void setFreeMessage(String freeMessage) {
		this.freeMessage = freeMessage;
	}
    public void setType(DownloadType type) {
		this.type = type;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public void setDate(long timestamp) {
		this.date = DATE_FORMAT.format(new Date(timestamp));
	}
	
	public void setDateByString(String s) throws ParseException {
		this.date = s;
		this.timestamp = DATE_FORMAT.parse(s).getTime();
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
	
	
	public String getHttpParam() {
		if (type.getHeaders().length > 0) {
			return type.getHeaders()[0];
		}
		return "standard";
	}
	
	public DownloadType getDownloadType() {
		return type;
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
	
	public String getTime() {
		return TIME_FORMAT.format(new Date(timestamp));
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

		private static final double MB = 1 << 20;

		@Override
		public Double unmarshal(String v) throws Exception {
			if (v == null || v.length() == 0) {
				return null;
			}
			return Double.parseDouble(v);
		}

		@Override
		public String marshal(Double v) throws Exception {
			return v == null ? "" : String.format(Locale.US, "%.1f", v / MB);
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
			} else if (v.equals("depthmap")) {
				dt = DownloadIndexesService.DownloadType.DEPTHMAP;
			} else if (v.equals("travel")) {
				dt = DownloadIndexesService.DownloadType.TRAVEL;
			} else if (v.equals("wikimap")) {
				dt = DownloadIndexesService.DownloadType.WIKIMAP;
			} else if (v.equals("srtm_map")) {
				dt = DownloadIndexesService.DownloadType.SRTM_MAP;
			} else if (v.equals("hillshade")) {
				dt = DownloadIndexesService.DownloadType.HILLSHADE;
			} else if (v.equals("heightmap")) {
				dt = DownloadIndexesService.DownloadType.HEIGHTMAP;
			} else if (v.equals("weather")) {
				dt = DownloadIndexesService.DownloadType.WEATHER;
			} else if (v.equals("geotiff")) {
				dt = DownloadIndexesService.DownloadType.GEOTIFF;
			} else if (v.equals("slope")) {
				dt = DownloadIndexesService.DownloadType.SLOPE;
			} else if (v.equals("road_map")) {
				dt = DownloadIndexesService.DownloadType.ROAD_MAP;
			} else if (v.equals("deleted_map")) {
				dt = DownloadIndexesService.DownloadType.DELETED_MAP;
			}
			return dt;
		}

		@Override
		public String marshal(DownloadIndexesService.DownloadType v) throws Exception {
			return v.getType();
		}
	}


	
}