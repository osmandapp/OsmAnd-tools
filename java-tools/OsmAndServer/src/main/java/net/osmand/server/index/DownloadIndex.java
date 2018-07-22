package net.osmand.server.index;

import net.osmand.server.index.DownloadIndexesService.DownloadType;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.text.SimpleDateFormat;
import java.util.Date;

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
}