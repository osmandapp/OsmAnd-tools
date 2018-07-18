package net.osmand.server.index;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.zip.ZipFile;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import net.osmand.server.index.DownloadIndexesService.DownloadType;


public class DownloadIndex {

	private static final double MB =  1 << 20;
	private DownloadType type;
	private File file;
	
    private long contentSize = -1;
    private String name;
    private String description = null;
    
    private SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("dd.MM.yyyy");
    
    private static class IndexXmlAttributes {
	    public static final String TYPE = "type";
	    public static final String CONTAINER_SIZE = "containerSize";
	    public static final String CONTENT_SIZE = "contentSize";
	    public static final String TIMESTAMP = "timestamp";
	    public static final String DATE = "date";
	    public static final String SIZE = "size";
	    public static final String TARGET_SIZE = "targetSize";
	    public static final String NAME = "name";
	    public static final String DESCRIPTION = "description";
	}
    
    public DownloadIndex(String name, File lf, DownloadType type) {
    	this.type = type;
    	this.file = lf;
    	this.name = name;
	}

	public DownloadType getType() {
		return type;
	}
    
    public void setType(DownloadType type) {
		this.type = type;
	}
    
    
	public boolean isValid() {
		boolean isValid = true;
		if (isZip()) {
			try {
				new ZipFile(file);
			} catch (IOException ex) {
				isValid = false;
			}
		}
		return isValid;
	}
	
    public boolean isZip() {
		return file.getName().endsWith(".zip");
	}
    
    public void setContentSize(long contentSize) {
		this.contentSize = contentSize;
	}
    
    public long getContentSize() {
    	if(contentSize == -1) {
    		contentSize = file.length();
    	}
        return contentSize;
    }

    
    public long getContainerSize() {
    	return file.length();
    }

    public long getTimestamp() {
        return file.lastModified();
    }


    public String getTargetSize() {
        return String.format("%.1f", getContentSize() / MB);
    }
    
    public String getSize() {
        return String.format("%.1f", getContainerSize() / MB);
    }
    
    public String getName() {
        return name;
    }

    public String getDescription() {
    	if(description == null) {
    		description = type.getDefaultTitle(name);
    	}
        return description;
    }
    
    public void setDescription(String description) {
    	
		this.description = description;
	}
    

    public String getDate() {
        return DATE_FORMAT.format(new Date(getTimestamp()));
    }
    
    public void writeType(XMLStreamWriter writer) throws XMLStreamException {
		writer.writeCharacters("\n");
		writer.writeEmptyElement(type.getXmlTag());
		writer.writeAttribute(IndexXmlAttributes.TYPE, type.getType());
		writer.writeAttribute(IndexXmlAttributes.CONTAINER_SIZE, getContainerSize() +"");
		writer.writeAttribute(IndexXmlAttributes.CONTENT_SIZE, getContentSize() + "");
		writer.writeAttribute(IndexXmlAttributes.TIMESTAMP, getTimestamp() + "");
		writer.writeAttribute(IndexXmlAttributes.DATE, getDate());
		writer.writeAttribute(IndexXmlAttributes.SIZE, getSize());
		writer.writeAttribute(IndexXmlAttributes.TARGET_SIZE, getTargetSize());
		writer.writeAttribute(IndexXmlAttributes.NAME, file.getName());
		writer.writeAttribute(IndexXmlAttributes.DESCRIPTION, getDescription());
	}

}


