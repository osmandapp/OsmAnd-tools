package net.osmand.server.service;

import net.osmand.server.index.IndexAttributes;
import net.osmand.server.index.dao.AbstractDAO;
import net.osmand.server.index.type.Type;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

@Service
public class UpdateIndexesService implements UpdateIndexes {
    private static final Log LOGGER = LogFactory.getLog(UpdateIndexesService.class);

    private static final String MAP_INDEX_FILE = "/var/www-download/my_indexes.xml";

    @Autowired
    @Qualifier("Maps")
    private AbstractDAO mapsDAO;

    @Autowired
    @Qualifier("Voices")
    private AbstractDAO voicesDAO;

    @Autowired
    @Qualifier("Fonts")
    private AbstractDAO fontsDAO;

    @Autowired
    @Qualifier("Depths")
    private AbstractDAO depthsDAO;

    @Autowired
    @Qualifier("Wikimaps")
    private AbstractDAO wikimapsDAO;

    @Autowired
    @Qualifier("Wikivoyage")
    private AbstractDAO wikivoyageDAO;

    @Autowired
    @Qualifier("RoadMaps")
    private AbstractDAO roadMapsDAO;

    @Autowired
    @Qualifier("SrtmMaps")
    private AbstractDAO srtmMapsDAO;

    @Autowired
    @Qualifier("Hillshades")
    private AbstractDAO hillshadesDAO;

    private void close(XMLStreamWriter writer) {
        try {
            writer.close();
        } catch (XMLStreamException ex) {
            LOGGER.error(ex.getMessage(), ex);
        }
    }

    private void writeElement(XMLStreamWriter writer, Type type) throws XMLStreamException {
        writer.writeEmptyElement(type.getElementName());
        writer.writeAttribute(IndexAttributes.TYPE, type.getType());
        writer.writeAttribute(IndexAttributes.CONTAINER_SIZE, type.getContainerSize());
        writer.writeAttribute(IndexAttributes.CONTENT_SIZE, type.getContentSize());
        writer.writeAttribute(IndexAttributes.TIMESTAMP, type.getTimestamp());
        writer.writeAttribute(IndexAttributes.DATE, type.getDate());
        writer.writeAttribute(IndexAttributes.SIZE, type.getSize());
        writer.writeAttribute(IndexAttributes.TARGET_SIZE, type.getTargetSize());
        writer.writeAttribute(IndexAttributes.NAME, type.getName());
        writer.writeAttribute(IndexAttributes.DESCRIPTION, type.getDescription());
    }

    private void writeType(XMLStreamWriter writer, List<Type> types) throws XMLStreamException {
        for (Type type : types) {
            writer.writeCharacters("\n\t");
            writeElement(writer, type);
        }
    }

    @Override
    public void update() {
        try (FileOutputStream fos = new FileOutputStream(MAP_INDEX_FILE)) {
            XMLOutputFactory factory = XMLOutputFactory.newInstance();
            XMLStreamWriter writer = null;
            try {
                writer = factory.createXMLStreamWriter(fos);
                writer.writeStartDocument();
                writer.writeCharacters("\n");
                writer.writeStartElement("osmand_regions");
                writer.writeAttribute("mapversion", "1");
                writeType(writer, mapsDAO.getAll());
                writeType(writer, voicesDAO.getAll());
                writeType(writer, fontsDAO.getAll());
                writeType(writer, depthsDAO.getAll());
                writeType(writer, wikimapsDAO.getAll());
                writeType(writer, wikivoyageDAO.getAll());
                writeType(writer, roadMapsDAO.getAll());
                writeType(writer, srtmMapsDAO.getAll());
                writeType(writer, hillshadesDAO.getAll());
                writer.writeCharacters("\n");
                writer.writeEndElement();
                writer.writeEndDocument();
            } catch (XMLStreamException ex) {
                LOGGER.error(ex.getMessage(), ex);
            } finally {
                if (writer != null) {
                    close(writer);
                }
            }
        } catch (IOException ex) {
            LOGGER.error(ex.getMessage(), ex);
        }
    }
}
