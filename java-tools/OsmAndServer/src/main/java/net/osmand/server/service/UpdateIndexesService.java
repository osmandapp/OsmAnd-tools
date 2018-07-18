package net.osmand.server.service;

import net.osmand.server.index.MapIndexGenerator;
import net.osmand.server.index.Type;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

@Service
public class UpdateIndexesService implements UpdateIndexes {
	private static final Log LOGGER = LogFactory.getLog(UpdateIndexesService.class);

	private static final String MAP_INDEX_FILE = "/var/www-download/my_indexes.xml";

	private static final Path BASE_PATH = Paths.get("/var/www-download/");

	private final MapIndexGenerator generator;

	@Autowired
	public UpdateIndexesService(MapIndexGenerator generator) {
		this.generator = generator;
	}

	private void close(XMLStreamWriter writer) {
		try {
			writer.close();
		} catch (XMLStreamException ex) {
			LOGGER.error(ex.getMessage(), ex);
		}
	}

	private void writeElements(XMLStreamWriter writer) throws IOException, XMLStreamException {
		generator.generate(BASE_PATH.resolve("indexes/"), "*.obf.zip", Type.MAP, writer);
		generator.generate(BASE_PATH.resolve("indexes/"), "*.voice.zip", Type.VOICE, writer);
		generator.generate(BASE_PATH.resolve("indexes/fonts/"), "*.otf.zip", Type.FONTS, writer);
		generator.generate(BASE_PATH.resolve("indexes/inapp/depth/"), "*.obf.zip", Type.DEPTH, writer);
		generator.generate(BASE_PATH.resolve("wiki/"), "*.wiki.obf.zip", Type.WIKI, writer);
		generator.generate(BASE_PATH.resolve("wikivoyage/"), "*.sqlite", Type.WIKIVOYAGE, writer);
		generator.generate(BASE_PATH.resolve("road-indexes/"), "*.road.obf.zip", Type.ROAD_MAP, writer);
		generator.generate(BASE_PATH.resolve("srtm-countries/"), "*.srtm.obf.zip", Type.SRTM_COUNTRY, writer);
		generator.generate(BASE_PATH.resolve("hillshade/"), "*.sqlitedb", Type.HILLSHADE, writer);
	}

	private void updateIndexes() {
		try (FileOutputStream fos = new FileOutputStream(MAP_INDEX_FILE)) {
			XMLOutputFactory factory = XMLOutputFactory.newInstance();
			XMLStreamWriter writer = null;
			try {
				writer = factory.createXMLStreamWriter(fos);
				writer.writeStartDocument();
				writer.writeCharacters("\n");
				writer.writeStartElement("osmand_regions");
				writer.writeAttribute("mapversion", "1");
				writeElements(writer);
				writer.writeCharacters("\n");
				writer.writeEndElement();
				writer.writeEndDocument();
				writer.flush();
			} catch (XMLStreamException ex) {
				LOGGER.error(ex.getMessage(), ex);
				ex.printStackTrace();
			} finally {
				if (writer != null) {
					close(writer);
				}
			}
		} catch (IOException ex) {
			LOGGER.error(ex.getMessage(), ex);
			ex.printStackTrace();
		}
	}

	@Override
	public void update() {
		updateIndexes();
	}
}
