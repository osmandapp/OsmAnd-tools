package net.osmand.server.service;

import net.osmand.server.index.MapIndexGenerator;
import net.osmand.server.index.type.TypeFactory;
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
	private final TypeFactory typeFactory;

	@Autowired
	public UpdateIndexesService(MapIndexGenerator generator, TypeFactory typeFactory) {
		this.generator = generator;
		this.typeFactory = typeFactory;
	}

	private void close(XMLStreamWriter writer) {
		try {
			writer.close();
		} catch (XMLStreamException ex) {
			LOGGER.error(ex.getMessage(), ex);
		}
	}

	@Override
	public void update() {
	    System.out.println("Update");
		try (FileOutputStream fos = new FileOutputStream(MAP_INDEX_FILE)) {
			XMLOutputFactory factory = XMLOutputFactory.newInstance();
			XMLStreamWriter writer = null;
			try {
			    System.out.println("Write");
				writer = factory.createXMLStreamWriter(fos);
				writer.writeStartDocument();
				writer.writeCharacters("\n");
				writer.writeStartElement("osmand_regions");
				writer.writeAttribute("mapversion", "1");

				generator.generate(BASE_PATH.resolve("indexes/"), "*.obf.zip", typeFactory.newMapType(), writer);
				generator.generate(BASE_PATH.resolve("indexes/"), "*.voice.zip", typeFactory.newVoiceType(), writer);
				generator.generate(BASE_PATH.resolve("indexes/fonts/"), "*.otf.zip", typeFactory.newFontsType(), writer);
				generator.generate(BASE_PATH.resolve("indexes/inapp/depth/"), "*.obf.zip", typeFactory.newDepthType(), writer);
				generator.generate(BASE_PATH.resolve("road-indexes/"), "*.road.obf.zip", typeFactory.newRoadmapType(), writer);
				generator.generate(BASE_PATH.resolve("srtm-countries/"), "*.srtm.obf.zip", typeFactory.newSrtmMapType(), writer);
				generator.generate(BASE_PATH.resolve("wikivoyage/"), "*.sqlite", typeFactory.newWikivoyageType(), writer);
				generator.generate(BASE_PATH.resolve("wiki/"), "*.wiki.obf.zip", typeFactory.newWikimapType(), writer);
				generator.generate(BASE_PATH.resolve("hillshade/"), "*.sqlitedb", typeFactory.newHillshadeType(), writer);
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
}
