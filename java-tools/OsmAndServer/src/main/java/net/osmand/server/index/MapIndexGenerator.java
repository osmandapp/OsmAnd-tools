package net.osmand.server.index;

import org.springframework.stereotype.Component;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

@Component
public class MapIndexGenerator {
	private static final String ZIP_EXT = ".zip";

	private boolean isZip(Path path) {
		String fileName = path.getFileName().toString();
		return fileName.endsWith(".zip");
	}

	private boolean validateZipFile(Path zipFile) {
		boolean isValid = true;
		try {
			new ZipFile(zipFile.toFile());
		} catch (IOException ex) {
			isValid = false;
		}
		return isValid;
	}

	private long calculateContentSizeForZipFile(Path path) throws IOException {
		ZipFile zipFile = new ZipFile(path.toFile());
		return zipFile.stream().mapToLong(ZipEntry::getSize).sum();
	}

	private void processAttributes(Type type, Path path) throws IOException {
		BasicFileAttributes fileAttributes = Files.readAttributes(path, BasicFileAttributes.class);
		String fileName = path.getFileName().toString();
		long sizeInBytes = fileAttributes.size();
		if (isZip(path)) {
			long contentSize = calculateContentSizeForZipFile(path);
			type.setContentSize(contentSize);
			type.setTargetSize(contentSize);
		} else {
			type.setContentSize(sizeInBytes);
			type.setTargetSize(sizeInBytes);
		}
		type.setContainerSize(sizeInBytes);
		type.setSize(sizeInBytes);
		type.setName(fileName);
		type.setTimestamp(fileAttributes.creationTime().to(TimeUnit.MILLISECONDS));
	}

	private void writeType(Type type, XMLStreamWriter writer) throws XMLStreamException {
		writer.writeCharacters("\n");
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

	public void generate(Path path, String glob, Type type, XMLStreamWriter writer)
			throws IOException, XMLStreamException {
		if (!Files.isDirectory(path)) {
			throw new IOException("It's not a directory");
		}
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(path, glob)) {
			for (Path file : stream) {
				if (isZip(file) && !validateZipFile(file)) {
					continue;
				}
				processAttributes(type, file);
				writeType(type, writer);
			}
		}
	}
}