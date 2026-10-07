package net.osmand.server.api.services;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import net.osmand.data.LatLon;
import net.osmand.tester.GenerateTurnLanesTest;

/**
 * The files the turn-lanes pages keep - CSV with a header, meta.json beside it - read and written the one way: a
 * file is replaced whole, through a temporary one, so a reader never sees half of it.
 */
final class TurnLanesFiles {

	private static final CSVFormat WITH_HEADER = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build();

	private TurnLanesFiles() {
	}

	/** the rows of a CSV that {@code keep} takes, as header name -> value, at most {@code limit}; none without it */
	static List<Map<String, String>> readCsv(File f, Predicate<CSVRecord> keep, int limit) throws IOException {
		List<Map<String, String>> out = new ArrayList<>();
		if (f == null || !f.exists()) {
			return out;
		}
		try (Reader r = Files.newBufferedReader(f.toPath(), StandardCharsets.UTF_8);
		     CSVParser parser = WITH_HEADER.parse(r)) {
			for (CSVRecord rec : parser) {
				if (out.size() >= limit) {
					break;
				}
				if (keep.test(rec)) {
					out.add(new LinkedHashMap<>(rec.toMap()));
				}
			}
		}
		return out;
	}

	static List<Map<String, String>> readCsv(File f) throws IOException {
		return readCsv(f, rec -> true, Integer.MAX_VALUE);
	}

	/** every record of a CSV, in order, for a reader that needs them as they are */
	static List<CSVRecord> readRecords(File f) throws IOException {
		try (Reader r = Files.newBufferedReader(f.toPath(), StandardCharsets.UTF_8);
		     CSVParser parser = WITH_HEADER.parse(r)) {
			return parser.getRecords();
		}
	}

	/** rows given as header name -> value, the columns in the order of {@code header} */
	static void writeCsv(File f, String[] header, Iterable<Map<String, String>> rows) throws IOException {
		StringWriter w = new StringWriter();
		GenerateTurnLanesTest.writeCsvRow(w, header);
		for (Map<String, String> row : rows) {
			String[] values = new String[header.length];
			for (int i = 0; i < header.length; i++) {
				values[i] = row.get(header[i]);
			}
			GenerateTurnLanesTest.writeCsvRow(w, values);
		}
		write(f.toPath(), w.toString());
	}

	/** the whole of a file, replaced at once */
	static void write(Path file, String content) throws IOException {
		Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
		Files.writeString(tmp, content, StandardCharsets.UTF_8);
		Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
	}

	/** a folder and all in it; nothing when there is none */
	static void deleteTree(Path root) throws IOException {
		if (!Files.exists(root)) {
			return;
		}
		try (Stream<Path> walk = Files.walk(root)) {
			for (Path p : walk.sorted(Comparator.reverseOrder()).toList()) {
				Files.deleteIfExists(p);
			}
		}
	}

	/** "lat,lon" as the datasets write it */
	static LatLon latLon(String s) {
		String[] p = s.split(",");
		if (p.length != 2) {
			throw new IllegalArgumentException("Not a point: " + s);
		}
		return new LatLon(Double.parseDouble(p[0].trim()), Double.parseDouble(p[1].trim()));
	}

	/** a point as the datasets write it: "lat,lon" with six decimals */
	static String format(LatLon p) {
		return String.format(Locale.US, "%.6f,%.6f", p.getLatitude(), p.getLongitude());
	}
}
