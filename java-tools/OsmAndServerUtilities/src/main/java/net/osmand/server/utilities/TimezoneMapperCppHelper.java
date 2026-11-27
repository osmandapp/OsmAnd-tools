package net.osmand.server.utilities;

import static net.osmand.server.utilities.TimezoneMapperTestDataGenerator.CITIES_FILE;
import static net.osmand.server.utilities.TimezoneMapperTestDataGenerator.IANA_FILE;
import static net.osmand.server.utilities.TimezoneMapperTestDataGenerator.normalize;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Polygon;

import net.osmand.server.utilities.TimezoneMapperGenerator.Node;
import net.osmand.server.utilities.TimezoneMapperGenerator.OutputFormat;
import net.osmand.server.utilities.TimezoneMapperGenerator.TimezoneBoundaryRelease;
import net.osmand.server.utilities.TimezoneMapperTestDataGenerator.TimezoneTestData;
import net.osmand.server.utilities.TimezoneMapperValidator.CompiledTimezoneMapper;

/** Emits a standalone C++ mapper and a native test with cases prepared from release JSON. */
public final class TimezoneMapperCppHelper {

	private static final String TEST_SOURCE = "TimezoneMapperValidation.cpp";
	private static final String TEST_BINARY = "timezone-mapper-validation";

	private TimezoneMapperCppHelper() {
	}

	static void writeMapper(TimezoneMapperGenerator generator, Node root, Path output, String release, String hash)
			throws IOException {
		try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
			TimezoneMapperGenerator.writeHeader(writer, release, hash);
			writer.write("""
					#include <array>
					#include <cmath>
					#include <cstddef>

					class TimezoneMapper {
					public:
						static const char* latLngToTimezoneString(double lat, double lng) {
							if (!std::isfinite(lat) || !std::isfinite(lng) || lat < -90 || lat > 90
									|| lng < -180 || lng > 180) return "unknown";
							return TIMEZONES[lookup0(static_cast<float>(lat), static_cast<float>(lng))];
						}
					private:
						struct Ring {
							const float* points;
							std::size_t count;
							bool contains(float lat, float lng) const {
								bool inside = false;
								for (std::size_t i = 0, j = count - 2; i < count; j = i, i += 2) {
									double yi = points[i], xi = points[i + 1], yj = points[j], xj = points[j + 1];
									if ((yi > lat) != (yj > lat) && lng < (xj - xi) * (lat - yi) / (yj - yi) + xi) {
										inside = !inside;
									}
								}
								return inside;
							}
						};
						struct TzPolygon {
							const Ring* rings;
							std::size_t count;
							bool contains(float lat, float lng) const {
								if (!rings[0].contains(lat, lng)) return false;
								for (std::size_t i = 1; i < count; i++) {
									if (rings[i].contains(lat, lng)) return false;
								}
								return true;
							}
						};
						static constexpr const char* TIMEZONES[] = {
					""");
			for (String timezone : generator.timezones) {
				writer.write("\t\t" + quote(timezone) + ",\n");
			}
			writer.write("\t};\n\n");
			generator.writeLookupMethods(root, writer, OutputFormat.CPP);
			for (int i = 0; i < generator.outputPolygons.size(); i++) {
				Polygon polygon = generator.outputPolygons.get(i).polygon;
				for (int ring = 0; ring <= polygon.getNumInteriorRing(); ring++) {
					Coordinate[] points = (ring == 0 ? polygon.getExteriorRing()
							: polygon.getInteriorRingN(ring - 1)).getCoordinates();
					writer.write("\tstatic constexpr float POINTS_" + i + "_" + ring + "[] = {");
					for (int p = 0; p < points.length - 1; p++) {
						if (p > 0) writer.write(", ");
						writer.write((float) (Math.round(points[p].y * 1e6) / 1e6) + "f, "
								+ (float) (Math.round(points[p].x * 1e6) / 1e6) + "f");
					}
					writer.write("};\n");
				}
				writer.write("\tstatic constexpr Ring RINGS_" + i + "[] = {\n");
				for (int ring = 0; ring <= polygon.getNumInteriorRing(); ring++) {
					String name = "POINTS_" + i + "_" + ring;
					writer.write("\t\t{" + name + ", sizeof(" + name + ") / sizeof(*" + name + ")},\n");
				}
				writer.write("\t};\n\n");
			}
			writer.write("\tstatic constexpr std::array<TzPolygon, " + generator.outputPolygons.size()
					+ "> POLYGONS = {{\n");
			for (int i = 0; i < generator.outputPolygons.size(); i++) {
				writer.write("\t\t{RINGS_" + i + ", sizeof(RINGS_" + i + ") / sizeof(*RINGS_" + i + ")},\n");
			}
			writer.write("\t}};\n};\n");
			writer.write("\nconstexpr const char* TimezoneMapper::TIMEZONES[];\n");
			for (int i = 0; i < generator.outputPolygons.size(); i++) {
				Polygon polygon = generator.outputPolygons.get(i).polygon;
				for (int ring = 0; ring <= polygon.getNumInteriorRing(); ring++) {
					writer.write("constexpr float TimezoneMapper::POINTS_" + i + "_" + ring + "[];\n");
				}
				writer.write("constexpr TimezoneMapper::Ring TimezoneMapper::RINGS_" + i + "[];\n");
			}
			writer.write("constexpr std::array<TimezoneMapper::TzPolygon, " + generator.outputPolygons.size()
					+ "> TimezoneMapper::POLYGONS;\n");
		}
	}

	static boolean validateMapper(TimezoneBoundaryRelease release, Path basemap, Path output) throws Exception {
		Path directory = Files.createTempDirectory("timezone-mapper-cpp-");
		Path archive = directory.resolve("polygons.zip");
		Path javaSource = directory.resolve(OutputFormat.JAVA.sourceFileName());
		Path cppSource = directory.resolve(OutputFormat.CPP.sourceFileName());
		try {
			TimezoneMapperGenerator.download(release, archive);
			TimezoneMapperGenerator.generateMapper(release, archive, javaSource, OutputFormat.JAVA);
			TimezoneTestData data;
			try (CompiledTimezoneMapper mapper = TimezoneMapperValidator.compileMapper(directory, OutputFormat.JAVA)) {
				data = TimezoneMapperTestDataGenerator.generateTestData(release.version(), basemap, mapper);
			}
			TimezoneMapperGenerator.generateMapper(release, archive, cppSource, OutputFormat.CPP);
			TimezoneMapperValidator.saveReleaseFiles(cppSource, output, data);
			System.out.println("Validating C++ TimezoneMapper " + release.version());
			boolean valid = validateReleaseMapper(cppSource, data);
			if (!valid) {
				TimezoneMapperValidator.printValidationFailure(
						List.of(TimezoneMapperValidator.RELEASE_CHECK_STEP), output, OutputFormat.CPP);
			}
			return valid;
		} finally {
			Files.deleteIfExists(archive);
			Files.deleteIfExists(javaSource);
			Files.deleteIfExists(cppSource);
			Files.deleteIfExists(directory.resolve(TimezoneMapperValidator.CLASSES_JAR));
			Files.delete(directory);
		}
	}

	static boolean validateReleaseMapper(Path source, TimezoneTestData data) throws IOException, InterruptedException {
		Path directory = source.toAbsolutePath().getParent();
		Path testSource = directory.resolve(TEST_SOURCE);
		Path binary = directory.resolve(TEST_BINARY);
		try {
			System.out.println(TimezoneMapperValidator.RELEASE_CHECK_STEP);
			writeValidationSource(source, testSource, data);
			int compiled = runCommand("clang++", "-std=c++14", "-pedantic-errors", "-O2", "-ffp-contract=off",
					testSource.toString(), "-o", binary.toString());
			if (compiled != 0) {
				System.err.println("C++ compilation failed: exit code " + compiled);
				return false;
			}
			return runCommand(binary.toString()) == 0;
		} finally {
			Files.deleteIfExists(binary);
			Files.deleteIfExists(testSource);
		}
	}

	static void writeValidationSource(Path source, Path output, TimezoneTestData data) throws IOException {
		try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
			writer.write("#include " + quote(source.getFileName().toString()) + "\n");
			writer.write("""
					#include <cstring>
					#include <iomanip>
					#include <iostream>
					#include <string>

					struct TestCase { const char* city; double lat, lon; const char* expected; const char* accepted; };
					""");
			writeTestCases(writer, "IANA_CASES", data.iana(), data.aliases());
			writeTestCases(writer, "CITY_CASES", data.cities(), data.aliases());
			writer.write("""
					static bool check(const char* name, const TestCase* cases, std::size_t count) {
						std::size_t mismatches = 0;
						for (std::size_t i = 0; i < count; i++) {
							const auto& c = cases[i];
							const char* actual = TimezoneMapper::latLngToTimezoneString(c.lat, c.lon);
							const std::string token = "|" + std::string(actual) + "|";
							if (!std::strstr(c.accepted, token.c_str())) {
								mismatches++;
								std::cout << std::setprecision(15) << c.city << " (" << c.lat << ", " << c.lon
									<< "): expected=" << c.expected << " actual=" << actual << '\\n';
							}
						}
						std::cout << name << ": total=" << count << " mismatches=" << mismatches << '\\n';
						return mismatches == 0;
					}

					int main() {
					""");
			writer.write("\tbool iana = check(" + quote(IANA_FILE)
					+ ", IANA_CASES, sizeof(IANA_CASES) / sizeof(*IANA_CASES));\n");
			writer.write("\tbool cities = check(" + quote(CITIES_FILE)
					+ ", CITY_CASES, sizeof(CITY_CASES) / sizeof(*CITY_CASES));\n");
			writer.write("\treturn iana && cities ? 0 : 1;\n}\n");
		}
	}

	private static void writeTestCases(BufferedWriter writer, String name, JsonObject data, JsonObject aliases)
			throws IOException {
		if (data.getAsJsonArray("cases").size() == 0) {
			throw new IllegalArgumentException("Empty C++ test dataset: " + name);
		}
		writer.write("static const TestCase " + name + "[] = {\n");
		for (JsonElement element : data.getAsJsonArray("cases")) {
			JsonObject testCase = element.getAsJsonObject();
			String timezone = testCase.get("timezone").getAsString();
			Set<String> expected = new TreeSet<>(Set.of(normalize(timezone, aliases)));
			if (testCase.has("alternatives")) {
				for (JsonElement alternative : testCase.getAsJsonArray("alternatives")) {
					expected.add(normalize(alternative.getAsString(), aliases));
				}
			}
			Set<String> accepted = new TreeSet<>(expected);
			for (String alias : aliases.keySet()) {
				if (expected.contains(normalize(alias, aliases))) {
					accepted.add(alias);
				}
			}
			writer.write("\t{" + quote(testCase.get("city").getAsString()) + ", "
					+ testCase.get("lat").getAsDouble() + ", " + testCase.get("lon").getAsDouble() + ", "
					+ quote(timezone) + ", " + quote("|" + String.join("|", accepted) + "|") + "},\n");
		}
		writer.write("};\n\n");
	}

	private static String quote(String value) {
		StringBuilder result = new StringBuilder("\"");
		for (byte b : value.getBytes(StandardCharsets.UTF_8)) {
			int c = b & 255;
			if (c == '\\' || c == '"') {
				result.append('\\').append((char) c);
			} else if (c < 32 || c >= 127) {
				result.append('\\').append((char) ('0' + (c >> 6)))
						.append((char) ('0' + ((c >> 3) & 7))).append((char) ('0' + (c & 7)));
			} else {
				result.append((char) c);
			}
		}
		return result.append('"').toString();
	}

	private static int runCommand(String... command) throws IOException, InterruptedException {
		Process process;
		try {
			process = new ProcessBuilder(command).inheritIO().start();
		} catch (IOException e) {
			throw new IOException("Cannot run " + command[0] + ": " + e, e);
		}
		try {
			return process.waitFor();
		} finally {
			process.destroy();
		}
	}
}
