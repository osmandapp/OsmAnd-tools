package net.osmand.server.utilities;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import net.osmand.server.utilities.TimezoneMapperTestDataGenerator.TimezoneTestData;
import net.osmand.server.utilities.TimezoneMapperGenerator.OutputFormat;
import net.osmand.server.utilities.TimezoneMapperGenerator.TimezoneBoundaryRelease;
import net.osmand.server.utilities.TimezoneMapperValidator.CompiledTimezoneMapper;

public class TimezoneMapperGeneratorTest {

	private static final TimezoneBoundaryRelease RELEASE = new TimezoneBoundaryRelease(
			"test", URI.create("https://example.test/timezones.geojson.zip"));

	@Rule
	public TemporaryFolder temporary = new TemporaryFolder();

	@Test
	public void javaGenerationIsByteIdenticalForTheSameData() throws Exception {
		assertByteIdenticalGeneration(OutputFormat.JAVA);
	}

	@Test
	public void kotlinGenerationIsByteIdenticalForTheSameData() throws Exception {
		assertByteIdenticalGeneration(OutputFormat.KOTLIN);
	}

	@Test
	public void cppGenerationIsByteIdenticalForTheSameData() throws Exception {
		assertByteIdenticalGeneration(OutputFormat.CPP);
	}

	@Test
	public void cppTestCasesExpandAliasesAndAlternativesWithoutChangingJson() throws Exception {
		TimezoneTestData data = data("UTC");
		data.aliases().addProperty("UTC", "Etc/Universal");
		data.aliases().addProperty("Etc/Universal", "Etc/UTC");
		JsonObject testCase = data.iana().getAsJsonArray("cases").get(0).getAsJsonObject();
		testCase.addProperty("city", "Рим \"<>&\"\n");
		JsonArray alternatives = new JsonArray();
		alternatives.add("Europe/Paris");
		testCase.add("alternatives", alternatives);
		String original = data.iana().toString();
		Path first = temporary.getRoot().toPath().resolve("first.cpp");
		Path second = temporary.getRoot().toPath().resolve("second.cpp");
		Path source = Path.of(OutputFormat.CPP.sourceFileName());
		TimezoneMapperCppHelper.writeValidationSource(source, first, data);
		TimezoneMapperCppHelper.writeValidationSource(source, second, data);
		assertArrayEquals(Files.readAllBytes(first), Files.readAllBytes(second));
		String generated = Files.readString(first);
		assertTrue(generated.contains("|Etc/UTC|Etc/Universal|Europe/Paris|UTC|"));
		assertTrue(generated.contains("\\320"));
		assertTrue(generated.contains("\\\"<>&\\\"\\012"));
		assertEquals(original, data.iana().toString());
	}

	private void assertByteIdenticalGeneration(OutputFormat format) throws Exception {
		Path archive = archive();
		Path copy = Files.copy(archive, temporary.getRoot().toPath().resolve("renamed.zip"));
		Path first = temporary.newFolder("first").toPath().resolve(format.sourceFileName());
		Path second = temporary.newFolder("second").toPath().resolve(format.sourceFileName());
		TimezoneMapperGenerator.generateMapper(RELEASE, archive, first, format);
		TimezoneMapperGenerator.generateMapper(RELEASE, copy, second, format);
		assertArrayEquals(Files.readAllBytes(first), Files.readAllBytes(second));
	}

	@Test
	public void dynamicTestUsesGeneratedMapperInsteadOfBuiltMapper() throws Exception {
		assertEquals("Europe/Paris", TimezoneMapper.latLngToTimezoneString(48.8566, 2.3522));
		Path directory = temporary.newFolder("dynamic").toPath();
		TimezoneMapperGenerator.generateMapper(RELEASE, archive(),
				directory.resolve(TimezoneMapperGenerator.JAVA_FILE), OutputFormat.JAVA);
		try (CompiledTimezoneMapper mapper = TimezoneMapperValidator.compileMapper(directory, OutputFormat.JAVA)) {
			assertEquals("Etc/UTC", mapper.apply(48.8566, 2.3522));
		}
	}

	@Test
	public void dynamicValidationChecksBothDirectionsAndKeepsReleaseFilesOnFailure() throws Exception {
		Path directory = temporary.newFolder("regression").toPath();
		Path source = directory.resolve(TimezoneMapperGenerator.JAVA_FILE);
		Path output = temporary.getRoot().toPath().resolve("output");
		TimezoneMapperGenerator.generateMapper(RELEASE, archive(), source, OutputFormat.JAVA);
		try (CompiledTimezoneMapper mapper = TimezoneMapperValidator.compileMapper(directory, OutputFormat.JAVA)) {
			TimezoneTestData releaseData = data(mapper.apply(48.8566, 2.3522));
			String log = captureValidationFailure(() -> TimezoneMapperValidator.validateTimezoneMappings(
					source, output, data("Europe/Paris"), releaseData, mapper, OutputFormat.JAVA));
			assertTrue(log.contains("expected=Europe/Paris actual=Etc/UTC"));
			assertTrue(log.contains("expected=Etc/UTC actual=Europe/Paris"));
			String summary = log.substring(log.indexOf("VALIDATION FAILED"));
			assertFalse(summary.contains("Step 3:"));
			assertTrue(summary.contains("Step 4: Release mapper on current JSON"));
			assertTrue(summary.contains("Step 5: Current mapper on release JSON"));
			assertTrue(summary.contains("Release files have been saved in --output."));
			assertTrue(summary.contains("update the repository"));
			assertArrayEquals(Files.readAllBytes(source),
					Files.readAllBytes(output.resolve(TimezoneMapperGenerator.JAVA_FILE)));
			assertEquals(releaseData.cities(), new Gson().fromJson(
					Files.readString(output.resolve(TimezoneMapperTestDataGenerator.CITIES_FILE)), JsonObject.class));
			assertTrue(Files.isRegularFile(output.resolve(TimezoneMapperTestDataGenerator.ALIASES_FILE)));
			assertTrue(Files.isRegularFile(output.resolve(TimezoneMapperTestDataGenerator.IANA_FILE)));
			try (var files = Files.list(output)) {
				assertEquals(4, files.count());
			}
		}
	}

	@Test
	public void releaseSelfCheckReportsFailureAndContinuesCrossChecks() throws Exception {
		String log = captureValidationFailure(() -> TimezoneMapperValidator.validateTimezoneMappings(
				null, null, data("Europe/Paris"), data("Europe/Paris"), (lat, lon) -> "Etc/UTC", OutputFormat.JAVA));
		assertTrue(log.contains("Step 5: Current mapper on release JSON"));
		String summary = log.substring(log.indexOf("VALIDATION FAILED"));
		assertTrue(summary.contains("Step 3: Release mapper on release JSON"));
		assertTrue(summary.contains("Step 4: Release mapper on current JSON"));
		assertFalse(summary.contains("Step 5:"));
		assertTrue(summary.contains("--output=DIR"));
		assertTrue(summary.contains("resources/test-resources/tz/"));
		assertFalse(log.contains("SUCCESS!"));
	}

	@Test
	public void crossChecksNormalizeUsingEachDatasetsAliases() throws Exception {
		TimezoneTestData currentData = data("Europe/Paris");
		currentData.aliases().addProperty("Etc/UTC", "Europe/Paris");
		TimezoneTestData releaseData = data("Etc/UTC");
		releaseData.aliases().addProperty("Europe/Paris", "Etc/UTC");
		assertTrue(TimezoneMapperValidator.validateTimezoneMappings(
				null, null, currentData, releaseData, (lat, lon) -> "Etc/UTC", OutputFormat.JAVA));
	}

	@Test
	public void kotlinValidationSkipsBuiltJavaMapper() throws Exception {
		assertEquals("Europe/Paris", TimezoneMapper.latLngToTimezoneString(48.8566, 2.3522));
		assertTrue(TimezoneMapperValidator.validateTimezoneMappings(
				null, null, data("Etc/UTC"), data("Etc/UTC"), (lat, lon) -> "Etc/UTC", OutputFormat.KOTLIN));
	}

	@Test
	public void kotlinValidationReportsChangedEmbeddedExpectations() throws Exception {
		String log = captureValidationFailure(() -> TimezoneMapperValidator.validateTimezoneMappings(
				null, null, data("Europe/Paris"), data("Etc/UTC"), (lat, lon) -> "Etc/UTC", OutputFormat.KOTLIN));
		String summary = log.substring(log.indexOf("VALIDATION FAILED"));
		assertTrue(summary.contains("Step 4: Release mapper on current JSON"));
		assertTrue(summary.contains(OutputFormat.KOTLIN.sourceFileName()));
		assertFalse(log.contains("Step 5:"));
	}

	private static String captureValidationFailure(Callable<Boolean> validation) throws Exception {
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		PrintStream originalOut = System.out;
		PrintStream originalErr = System.err;
		try (PrintStream log = new PrintStream(output, true, StandardCharsets.UTF_8)) {
			System.setOut(log);
			System.setErr(log);
			assertFalse("A timezone regression must fail validation", validation.call());
		} finally {
			System.setOut(originalOut);
			System.setErr(originalErr);
		}
		return output.toString(StandardCharsets.UTF_8);
	}

	private static TimezoneTestData data(String timezone) {
		JsonObject cases = new Gson().fromJson("""
				{"cases":[{"city":"Paris","lat":48.8566,"lon":2.3522,"timezone":"%s"}]}
				""".formatted(timezone), JsonObject.class);
		return new TimezoneTestData(new JsonObject(), cases, cases);
	}

	private Path archive() throws Exception {
		Path archive = temporary.getRoot().toPath().resolve("timezones.geojson.zip");
		try (ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(archive))) {
			ZipEntry entry = new ZipEntry("combined.json");
			entry.setTime(0);
			zip.putNextEntry(entry);
			zip.write("""
					{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"tzid":"Etc/UTC"},
					"geometry":{"type":"Polygon","coordinates":[[[-5,42],[8,42],[8,51],[-5,51],[-5,42]]]}}]}
					""".getBytes(StandardCharsets.UTF_8));
			zip.closeEntry();
		}
		return archive;
	}
}
