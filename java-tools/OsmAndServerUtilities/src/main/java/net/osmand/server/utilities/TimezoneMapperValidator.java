package net.osmand.server.utilities;

import static net.osmand.server.utilities.TimezoneMapperTestDataGenerator.ALIASES_FILE;
import static net.osmand.server.utilities.TimezoneMapperTestDataGenerator.CITIES_FILE;
import static net.osmand.server.utilities.TimezoneMapperTestDataGenerator.IANA_FILE;
import static net.osmand.server.utilities.TimezoneMapperTestDataGenerator.checkTimezoneMappings;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiFunction;

import javax.tools.JavaCompiler;
import javax.tools.StandardJavaFileManager;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;

import net.osmand.MainUtilities.CommandLineOpts;
import net.osmand.server.utilities.TimezoneMapperGenerator.OutputFormat;
import net.osmand.server.utilities.TimezoneMapperGenerator.TimezoneBoundaryRelease;
import net.osmand.server.utilities.TimezoneMapperTestDataGenerator.TimezoneCheckResult;
import net.osmand.server.utilities.TimezoneMapperTestDataGenerator.TimezoneTestData;

/** Cross-checks a release mapper and its test data against the versions embedded in this JAR. */
public class TimezoneMapperValidator {

	private static final String CLASSES_JAR = "classes.jar";

	public static void main(String[] args) throws Exception {
		CommandLineOpts opts = new CommandLineOpts(args);
		if (opts.getBoolean("--help")) {
			System.out.println("""
					Usage: utilities.sh validate-timezone-mapper [options]

					Requires JDK 17+; Java needs javac, Kotlin needs kotlinc in PATH. Steps:
					  1. Download the selected timezone-boundary-builder release, generate and
					     compile its mapper, then load it in an isolated class loader.
					  2. Generate release JSON: aliases and reference points from the matching
					     IANA release, and basemap city expectations from the release mapper.
					     With --format=kotlin, city expectations use the compiled release Kotlin mapper.
					  3. Check the release mapper against its own release JSON.
					  4. Check the release mapper against the current JSON embedded in this JAR.
					  5. Java only: check the current mapper from this JAR against the release JSON.
					  6. With --output, keep the release source and all three JSON files,
					     including when checks fail, for review and updating the repository.

					Options:
					  --release=TAG   timezone-boundary-builder release (default: latest).
					  --format=TYPE   Mapper format: java or kotlin (default: java).
					  --basemap=FILE  World basemap OBF (default: found in OBF_LOCATION).
					  --output=DIR    Save release source and JSON (default: no files kept).
					  --help          Show this help.

					Runs checks 3-5 for Java, checks 3-4 for Kotlin. On mismatches, prints failed steps and update
					instructions, then exits with code 1 without a stack trace.
					Saved files: %s or %s, plus %s, %s, %s.
					For Java, replace the reviewed mapper source in OsmAndServerUtilities.
					For Kotlin, review the generated source before integrating it;
					copy reviewed JSON files to resources/test-resources/tz/.
					Rebuild and run TimezoneMapperTest after updating the repository.
					Temporary source and compiled JAR are always removed.
					""".formatted(OutputFormat.JAVA.sourceFileName(), OutputFormat.KOTLIN.sourceFileName(),
							ALIASES_FILE, IANA_FILE, CITIES_FILE));
			return;
		}
		String basemapOption = opts.getOpt("--basemap");
		Path basemap = basemapOption == null ? TimezoneMapperTestDataGenerator.findWorldBasemap()
				: Path.of(basemapOption);
		String outputOption = opts.getOpt("--output");
		Path output = outputOption == null ? null : Path.of(outputOption);
		OutputFormat format = OutputFormat.valueOf(opts.getOrDefault("--format", "java").toUpperCase(Locale.ROOT));
		TimezoneBoundaryRelease release = TimezoneMapperGenerator.fetchBoundaryRelease(
				opts.getOrDefault("--release", "latest"));
		if (!validateTimezoneMapper(release, basemap, output, format)) {
			System.exit(1);
		}
	}

	public static boolean validateTimezoneMapper(TimezoneBoundaryRelease release, Path basemap, Path output,
	                                             OutputFormat format)
			throws Exception {
		Path directory = Files.createTempDirectory("timezone-mapper-validation-");
		Path source = directory.resolve(format.sourceFileName());
		boolean valid;
		try {
			TimezoneMapperGenerator.generateMapper(release, source, format);
			try (CompiledTimezoneMapper mapper = compileMapper(directory, format)) {
				TimezoneTestData releaseData = TimezoneMapperTestDataGenerator.generateTestData(
						release.version(), basemap, mapper);
				TimezoneTestData currentData = TimezoneMapperTestDataGenerator.readTestDataResources();
				System.out.println("Validating " + format + " TimezoneMapper " + release.version());
				valid = validateTimezoneMappings(source, output, currentData, releaseData, mapper, format);
			}
		} finally {
			Files.deleteIfExists(directory.resolve(CLASSES_JAR));
			Files.deleteIfExists(source);
			Files.delete(directory);
		}
		if (valid) {
			System.out.println("SUCCESS! TimezoneMapper " + release.version() + ": all validation checks passed!");
		}
		return valid;
	}

	static CompiledTimezoneMapper compileMapper(Path directory, OutputFormat format) throws Exception {
		Path source = directory.resolve(format.sourceFileName());
		Path classes = directory.resolve(CLASSES_JAR);
		switch (format) {
			case JAVA -> compileJavaMapper(source, classes);
			case KOTLIN -> compileKotlinMapper(source, classes);
		}
		// The platform parent cannot load the built mapper with the same class name.
		URLClassLoader loader = new URLClassLoader(new URL[] { classes.toUri().toURL() },
				ClassLoader.getPlatformClassLoader());
		try {
			Method lookup = loader.loadClass(TimezoneMapper.class.getName())
					.getMethod("latLngToTimezoneString", double.class, double.class);
			return new CompiledTimezoneMapper(loader, lookup);
		} catch (ReflectiveOperationException e) {
			loader.close();
			throw e;
		}
	}

	private static void compileJavaMapper(Path source, Path classes) throws IOException {
		JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
		if (compiler == null) {
			throw new IllegalStateException("Dynamic timezone tests require a JDK with javac");
		}
		try (FileSystem jar = FileSystems.newFileSystem(classes, Map.of("create", "true"));
				StandardJavaFileManager files = compiler.getStandardFileManager(null, null, StandardCharsets.UTF_8)) {
			files.setLocationFromPaths(StandardLocation.CLASS_OUTPUT, List.of(jar.getPath("/")));
			boolean compiled = compiler.getTask(null, files, null,
					List.of("--release", "17", "-encoding", "UTF-8", "-proc:none"),
					null, files.getJavaFileObjects(source)).call();
			if (!compiled) {
				throw new IOException("Generated TimezoneMapper compilation failed");
			}
		}
	}

	private static void compileKotlinMapper(Path source, Path classes) throws IOException, InterruptedException {
		Process compiler;
		try {
			compiler = new ProcessBuilder("kotlinc", "-J-Xmx2g", source.toString(), "-jvm-target", "17",
					"-include-runtime", "-d", classes.toString()).inheritIO().start();
		} catch (IOException e) {
			throw new IOException("Cannot start kotlinc. Install the Kotlin compiler and add it to PATH.", e);
		}
		try {
			int exitCode = compiler.waitFor();
			if (exitCode != 0) {
				throw new IOException("Generated Kotlin TimezoneMapper compilation failed: exit code " + exitCode);
			}
		} finally {
			compiler.destroy();
		}
	}

	static boolean validateTimezoneMappings(Path source, Path output, TimezoneTestData currentData,
	                                        TimezoneTestData releaseData,
	                                        BiFunction<Double, Double, String> releaseLookup, OutputFormat format)
			throws IOException {
		if (output != null) {
			releaseData.writeJsonFiles(output);
			Files.copy(source, output.resolve(format.sourceFileName()), StandardCopyOption.REPLACE_EXISTING);
		}
		List<String> failures = new ArrayList<>();
		collectTimezoneFailures("Step 3: Release mapper on release JSON", releaseData, releaseLookup, failures);
		collectTimezoneFailures("Step 4: Release mapper on current JSON", currentData, releaseLookup, failures);
		if (format == OutputFormat.JAVA) {
			collectTimezoneFailures("Step 5: Current mapper on release JSON", releaseData,
					TimezoneMapper::latLngToTimezoneString, failures);
		}
		if (!failures.isEmpty()) {
			System.err.println("VALIDATION FAILED at:\n" + String.join("\n", failures));
			if (output == null) {
				System.err.println("Run validate-timezone-mapper again with --output=DIR to save release files.");
			} else {
				System.err.println("Release files have been saved in --output.");
			}
			System.err.println("Review the failures and update the repository with verified release files:");
			if (format == OutputFormat.JAVA) {
				System.err.println("Replace " + format.sourceFileName()
						+ " in OsmAndServerUtilities/src/main/java/net/osmand/server/utilities/;");
			} else {
				System.err.println("Review " + format.sourceFileName() + " before integrating the Kotlin mapper;");
			}
			System.err.println("replace " + ALIASES_FILE + ", " + IANA_FILE + ", " + CITIES_FILE
					+ " in resources/test-resources/tz/.");
			System.err.println("Rebuild and run TimezoneMapperTest.");
		}
		return failures.isEmpty();
	}

	private static void collectTimezoneFailures(String direction, TimezoneTestData data,
	                                            BiFunction<Double, Double, String> timezoneLookup,
	                                            List<String> failures) {
		System.out.println(direction);
		boolean failed = false;
		for (TimezoneCheckResult result : List.of(
				checkTimezoneMappings(IANA_FILE, data.iana(), data.aliases(), timezoneLookup),
				checkTimezoneMappings(CITIES_FILE, data.cities(), data.aliases(), timezoneLookup))) {
			System.out.println(result.formatSummary());
			if (!result.mismatches().isEmpty()) {
				System.out.println(String.join("\n", result.mismatches()));
				failed = true;
			}
		}
		if (failed) {
			failures.add(direction);
		}
	}

	record CompiledTimezoneMapper(URLClassLoader loader, Method lookup)
			implements AutoCloseable, BiFunction<Double, Double, String> {
		@Override
		public String apply(Double lat, Double lon) {
			try {
				return (String) lookup.invoke(null, lat, lon);
			} catch (ReflectiveOperationException e) {
				throw new IllegalStateException("Generated TimezoneMapper lookup failed", e);
			}
		}

		@Override
		public void close() throws IOException {
			loader.close();
		}
	}
}
