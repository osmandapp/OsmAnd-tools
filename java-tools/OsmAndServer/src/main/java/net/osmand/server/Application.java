package net.osmand.server;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.imageio.spi.IIORegistry;
import javax.imageio.spi.ImageReaderSpi;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.http.converter.json.KotlinSerializationJsonHttpMessageConverter;
import org.springframework.scheduling.annotation.EnableScheduling;

import kotlinx.serialization.json.Json;
import kotlinx.serialization.json.Json.Default;
import net.osmand.router.HHRouteDataStructure.HHRoutingConfig;
import net.osmand.router.HHRoutePlanner;
import net.osmand.router.RouteResultPreparation;
import net.osmand.server.api.services.StorageService;
import net.osmand.server.monitor.OsmAndGithubProjectMonitorTasks;
import net.osmand.util.Algorithms;

import static org.apache.commons.io.FileUtils.copyDirectory;

@SpringBootApplication
@EnableScheduling
@EnableJpaAuditing
@ServletComponentScan
public class Application  {

	@Autowired
	TelegramBotManager telegram;
	
	@Autowired 
	StorageService storageService;
	
	@Autowired
	OsmAndGithubProjectMonitorTasks githubProject;

	private static Path colorPalettePath;
	
	public static void main(String[] args) {
		System.out.println("Test parsing with kotlin: " + Json.Default.parseToJsonElement("{}"));
		System.setProperty("spring.devtools.restart.enabled", "false");
		SpringApplication.run(Application.class, args);
	}

	@Bean
	public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
		return args -> {
			telegram.init();
			if (Algorithms.isEmpty(System.getenv("ROUTING_VERBOSE"))) {
				RouteResultPreparation.PRINT_TO_CONSOLE_ROUTE_INFORMATION = false;
				HHRoutePlanner.DEBUG_VERBOSE_LEVEL = 0;
				HHRoutingConfig.STATS_VERBOSE_LEVEL = 0;
			} else {
				HHRoutePlanner.DEBUG_VERBOSE_LEVEL = 1;
				RouteResultPreparation.PRINT_TO_CONSOLE_ROUTE_INFORMATION = true;
				HHRoutingConfig.STATS_VERBOSE_LEVEL = 1;
			}
			System.out.println("Application has started");
			configureImageIO();
			createColorPaletteDirectory("/colorPalette.zip");
			
//			githubProject.syncGithubProject(); // to test
		};
	}
	
	public void configureImageIO() {
		IIORegistry registry = IIORegistry.getDefaultInstance();
		
		ImageReaderSpi twelvemonkeysSpi = null;
		ImageReaderSpi geoSolutionsSpi = null;
		
		try {
			Class<?> twelvemonkeysTiffReaderClass = Class.forName("com.twelvemonkeys.imageio.plugins.tiff.TIFFImageReaderSpi");
			Class<?> geoSolutionsTiffReaderClass = Class.forName("it.geosolutions.imageioimpl.plugins.tiff.TIFFImageReaderSpi");
			
			for (ImageReaderSpi spi : ServiceLoader.load(ImageReaderSpi.class)) {
				if (twelvemonkeysTiffReaderClass.isInstance(spi)) {
					twelvemonkeysSpi = spi;
				} else if (geoSolutionsTiffReaderClass.isInstance(spi)) {
					geoSolutionsSpi = spi;
				}
			}
			
			if (twelvemonkeysSpi != null && geoSolutionsSpi != null) {
				registry.setOrdering(ImageReaderSpi.class, geoSolutionsSpi, twelvemonkeysSpi);
				System.out.println("The ImageIO service provider order has been successfully set.");
			} else {
				System.err.println("Failed to find the required service providers to set the order.");
			}
			
		} catch (ClassNotFoundException e) {
			System.err.println("One of the service provider classes was not found: " + e.getMessage());
		}
	}

	public static void createColorPaletteDirectory(String resourceFolderPath) throws IOException {
		Path tempDirectory = Files.createTempDirectory("colorPalette");
		File tempDir = tempDirectory.toFile();
		tempDir.deleteOnExit(); // remove the directory when the JVM exits

		try (InputStream zipStream = Application.class.getResourceAsStream(resourceFolderPath)) {
			if (zipStream == null) {
				System.err.println("Resource not found: " + resourceFolderPath);
				return;
			}

			try (ZipInputStream zis = new ZipInputStream(zipStream)) {
				ZipEntry entry;
				while ((entry = zis.getNextEntry()) != null) {
					File newFile = new File(tempDir, entry.getName());
					if (entry.isDirectory()) {
						newFile.mkdirs();
					} else {
						try (FileOutputStream fos = new FileOutputStream(newFile)) {
							byte[] buffer = new byte[1024];
							int len;
							while ((len = zis.read(buffer)) > 0) {
								fos.write(buffer, 0, len);
							}
						}
					}
					zis.closeEntry();
				}
			}
		}
		colorPalettePath = tempDirectory;
	}

	public static Path getColorPaletteDirectory() {
		return colorPalettePath;
	}
}
