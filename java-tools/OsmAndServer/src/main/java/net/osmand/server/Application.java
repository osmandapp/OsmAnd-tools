package net.osmand.server;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.scheduling.annotation.EnableScheduling;

import net.osmand.router.HHRouteDataStructure.HHRoutingConfig;
import net.osmand.router.HHRouteDataStructure.HHRoutingContext;
import net.osmand.router.HHRoutePlanner;
import net.osmand.router.RouteResultPreparation;
import net.osmand.server.api.services.StorageService;

import javax.imageio.spi.IIORegistry;
import javax.imageio.spi.ImageReaderSpi;
import java.util.ServiceLoader;

@SpringBootApplication
@EnableScheduling
@EnableJpaAuditing
@ServletComponentScan
public class Application  {

	@Autowired
	TelegramBotManager telegram;
	
	@Autowired 
	StorageService storageService;
	
	public static void main(String[] args) {
		System.setProperty("spring.devtools.restart.enabled", "false");
		SpringApplication.run(Application.class, args);
	}

	@Bean
	public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
		return args -> {
			telegram.init();
			RouteResultPreparation.PRINT_TO_CONSOLE_ROUTE_INFORMATION = false;
//			HHRoutePlanner.DEBUG_VERBOSE_LEVEL = 1 ;
//			HHRoutingConfig .STATS_VERBOSE_LEVEL = 1 ;
			System.out.println("Application has started");
			configureImageIO();
		};
	}
	
	public void configureImageIO() {
		IIORegistry registry = IIORegistry.getDefaultInstance();
		
		ImageReaderSpi geoSolutionsSpi = null;
		ImageReaderSpi sunMediaSpi = null;
		
		try {
			Class<?> geoSolutionsTiffReaderClass = Class.forName("it.geosolutions.imageioimpl.plugins.tiff.TIFFImageReaderSpi");
			Class<?> sunMediaTiffReaderClass = Class.forName("com.sun.media.imageioimpl.plugins.tiff.TIFFImageReaderSpi");
			
			for (ImageReaderSpi spi : ServiceLoader.load(ImageReaderSpi.class)) {
				if (geoSolutionsTiffReaderClass.isInstance(spi)) {
					geoSolutionsSpi = spi;
				} else if (sunMediaTiffReaderClass.isInstance(spi)) {
					sunMediaSpi = spi;
				}
			}
			
			if (geoSolutionsSpi != null && sunMediaSpi != null) {
				registry.setOrdering(ImageReaderSpi.class, geoSolutionsSpi, sunMediaSpi);
				System.out.println("The ImageIO service provider order has been successfully set.");
			} else {
				System.err.println("Failed to find the required service providers to set the order.");
			}
			
		} catch (ClassNotFoundException e) {
			System.err.println("One of the service provider classes was not found: " + e.getMessage());
		}
	}
}
