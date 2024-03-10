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

import net.osmand.router.RouteResultPreparation;
import net.osmand.server.api.services.StorageService;

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
			System.out.println("Application has started");
		};
	}
}
