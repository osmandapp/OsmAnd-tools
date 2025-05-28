package net.osmand.server;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.Map;


@Component
public class PurchasesDataLoader {

	private Map<String, Subscription> subscriptions;
	private Map<String, InApp> inapps;

	@Value("${osmand.web.location}")
	private String websiteLocation;

	@PostConstruct
	public void init() throws IOException {
		initSubscriptions();
		initInApps();
	}

	public void reload() throws IOException {
		init();
	}

	private void initSubscriptions() throws IOException {
		File file = new File(websiteLocation, "sku-subscriptions.json");
		ObjectMapper mapper = new ObjectMapper();
		subscriptions = mapper.readValue(
				file, new TypeReference<>() {}
		);
	}

	private void initInApps() throws IOException {
		File file = new File(websiteLocation, "sku-inapps.json");
		ObjectMapper mapper = new ObjectMapper();
		inapps = mapper.readValue(
				file, new TypeReference<>() {}
		);
	}

	public Map<String, Subscription> getSubscriptions() {
		return subscriptions;
	}

	public Map<String, InApp> getInApps() {
		return inapps;
	}

	public record Subscription(
			String name,
			String platform,
			@JsonProperty("cross-platform") String crossPlatform,
			String type
	) {}

	public record InApp(
			String name,
			String platform,
			@JsonProperty("cross-platform") String crossPlatform
	) {}
}
