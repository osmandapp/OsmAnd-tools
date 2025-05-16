package net.osmand.server;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Value;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static net.osmand.server.WebSecurityConfiguration.ROLE_ADMIN;
import static net.osmand.server.WebSecurityConfiguration.ROLE_SUPPORT;

@Component
public class WebAccessConfig {
	private Map<String, List<String>> roles;

	@Value("${osmand.web.location}")
	private String websiteLocation;

	@PostConstruct
	public void init() throws IOException {
		File f = new File(websiteLocation, "web-access.json");
		ObjectMapper om = new ObjectMapper();
		JsonNode root = om.readTree(f);
		JsonNode r = root.get("roles");
		roles = Map.of(
				ROLE_ADMIN, om.convertValue(r.get("admin"), new TypeReference<>() {
				}),
				ROLE_SUPPORT, om.convertValue(r.get("support"), new TypeReference<>() {
				})
		);
	}

	public void reload() throws IOException {
		init();
	}

	public List<String> getAdmins() {
		return roles.getOrDefault(ROLE_ADMIN, List.of());
	}

	public List<String> getSupport() {
		return roles.getOrDefault(ROLE_SUPPORT, List.of());
	}
}
