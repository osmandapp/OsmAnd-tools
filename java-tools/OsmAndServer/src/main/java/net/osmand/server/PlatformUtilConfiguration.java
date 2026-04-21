package net.osmand.server;

import net.osmand.obf.ToolsOsmAndContextImpl;
import net.osmand.shared.api.OsmAndContext;
import net.osmand.shared.util.PlatformUtil;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PlatformUtilConfiguration {

	@Bean
	public OsmAndContext osmAndContext() {
		ToolsOsmAndContextImpl ctx = new ToolsOsmAndContextImpl();
		PlatformUtil.INSTANCE.initialize(ctx);
		return ctx;
	}
}
