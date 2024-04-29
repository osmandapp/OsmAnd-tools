package net.osmand.server;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;

@Configuration
public class DatasourceConfiguration extends WebSecurityConfigurerAdapter {
	
	protected static final Log LOG = LogFactory.getLog(DatasourceConfiguration.class);
	
    
	@Bean
	@Primary
	@ConfigurationProperties(prefix="spring.datasource")
	public DataSource primaryDataSource() {
	    return DataSourceBuilder.create().build();
	}

	@Bean
	@ConfigurationProperties(prefix="spring.wikidatasource")
	public DataSource wikiDataSource() {
	    return DataSourceBuilder.create().build();
	}
    
	@Bean
	public JdbcTemplate wikiJdbcTemplate(@Qualifier("wikiDataSource") DataSource dataSource) {
	    return new JdbcTemplate(dataSource);
	}
	
}