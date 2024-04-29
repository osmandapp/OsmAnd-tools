package net.osmand.server;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

import com.clickhouse.jdbc.ClickHouseDriver;

@Configuration
public class DatasourceConfiguration {
	
	protected static final Log LOG = LogFactory.getLog(DatasourceConfiguration.class);
	
	
    @Bean
	@ConfigurationProperties(prefix="spring.datasource")
    public DataSourceProperties primaryDataSourceProperties() {
        return new DataSourceProperties();
    }
    
    
    @Bean
	@ConfigurationProperties(prefix="spring.wikidatasource")
    public DataSourceProperties wikiDataSourceProperties() {
        return new DataSourceProperties();
    }
    
	@Bean
	@Primary
	public DataSource primaryDataSource() {
		return primaryDataSourceProperties().initializeDataSourceBuilder().build();
	}

	@Bean
	public DataSource wikiDataSource() {
		System.out.println("Load Clickhouse class: " + ClickHouseDriver.class);
		try {
			DataSource ds = wikiDataSourceProperties().initializeDataSourceBuilder().build();
			return ds;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
    
	@Bean
	public JdbcTemplate wikiJdbcTemplate(@Qualifier("wikiDataSource") DataSource dataSource) {
	    return new JdbcTemplate(dataSource);
	}
	
}