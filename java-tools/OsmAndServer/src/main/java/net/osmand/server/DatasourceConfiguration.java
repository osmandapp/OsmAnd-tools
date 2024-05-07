package net.osmand.server;

import java.sql.Connection;
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
import org.springframework.jdbc.datasource.AbstractDataSource;

@Configuration
public class DatasourceConfiguration {
	
	protected static final Log LOG = LogFactory.getLog(DatasourceConfiguration.class);
	private boolean wikiInitialzed;
	private boolean monitorInitialzed;
	
	
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
	@ConfigurationProperties(prefix="spring.monitordatasource")
    public DataSourceProperties monitorDataSourceProperties() {
        return new DataSourceProperties();
    }
    
	@Bean
	@Primary
	public DataSource primaryDataSource() {
		return primaryDataSourceProperties().initializeDataSourceBuilder().build();
	}

	public boolean wikiInitialized() {
		return wikiInitialzed;
	}
	
	public boolean monitorInitialized() {
		return monitorInitialzed;
	}
	
	@Bean
	public DataSource wikiDataSource() {
		try {
			DataSource ds = wikiDataSourceProperties().initializeDataSourceBuilder().build();
			wikiInitialzed = true;
			return ds;
		} catch (Exception e) {
			LOG.warn("Warning - Wiki database not configured: " + e.getMessage());
		}
		return new AbstractDataSource() {
			
			@Override
			public Connection getConnection(String username, String password) throws SQLException {
				return null;
			}
			
			@Override
			public Connection getConnection() throws SQLException {
				return null;
			}
		};
	}
	
	@Bean
	public DataSource monitorDataSource() {
		try {
			DataSource ds = monitorDataSourceProperties().initializeDataSourceBuilder().build();
			monitorInitialzed = true;
			return ds;
		} catch (Exception e) {
			LOG.warn("INFO - Monitor database not configured: " + e.getMessage());
		}
		return new AbstractDataSource() {
			
			@Override
			public Connection getConnection(String username, String password) throws SQLException {
				return null;
			}
			
			@Override
			public Connection getConnection() throws SQLException {
				return null;
			}
		};
	}
    
	@Bean
	public JdbcTemplate wikiJdbcTemplate(@Qualifier("wikiDataSource") DataSource dataSource) {
		if (dataSource == null) {
			return null;
		}
		return new JdbcTemplate(dataSource);
	}
	
	@Bean
	@Primary
	public JdbcTemplate jdbcTemplate(@Qualifier("primaryDataSource") DataSource dataSource) {
		if (dataSource == null) {
			return null;
		}
		return new JdbcTemplate(dataSource);
	}
	
	@Bean
	public JdbcTemplate monitorJdbcTemplate(@Qualifier("monitorDataSource") DataSource dataSource) {
		if (dataSource == null) {
			return null;
		}
		return new JdbcTemplate(dataSource);
	}
	
}