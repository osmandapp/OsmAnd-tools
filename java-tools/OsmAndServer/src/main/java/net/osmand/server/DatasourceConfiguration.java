package net.osmand.server;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import javax.sql.DataSource;

import jakarta.persistence.EntityManagerFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateProperties;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateSettings;
import org.springframework.boot.autoconfigure.orm.jpa.JpaProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.orm.jpa.EntityManagerFactoryBuilder;
import org.springframework.context.annotation.*;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.AbstractDataSource;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableJpaRepositories(
		basePackages = Application.PACKAGE_NAME,
		excludeFilters = @ComponentScan.Filter(
				type = FilterType.ANNOTATION, classes = SearchTestRepository.class))
public class DatasourceConfiguration {
	
	protected static final Log LOG = LogFactory.getLog(DatasourceConfiguration.class);
	private boolean wikiInitialzed;
	private boolean monitorInitialzed;
	private boolean changesetInitialzed;
	private boolean osmgpxInitialzed;
	
	
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
	@ConfigurationProperties(prefix="spring.osmgpxdatasource")
	public DataSourceProperties osmgpxDataSourceProperties() {
		return new DataSourceProperties();
	}
    
    @Bean
	@ConfigurationProperties(prefix="spring.changesetdatasource")
    public DataSourceProperties changesetSourceProperties() {
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

	public boolean osmgpxInitialized() {
		return osmgpxInitialzed;
	}

	public boolean wikiInitialized() {
		return wikiInitialzed;
	}
	
	public boolean monitorInitialized() {
		return monitorInitialzed;
	}
	
	public boolean changesetInitialized() {
		return changesetInitialzed;
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
		return emptyDataSource();
	}

	@Bean
	public DataSource osmgpxDataSource() {
		try {
			DataSource ds = osmgpxDataSourceProperties().initializeDataSourceBuilder().build();
			osmgpxInitialzed = true;
			return ds;
		} catch (Exception e) {
			LOG.warn("Warning - Osmgpx database not configured: " + e.getMessage());
		}
		return emptyDataSource();
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
		return emptyDataSource();
	}
	
	
	@Bean
	public DataSource changesetDataSource() {
		try {
			DataSource ds = changesetSourceProperties().initializeDataSourceBuilder().build();
			changesetInitialzed = true;
			return ds;
		} catch (Exception e) {
			LOG.warn("WARN - Changeset database not configured: " + e.getMessage());
		}
		return emptyDataSource();
	}


	public static DataSource emptyDataSource() {
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
	@Primary
	public JdbcTemplate jdbcTemplate(@Qualifier("primaryDataSource") DataSource dataSource) {
		if (dataSource == null) {
			return null;
		}
		return new JdbcTemplate(dataSource);
	}
	
	@Bean
	public JdbcTemplate wikiJdbcTemplate(@Qualifier("wikiDataSource") DataSource dataSource) {
		if (dataSource == null) {
			return null;
		}
		return new JdbcTemplate(dataSource);
	}

	@Bean
	public JdbcTemplate osmgpxJdbcTemplate(@Qualifier("osmgpxDataSource") DataSource dataSource) {
		if (dataSource == null) {
			return null;
		}
		return new JdbcTemplate(dataSource);
	}

	@Bean
	public JdbcTemplate changesetJdbcTemplate(@Qualifier("changesetDataSource") DataSource dataSource) {
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

	@Bean(name = "entityManagerFactory")
	@Primary
	public LocalContainerEntityManagerFactoryBean entityManagerFactory(
			EntityManagerFactoryBuilder builder,
			@Qualifier("primaryDataSource") DataSource dataSource,
            JpaProperties jpaProperties,
            HibernateProperties hibernateProperties) {
        Map<String, Object> vendorProps = new java.util.HashMap<>(
                hibernateProperties.determineHibernateProperties(
                        jpaProperties.getProperties(), new HibernateSettings()
                )
        );

		return builder
				.dataSource(dataSource)
				.packages(Application.PACKAGE_NAME + ".api.repo",
						Application.PACKAGE_NAME + ".assist.data",
						Application.PACKAGE_NAME + ".monitor")
				.persistenceUnit("default")
                .properties(vendorProps)
				.build();
	}

	@Bean(name = "transactionManager")
	@Primary
	public PlatformTransactionManager transactionManager(
			@Qualifier("entityManagerFactory") EntityManagerFactory entityManagerFactory) {
		return new JpaTransactionManager(entityManagerFactory);
	}
}