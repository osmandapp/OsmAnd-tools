package net.osmand.server;

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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;
import java.util.Map;

/**
 * Configures all standard JPA repositories to use the SQLite datasource.
 */
@Configuration
@EnableTransactionManagement
@EnableJpaRepositories(
        basePackages = Application.PACKAGE_NAME,
        includeFilters = @ComponentScan.Filter(
                type = FilterType.ANNOTATION, classes = SearchTestRepository.class),
        entityManagerFactoryRef = "searchTestEntityManagerFactory",
        transactionManagerRef = "searchTestTransactionManager"
)
public class SearchTestRepositoryConfiguration {
    protected static final Log LOG = LogFactory.getLog(SearchTestRepositoryConfiguration.class);

    private boolean searchTestDataSourceInitialized = false;

    @Bean
    @ConfigurationProperties(prefix = "spring.searchtestdatasource")
    public DataSourceProperties searchTestDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean(name = "searchTestDataSource")
    public DataSource searchTestDataSource() {
        try {
            DataSource ds = searchTestDataSourceProperties().initializeDataSourceBuilder().build();
            searchTestDataSourceInitialized = true;
            return ds;
        } catch (Exception e) {
            LOG.warn("WARN - Search-test database is not configured: " + e.getMessage());
        }
        return DatasourceConfiguration.emptyDataSource();
    }

    @Bean(name = "searchTestJdbcTemplate")
    public JdbcTemplate searchTestJdbcTemplate(@Qualifier("searchTestDataSource") DataSource ds) {
        return new JdbcTemplate(ds);
    }

    @Bean(name = "searchTestEntityManagerFactory")
    public LocalContainerEntityManagerFactoryBean testEntityManagerFactory(
            @Qualifier("searchTestDataSource") DataSource dataSource,
            EntityManagerFactoryBuilder builder,
            JpaProperties jpaProperties,
            HibernateProperties hibernateProperties) {
        // Derive vendor properties from Spring Boot configuration (includes ddl-auto, naming, etc.)
        Map<String, Object> vendorProps = new java.util.HashMap<>(
                hibernateProperties.determineHibernateProperties(
                        jpaProperties.getProperties(), new HibernateSettings()
                )
        );
        // Force SQLite dialect for this EMF and avoid metadata-heavy schema probing
        vendorProps.put("hibernate.dialect", "net.osmand.server.StrictSQLiteDialect");
        // Disable any schema generation/validation that triggers compound metadata SELECTs on SQLite
        vendorProps.put("hibernate.temp.use_jdbc_metadata_defaults", "false");
        vendorProps.put("jakarta.persistence.schema-generation.database.action", "none");
	    if (!isSearchTestDataSourceInitialized()) {
		    // Allow to start without searchtestdatasource
		    vendorProps.put("hibernate.hbm2ddl.auto", "none");
	    }

        return builder
                .dataSource(dataSource)
                .packages(Application.PACKAGE_NAME + ".api.searchtest")
                .properties(vendorProps)
                .persistenceUnit("searchTest")
                .build();
    }

    @Bean(name = "searchTestTransactionManager")
    public PlatformTransactionManager searchTestTransactionManager(
            @Qualifier("searchTestEntityManagerFactory") EntityManagerFactory entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory);
    }

    public boolean isSearchTestDataSourceInitialized() {
        return searchTestDataSourceInitialized;
    }
}
