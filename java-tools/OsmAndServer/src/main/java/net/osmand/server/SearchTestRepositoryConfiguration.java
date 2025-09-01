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
import org.springframework.jdbc.datasource.AbstractDataSource;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
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

    private static final String SQLITE_SHARED_MEM_URL = "jdbc:sqlite:file:searchtest?mode=memory&cache=shared";
    private Connection memoryKeeperConnection = null;

    public boolean isPersisted() {
        return memoryKeeperConnection == null;
    }

    @Bean
    @ConfigurationProperties(prefix = "spring.searchtestdatasource")
    public DataSourceProperties searchTestDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean(name = "searchTestDataSource")
    public DataSource searchTestDataSource() {
        try {
            return searchTestDataSourceProperties().initializeDataSourceBuilder().build();
        } catch (Exception e) {
            LOG.warn("WARN - Search-test database not configured to be persisted: " + e.getMessage());
        }
        return memorySqliteDataSource();
    }

    /**
     * Attempts to create a minimal in-memory SQLite DataSource via DriverManager.
     * Returns null if the SQLite JDBC driver is not present.
     */
    private synchronized DataSource memorySqliteDataSource() {
        if (memoryKeeperConnection == null) {
            try {
                // Ensure driver is available; if not, bail out to keep startup safe
                Class.forName("org.sqlite.JDBC");
                // Open and keep a persistent connection to a shared in-memory DB
                memoryKeeperConnection = DriverManager.getConnection(SQLITE_SHARED_MEM_URL);
            } catch (Exception ex) {
                LOG.info("SQLite JDBC driver not found on classpath; skipping dummy in-memory SQLite fallback.");
                return null;
            }
        }

        return new AbstractDataSource() {
            @Override
            public Connection getConnection() throws SQLException {
                // Return a new connection to the same shared in-memory DB
                return DriverManager.getConnection(SQLITE_SHARED_MEM_URL);
            }

            @Override
            public Connection getConnection(String username, String password) throws SQLException {
                return getConnection();
            }
        };
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
        // Force SQLite dialect for this EMF
        vendorProps.put("hibernate.dialect", "net.osmand.server.StrictSQLiteDialect");
        if (!isPersisted()) {
            // In-memory SQLite: let Hibernate create and drop schema automatically
            vendorProps.put("hibernate.hbm2ddl.auto", "create-drop");
            // Allow JDBC metadata access so Hibernate can determine capabilities for DDL
            vendorProps.put("hibernate.boot.allow_jdbc_metadata_access", "true");
            // Some drivers misbehave with auto-commit toggles; hint Hibernate that provider may disable it
            vendorProps.putIfAbsent("hibernate.connection.provider_disables_autocommit", "true");
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
}
