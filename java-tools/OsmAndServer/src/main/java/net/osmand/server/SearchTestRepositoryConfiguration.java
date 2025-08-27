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

    @Bean
    @ConfigurationProperties(prefix = "spring.searchtestdatasource")
    public DataSourceProperties searchTestDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean(name = "searchTestDataSource")
    public DataSource searchTestDataSource() {
        return searchTestDataSourceProperties().initializeDataSourceBuilder().build();
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
