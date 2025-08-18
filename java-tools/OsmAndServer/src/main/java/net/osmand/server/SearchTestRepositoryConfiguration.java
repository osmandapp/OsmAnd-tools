package net.osmand.server;

import net.osmand.server.api.searchtest.entity.Dataset;
import net.osmand.server.api.searchtest.repo.TestCaseRepository;
import net.osmand.server.api.searchtest.repo.DatasetRepository;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.orm.jpa.EntityManagerFactoryBuilder;
import org.springframework.context.annotation.*;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;
import java.util.Map;

import jakarta.persistence.EntityManagerFactory;

/**
 * Configures all standard JPA repositories to use the SQLite datasource.
 */
@Configuration
@EnableTransactionManagement
@EnableJpaRepositories(
        basePackageClasses = {DatasetRepository.class, TestCaseRepository.class},
        includeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes =
                {DatasetRepository.class, TestCaseRepository.class}),
        entityManagerFactoryRef = "testEntityManagerFactory",
        transactionManagerRef = "testTransactionManager"
)
public class SearchTestRepositoryConfiguration {
    protected static final Log LOG = LogFactory.getLog(SearchTestRepositoryConfiguration.class);

    @Bean
    @ConfigurationProperties(prefix = "spring.searchtestdatasource")
    public DataSourceProperties testDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean(name = "testDataSource")
    public DataSource testDataSource() {
        return testDataSourceProperties().initializeDataSourceBuilder().build();
    }

    @Bean(name = "testJdbcTemplate")
    public JdbcTemplate testJdbcTemplate(@Qualifier("testDataSource") DataSource ds) {
        return new JdbcTemplate(ds);
    }

    @Bean(name = "testEntityManagerFactory")
    public LocalContainerEntityManagerFactoryBean testEntityManagerFactory(
            @Qualifier("testDataSource") DataSource dataSource,
            EntityManagerFactoryBuilder builder) {
        return builder
                .dataSource(dataSource)
                .packages(Dataset.class.getPackage().getName())
                .persistenceUnit("test")
                .properties(Map.of(
                        "hibernate.dialect", "net.osmand.server.StrictSQLiteDialect",
                        "hibernate.hbm2ddl.auto", "update",
                        "hibernate.physical_naming_strategy", "org.hibernate.boot.model.naming.CamelCaseToUnderscoresNamingStrategy"))
                .build();
    }

    @Bean(name = "testTransactionManager")
    public PlatformTransactionManager testTransactionManager(
            @Qualifier("testEntityManagerFactory") EntityManagerFactory entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory);
    }
}
