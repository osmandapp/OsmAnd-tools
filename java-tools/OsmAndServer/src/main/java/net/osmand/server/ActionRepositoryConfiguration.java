package net.osmand.server;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

@Configuration
public class ActionRepositoryConfiguration {
    protected static final Log LOG = LogFactory.getLog(ActionRepositoryConfiguration.class);

    private boolean actionDataSourceInitialized = false;

    @Bean
    @ConfigurationProperties(prefix = "spring.actiondatasource")
    public DataSourceProperties actionDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean(name = "actionDataSource")
    public DataSource actionDataSource() {
        try {
            DataSource ds = actionDataSourceProperties().initializeDataSourceBuilder().build();
            actionDataSourceInitialized = true;
            return ds;
        } catch (Exception e) {
            LOG.warn("WARN - Action database is not configured: " + e.getMessage());
        }
        return DatasourceConfiguration.emptyDataSource();
    }

    @Bean(name = "actionJdbcTemplate")
    public JdbcTemplate actionJdbcTemplate(@Qualifier("actionDataSource") DataSource ds) {
        return new JdbcTemplate(ds);
    }

    public boolean isActionDataSourceInitialized() {
        return actionDataSourceInitialized;
    }
}
