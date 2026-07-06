package net.osmand.server.api.operation;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import net.osmand.server.DatasourceConfiguration;

@Configuration
public class OperationRepositoryConfiguration {
	protected static final Log LOG = LogFactory.getLog(OperationRepositoryConfiguration.class);

	private boolean operationDataSourceInitialized = false;

	@Bean
	@ConfigurationProperties(prefix = "spring.operationdatasource")
	public DataSourceProperties operationDataSourceProperties() {
		return new DataSourceProperties();
	}

	@Bean(name = "operationDataSource")
	public DataSource operationDataSource() {
		try {
			DataSource ds = operationDataSourceProperties().initializeDataSourceBuilder().build();
			operationDataSourceInitialized = true;
			return ds;
		} catch (Exception e) {
			LOG.warn("WARN - Operation database is not configured: " + e.getMessage());
		}
		return DatasourceConfiguration.emptyDataSource();
	}

	@Bean(name = "operationJdbcTemplate")
	public JdbcTemplate operationJdbcTemplate(@Qualifier("operationDataSource") DataSource ds) {
		return new JdbcTemplate(ds);
	}

	@Bean
	public ApplicationRunner operationSchemaInitializer(@Qualifier("operationJdbcTemplate") JdbcTemplate jdbc) {
		return args -> {
			if (!operationDataSourceInitialized) {
				return;
			}
			jdbc.execute("CREATE TABLE IF NOT EXISTS operation (" +
					"class_name TEXT PRIMARY KEY, name TEXT NOT NULL, params_json TEXT NOT NULL, " +
					"result_type TEXT NOT NULL, valid INTEGER NOT NULL DEFAULT 1, " +
					"updated_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)");
			jdbc.execute("CREATE TABLE IF NOT EXISTS job (" +
					"id INTEGER PRIMARY KEY AUTOINCREMENT, class_name TEXT NOT NULL, name TEXT, description TEXT, " +
					"labels TEXT, params_json TEXT NOT NULL, created_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, " +
					"updated_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, " +
					"FOREIGN KEY (class_name) REFERENCES operation(class_name))");
			jdbc.execute("CREATE TABLE IF NOT EXISTS run (" +
					"id INTEGER PRIMARY KEY AUTOINCREMENT, job_id INTEGER NOT NULL, status TEXT NOT NULL, " +
					"params_json TEXT NOT NULL, result_json TEXT, error_text TEXT, elapsed_ms INTEGER NOT NULL DEFAULT 0, " +
					"started_time TIMESTAMP, finished_time TIMESTAMP, created_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, " +
					"updated_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, " +
					"FOREIGN KEY (job_id) REFERENCES job(id) ON DELETE CASCADE)");
			jdbc.execute("CREATE TABLE IF NOT EXISTS inactive_user_notice (" +
					"userid INTEGER PRIMARY KEY, email TEXT, category TEXT NOT NULL, status TEXT NOT NULL, " +
					"notified_time TIMESTAMP NOT NULL, deleted_time TIMESTAMP, " +
					"updated_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)");
			jdbc.execute("CREATE INDEX IF NOT EXISTS inactive_user_notice_status_idx ON inactive_user_notice(status)");
			jdbc.execute("CREATE INDEX IF NOT EXISTS job_class_name_idx ON job(class_name)");
			jdbc.execute("CREATE INDEX IF NOT EXISTS job_updated_time_idx ON job(updated_time)");
			jdbc.execute("CREATE INDEX IF NOT EXISTS run_job_id_idx ON run(job_id)");
			jdbc.execute("CREATE INDEX IF NOT EXISTS run_status_idx ON run(status)");
			jdbc.execute("CREATE INDEX IF NOT EXISTS run_created_time_idx ON run(created_time)");
		};
	}

	public boolean isOperationDataSourceInitialized() {
		return operationDataSourceInitialized;
	}
}
