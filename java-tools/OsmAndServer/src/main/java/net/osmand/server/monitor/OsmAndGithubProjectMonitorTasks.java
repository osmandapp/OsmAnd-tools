package net.osmand.server.monitor;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import net.osmand.util.Algorithms;

@Component
public class OsmAndGithubProjectMonitorTasks {

	// DB SCHEMA BELOW
	
	protected static final Log LOG = LogFactory.getLog(OsmAndGithubProjectMonitorTasks.class);
	private static final Log LOGGER = LogFactory.getLog(OsmAndGithubProjectMonitorTasks.class);


	private static final int MAX_PAGES = 100;
	private static final int BATCH_SIZE = 100; 
	private static final String DB_TABLE_NAME = "githubProject";

	static SimpleDateFormat githubDateTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");// 2022-06-01T06:17:04Z
	static SimpleDateFormat githubDate = new SimpleDateFormat("yyyy-MM-dd");// 2022-06-01
	static SimpleDateFormat dbDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");// 2021-12-03 14:04:46
	
	@Value("${monitoring.enabled}")
	private boolean enabled;
	
	@Value("${monitoring.project.db}")
	private String projectdb;
	
	@Value("${monitoring.project.token}")
	private String loginToken;

	@Value("${osmand.web.location}")
	private String websiteLocation;

	private static final String ISSUES_FOLDER = "servers/github_issues/issues";
	private static final String BACKLOG_FILE = "project_backlog.parquet";
	
	Gson gson = new GsonBuilder().serializeNulls().create();

	int TOTAL_SYNC;
	
	
	public static class ProjectItem {
		String timestamp;
		int version;
		boolean archived;
		String id = "";
		String repo = "";
		String num = "";
		String githubId = "";
		String title = "";
		String statusName = "";
		String statusId = "";
		String iterationName = "";
		String iterationId = "";
		String iterationStartDate = "1970-01-01 01:00:00";
		String milestoneId;
		String milestoneName;
		double value;
		double storyPoints;
		double complexity;
		String url = "";
		String publishedAt = "1970-01-01 01:00:00";
		String closedAt = "1970-01-01 01:00:00";
		boolean closed;
		String author = "";
		List<String> assignees = new ArrayList<>();
		List<String> labels = new ArrayList<>();
		List<String> labelIds = new ArrayList<>();

		public ProjectItem(String repo, String num) {
			setCurrentDate();
			this.id = repo + "/" + num;
			this.repo = repo;
			this.num = num;
		}

		private String setCurrentDate() {
			return this.timestamp = dbDate.format(new Date());
		}

		@Override
		public String toString() {
			ProjectItem proj = this;
			return String.format("%s %s it=%s vl=%.0f cmpl=%.0f sp=%.0f - %s", proj.id, proj.statusName,
					proj.iterationName, proj.value, proj.complexity, proj.storyPoints, proj.title);
		}
	}
	

	@Scheduled(fixedRateString = "PT30M")
	public void syncGithubProject() throws IOException, ParseException {
		if (Algorithms.isEmpty(projectdb)) {
			return;
		}
		long time = System.currentTimeMillis();
		LOG.info("SYNC Github project - Start...");
		if (Algorithms.isEmpty(loginToken)) {
			System.out.println("ERROR: Github token is not configured");
		}
		Map<String, ProjectItem> dbItems = loadItemsFromSQLQuery();
		Map<String, ProjectItem> items = loadGithubProjectItems();

		List<ProjectItem> toUpd = new ArrayList<>();
		int upd = 0, arch = 0;
		for (ProjectItem it : items.values()) {
			ProjectItem existing = dbItems.remove(it.id);
			if (existing != null) {
				// compare only content
				existing.timestamp = it.timestamp;
				it.version = existing.version;
				if (!Algorithms.stringsEqual(gson.toJson(existing), gson.toJson(it))) {
					System.out.println("Update - " + it);
					toUpd.add(it);
					upd++;
				}
			} else {
				System.out.println("Add - " + it);
				toUpd.add(it);
				upd++;
			}
		}
		insertIntoProject(toUpd);
		List<ProjectItem> toArch = new ArrayList<>();
		for (ProjectItem it : dbItems.values()) {
			System.out.println("Archive -  " + it);
			it.setCurrentDate();
			it.archived = true;
			arch++;
			toArch.add(it);
		}
		insertIntoProject(toArch);
		LOG.info(String.format("SYNC Github project (%d items, %d updated, %d archived) - DONE in %.1f s", items.size(),
				upd, arch, (System.currentTimeMillis() - time) / 1.0e3));
		
		try {
			updateProjectBacklogParquet();
		} catch (Exception e) {
			LOGGER.error("Failed to update project backlog parquet file.", e);
		}
	}

	public void updateProjectBacklogParquet() throws IOException {
		long time = System.currentTimeMillis();
		LOGGER.info("Updating project backlog parquet file - Start...");
		
		if (Algorithms.isEmpty(websiteLocation)) {
			LOGGER.error("osmand.web.location is not configured. Parquet file will not be updated.");
			return;
		}

		File folder = new File(websiteLocation, ISSUES_FOLDER);
		if (!folder.exists() && !folder.mkdirs()) {
			LOGGER.error("Failed to create directory: " + folder.getAbsolutePath());
			return;
		}

		// 1. Load all current items from ClickHouse DB. This is the only source of truth.
		Map<String, ProjectItem> dbItems = loadItemsFromSQLQuery();
		List<Group> records = new ArrayList<>();
		MessageType schema = getProjectBacklogSchema();
		SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

		// 2. Convert each item from the database into a Parquet record.
		for (ProjectItem dbItem : dbItems.values()) {
			SimpleGroup group = (SimpleGroup) groupFactory.newGroup();
			
			// Main identifiers
			group.add("id", dbItem.id);
			group.add("num", Long.parseLong(dbItem.num));
			group.add("repo", dbItem.repo);
			group.add("title", dbItem.title);
			group.add("url", dbItem.url);

			// Status and planning fields
			if (dbItem.statusName != null) {
				group.add("project_status", dbItem.statusName);
			}
			if (dbItem.iterationName != null) {
				group.add("iterationName", dbItem.iterationName);
			}
			if (dbItem.milestoneName != null) {
				group.add("milestone", dbItem.milestoneName);
			}

			// Numeric fields
			group.add("complexity", dbItem.complexity);
			group.add("storyPoints", dbItem.storyPoints);
			group.add("value", dbItem.value);

			// State and authoring
			group.add("state", dbItem.closed ? "closed" : "open");
			group.add("project_archived", dbItem.archived);
			if (dbItem.author != null) {
				group.add("user", dbItem.author);
			}
			
			// Timestamps and version
			group.add("timestamp", dbItem.timestamp);
			group.add("version", dbItem.version);
			if (dbItem.publishedAt != null) {
				group.add("created_at", dbItem.publishedAt);
			}
			if (dbItem.closedAt != null) {
				group.add("closed_at", dbItem.closedAt);
			}
			
			// Lists (assignees and labels)
			if (dbItem.assignees != null && !dbItem.assignees.isEmpty()) {
				Group assigneesGroup = group.addGroup("assignees");
				for (String assignee : dbItem.assignees) {
					assigneesGroup.addGroup("list").add("element", assignee);
				}
			}
			if (dbItem.labels != null && !dbItem.labels.isEmpty()) {
				Group labelsGroup = group.addGroup("labels");
				for (String label : dbItem.labels) {
					labelsGroup.addGroup("list").add("element", label);
				}
			}

			records.add(group);
		}

		// 3. Write the records to the new parquet file, overwriting the old one.
		if (!records.isEmpty()) {
			Path destinationParquetPath = Paths.get(folder.getAbsolutePath(), BACKLOG_FILE);
			writeToParquet(destinationParquetPath, records, schema);
		}
		
		LOGGER.info(String.format("Updating project backlog parquet file with %d records - DONE in %.1f s", 
				records.size(), (System.currentTimeMillis() - time) / 1.0e3));
	}
	
	private void writeToParquet(Path destinationPath, List<Group> records, MessageType schema) throws IOException {
		Configuration conf = new Configuration();
		// To overwrite the file if it exists
		if (Files.exists(destinationPath)) {
			Files.delete(destinationPath);
		}
		
		org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(destinationPath.toUri());

		try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(hadoopPath)
				.withType(schema)
				.withConf(conf)
				.build()) {
			for (Group record : records) {
				writer.write(record);
			}
		} catch (Exception e) {
			LOGGER.error("Error writing to parquet file: " + destinationPath, e);
		}
	}

	private MessageType getProjectBacklogSchema() {
		// Defines the schema for the output Parquet file.
		return Types.buildMessage()
			// Main identifiers
			.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("id")
			.optional(PrimitiveTypeName.INT64).named("num")
			.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("repo")
			.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("title")
			.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("url")

			// Status and planning fields
			.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("project_status")
			.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("iterationName")
			.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("milestone")

			// Numeric fields
			.optional(PrimitiveTypeName.DOUBLE).named("complexity")
			.optional(PrimitiveTypeName.DOUBLE).named("storyPoints")
			.optional(PrimitiveTypeName.DOUBLE).named("value")
			
			// State and authoring
			.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("state")
			.optional(PrimitiveTypeName.BOOLEAN).named("project_archived")
			.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("user")
			
			// Timestamps and version
			.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("timestamp")
			.optional(PrimitiveTypeName.INT32).named("version")
			.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("created_at")
			.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("closed_at")

			// Lists (assignees and labels) - Defined using the standard 3-level structure
			.optionalGroup().as(LogicalTypeAnnotation.listType())
				.repeatedGroup()
					.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("element")
				.named("list")
			.named("assignees")

			.optionalGroup().as(LogicalTypeAnnotation.listType())
				.repeatedGroup()
					.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("element")
				.named("list")
			.named("labels")

			.named("issue");
	}


	private Map<String, ProjectItem> loadGithubProjectItems() throws IOException {
		int pages = 0;
//		StringBuilder projFieldsQL = Algorithms.readFromInputStream(this.getClass().getResourceAsStream("/projectFields.graphql"));
		StringBuilder projItemsQL = Algorithms.readFromInputStream(this.getClass().getResourceAsStream("/projectItems.graphql"));
		StringBuilder res = null;
		boolean hasNext = true;
		String nextCursor = "";
		Map<String, ProjectItem> items = new LinkedHashMap<>();
		
//		while (hasNext && pages++ < 1) {
		try {
			while (hasNext && pages++ < MAX_PAGES) {
				String graphQLQuery = projItemsQL.toString().replace(", after: null", 
						nextCursor.length() == 0 ? "" : (", after: \"" + nextCursor + "\" "));
				HttpURLConnection graphQLConn = (HttpURLConnection) new URL("https://api.github.com/graphql")
						.openConnection();
				graphQLConn.addRequestProperty("Authorization", "Bearer " + loginToken);
				graphQLConn.setDoInput(true);
				graphQLConn.setDoOutput(true);
				OutputStream out = graphQLConn.getOutputStream();
				out.write(gson.toJson(Collections.singletonMap("query", graphQLQuery)).getBytes());
				res = Algorithms.readFromInputStream(graphQLConn.getInputStream());
				JsonElement resJson = gson.fromJson(res.toString(), JsonElement.class);

				JsonObject projJson = resJson.getAsJsonObject().getAsJsonObject("data").getAsJsonObject("organization")
						.getAsJsonObject("projectV2");

				JsonArray nodes = projJson.getAsJsonObject("items").getAsJsonArray("nodes");
				for (int i = 0; i < nodes.size(); i++) {
					JsonObject obj = nodes.get(i).getAsJsonObject();
					JsonObject content = obj.getAsJsonObject("content");
					if (content == null || content.get("title") == null) {
						System.out.println("Skip PR " + obj);
						continue;
					}
					String repo = content.get("repository").getAsJsonObject().get("name").getAsString();
					String number = content.get("number").getAsString();
					ProjectItem proj = new ProjectItem(repo, number);
					items.put(proj.id, proj);
					proj.githubId = obj.get("id").getAsString();
					proj.title = content.get("title").getAsString();
					proj.url = getStringOrEmpty(content, "url");
					if (!Algorithms.isEmpty(getStringOrEmpty(content, "publishedAt"))) {
						proj.publishedAt = dbDate
								.format(githubDateTime.parse(getStringOrEmpty(content, "publishedAt")).getTime());
					}
					if (!Algorithms.isEmpty(getStringOrEmpty(content, "closedAt"))) {
						proj.closedAt = dbDate
								.format(githubDateTime.parse(getStringOrEmpty(content, "closedAt")).getTime());
					}
					proj.closed = Boolean.parseBoolean(getStringOrEmpty(content, "closed"));
					proj.author = getStringOrEmpty(content, "author", "login");
					proj.milestoneName = getStringOrEmpty(content, "milestone", "title");
					proj.milestoneId = getStringOrEmpty(content, "milestone", "id");
					proj.assignees = new ArrayList<>();
					if (content.getAsJsonObject("assignees") != null) {
						JsonArray assignees = content.getAsJsonObject("assignees").getAsJsonArray("nodes");
						for (int j = 0; j < assignees.size(); j++) {
							String as = getStringOrEmpty(assignees.get(j).getAsJsonObject(), "login");
							if (!Algorithms.isEmpty(as)) {
								proj.assignees.add(as);
							}
						}
					}
					Collections.sort(proj.assignees);
					if (content.getAsJsonObject("labels") != null) {
						JsonArray labels = content.getAsJsonObject("labels").getAsJsonArray("nodes");
						for (int j = 0; j < labels.size(); j++) {
							String lb = getStringOrEmpty(labels.get(j).getAsJsonObject(), "name");
							String lbId = getStringOrEmpty(labels.get(j).getAsJsonObject(), "id");
							if (!Algorithms.isEmpty(lb)) {
								proj.labels.add(lb);
								proj.labelIds.add(lbId);
							}
						}
					}
					if (obj.getAsJsonObject("fieldValues") == null) {
						continue;
					}
					JsonArray fieldValues = obj.getAsJsonObject("fieldValues").getAsJsonArray("nodes");
					for (int j = 0; j < fieldValues.size(); j++) {
						JsonObject fldV = fieldValues.get(j).getAsJsonObject();
						if (fldV.getAsJsonObject("field") == null) {
							continue;
						}
						String fldName = fldV.getAsJsonObject("field").get("name").getAsString();
						if (fldName.equals("Status")) {
							proj.statusId = getStringOrEmpty(fldV, "optionId");
							proj.statusName = getStringOrEmpty(fldV, "name");
						} else if (fldName.equals("Iteration")) {
							proj.iterationId = getStringOrEmpty(fldV, "iterationId");
							proj.iterationName = getStringOrEmpty(fldV, "title");
							if (!Algorithms.isEmpty(getStringOrEmpty(content, "startDate"))) {
								proj.iterationStartDate = dbDate
										.format(githubDate.parse(getStringOrEmpty(fldV, "startDate")).getTime());
							}
						} else if (fldName.equals("Complexity")) {
							proj.complexity = fldV.get("number").getAsDouble();
						} else if (fldName.equals("StoryPoints")) {
							proj.storyPoints = fldV.get("number").getAsDouble();
						} else if (fldName.equals("Value")) {
							proj.value = fldV.get("number").getAsDouble();
						}
					}
//				System.out.println((ind++) + " " + proj.toString()); // debug
				}
				JsonObject info = projJson.getAsJsonObject("items").getAsJsonObject("pageInfo");
				hasNext = info.get("hasNextPage").getAsBoolean();
				nextCursor = info.get("endCursor").getAsString();

			}
		} catch (Exception e) {
			System.err.println("Error parsing github project info: " + res);
			throw new RuntimeException(e);
		}
		return items;
	}


	private String getStringOrEmpty(JsonObject c, String... keys) {
		JsonElement content = c; 
		for (int i = 0; i < keys.length; i++) {
			if (content == null || content.isJsonNull()) {
				return "";
			}
			content = content.getAsJsonObject().get(keys[i]);
		}
		if (content == null || content.isJsonNull()) {
			return "";
		}
		return content.getAsString();
	}
	
	private static class DBConfig {
		String table;
		String url;
		String usernamePwd;
	}
	
	public DBConfig getConfig() {
		DBConfig config = new DBConfig();
		String[] dbConfig = projectdb.split("/");
		config.table = dbConfig[1] + "." + DB_TABLE_NAME;
		config.url = dbConfig[0];
		config.usernamePwd = dbConfig[2];
		return config;
	}


	private void insertIntoProject(List<ProjectItem> lst) throws IOException {
		DBConfig cfg = getConfig();
		for (int ind = 0; ind < lst.size();) {
			String sql = "insert into " + cfg.table + " FORMAT JSONEachRow";
			URL url = new URL("http://" + cfg.url + "?query=" + URLEncoder.encode(sql, "UTF-8"));
			HttpURLConnection http = (HttpURLConnection) url.openConnection();
			String basicAuth = "Basic " + new String(Base64.getEncoder().encode(cfg.usernamePwd.getBytes()));
			http.addRequestProperty("Authorization", basicAuth);
			http.setDoInput(true);
			http.setDoOutput(true);
			OutputStream out = http.getOutputStream();
			int start = ind;
			while ((ind - start) < BATCH_SIZE && ind < lst.size()) {
				ProjectItem it = lst.get(ind);
				it.version++;
				out.write((gson.toJson(it) + "\n").getBytes());
				ind++;
			}
			out.close();
//			System.out.println(http.getResponseCode() + " " + http.getResponseMessage());
			Algorithms.readFromInputStream(http.getInputStream());
//			System.out.println(res.toString());
		}
	}

//		select * from main.githubProject M join (select max(timestamp) timestamp, id from main.githubProject group by id) S ON M.id = S.id where M.timestamp = S.timestamp limit 1 FORMAT JSON;
	protected Map<String, ProjectItem> loadItemsFromSQLQuery() throws IOException {
		DBConfig conf = getConfig();
		String sql = "SELECT * from " + conf.table + " M join ";
		sql += " (SELECT max(timestamp) timestamp, id from " + conf.table + " group by id) S ON M.id = S.id ";
		sql += " WHERE M.timestamp = S.timestamp FORMAT JSONEachRow";
		Map<String, ProjectItem> mp = new LinkedHashMap<>();
		URL url = new URL("http://" + conf.url + "?query=" + URLEncoder.encode(sql, "UTF-8"));
		HttpURLConnection http = (HttpURLConnection) url.openConnection();
		String basicAuth = "Basic " + new String(Base64.getEncoder().encode(conf.usernamePwd.getBytes()));
		http.addRequestProperty("Authorization", basicAuth);
		StringBuilder res = Algorithms.readFromInputStream(http.getInputStream());
		BufferedReader br = new BufferedReader(new StringReader(res.toString()));
		String line = null;
		while ((line = br.readLine()) != null) {
			ProjectItem pi = gson.fromJson(line, ProjectItem.class);
			// We keep archived items now to set the 'project_archived' flag
			mp.put(pi.id, pi);
		}
		return mp;
	}
		
	protected void executeSimpleSQLQuery() throws IOException {
		String sql = "select * from system.mutations FORMAT JSON";
		DBConfig conf = getConfig();

		URL url = new URL("http://" + conf.url + "?query=" + URLEncoder.encode(sql, "UTF-8"));
		HttpURLConnection http = (HttpURLConnection) url.openConnection();
		String basicAuth = "Basic " + new String(Base64.getEncoder().encode(conf.usernamePwd.getBytes()));
		http.addRequestProperty("Authorization", basicAuth);
		StringBuilder res = Algorithms.readFromInputStream(http.getInputStream());
		System.out.println(res.toString());
	}

	// DB SCCHEMA
//	CREATE TABLE servers.githubProject	(
//			  `timestamp` DateTime, `version` UInt32,
//		    `archived` Bool, `id` String, `repo` String, `num` String, `githubId` String, 
//		    `title` String, `statusName` String, `statusId` String,
//		    `iterationName` String, `iterationId` String, `iterationStartDate` DateTime, 
//		    `milestoneId` String, `milestoneName` String, 
//		    `value` Float32, `storyPoints` Float32, `complexity` Float32,
//		    `url` String, `publishedAt` DateTime, `closedAt` DateTime, `closed` Bool, `author` String,
//		    `assignees` Array(String), `labels` Array(String), `labelIds` Array(String)  
//		)
//		ENGINE = MergeTree()
//		ORDER BY (timestamp, id);
}