package net.osmand.server.monitor;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import net.osmand.util.Algorithms;

@Component
public class OsmAndGithubProjectMonitorTasks {

	// DB SCHEMA BELOW
	
	protected static final Log LOG = LogFactory.getLog(OsmAndGithubProjectMonitorTasks.class);

	private static final int MAX_PAGES = 100;
	private static final int BATCH_SIZE = 100; 
	private static final String DB_TABLE_NAME = "main.githubProject";


	@Value("${monitoring.enabled}")
	private boolean enabled;
	
	@Value("${monitoring.project.db}")
	private String projectdb;
	
	@Value("${monitoring.project.token}")
	private String loginToken;
	
	Gson gson = new Gson();

	int TOTAL_SYNC;
	
	
	public static class ProjectItem {
		long timestamp;
		boolean archived;
		String id;
		String repo;
		String num;
		String githubId;
		String title;
		String statusName;
		String statusId;
		String iterationName;
		String iterationId;
		String iterationStartDate;
		String milestoneId;
		String milestoneName;
		double value;
		double storyPoints;
		double complexity;
		String url;
		String publishedAt;
		String closedAt;
		boolean closed;
		String author;
		List<String> assignees = new ArrayList<>();
		List<String> labels = new ArrayList<>();
		List<String> labelIds = new ArrayList<>();
		public ProjectItem(String repo, String num) {
			this.timestamp = System.currentTimeMillis() / 1000;
			this.id = repo + "/" + num;
			this.repo = repo;
			this.num = num;
		}
		
		@Override
		public String toString() {
			ProjectItem proj = this;
			return String.format("%s %s it=%s vl=%.0f cmpl=%.0f sp=%.0f - %s", proj.id, proj.statusName,
					proj.iterationName, proj.value, proj.complexity, proj.storyPoints, proj.title);
		}
	}
	

	//@Scheduled(fixedRateString = "PT30M")
	@Scheduled(fixedRateString = "PT5S")
	public void syncGithubProject() throws IOException {
		if (Algorithms.isEmpty(projectdb) || ++TOTAL_SYNC > 1) {
			return;
		}
		long time = System.currentTimeMillis();
		LOG.info("SYNC Github project - Start...");
		if (Algorithms.isEmpty(loginToken)) {
			System.out.println("ERROR: Github token is not configured");
		}

		Map<String, ProjectItem> items = loadGithubProjectItems();
		insertIntoProject(items);
		LOG.info(String.format("SYNC Github project (%d items) - DONE in %.1f s", items.size(),
				(System.currentTimeMillis() - time)/1.0e3));
	}


	private Map<String, ProjectItem> loadGithubProjectItems() throws IOException, MalformedURLException {
		int ind = 0, pages = 0;
//		StringBuilder projFieldsQL = Algorithms.readFromInputStream(this.getClass().getResourceAsStream("/projectFields.graphql"));
		StringBuilder projItemsQL = Algorithms.readFromInputStream(this.getClass().getResourceAsStream("/projectItems.graphql"));
		boolean hasNext = true;
		String nextCursor = "null";
		Map<String, ProjectItem> items = new LinkedHashMap<>();
		while (hasNext && pages++ < MAX_PAGES) {
			String graphQLQuery = projItemsQL.toString().replace("after: null", "after: \"" + nextCursor+"\" ");
			HttpURLConnection graphQLConn = (HttpURLConnection) new URL("https://api.github.com/graphql")
					.openConnection();
			graphQLConn.addRequestProperty("Authorization", "Bearer " + loginToken);
			graphQLConn.setDoInput(true);
			graphQLConn.setDoOutput(true);
			OutputStream out = graphQLConn.getOutputStream();
			out.write(gson.toJson(Collections.singletonMap("query", graphQLQuery)).getBytes());
			StringBuilder res = Algorithms.readFromInputStream(graphQLConn.getInputStream());
			JsonElement resJson = gson.fromJson(res.toString(), JsonElement.class);

			JsonObject projJson = resJson.getAsJsonObject().getAsJsonObject("data").getAsJsonObject("organization").getAsJsonObject("projectV2");
			
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
				proj.publishedAt = getStringOrEmpty(content, "publishedAt");
				proj.closedAt = getStringOrEmpty(content, "closedAt");
				proj.closed = Boolean.parseBoolean(getStringOrEmpty(content, "closed"));
				proj.author = getStringOrEmpty(content, "author", "login");
				proj.milestoneName = getStringOrEmpty(content, "milestone", "title");
				proj.milestoneId = getStringOrEmpty(content, "milestone", "id");
				proj.assignees = new ArrayList<>();
				JsonArray assignees = obj.getAsJsonObject("assignees").getAsJsonArray("nodes");
				for (int j = 0; j < assignees.size(); j++) {
					String as = getStringOrEmpty(assignees.get(j).getAsJsonObject(), "login");
					if (!Algorithms.isEmpty(as)) {
						proj.assignees.add(as);
					}
				}
				Collections.sort(proj.assignees);
				JsonArray labels = obj.getAsJsonObject("labels").getAsJsonArray("nodes");
				for (int j = 0; j < labels.size(); j++) {
					String lb = getStringOrEmpty(labels.get(j).getAsJsonObject(), "name");
					String lbId = getStringOrEmpty(labels.get(j).getAsJsonObject(), "id");
					if (!Algorithms.isEmpty(lb)) {
						proj.labels.add(lb);
						proj.labelIds.add(lbId);
					}
				}
				JsonArray fieldValues = obj.getAsJsonObject("fieldValues").getAsJsonArray("nodes");
				for (int j = 0; j < fieldValues.size(); j++) {
					JsonObject fldV = fieldValues.get(j).getAsJsonObject();
					if (fldV.getAsJsonObject("field") == null) {
						continue;
					}
					String fldName = fldV.getAsJsonObject("field").get("name").getAsString();
					if (fldName.equals("Status")) {
						proj.statusId = fldV.get("optionId").getAsString();
						proj.statusName = fldV.get("name").getAsString();
					} else if (fldName.equals("Iteration")) {
						proj.iterationId = fldV.get("iterationId").getAsString();
						proj.iterationName = fldV.get("title").getAsString();
						proj.iterationStartDate = fldV.get("startDate").getAsString();
					} else if (fldName.equals("Complexity")) {
						proj.complexity = fldV.get("number").getAsDouble();
					} else if (fldName.equals("StoryPoints")) {
						proj.storyPoints = fldV.get("number").getAsDouble();
					} else if (fldName.equals("Value")) {
						proj.value = fldV.get("number").getAsDouble();
					}
				}
				System.out.println((ind++) + " " + proj.toString()); // debug
			}
			JsonObject info = projJson.getAsJsonObject("items").getAsJsonObject("pageInfo");
			hasNext = info.get("hasNextPage").getAsBoolean();
			nextCursor = info.get("endCursor").getAsString();
			
		}
		return items;
	}


	private String getStringOrEmpty(JsonObject content, String... keys) {
		if (content == null) {
			return "";
		}
		for (int i = 0; i < keys.length - 1; i++) {
			content = content.getAsJsonObject(keys[i]);
			if (content == null) {
				return "";
			}
		}
		JsonElement el = content.get(keys[keys.length - 1]);
		if (el == null || el.getAsString() == null) {
			return "";
		}
		return el.getAsString();
	}


	private void insertIntoProject(Map<String, ProjectItem> items) throws IOException {
		List<ProjectItem> lst = new ArrayList<>(items.values());
		for (int ind = 0; ind < lst.size();) {
			String sql = "insert into " + DB_TABLE_NAME + " FORMAT JSONEachRow";
			String[] dbConfig = projectdb.split("/");
			URL url = new URL("http://" + dbConfig[0] + "?query=" + URLEncoder.encode(sql, "UTF-8"));
			HttpURLConnection http = (HttpURLConnection) url.openConnection();
			String basicAuth = "Basic " + new String(Base64.getEncoder().encode(dbConfig[1].getBytes()));
			http.addRequestProperty("Authorization", basicAuth);
			http.setDoInput(true);
			http.setDoOutput(true);
			OutputStream out = http.getOutputStream();
			int start = ind;
			while ((ind - start) < BATCH_SIZE && ind < lst.size()) {
				ProjectItem it = lst.get(ind);
				out.write((gson.toJson(it) + "\n").getBytes());
				ind++;
			}
			out.close();
			StringBuilder res = Algorithms.readFromInputStream(http.getInputStream());
			System.out.println(res.toString());
		}
	}

		
	protected void executeSimpleSQLQuery() throws MalformedURLException, UnsupportedEncodingException, IOException {
		String sql = "select * from system.mutations FORMAT JSON";
		String[] dbConfig = projectdb.split("/");

		URL url = new URL("http://" + dbConfig[0] + "?query=" + URLEncoder.encode(sql, "UTF-8"));
		HttpURLConnection http = (HttpURLConnection) url.openConnection();
		String basicAuth = "Basic " + new String(Base64.getEncoder().encode(dbConfig[1].getBytes()));
		http.addRequestProperty("Authorization", basicAuth);
		StringBuilder res = Algorithms.readFromInputStream(http.getInputStream());
		System.out.println(res.toString());
	}

	// DB SCCHEMA
//	CREATE TABLE main.githubProject	(
//			  `timestamp` DateTime,
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
