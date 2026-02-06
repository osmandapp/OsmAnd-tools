package net.osmand.server.controllers.user;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Controller to serve GitHub issue data to the frontend UI. This controller
 * reads data from Parquet files and provides endpoints for searching,
 * filtering, retrieving issue details, and analyzing issues with LLMs.
 */
@Controller
@RequestMapping("/admin/issues")
public class IssuesController {

	@Value("${osmand.web.location}")
	private String websiteLocation;

	private static final Log LOGGER = LogFactory.getLog(IssuesController.class);

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

	private static final String ISSUES_FOLDER = "servers/github_issues/issues";
	private static final String CATEGORIES_FILE = "categories.parquet";
	private static final String ISSUES_FILE = "osmandapp_issues.parquet";
	private static final String PROJECT_BACKLOG_FILE = "project_backlog.parquet";
	private static final long CACHE_TTL_MS = TimeUnit.MINUTES.toMillis(1);
	private static final long MODEL_PRICING_CACHE_TTL_MS = TimeUnit.HOURS.toMillis(1);
	private static final int MAX_CONTEXT_BYTES = 1_000_000;

	private volatile List<IssueDto> issuesCache;
	private volatile long lastCacheRefresh = 0;

	private final HttpClient httpClient = HttpClient.newHttpClient();
	private final ObjectMapper objectMapper = new ObjectMapper();
	private static final Map<String, ModelPricing> modelPricingCache = new ConcurrentHashMap<>();

	private static final double COST_MULTIPLIER = 100;
	private static volatile long lastPricingRefresh = 0;

	// --- Data Transfer Objects (DTOs) ---
	public static class IssueFileDto {
		public String name;
		public long size;
		public Date lastModified;

		public IssueFileDto(String name, long size, Date lastModified) {
			this.name = name;
			this.size = size;
			this.lastModified = lastModified;
		}
	}

	public static class CommentDto {
		public long id;
		public String user;
		public String body;
		public String createdAt;
	}

	public static class IssueDto {
		public long id;
		public String repo;
		public long number;
		public String title;
		public List<String> labels = new ArrayList<>();
		public List<String> llmCategories = new ArrayList<>();
		public String shortSummary;
		public String state;
		public Date createdAt;
		public String user;
		public String body;
		public List<CommentDto> comments = new ArrayList<>();
		public String milestone;
		public List<String> assignees = new ArrayList<>();
		public String project_status;
		public Boolean project_archived;
		public Date updatedAt;
		public Date closedAt;

		public Date getUpdateTimestamp() {
			return updatedAt == null ? createdAt : updatedAt;
		}

		public Date getClosedTimestamp() {
			return closedAt == null ? createdAt : closedAt;
		}
	}

	private static class ProjectBacklogDto {
		String projectStatus;
		String milestone;
		Boolean projectArchived;
		List<String> assignees = new ArrayList<>();
	}

	public static class ChatMessage {
		public String role;
		public String content;
	}

	public static class AnalyzeRequest {
		public List<Long> issueIds;
		public String model;
		public String prompt;
		public List<ChatMessage> history;
	}

	public static class ModelPricing {
		public double inputCost; // per 1M tokens
		public double outputCost; // per 1M tokens

		public ModelPricing(double inputCost, double outputCost) {
			this.inputCost = inputCost;
			this.outputCost = outputCost;
		}
	}

	@GetMapping("/files")
	public ResponseEntity<List<IssueFileDto>> listAvailableFiles() {
		File folder = new File(websiteLocation, ISSUES_FOLDER);
		if (!folder.exists() || !folder.isDirectory()) {
			return ResponseEntity.ok(Collections.emptyList());
		}
		List<IssueFileDto> fileList = Arrays
				.stream(Objects.requireNonNull(
						folder.listFiles((dir, name) -> name.endsWith(".parquet") || name.endsWith(".csv"))))
				.map(fsr -> new IssueFileDto(fsr.getName(), fsr.length(), new Date(fsr.lastModified())))
				.collect(Collectors.toList());
		return ResponseEntity.ok(fileList);
	}

	@GetMapping
	public String index(Model model) throws IOException {
		File[] files = new File(websiteLocation, ISSUES_FOLDER).listFiles();
		List<IssueFileDto> issueFiles = new ArrayList<>();
		if (files != null) {
			for (File f : files) {
				if (f.getName().endsWith(".parquet") || f.getName().endsWith(".csv")) {
					issueFiles.add(new IssueFileDto(f.getName(), f.length(), new Date(f.lastModified())));
					model.addAttribute("issueFiles", issueFiles);
				}
			}
		}
		return "admin/issues";
	}

	@GetMapping("/download")
	public ResponseEntity<FileSystemResource> downloadFile(@RequestParam("file") String filename) {
		if (filename.contains("..") || filename.contains("/") || filename.contains("\\")) {
			return ResponseEntity.badRequest().build();
		}
		File file = new File(new File(websiteLocation, ISSUES_FOLDER), filename);
		if (!file.exists()) {
			return ResponseEntity.notFound().build();
		}
		HttpHeaders headers = new HttpHeaders();
		headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + file.getName());
		headers.add(HttpHeaders.CONTENT_TYPE, "application/octet-stream");
		headers.add(HttpHeaders.CONTENT_LENGTH, String.valueOf(file.length()));
		return ResponseEntity.ok().headers(headers).body(new FileSystemResource(file));
	}

	private void refreshCache() throws IOException {
		if (issuesCache != null && (System.currentTimeMillis() - lastCacheRefresh) < CACHE_TTL_MS) {
			return;
		}
		Map<Long, IssueDto> categoriesData = readCategoriesParquet();
		Map<Long, IssueDto> issuesDetailData = readIssuesDetailParquet();
		Map<String, ProjectBacklogDto> backlogData = readProjectBacklogParquet();

		issuesDetailData.forEach((id, detail) -> {
			IssueDto existing = categoriesData.get(id);
			if (existing != null) {
				existing.body = detail.body;
				existing.comments = detail.comments;
				existing.milestone = detail.milestone;
				existing.assignees = detail.assignees;
				return;
			}
			categoriesData.put(id, detail);
		});

		List<IssueDto> mergedIssues = new ArrayList<>(categoriesData.values());

		// Second merge with backlog data
		for (IssueDto issue : mergedIssues) {
			if (issue.repo != null && issue.number > 0) {
				String key = (issue.repo + "/" + issue.number).substring("osmandapp/".length()).toLowerCase();
				ProjectBacklogDto backlogInfo = backlogData.get(key);
				if (backlogInfo != null) {
					issue.project_status = backlogInfo.projectStatus;
					issue.project_archived = backlogInfo.projectArchived;
					if (issue.milestone == null || issue.milestone.trim().isEmpty()) {
						issue.milestone = backlogInfo.milestone;
					}
					if (issue.assignees.isEmpty()) {
						issue.assignees = backlogInfo.assignees;
					}
				}
			}
		}

		issuesCache = mergedIssues;
		lastCacheRefresh = System.currentTimeMillis();
	}

	@GetMapping("/repos")
	public ResponseEntity<Set<String>> getRepositories() throws IOException {
		refreshCache();
		if (issuesCache == null || issuesCache.isEmpty()) {
			return ResponseEntity.ok(Collections.emptySet());
		}
		Set<String> repos = issuesCache.stream().map(issue -> issue.repo).filter(Objects::nonNull)
				.collect(Collectors.toSet());
		return ResponseEntity.ok(repos);
	}

	@GetMapping("/search")
	public ResponseEntity<List<IssueDto>> getIssues(@RequestParam(required = false) String q,
			@RequestParam(required = false, defaultValue = "title,short_summary,labels,llm_categories,user,milestone,assignees,project_status") List<String> fields,
			@RequestParam(defaultValue = "100") int limit,
			@RequestParam(defaultValue = "false") boolean includeExtended,
			@RequestParam(required = false, defaultValue = "all") String state,
			@RequestParam(required = false) List<String> repo,
			@RequestParam(required = false) List<String> project_statuses,
			@RequestParam(required = false) List<String> exclude_project_statuses,
			@RequestParam(required = false) Boolean archived,
			@RequestParam(required = false, defaultValue = "updated") String sortBy,
			@RequestParam(required = false, defaultValue = "desc") String sortOrder) {
		try {
			refreshCache();
			List<IssueDto> filteredIssues = filterAndSortIssues(issuesCache, q, fields, includeExtended, state, repo,
					project_statuses, exclude_project_statuses, archived, sortBy, sortOrder);
			return ResponseEntity.ok(filteredIssues.stream().limit(limit).collect(Collectors.toList()));
		} catch (IOException e) {
			e.printStackTrace();
			return ResponseEntity.status(500).build();
		}
	}

	@PostMapping("/analyze")
	public ResponseEntity<StreamingResponseBody> analyzeIssues(@RequestBody AnalyzeRequest request)
			throws IOException, InterruptedException {
		String apiKey = System.getenv("ISSUE_OPENROUTER_TOKEN");
		if (apiKey == null || apiKey.isEmpty()) {
			apiKey = "ollama";
		}
		String apiUrl = System.getenv("ISSUE_API_URL");
		if (apiUrl == null || apiUrl.isEmpty()) {
			apiUrl = "https://veles.osmand.net/api/chat"; // https://openrouter.ai/api/v1/chat/completions";
		}

		refreshCache();

		List<ChatMessage> messages = new ArrayList<>();
		if (request.history == null || request.history.isEmpty()) {
			String context = buildContextFromIssues(request.issueIds);
			if (context.getBytes(StandardCharsets.UTF_8).length > MAX_CONTEXT_BYTES) {
				return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE).body(writer -> writer.write(
						"Error: Selected issues exceed the 100,000-byte context limit. Please select fewer issues."
								.getBytes()));
			}
			messages.add(createMessage("system",
					"You are a helpful assistant. The user has provided details for one or more GitHub issues. Analyze them based on the user's prompt and provide a concise, well-structured answer in Markdown format."));
			messages.add(createMessage("user",
					"Here is the issue data:\n\n" + context + "\n\nMy prompt is: " + request.prompt));
		} else {
			messages.addAll(request.history);
			messages.add(createMessage("user", request.prompt));
		}

		ModelPricing pricing;
		if (!request.model.startsWith("ollama")) {
			refreshModelPricingCache();
			pricing = modelPricingCache.get(request.model);
		} else {
			pricing = null;
		}

		final String finalApiUrl = apiUrl, finalApiKey = apiKey;
		StreamingResponseBody stream = out -> {
			PrintWriter writer = new PrintWriter(out);
			try {
				String jsonReq = objectMapper.writeValueAsString(
						Map.of("model", request.model, "messages", messages, "stream", true));
				// LOGGER.info("LLM Request: " + jsonReq);

				HttpRequest openRouterRequest = HttpRequest.newBuilder()
						.uri(URI.create(finalApiUrl))
						.header("Authorization", "Bearer " + finalApiKey).header("Content-Type", "application/json")
						.timeout(Duration.of(5, ChronoUnit.MINUTES))
						.POST(HttpRequest.BodyPublishers.ofString(jsonReq))
						.build();

				HttpResponse<Stream<String>> response = httpClient.send(openRouterRequest,
						HttpResponse.BodyHandlers.ofLines());

				if (response.statusCode() != 200) {
					String errorBody = response.body().collect(Collectors.joining("\n"));
					writer.write("Error: LLM API request failed with status " + response.statusCode() + ". Response: "
							+ errorBody);
					return;
				}
				LOGGER.info("LLM Response status: " + response.statusCode());

				final int[] inputTokens = { 0 };
				final int[] outputTokens = { 0 };
				final StringBuilder streamedContent = new StringBuilder();
				final boolean[] streamDone = { false };

				response.body().forEach(line -> {
					// LOGGER.info("LLM Response line: " + line);
					if (line == null || line.isBlank()) {
						return;
					}
					if (!line.startsWith("data: ") && !line.startsWith("{")) {
						return;
					}

					String json = line;
					if (line.startsWith("data: ")) {
						json = line.substring(6);
						if ("[DONE]".equals(json)) {
							return;
						}
					}

					try {
						JsonNode node = objectMapper.readTree(json);

						// OpenRouter: { usage: {prompt_tokens, completion_tokens} }
						if (node.has("usage")) {
							JsonNode usageNode = node.get("usage");
							if (usageNode.has("prompt_tokens")) {
								inputTokens[0] = usageNode.get("prompt_tokens").asInt();
							}
							if (usageNode.has("completion_tokens")) {
								outputTokens[0] = usageNode.get("completion_tokens").asInt();
							}
						}

						// Ollama (/api/chat): { prompt_eval_count, eval_count }
						if (node.has("prompt_eval_count")) {
							inputTokens[0] = node.get("prompt_eval_count").asInt(inputTokens[0]);
						}
						if (node.has("eval_count")) {
							outputTokens[0] = node.get("eval_count").asInt(outputTokens[0]);
						}

						String content = null;

						// OpenRouter SSE chunk: { choices: [ { delta: { content } } ] }
						if (node.has("choices")) {
							JsonNode choices = node.get("choices");
							if (choices.isArray() && !choices.isEmpty()) {
								JsonNode delta = choices.get(0).get("delta");
								if (delta != null && delta.has("content")) {
									content = delta.get("content").asText();
								}
							}
						}

						// Ollama JSONL chunk: { message: { content }, done: boolean }
						if (content == null && node.has("message")) {
							JsonNode messageNode = node.get("message");
							if (messageNode != null && messageNode.has("content")) {
								content = messageNode.get("content").asText();
							}
						}
						if (node.has("done") && node.get("done").asBoolean(false)) {
							streamDone[0] = true;
						}

						if (content != null && !content.isEmpty()) {
							streamedContent.append(content);
							writer.write(content);
							writer.flush();
						}
					} catch (IOException e) {
						LOGGER.error("LLM issues parsing error: " + e.getMessage());
						LOGGER.info("Response: " + json);
					}
				});
				if (streamDone[0]) {
					LOGGER.info("LLM stream done. Total chars: " + streamedContent.length());
				}

				if (pricing != null) {
					double cost = ((double) inputTokens[0] / 1_000_000 * pricing.inputCost)
							+ ((double) outputTokens[0] / 1_000_000 * pricing.outputCost);
					DecimalFormat df = new DecimalFormat("#.######");
					String costInfo = "\n\n---\n**Tokens:** " + inputTokens[0] + " input / " + outputTokens[0]
							+ " output. **Cost:** $" + df.format(cost * COST_MULTIPLIER);
					LOGGER.info("LLM issues " + request.model + " messages: " + messages.size() + " "
							+ costInfo.replace("\n", " "));
					writer.write(costInfo);
					writer.flush();
				}

			} catch (Exception e) {
				e.printStackTrace();
				writer.write("\n\nError processing LLM stream: " + e.getMessage());
			} finally {
				writer.close();
			}
		};

		return ResponseEntity.ok().contentType(MediaType.TEXT_PLAIN).body(stream);
	}

	private void refreshModelPricingCache() throws IOException, InterruptedException {
		if ((System.currentTimeMillis() - lastPricingRefresh) < MODEL_PRICING_CACHE_TTL_MS) {
			return;
		}
		LOGGER.info("Refreshing OpenRouter model pricing cache...");
		HttpRequest request = HttpRequest.newBuilder().uri(URI.create("https://openrouter.ai/api/v1/models")).GET()
				.build();

		HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
		if (response.statusCode() == 200) {
			JsonNode modelsNode = objectMapper.readTree(response.body());
			if (modelsNode.has("data")) {
				for (JsonNode modelNode : modelsNode.get("data")) {
					String id = modelNode.get("id").asText();
					double inputCost = modelNode.get("pricing").get("prompt").asDouble(0.0);
					double outputCost = modelNode.get("pricing").get("completion").asDouble(0.0);
					modelPricingCache.put(id, new ModelPricing(inputCost * 1_000_000, outputCost * 1_000_000));
				}
				lastPricingRefresh = System.currentTimeMillis();
				LOGGER.info(
						"Successfully refreshed model pricing cache. Loaded " + modelPricingCache.size() + " models.");
			}
		} else {
			LOGGER.info("Failed to refresh model pricing. Status: " + response.statusCode());
		}
	}

	private String buildContextFromIssues(List<Long> issueIds) {
		if (issuesCache == null)
			return "";
		Set<Long> idSet = new HashSet<>(issueIds);
		StringBuilder sb = new StringBuilder();
		List<IssueDto> selectedIssues = issuesCache.stream().filter(issue -> idSet.contains(issue.id))
				.collect(Collectors.toList());

		for (IssueDto issue : selectedIssues) {
			sb.append("---------------------------------\n");
			sb.append("Issue ID: ").append(issue.id).append("\n");
			sb.append("Repo: ").append(issue.repo).append("#").append(issue.number).append("\n");
			sb.append("Title: ").append(issue.title).append("\n");
			sb.append("Author: ").append(issue.user).append(" | Created: ").append(issue.createdAt).append("\n");
			sb.append("State: ").append(issue.state).append(" | Milestone: ").append(issue.milestone).append("\n");
			sb.append("Project Status: ").append(issue.project_status).append("\n");
			sb.append("Labels: ").append(String.join(", ", issue.labels)).append("\n");
			sb.append("Assignees: ").append(String.join(", ", issue.assignees)).append("\n\n");
			sb.append("BODY:\n").append(issue.body).append("\n\n");

			if (issue.comments != null && !issue.comments.isEmpty()) {
				sb.append("COMMENTS:\n");
				for (CommentDto comment : issue.comments) {
					sb.append("  Comment by ").append(comment.user).append(" at ").append(comment.createdAt)
							.append(":\n");
					sb.append("  > ").append(comment.body.replaceAll("\n", "\n  > ")).append("\n");
				}
			}
			sb.append("\n");
		}
		return sb.toString();
	}

	private ChatMessage createMessage(String role, String content) {
		ChatMessage msg = new ChatMessage();
		msg.role = role;
		msg.content = content;
		return msg;
	}

	/**
	 * Reads the 'categories.parquet' file and maps its records to IssueDto objects.
	 */
	private Map<Long, IssueDto> readCategoriesParquet() throws IOException {
		Map<Long, IssueDto> data = new HashMap<>();
		Path path = Paths.get(websiteLocation, ISSUES_FOLDER, CATEGORIES_FILE);

		readParquetFile(path, (group, schema) -> {
			IssueDto issue = new IssueDto();
			long id = getLong(group, "id");
			if (id == 0)
				return; // Skip if no ID

			issue.id = id;
			issue.repo = getString(group, "repo");
			issue.number = getLong(group, "number");
			issue.title = getString(group, "title");
			issue.labels = getStringList(group, "labels");
			issue.llmCategories = getStringList(group, "llm_categories");
			issue.shortSummary = getString(group, "short_summary");
			issue.state = getString(group, "state");
			issue.user = getString(group, "user");

			// Handle timestamp (stored as INT64 NANOS)
			long nanoTimestamp = getLong(group, "created_at");
			if (nanoTimestamp > 0) {
				issue.createdAt = new Date(TimeUnit.NANOSECONDS.toMillis(nanoTimestamp));
			}
			nanoTimestamp = getLong(group, "updated_at");
			if (nanoTimestamp > 0) {
				issue.updatedAt = new Date(TimeUnit.NANOSECONDS.toMillis(nanoTimestamp));
			}
			nanoTimestamp = getLong(group, "closed_at");
			if (nanoTimestamp > 0) {
				issue.closedAt = new Date(TimeUnit.MICROSECONDS.toMillis(nanoTimestamp));
			}
			data.put(issue.id, issue);
		});
		return data;
	}

	private Map<String, ProjectBacklogDto> readProjectBacklogParquet() throws IOException {
		Map<String, ProjectBacklogDto> data = new HashMap<>();
		Path path = Paths.get(websiteLocation, ISSUES_FOLDER, PROJECT_BACKLOG_FILE);
		if (!path.toFile().exists()) {
			LOGGER.warn(PROJECT_BACKLOG_FILE + " not found. Skipping.");
			return data;
		}

		readParquetFile(path, (group, schema) -> {
			String id = getString(group, "id"); // e.g., "OsmAnd-Issues/2995"
			if (id == null || id.isEmpty())
				return;

			ProjectBacklogDto backlogInfo = new ProjectBacklogDto();
			backlogInfo.projectStatus = getString(group, "project_status");
			backlogInfo.milestone = getString(group, "milestone");
			backlogInfo.assignees = getStringList(group, "assignees");
			backlogInfo.projectArchived = getBoolean(group, "project_archived");

			data.put(id.toLowerCase(), backlogInfo);
		});
		return data;
	}

	/**
	 * Reads the 'osmandapp_issues.parquet' file and maps its records to IssueDto
	 * objects.
	 */
	private Map<Long, IssueDto> readIssuesDetailParquet() throws IOException {
		Map<Long, IssueDto> data = new HashMap<>();
		Path path = Paths.get(websiteLocation, ISSUES_FOLDER, ISSUES_FILE);

		readParquetFile(path, (group, schema) -> {
			IssueDto issue = new IssueDto();
			long id = getLong(group, "id");
			if (id == 0)
				return;

			issue.id = id;
			issue.repo = getString(group, "repo");
			issue.number = getLong(group, "number");
			issue.title = getString(group, "title");
			issue.body = getString(group, "body");
			issue.state = getString(group, "state");
			issue.user = getString(group, "user");
			issue.labels = getStringList(group, "labels");
			issue.createdAt = getDate(group, "created_at");
			issue.closedAt = getDate(group, "closed_at");
			issue.milestone = getString(group, "milestone");
			issue.assignees = getStringList(group, "assignees");
			issue.updatedAt = getDate(group, "updated_at");

			// Handle nested list of comments
			if (hasField(group, "comments") && group.getFieldRepetitionCount("comments") > 0) {
				Group commentsList = group.getGroup("comments", 0);
				if (hasField(commentsList, "list")) {
					int commentCount = commentsList.getFieldRepetitionCount("list");
					for (int i = 0; i < commentCount; i++) {
						Group list = commentsList.getGroup("list", i);
						Group commentGroup = list.getGroup("element", 0);
						CommentDto comment = new CommentDto();
						comment.id = getLong(commentGroup, "id");
						comment.user = getString(commentGroup, "user");
						comment.body = getString(commentGroup, "body");
						comment.createdAt = getString(commentGroup, "created_at");
						issue.comments.add(comment);
					}
				}
			}
			data.put(issue.id, issue);
		});
		return data;
	}

	private Date getDate(Group group, String string) {
		String date = getString(group, string);
		if (date != null && !date.isEmpty()) {
			try {
				return sdf.parse(date);
			} catch (ParseException e) {
				LOGGER.warn(e.getMessage(), e);
			}
		}
		return null;
	}

	/**
	 * Generic utility to read a Parquet file and process each record.
	 */
	private void readParquetFile(Path path, BiConsumer<Group, MessageType> recordConsumer) throws IOException {
		File file = path.toFile();
		if (!file.exists()) {
			LOGGER.error("File not found: " + path);
			return;
		}

		try (ParquetFileReader reader = ParquetFileReader.open(
				HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(file.toURI()), new Configuration()),
				ParquetReadOptions.builder().build())) {
			MessageType schema = reader.getFooter().getFileMetaData().getSchema();
			MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
			PageReadStore pages;
			while ((pages = reader.readNextRowGroup()) != null) {
				long rows = pages.getRowCount();
				RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
				for (int i = 0; i < rows; i++) {
					Group group = recordReader.read();
					recordConsumer.accept(group, schema);
				}
			}
		}
	}

	// --- Parquet Group Helper Methods ---
	private boolean hasField(Group group, String fieldName) {
		return group.getType().containsField(fieldName);
	}

	private String getString(Group group, String fieldName) {
		if (hasField(group, fieldName) && group.getFieldRepetitionCount(fieldName) > 0) {
			return group.getString(fieldName, 0);
		}
		return null;
	}

	private long getLong(Group group, String fieldName) {
		if (hasField(group, fieldName) && group.getFieldRepetitionCount(fieldName) > 0) {
			return group.getLong(fieldName, 0);
		}
		return 0L;
	}

	private Boolean getBoolean(Group group, String fieldName) {
		if (hasField(group, fieldName) && group.getFieldRepetitionCount(fieldName) > 0) {
			try {
				return group.getBoolean(fieldName, 0);
			} catch (Exception e) {
				LOGGER.warn("Could not read boolean value for field: " + fieldName);
				return null;
			}
		}
		return null;
	}

	private List<String> getStringList(Group group, String fieldName) {
		List<String> list = new ArrayList<>();
		if (hasField(group, fieldName) && group.getFieldRepetitionCount(fieldName) > 0) {
			Group listGroup = group.getGroup(fieldName, 0);
			if (hasField(listGroup, "list")) {
				int count = listGroup.getFieldRepetitionCount("list");
				for (int i = 0; i < count; i++) {
					list.add(listGroup.getGroup("list", i).getString("element", 0));
				}
			}
		}
		return list;
	}

	/**
	 * Filters and sorts a list of issues based on search criteria.
	 */
	private List<IssueDto> filterAndSortIssues(List<IssueDto> allIssues, String query, List<String> fields,
			boolean includeExtended, String state, List<String> repos, List<String> project_statuses,
			List<String> exclude_project_statuses, Boolean archived, String sortBy, String sortOrder) {

		// 1. Create Sorter
		Function<IssueDto, Date> keyExtractor;
		switch (sortBy.toLowerCase()) {
		case "created":
			keyExtractor = issue -> issue.createdAt;
			break;
		case "closed":
			keyExtractor = issue -> issue.getClosedTimestamp();
			break;
		case "updated":
		default:
			keyExtractor = issue -> issue.getUpdateTimestamp();
			break;
		}

		Comparator<IssueDto> comparator = Comparator.comparing(keyExtractor,
				Comparator.nullsLast(Comparator.naturalOrder()));
		if ("desc".equalsIgnoreCase(sortOrder)) {
			comparator = comparator.reversed();
		}

		// 2. Initial sort of all issues
		allIssues.sort(comparator);

		// 3. Apply filters sequentially
		final List<IssueDto> repoFilteredIssues;
		if (repos != null && !repos.isEmpty() && !repos.contains("all")) {
			Set<String> repoSet = new HashSet<>(repos);
			repoFilteredIssues = allIssues.stream().filter(issue -> issue.repo != null && repoSet.contains(issue.repo))
					.collect(Collectors.toList());
		} else {
			repoFilteredIssues = allIssues;
		}

		final List<IssueDto> stateFilteredIssues;
		if (state != null && !state.equalsIgnoreCase("all") && !state.isEmpty()) {
			stateFilteredIssues = repoFilteredIssues.stream().filter(issue -> state.equalsIgnoreCase(issue.state))
					.collect(Collectors.toList());
		} else {
			stateFilteredIssues = repoFilteredIssues;
		}

		final List<IssueDto> archivedFilteredIssues;
		if (archived != null) {
			archivedFilteredIssues = stateFilteredIssues.stream().filter(issue -> {
				boolean isArchived = issue.project_archived != null && issue.project_archived;
				return isArchived == archived;
			}).collect(Collectors.toList());
		} else {
			archivedFilteredIssues = stateFilteredIssues;
		}

		final List<IssueDto> statusFilteredIssues;
		final boolean hasInclusionFilter = project_statuses != null && !project_statuses.isEmpty();
		final boolean hasExclusionFilter = exclude_project_statuses != null && !exclude_project_statuses.isEmpty();

		if (hasInclusionFilter || hasExclusionFilter) {
			final Set<String> includeSet = hasInclusionFilter ? new HashSet<>(project_statuses)
					: Collections.emptySet();
			final Set<String> excludeSet = hasExclusionFilter ? new HashSet<>(exclude_project_statuses)
					: Collections.emptySet();

			statusFilteredIssues = archivedFilteredIssues.stream().filter(issue -> {
				boolean passesInclude = !hasInclusionFilter
						|| (issue.project_status != null && includeSet.contains(issue.project_status));
				boolean passesExclude = !hasExclusionFilter
						|| (issue.project_status == null || !excludeSet.contains(issue.project_status));
				return passesInclude && passesExclude;
			}).collect(Collectors.toList());
		} else {
			statusFilteredIssues = archivedFilteredIssues;
		}

		if (query == null || query.trim().isEmpty()) {
			return statusFilteredIssues;
		}

		// 4. Parse search query for inclusion and exclusion terms
		List<String> includedQuotedTerms = new ArrayList<>();
		List<String> excludedTerms = new ArrayList<>();

		// Regex to find -"..." exclusion phrases and "..." inclusion phrases
		Pattern pattern = Pattern.compile("(?:^|\\s)(-|)\"([^\"]*)\"");
		Matcher matcher = pattern.matcher(query);
		StringBuffer sb = new StringBuffer();
		while (matcher.find()) {
			boolean isExcluded = "-".equals(matcher.group(1));
			String term = matcher.group(2).toLowerCase();
			if (isExcluded) {
				excludedTerms.add(term);
			} else {
				includedQuotedTerms.add(term);
			}
			matcher.appendReplacement(sb, "");
		}
		matcher.appendTail(sb);

		String remainingQuery = sb.toString().toLowerCase();
		final boolean isOrLogic = remainingQuery.contains(",");
		List<String> includedRegularTerms = Stream
				.of(isOrLogic ? remainingQuery.split(",") : remainingQuery.split("\\s+")).map(String::trim)
				.filter(s -> !s.isEmpty()).filter(s -> {
					if (s.startsWith("-")) {
						if (s.length() > 1) {
							excludedTerms.add(s.substring(1));
						}
						return false; // This term is for exclusion
					}
					return true; // This term is for inclusion
				}).collect(Collectors.toList());

		if (includedQuotedTerms.isEmpty() && includedRegularTerms.isEmpty() && excludedTerms.isEmpty()) {
			return statusFilteredIssues;
		}

		return statusFilteredIssues.stream().filter(issue -> {
			StringBuilder searchableContentBuilder = new StringBuilder();
			if (fields.contains("title") && issue.title != null)
				searchableContentBuilder.append(issue.title).append(" ");
			if (fields.contains("short_summary") && issue.shortSummary != null)
				searchableContentBuilder.append(issue.shortSummary).append(" ");
			if (fields.contains("user") && issue.user != null)
				searchableContentBuilder.append(issue.user).append(" ");
			if (fields.contains("labels") && issue.labels != null)
				searchableContentBuilder.append(String.join(" ", issue.labels)).append(" ");
			if (fields.contains("llm_categories") && issue.llmCategories != null)
				searchableContentBuilder.append(String.join(" ", issue.llmCategories)).append(" ");
			if (fields.contains("milestone") && issue.milestone != null)
				searchableContentBuilder.append(issue.milestone).append(" ");
			if (fields.contains("assignees") && issue.assignees != null)
				searchableContentBuilder.append(String.join(" ", issue.assignees)).append(" ");
			if (fields.contains("project_status") && issue.project_status != null)
				searchableContentBuilder.append(issue.project_status).append(" ");
			if (includeExtended) {
				if (issue.body != null)
					searchableContentBuilder.append(issue.body).append(" ");
				if (issue.comments != null) {
					issue.comments.forEach(
							c -> searchableContentBuilder.append(c.user).append(" ").append(c.body).append(" "));
				}
			}
			String finalContent = searchableContentBuilder.toString().toLowerCase();

			// Check exclusions first
			if (!excludedTerms.isEmpty() && excludedTerms.stream().anyMatch(finalContent::contains)) {
				return false;
			}

			// Check required quoted terms
			if (!includedQuotedTerms.isEmpty() && !includedQuotedTerms.stream().allMatch(finalContent::contains)) {
				return false;
			}

			// Check required regular terms
			if (!includedRegularTerms.isEmpty()) {
				if (isOrLogic) {
					if (!includedRegularTerms.stream().anyMatch(finalContent::contains)) {
						return false;
					}
				} else {
					if (!includedRegularTerms.stream().allMatch(finalContent::contains)) {
						return false;
					}
				}
			}

			return true;
		}).collect(Collectors.toList());
	}
}