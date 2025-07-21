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
@RequestMapping("/admin/issues/")
public class IssuesController {

	@Value("${osmand.web.location}")
	private String websiteLocation;
	
	private static final Log LOGGER = LogFactory.getLog(IssuesController.class);


	private static final String ISSUES_FOLDER = "servers/github_issues/issues";
	private static final String CATEGORIES_FILE = "categories.parquet";
	private static final String ISSUES_FILE = "osmandapp_issues.parquet";
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

		issuesDetailData.forEach((id, detail) -> categoriesData.merge(id, detail, (cat, det) -> {
			cat.body = det.body;
			cat.comments = det.comments;
			cat.milestone = det.milestone;
			cat.assignees = det.assignees;
			return cat;
		}));
		issuesCache = new ArrayList<>(categoriesData.values());
		lastCacheRefresh = System.currentTimeMillis();
	}

	@GetMapping("/search")
	public ResponseEntity<List<IssueDto>> getIssues(@RequestParam(required = false) String query,
			@RequestParam(required = false, defaultValue = "title,short_summary,labels,llm_categories,user,milestone,assignees") List<String> fields,
			@RequestParam(defaultValue = "100") int limit,
			@RequestParam(defaultValue = "false") boolean includeExtended,
			@RequestParam(required = false, defaultValue = "all") String state) {
		try {
			refreshCache();
			List<IssueDto> filteredIssues = filterAndSortIssues(issuesCache, query, fields, includeExtended, state);
			return ResponseEntity.ok(filteredIssues.stream().limit(limit).collect(Collectors.toList()));
		} catch (IOException e) {
			e.printStackTrace();
			return ResponseEntity.status(500).build();
		}
	}

	@PostMapping("/analyze")
	public ResponseEntity<StreamingResponseBody> analyzeIssues(@RequestBody AnalyzeRequest request) throws IOException, InterruptedException {
		String apiKey = System.getenv("ISSUE_OPENROUTER_TOKEN");
		if (apiKey == null || apiKey.isEmpty()) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(writer -> writer.write("Error: ISSUE_OPENROUTER_TOKEN environment variable not set.".getBytes()));
		}

		refreshCache();

		List<ChatMessage> messages = new ArrayList<>();
		if (request.history == null || request.history.isEmpty()) {
			String context = buildContextFromIssues(request.issueIds);
			if (context.getBytes(StandardCharsets.UTF_8).length > MAX_CONTEXT_BYTES) {
				return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE).body(writer -> writer.write(
						"Error: Selected issues exceed the 100,000-byte context limit. Please select fewer issues.".getBytes()));
			}
			messages.add(createMessage("system",
					"You are a helpful assistant. The user has provided details for one or more GitHub issues. Analyze them based on the user's prompt and provide a concise, well-structured answer in Markdown format."));
			messages.add(createMessage("user",
					"Here is the issue data:\n\n" + context + "\n\nMy prompt is: " + request.prompt));
		} else {
			messages.addAll(request.history);
			messages.add(createMessage("user", request.prompt));
		}

		refreshModelPricingCache();
		ModelPricing pricing = modelPricingCache.get(request.model);

		StreamingResponseBody stream = out -> {
			PrintWriter writer = new PrintWriter(out);
			try {
				HttpRequest openRouterRequest = HttpRequest.newBuilder()
						.uri(URI.create("https://openrouter.ai/api/v1/chat/completions"))
						.header("Authorization", "Bearer " + apiKey).header("Content-Type", "application/json")
				        .timeout(Duration.of(5, ChronoUnit.MINUTES))
						.POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(
								Map.of("model", request.model, "messages", messages, "stream", true))))
						.build();

				HttpResponse<Stream<String>> response = httpClient.send(openRouterRequest,
						HttpResponse.BodyHandlers.ofLines());

				if (response.statusCode() != 200) {
					String errorBody = response.body().collect(Collectors.joining("\n"));
					writer.write("Error: LLM API request failed with status " + response.statusCode() + ". Response: "
							+ errorBody);
					return;
				}

				final int[] inputTokens = { 0 };
				final int[] outputTokens = { 0 };

				response.body().forEach(line -> {
					if (line.startsWith("data: ")) {
						String json = line.substring(6);
						if ("[DONE]".equals(json))
							return;

						try {
							JsonNode node = objectMapper.readTree(json);

							// Extract token usage from the 'x-openrouter' extension
							if (node.has("usage")) {
								JsonNode usageNode = node.get("usage");
								inputTokens[0] = usageNode.get("prompt_tokens").asInt();
								outputTokens[0] = usageNode.get("completion_tokens").asInt();
							}

							if (node.has("choices")) {
								JsonNode choices = node.get("choices");
								if (choices.isArray() && !choices.isEmpty()) {
									JsonNode delta = choices.get(0).get("delta");
									if (delta != null && delta.has("content")) {
										String content = delta.get("content").asText();
										if (!content.isEmpty()) {
											writer.write(content);
											writer.flush();
										}
									}
								}
							}
						} catch (IOException e) {
							// Ignore parsing errors for now
						}
					}
				});

				if (pricing != null) {
					double cost = ((double) inputTokens[0] / 1_000_000 * pricing.inputCost)
							+ ((double) outputTokens[0] / 1_000_000 * pricing.outputCost);
					DecimalFormat df = new DecimalFormat("#.######");
					String costInfo = "\n\n---\n**Tokens:** " + inputTokens[0] + " input / " + outputTokens[0]
							+ " output. **Cost:** $" + df.format(cost * COST_MULTIPLIER);
					LOGGER.info("LLM issues " + request.model + " messages: " + messages.size() + " " + costInfo.replace("\n", " "));
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
			data.put(issue.id, issue);
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
			issue.body = getString(group, "body");
			issue.milestone = getString(group, "milestone");
			issue.assignees = getStringList(group, "assignees");

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
	 * Filters a list of issues based on the search query and selected fields.
	 */
	private List<IssueDto> filterAndSortIssues(List<IssueDto> allIssues, String query, List<String> fields,
			boolean includeExtended, String state) {
		allIssues.sort(Comparator.comparing(issue -> issue.createdAt, Comparator.nullsLast(Comparator.reverseOrder())));

		final List<IssueDto> stateFilteredIssues;
		if (state != null && !state.equalsIgnoreCase("all") && !state.isEmpty()) {
			stateFilteredIssues = allIssues.stream().filter(issue -> state.equalsIgnoreCase(issue.state))
					.collect(Collectors.toList());
		} else {
			stateFilteredIssues = allIssues;
		}

		if (query == null || query.trim().isEmpty()) {
			return stateFilteredIssues;
		}

		List<String> quotedTerms = new ArrayList<>();
		Pattern pattern = Pattern.compile("\"([^\"]*)\"");
		Matcher matcher = pattern.matcher(query);
		while (matcher.find()) {
			quotedTerms.add(matcher.group(1).toLowerCase());
		}

		String unquotedQuery = matcher.replaceAll("").toLowerCase();
		final boolean isOrLogic = unquotedQuery.contains(",");
		List<String> regularTerms = Stream.of(isOrLogic ? unquotedQuery.split(",") : unquotedQuery.split("\\s+"))
				.map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList());

		if (quotedTerms.isEmpty() && regularTerms.isEmpty()) {
			return stateFilteredIssues;
		}

		return stateFilteredIssues.stream().filter(issue -> {
			StringBuilder searchableContent = new StringBuilder();
			if (fields.contains("title") && issue.title != null)
				searchableContent.append(issue.title).append(" ");
			if (fields.contains("short_summary") && issue.shortSummary != null)
				searchableContent.append(issue.shortSummary).append(" ");
			if (fields.contains("user") && issue.user != null)
				searchableContent.append(issue.user).append(" ");
			if (fields.contains("labels") && issue.labels != null)
				searchableContent.append(String.join(" ", issue.labels)).append(" ");
			if (fields.contains("llm_categories") && issue.llmCategories != null)
				searchableContent.append(String.join(" ", issue.llmCategories)).append(" ");
			if (fields.contains("milestone") && issue.milestone != null)
				searchableContent.append(issue.milestone).append(" ");
			if (fields.contains("assignees") && issue.assignees != null)
				searchableContent.append(String.join(" ", issue.assignees)).append(" ");
			if (includeExtended) {
				if (issue.body != null)
					searchableContent.append(issue.body).append(" ");
				if (issue.comments != null) {
					issue.comments
							.forEach(c -> searchableContent.append(c.user).append(" ").append(c.body).append(" "));
				}
			}
			String finalContent = searchableContent.toString().toLowerCase();

			boolean matchesQuoted = quotedTerms.stream().allMatch(finalContent::contains);
			if (!matchesQuoted) {
				return false;
			}

			if (regularTerms.isEmpty()) {
				return true;
			}

			if (isOrLogic) {
				return regularTerms.stream().anyMatch(finalContent::contains);
			} else {
				return regularTerms.stream().allMatch(finalContent::contains);
			}
		}).collect(Collectors.toList());
	}
}