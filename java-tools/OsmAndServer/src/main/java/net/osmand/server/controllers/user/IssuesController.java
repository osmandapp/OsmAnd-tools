package net.osmand.server.controllers.user;

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
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Controller to serve GitHub issue data to the frontend UI. This
 * controller reads data from Parquet files and provides endpoints for
 * searching, filtering, and retrieving issue details.
 */
@Controller
@RequestMapping("/admin/issues/")
public class IssuesController {

	@Value("${osmand.web.location}")
	private String websiteLocation;

	private static final String ISSUES_FOLDER = "servers/github_issues/issues";
	private static final String CATEGORIES_FILE = "categories.parquet";
	private static final String ISSUES_FILE = "osmandapp_issues.parquet";
	private static final long CACHE_TTL_MS = TimeUnit.MINUTES.toMillis(5);

	private volatile List<IssueDto> issuesCache;
	private volatile long lastCacheRefresh = 0;


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
		public String repository;
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
				.map(file -> new IssueFileDto(file.getName(), file.length(), new Date(file.lastModified())))
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
			return cat;
		}));
		issuesCache = new ArrayList<>(categoriesData.values());
		lastCacheRefresh = System.currentTimeMillis();
	}


	@GetMapping("/search")
	public ResponseEntity<List<IssueDto>> getIssues(@RequestParam(required = false) String query,
													@RequestParam(required = false, defaultValue = "title,short_summary,labels,llm_categories,user") List<String> fields,
													@RequestParam(defaultValue = "100") int limit,
													@RequestParam(defaultValue = "false") boolean includeExtended) {
		try {
			refreshCache();
			List<IssueDto> filteredIssues = filterAndSortIssues(issuesCache, query, fields,
					includeExtended);
			return ResponseEntity.ok(filteredIssues.stream().limit(limit).collect(Collectors.toList()));
		} catch (IOException e) {
			e.printStackTrace();
			return ResponseEntity.status(500).build();
		}
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
			issue.repository = getString(group, "repository");
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
			System.err.println("File not found: " + path);
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
            if (hasField(listGroup, "list") ) {
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
											   boolean includeExtended) {
		allIssues.sort(Comparator.comparing(issue -> issue.createdAt, Comparator.nullsLast(Comparator.reverseOrder())));
		if (query == null || query.trim().isEmpty()) {
			return allIssues;
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
			return allIssues;
		}

		return allIssues.stream().filter(issue -> {
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