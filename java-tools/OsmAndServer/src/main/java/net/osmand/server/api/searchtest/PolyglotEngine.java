package net.osmand.server.api.searchtest;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.osmand.data.LatLon;
import net.osmand.server.api.searchtest.repo.SearchTestCaseRepository.TestCase;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.PolyglotException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

@Component
public class PolyglotEngine {
	protected static final Logger LOGGER = LoggerFactory.getLogger(PolyglotEngine.class);

	@Autowired
	private ObjectMapper objectMapper;
	private Engine polyglotEngine;

	public PolyglotEngine() {
		// Initialize GraalVM Polyglot Engine and redirect logs.
		// We disable the interpreter-only warning and send Truffle logs to a temp file.
		try {
			Path logPath = Path.of(System.getProperty("java.io.tmpdir"), "osmand-polyglot.log");
			try {
				Files.createDirectories(logPath.getParent());
			} catch (IOException ignore) {
				// Directory usually exists; ignore if creation fails, Engine will still initialize.
			}
			this.polyglotEngine = Engine.newBuilder().option("engine.WarnInterpreterOnly", "false").option("log.file",
					logPath.toString()).build();
			LOGGER.info("Initialized GraalVM Polyglot Engine. Truffle logs -> {}", logPath.toAbsolutePath());
		} catch (Throwable t) {
			// Fall back to an Engine that only disables the warning, if log redirection fails.
			LOGGER.warn("Failed to initialize Polyglot Engine logging; continuing without log.file redirection", t);
			this.polyglotEngine = Engine.newBuilder().option("engine.WarnInterpreterOnly", "false").build();
		}
	}

	private static LatLon parseLatLon(String lat, String lon) {
		if (lat == null || lon == null) {
			return null;
		}
		try {
			return new LatLon(Double.parseDouble(lat), Double.parseDouble(lon));
		} catch (Exception e) {
			return null;
		}
	}

	public List<GenRow> execute(String script, TestCase test, List<String> delCols,
								List<Map<String, Object>> rows) throws Exception {
		String columnsJson = test.selCols;
		String[] selectParams = objectMapper.readValue(test.selectParams, String[].class);
		String[] whereParams = objectMapper.readValue(test.whereParams, String[].class);

		if (script == null || script.trim().isEmpty()) {
			throw new IllegalArgumentException("Script must not be null or empty.");
		}
		if (test.selectFun == null || test.selectFun.trim().isEmpty()) {
			throw new IllegalArgumentException("Function name must not be null or empty.");
		}
		try {
			// Execute with GraalVM JavaScript (Polyglot API)
			List<GenRow> results = new ArrayList<>();
			try (Context context = Context.newBuilder("js").engine(polyglotEngine).option("js.ecmascript-version",
					"2022").allowAllAccess(false).build()) {
				// 1) Evaluate user script (should define the target function)
				context.eval("js", script);

				// 2) Prepare JS-native arguments
				for (Map<String, Object> origRow : rows) {
					long start = System.currentTimeMillis();
					Map<String, Object> row = origRow;
					if (!delCols.isEmpty()) {
						row = new HashMap<>(origRow);
						for (String key : delCols) {
							row.remove(key);
						}
					}

					String rowJson = objectMapper.writeValueAsString(row);
					org.graalvm.polyglot.Value jsonParse = context.eval("js", "JSON.parse");
					org.graalvm.polyglot.Value jsRow = jsonParse.execute(rowJson);
					org.graalvm.polyglot.Value jsColumns = jsonParse.execute(columnsJson);

					List<Object> selectArgs = getArgs(selectParams);
					selectArgs.add(0, jsColumns);
					selectArgs.add(0, jsRow);

					String lat = (String) origRow.get("lat");
					String lon = (String) origRow.get("lon");
					boolean where = true;
					String errorMessage = null;
					if (test.whereFun != null && !test.whereFun.trim().isEmpty()) {
						List<Object> whereArgs = getArgs(whereParams);
						whereArgs.add(0, jsColumns);
						whereArgs.add(0, jsRow);
						try {
							where = (Boolean) execute(context, test.whereFun, whereArgs);
						} catch (PolyglotException pe) {
							// Capture JS error from where() and continue processing this row
							errorMessage = extractJsErrorMessage(pe);
						}
					}

					int count = -1;
					String[] output = null;
					if (where) {
						try {
							output = (String[]) execute(context, test.selectFun, selectArgs);
							count = output.length;
						} catch (PolyglotException pe) {
							// Capture JS error from select() and persist a failed record with empty query
							errorMessage = extractJsErrorMessage(pe);
						}
					}
					String outputJson = output == null ? null : objectMapper.writeValueAsString(output);
					results.add(new GenRow(parseLatLon(lat, lon), origRow, outputJson, count, errorMessage,
							System.currentTimeMillis() - start));
				}
				return results;
			}
		} catch (PolyglotException e) {
			throw new RuntimeException("JavaScript execution error: " + (e.getMessage() == null ? e.toString() :
					e.getMessage()), e);
		} catch (IOException e) {
			throw new RuntimeException("Failed to serialize parameters to JSON: " + (e.getMessage() == null ?
					e.toString() : e.getMessage()), e);
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException("Failed to execute script function '" + test.selectFun + "': " +
					(e.getMessage() == null ? e.toString() : e.getMessage()), e);
		}
	}

	private List<Object> getArgs(String[] selectParams) {
		List<Object> args = new ArrayList<>();
		if (selectParams != null) {
			for (String p : new ArrayList<>(Arrays.asList(selectParams)).subList(Math.min(2, selectParams.length),
					selectParams.length)) {
				if (p == null) {
					args.add(null);
					continue;
				}
				String t = p.trim();
				// Convert to number if it looks numeric; otherwise pass as string
				if (t.matches("^-?\\d+$")) {
					try {
						args.add(Long.parseLong(t));
					} catch (NumberFormatException ex) {
						args.add(t);
					}
				} else if (t.matches("^-?\\d*\\.\\d+(?:[eE]-?\\d+)?$")) {
					try {
						args.add(Double.parseDouble(t));
					} catch (NumberFormatException ex) {
						args.add(t);
					}
				} else {
					args.add(p);
				}
			}
		}
		return args;
	}

	private Object execute(Context context, String functionName, List<Object> args) throws IOException {
		// 3) Resolve and invoke the target function
		org.graalvm.polyglot.Value fn = context.getBindings("js").getMember(functionName);
		if (fn == null || !fn.canExecute()) {
			throw new IllegalArgumentException("Function not found or not executable: " + functionName);
		}
		org.graalvm.polyglot.Value result = fn.execute(args.toArray());
		if (result.isBoolean()) {
			return result.asBoolean();
		}

		if (result.hasArrayElements()) {
			org.graalvm.polyglot.Value stringify = context.eval("js", "JSON.stringify");
			return stringify.execute(result).asString();
		}
		return new String[]{result.asString()};
	}

	/**
	 * Extracts a concise error message from a PolyglotException thrown by guest JS code.
	 * If the guest object has a 'message' property, it is preferred; otherwise, the first line
	 * of the exception message is returned.
	 */
	private String extractJsErrorMessage(PolyglotException pe) {
		try {
			if (pe.isGuestException()) {
				org.graalvm.polyglot.Value guest = pe.getGuestObject();
				if (guest != null && guest.hasMembers()) {
					org.graalvm.polyglot.Value msg = guest.getMember("message");
					if (msg != null && msg.isString()) {
						return msg.asString();
					}
				}
			}
			String raw = pe.getMessage();
			if (raw == null) {
				return pe.toString();
			}
			int nl = raw.indexOf('\n');
			return nl > 0 ? raw.substring(0, nl) : raw;
		} catch (Throwable t) {
			return pe.getMessage() == null ? pe.toString() : pe.getMessage();
		}
	}

	public record GenRow(LatLon point, Map<String, Object> row, String output, int count, String error,
						 long duration) {
	}
}
