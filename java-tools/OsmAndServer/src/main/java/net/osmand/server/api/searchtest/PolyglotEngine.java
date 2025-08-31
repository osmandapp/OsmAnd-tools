package net.osmand.server.api.searchtest;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.osmand.data.LatLon;
import net.osmand.server.api.searchtest.repo.SearchTestCaseRepository.TestCase;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Value;
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

	public List<GenRow> execute(String dir, TestCase test, List<String> delCols,
								List<Map<String, Object>> rows) throws Exception {
		BaseService.ProgrammaticConfig program = objectMapper.readValue(test.progCfg, BaseService.ProgrammaticConfig.class);

		// Load default JS helper script from web-server-config repository
		Path scriptPath = Path.of(dir, "js", "search-test", "modules", "lib", program == null ? "no-code.js" : "main.js");
		if (!Files.exists(scriptPath)) {
			throw new RuntimeException("Script file not found: " + scriptPath.toAbsolutePath());
		}
		String script = Files.readString(scriptPath);

		String columnsJson = test.selCols;
		try {
			// Execute with GraalVM JavaScript (Polyglot API)
			List<GenRow> results = new ArrayList<>();
			try (Context context = Context.newBuilder("js").engine(polyglotEngine).option("js.ecmascript-version",
					"2022").allowAllAccess(false).build()) {
				// 1) Evaluate user script (should define the target function)
				context.eval("js", script);
				Value jsonParse = context.eval("js", "JSON.parse");

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
					Value jsRow = jsonParse.execute(rowJson);

					String lat = (String) origRow.get("lat");
					String lon = (String) origRow.get("lon");

					Object output = null;
					String errorMessage = null;
					try {
						output = program != null ?
							 executeProgram(context, program, jsRow, jsonParse.execute(columnsJson)) :
								executeNocode(context, jsonParse.execute(test.progCfg), jsRow);
					} catch (PolyglotException pe) {
						errorMessage = extractJsErrorMessage(pe);
					}

					int count = output instanceof String[] ? ((String[])output).length : -1;
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
			throw new RuntimeException("Failed to execute script function '" + program.selectFun() + "': " +
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

	private Object executeNocode(Context context, Value jsCfg, Value jsRow) throws PolyglotException, IOException {
		List<Object> args = new ArrayList<>();
		args.add(0, jsCfg);
		args.add(0, jsRow);

		return execute(context, "evalNocode", args);
	}

	private Object executeProgram(Context context, BaseService.ProgrammaticConfig program, Value jsRow, Value jsColumns) throws PolyglotException, IOException {
		if (program.selectFun() == null) {
			throw new IllegalArgumentException("Function name must not be null or empty.");
		}
		String[] selectParams = program.selectParamValues();
		String[] whereParams = program.whereParamValues();
		List<Object> selectArgs = getArgs(selectParams);
		selectArgs.add(0, jsColumns);
		selectArgs.add(0, jsRow);

		boolean where = true;
		if (program.whereFun() != null) {
			List<Object> whereArgs = getArgs(whereParams);
			whereArgs.add(0, jsColumns);
			whereArgs.add(0, jsRow);
			where = (Boolean) execute(context, program.whereFun(), whereArgs);
		}
		return where ? execute(context, program.selectFun(), selectArgs) : null;
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
			return result.as(String[].class);
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
