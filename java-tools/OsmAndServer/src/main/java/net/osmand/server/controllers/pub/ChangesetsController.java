package net.osmand.server.controllers.pub;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.gson.Gson;


@RestController
@RequestMapping("/changesets")
public class ChangesetsController {

	private Gson gson = new Gson();

	@Autowired
	private JdbcTemplate jdbcTemplate;

	protected class UserChangesResult {
		String user;
		Map<String, Integer> results = new LinkedHashMap<String, Integer>();
	}

	@GetMapping(value = "/user-changes")
	@ResponseBody
	public ResponseEntity<String> getUserChanges(@RequestParam(name = "name", required = true) String name)
			throws IOException, SQLException {
		UserChangesResult res = new UserChangesResult();
		res.user = name;
		jdbcTemplate.query(
				"select substr(closed_at_day, 0, 8), count(*) from changesets where username = '?' group by substr(closed_at_day, 0, 8) order by 1 desc",
				new String[] { name }, new RowCallbackHandler() {

					@Override
					public void processRow(ResultSet rs) throws SQLException {
						res.results.put(rs.getString(1), rs.getInt(2));
					}
				});

		return ResponseEntity.ok(gson.toJson(res));

	}

}