package net.osmand.server.controllers.user;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.google.gson.Gson;

import net.osmand.server.api.services.AdminService;
import net.osmand.server.api.services.OrderManagementService;

@RestController
@RequestMapping("/admin/mcp/")
public class McpDiscoveryController {

	protected static final Log LOG = LogFactory.getLog(McpDiscoveryController.class);

	@Autowired
	OrderManagementService orderManagementService;
	Gson gson = new Gson();

	@PostMapping(produces = MediaType.APPLICATION_JSON_VALUE)
	public Map<String, Object> handleRpc(@RequestBody Map<String, Object> request) {
		String method = (String) request.get("method");
		Object id = request.get("id");
		System.out.println(request);
		if ("tools/list".equals(method)) {
			return Map.of("jsonrpc", "2.0", "id", id, "result",
					Map.of("tools", List.of(Map.of("name", "get-osmand-orders", "description",
							"Fetch list of orders from the system by email", "inputSchema", Map.of( 
									"type", "object", "properties",
									Map.of("query",
											Map.of("type", "string", "description", "Email address, transaction number or user name")),
									"required", List.of("email"))))));
		}
		if ("notifications/initialized".equals(method)) {
			return Map.of("jsonrpc", "2.0", "result", Map.of("status", "ok"));
		}
		if ("initialize".equals(method)) {
			return Map.of("jsonrpc", "2.0", "id", id, "result",
					Map.of("protocolVersion", "2025-06-18", "capabilities",
							Map.of("tools", Map.of("listChanged", true)), "serverInfo",
							Map.of("name", "osmand-mcp-server", "version", "0.0.1")));
		}
		if ("tools/call".equals(method)) {
			@SuppressWarnings("unchecked")
			Map<String, Object> params = (Map<String, Object>) request.get("params");
			@SuppressWarnings("unchecked")
			Map<String, Object> args = (Map<String, Object>) params.get("arguments");
			String toolName = (String) params.get("name");
			LOG.info(String.format("MCP tool call %s : %s", toolName, args));
			if ("get-osmand-orders".equals(toolName)) {
				String query = (String) args.get("query");

				List<AdminService.Purchase> purchases = orderManagementService.searchPurchases(query, 25);
				for (AdminService.Purchase p : purchases) {
					if (p.purchaseToken != null && p.purchaseToken.length() > 50) {
						p.purchaseToken = p.purchaseToken.substring(0, 50) + "...";
					}
				}
				return Map.of("jsonrpc", "2.0", "id", id, "result", Map.of("content", List.of(Map.of("type", "text", 
						"text", gson.toJson(purchases) 
				))));
			}
		}

		return Map.of("jsonrpc", "2.0", "id", id, "error",
				Map.of("code", -32601, "message", "Method not found: " + method));
	}

//	@RequestMapping(value = "/tools/get-osmand-orders", produces = MediaType.APPLICATION_JSON_VALUE)
//	public List<AdminService.Purchase> callTool(@RequestParam(name = "text", required = true) String user) {
//		List<AdminService.Purchase> purchases = orderManagementService.searchPurchases(user, 25);
//		for (Purchase p : purchases) {
//			if (p.purchaseToken != null && p.purchaseToken.length() > 50) {
//				p.purchaseToken = p.purchaseToken.substring(0, 50) + "...";
//			}
//		}
//
//		return purchases;
//	}
}