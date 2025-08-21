package net.osmand.server.controllers.user;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import net.osmand.server.api.services.AdminService;
import net.osmand.server.api.services.AdminService.Purchase;
import net.osmand.server.api.services.OrderManagementService;

@RestController
@RequestMapping("/admin/mcp")
public class McpDiscoveryController {

	@Autowired
	OrderManagementService orderManagementService;

	@RequestMapping("/tools")
	public ResponseEntity<String> getTools() {
		String json = """
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "tools": [
      {
        "name": "get-osmand-orders",
        "description": "Fetch list of orders from the system by email",
        "input_schema": {
          "type": "object",
          "properties": {
            "email": {
              "type": "string",
              "description": "Email address or user name"
            }
          },
          "required": ["email"]
        }
      }
    ]
  }
}
				""";

		return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(json);
	}

	@RequestMapping(value = "/tools/get-osmand-orders", produces = MediaType.APPLICATION_JSON_VALUE)
	public List<AdminService.Purchase> callTool(@RequestParam(name = "text", required = true) String user) {
		List<AdminService.Purchase> purchases = orderManagementService.searchPurchases(user, 25);
		for (Purchase p : purchases) {
			if (p.purchaseToken != null && p.purchaseToken.length() > 50) {
				p.purchaseToken = p.purchaseToken.substring(0, 50) + "...";
			}
		}

		return purchases;
	}
}