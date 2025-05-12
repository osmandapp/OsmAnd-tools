package net.osmand.server.controllers.user;

import net.osmand.server.api.repo.OrderInfoRepository;
import net.osmand.server.api.repo.PremiumUsersRepository;
import net.osmand.server.api.services.AdminService;
import net.osmand.server.api.services.OrderManagementService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.List;

@Controller
@RequestMapping("/admin/order-mgmt")
public class OrderManagementController {

	@Autowired
	OrderManagementService orderManagementService;

	@Autowired
	private PremiumUsersRepository usersRepository;

	@GetMapping("/")
	public String orderManagementPage() {
		return "admin/order-management";
	}

	@GetMapping(value = "/orders", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
	public List<AdminService.Purchase> orders(
			@RequestParam(name = "text", required = false) String text,
			@RequestParam(name = "limit", defaultValue = "25") int limit) {
		if (StringUtils.isBlank(text)) {
			return Collections.emptyList();
		}
		return orderManagementService.searchPurchases(text, limit);
	}

	@GetMapping("/skus")
	@ResponseBody
	public List<String> topSkus() {
		return orderManagementService.getTopSkus(30);
	}

	@PostMapping("/orders/register")
	@ResponseBody
	public ResponseEntity<?> registerOrder(
			@RequestParam String email,
			@RequestParam String sku,
			@RequestParam(required = false) Integer period,
			@RequestParam(required = false) String interval,
			@RequestParam String orderId,
			@RequestParam String purchaseToken) {

		if (usersRepository.findByEmailIgnoreCase(email) == null) {
			return ResponseEntity
					.status(HttpStatus.BAD_REQUEST)
					.body("User with email “" + email + "” not found");
		}

		if (orderManagementService.orderWithSkuExists(orderId, sku)) {
			return ResponseEntity
					.status(HttpStatus.BAD_REQUEST)
					.body("Order ID “" + orderId + "” already exists");
		}

		try {
			orderManagementService.registerNewOrder(email, sku, period, interval, orderId, purchaseToken);
			List<AdminService.Purchase> pList = orderManagementService.findPurchaseByOrderAndSku(orderId, sku);
			if (pList.isEmpty()) {
				return ResponseEntity
						.status(HttpStatus.BAD_REQUEST)
						.body("Order ID “" + orderId + "” not found");
			}
			return ResponseEntity.ok(pList);
		} catch (IllegalArgumentException e) {
			return ResponseEntity
					.status(HttpStatus.BAD_REQUEST)
					.body(e.getMessage());
		}
	}

	@GetMapping("/orders/versions")
	@ResponseBody
	public List<OrderInfoRepository.OrderInfoDto> versions(@RequestParam String sku, @RequestParam String orderId) {
		return orderManagementService.listOrderVersions(sku, orderId);
	}

	@PostMapping("/orders/versions/create")
	@ResponseBody
	public ResponseEntity<String> create(@RequestParam String sku,
	                                     @RequestParam String orderId,
	                                     @RequestBody String info) {
		boolean create = orderManagementService.saveNewOrderVersion(sku, orderId, info);
		if (!create) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Order version already exists");
		}
		return ResponseEntity.ok("Order version created");
	}
}
