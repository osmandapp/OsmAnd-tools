package net.osmand.server.api.repo;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import jakarta.persistence.*;
import net.osmand.server.assist.data.JsonbType;
import org.hibernate.annotations.Type;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;
import java.util.Date;
import java.util.List;

@Repository
public interface OrderInfoRepository extends JpaRepository<OrderInfoRepository.OrderInfo, Long> {

	List<OrderInfo> findBySkuAndOrderIdOrderByUpdateTimeDesc(String sku, String orderId);

	@Entity(name = "OrderInfo")
	@Table(name = "order_info")
	class OrderInfo implements Serializable {
		private static final Gson gson = new Gson();

		@Serial
		private static final long serialVersionUID = 1L;

		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		public long id;

		@Column(name = "sku", nullable = false)
		public String sku;

		@Column(name = "orderid", nullable = false)
		public String orderId;

		@Column(name = "updatetime", nullable = false)
		public Date updateTime;

		@Column(name = "editorid", nullable = false)
		public String editorId;

		@Column(name = "details", columnDefinition = "jsonb")
		@Type(JsonbType.class)
		public JsonObject details;

		@Serial
		private void writeObject(java.io.ObjectOutputStream out) throws IOException {
			out.defaultWriteObject();
			out.writeObject(details != null ? gson.toJson(details) : null);
		}

		@Serial
		private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
			in.defaultReadObject();
			String json = (String) in.readObject();
			if (json != null) {
				this.details = gson.fromJson(json, JsonObject.class);
			}
		}
	}

	public class OrderInfoDto {
		public String sku;
		public String orderId;
		public Date updateTime;
		public String editorId;
		public String details;

		public OrderInfoDto() {
		}

		public OrderInfoDto(String sku, String orderId, Date updateTime, String editorId, String details) {
			this.sku = sku;
			this.orderId = orderId;
			this.updateTime = updateTime;
			this.editorId = editorId;
			this.details = details;
		}
	}
}
