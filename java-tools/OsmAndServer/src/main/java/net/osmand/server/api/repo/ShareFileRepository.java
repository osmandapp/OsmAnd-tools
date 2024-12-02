package net.osmand.server.api.repo;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.Type;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import org.springframework.data.jpa.repository.Query;

import javax.persistence.*;
import java.io.IOException;
import java.io.Serial;
import java.io.Serializable;

@Repository
public interface ShareFileRepository extends JpaRepository<ShareFileRepository.ShareFile, Long> {

	ShareFile findByCode(String code);

	@Query("SELECT sf.info FROM ShareFile sf WHERE sf.code = :code")
	JsonObject findInfoByCode(String code);

	ShareFile findByUseridAndNameAndType(int userid, String name, String type);


	@Setter
	@Getter
	@Entity(name = "ShareFile")
	@Table(name = "share_files")
	class ShareFile implements Serializable {
		private static final Gson gson = new Gson();

		@Serial
		private static final long serialVersionUID = 1L;

		@Id
		@Column(nullable = false, unique = true)
		private String code;

		@Column(name = "name", nullable = false)
		public String name;

		@Column(name = "type", nullable = false)
		public String type;

		@Column(name = "userid", nullable = false)
		public int userid;

		@Column(name = "info", columnDefinition = "jsonb")
		@Type(type = "net.osmand.server.assist.data.JsonbType")
		private JsonObject info;

		@Serial
		private void writeObject(java.io.ObjectOutputStream out) throws IOException {
			out.defaultWriteObject();
			out.writeObject(info != null ? gson.toJson(info) : null);
		}

		@Serial
		private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
			in.defaultReadObject();
			String json = (String) in.readObject();
			if (json != null) {
				this.info = gson.fromJson(json, JsonObject.class);
			}
		}

	}
}