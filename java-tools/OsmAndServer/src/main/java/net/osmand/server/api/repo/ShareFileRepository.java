package net.osmand.server.api.repo;

import lombok.Getter;
import lombok.Setter;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.data.jpa.repository.Query;

import javax.persistence.*;
import java.io.Serial;
import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@Repository
public interface ShareFileRepository extends JpaRepository<ShareFileRepository.ShareFile, Long> {

	ShareFile findByUuid(String uuid);

	ShareFile findByOwneridAndFilepath(int ownerid, String filepath);

	@Query("SELECT a FROM ShareFilesAccess a WHERE a.id = :id")
	ShareFilesAccess findShareFilesAccessById(@Param("id") long id);

	<S extends ShareFilesAccess> S saveAndFlush(S entity);

	@Setter
	@Getter
	@Entity(name = "ShareFile")
	@Table(name = "user_share_files")
	class ShareFile implements Serializable {

		@Serial
		private static final long serialVersionUID = 1L;

		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		@Column(name = "file_id")
		public long id;

		@Column(nullable = false)
		public int ownerid;

		@Column(unique = true)
		private String uuid;

		@Column(nullable = false)
		public String filepath;

		@Column(nullable = false)
		public String name;

		@Column(nullable = false)
		public String type;

		@Column(nullable = false)
		public boolean publicAccess;

		@OneToMany(mappedBy = "file", cascade = CascadeType.ALL, orphanRemoval = true)
		private List<ShareFilesAccess> accessRecords;

		public void addAccessRecord(ShareFilesAccess access) {
			accessRecords.add(access);
			access.setFile(this);
		}
	}

	@Setter
	@Getter
	@Entity(name = "ShareFilesAccess")
	@Table(name = "user_share_files_access")
	class ShareFilesAccess implements Serializable {

		@Serial
		private static final long serialVersionUID = 1L;

		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		private long id;

		@ManyToOne
		@JoinColumn(name = "user_id", nullable = false)
		private PremiumUsersRepository.PremiumUser user;

		@Column(nullable = false)
		private String access;

		@Column(name = "date")
		@Temporal(TemporalType.TIMESTAMP)
		public Date requestDate;

		@ManyToOne
		@JoinColumn(name = "file_id", nullable = false)
		private ShareFile file;
	}

	@Getter
	@Setter
	public class ShareFileDTO {

		private long id;
		private int ownerid;
		private String uuid;
		private String filepath;
		private String name;
		private String type;
		private boolean publicAccess;
		private List<ShareFilesAccessDTO> accessRecords;

		public ShareFileDTO(ShareFile shareFile, boolean includeAccessRecords) {
			this.id = shareFile.getId();
			this.ownerid = shareFile.getOwnerid();
			this.uuid = shareFile.getUuid();
			this.filepath = shareFile.getFilepath();
			this.name = shareFile.getName();
			this.type = shareFile.getType();
			this.publicAccess = shareFile.isPublicAccess();
			if (includeAccessRecords && shareFile.getAccessRecords() != null) {
				this.accessRecords = shareFile.getAccessRecords().stream()
						.map((ShareFilesAccess access) -> new ShareFilesAccessDTO(access, false))
						.collect(Collectors.toList());
			}
		}
	}

	@Getter
	@Setter
	class ShareFilesAccessDTO {

		private long id;
		private String name;
		private String access;
		private Date requestDate;

		public ShareFilesAccessDTO(ShareFilesAccess access, boolean includeFile) {
			this.id = access.getId();
			this.name = access.getUser().email;
			this.access = access.getAccess();
			this.requestDate = access.getRequestDate();
			if (includeFile) {
				this.id = access.getFile().getId();
			}
		}
	}
}