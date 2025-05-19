package net.osmand.server.api.repo;

import java.io.Serial;
import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import jakarta.persistence.Temporal;
import jakarta.persistence.TemporalType;

@Repository
public interface ShareFileRepository extends JpaRepository<ShareFileRepository.ShareFile, Long> {

	ShareFile findByUuid(UUID uuid);

	ShareFile findByOwneridAndFilepath(int ownerid, String filepath);

	List<ShareFile> findByOwnerid(int ownerid);

	@Query("SELECT a FROM ShareFilesAccess a WHERE a.id = :id")
	ShareFilesAccess findShareFilesAccessById(@Param("id") long id);

	@Query("SELECT a FROM ShareFilesAccess a WHERE a.user.id = :userId")
	List<ShareFilesAccess> findShareFilesAccessListByUserId(int userId);

	@Transactional
	@Modifying
	@Query("DELETE FROM ShareFilesAccess a WHERE a.file.id = :id AND a.user.id = :userId")
	void removeShareFilesAccessById(long id, int userId);

	<S extends ShareFilesAccess> S saveAndFlush(S entity);

	@Entity(name = "ShareFile")
	@Table(name = "user_share_files")
	class ShareFile implements Serializable {

		@Serial
		private static final long serialVersionUID = 1L;

		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		@Column(nullable = false)
		public long id;

		@Column(nullable = false)
		public int ownerid;

		@Column(unique = true)
		public UUID uuid;

		@Column(nullable = false)
		public String filepath;

		@Column(nullable = false)
		public String name;

		@Column(nullable = false)
		public String type;

		@Column(nullable = false)
		public boolean publicAccess;

		@OneToMany(mappedBy = "file", cascade = CascadeType.ALL, orphanRemoval = true)
		public List<ShareFilesAccess> accessRecords;

		public void addAccessRecord(ShareFilesAccess access) {
			accessRecords.add(access);
			access.file = (this);
		}

		public String getType() {
			return type;
		}

		public String getFilepath() {
			return filepath;
		}
	}

	@Entity(name = "ShareFilesAccess")
	@Table(name = "user_share_files_access")
	class ShareFilesAccess implements Serializable {

		@Serial
		private static final long serialVersionUID = 1L;

		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		public long id;

		@ManyToOne
		@JoinColumn(name = "user_id", nullable = false)
		public CloudUsersRepository.CloudUser user;

		@Column(nullable = false)
		public String access;

		@Column(name = "date")
		@Temporal(TemporalType.TIMESTAMP)
		public Date requestDate;

		@ManyToOne
		@JoinColumn(name = "file_id", nullable = false)
		public ShareFile file;

		public CloudUsersRepository.CloudUser getUser() {
			return user;
		}

		public long getId() {
			return id;
		}

		public String getAccess() {
			return access;
		}
	}

	public class ShareFileDTO {

		public long id;
		public int ownerid;
		public String uuid;
		public String filepath;
		public String name;
		public String type;
		public boolean publicAccess;
		public List<ShareFilesAccessDTO> accessRecords;

		public ShareFileDTO(ShareFile shareFile, boolean includeAccessRecords) {
			this.id = shareFile.id;
			this.ownerid = shareFile.ownerid;
			this.uuid = shareFile.uuid != null ? shareFile.uuid.toString() : null;
			this.filepath = shareFile.filepath;
			this.name = shareFile.name;
			this.type = shareFile.type;
			this.publicAccess = shareFile.publicAccess;
			if (includeAccessRecords && shareFile.accessRecords != null) {
				this.accessRecords = shareFile.accessRecords.stream()
						.map((ShareFilesAccess access) -> new ShareFilesAccessDTO(access, false))
						.collect(Collectors.toList());
			}
		}
	}

	class ShareFilesAccessDTO {

		public long id;
		public String name;
		public String access;
		public Date requestDate;

		public ShareFilesAccessDTO(ShareFilesAccess access, boolean includeFile) {
			this.id = access.getId();
			this.name = access.getUser().nickname;
			this.access = access.getAccess();
			this.requestDate = access.requestDate;
			if (includeFile) {
				this.id = access.file.id;
			}
		}
	}
}