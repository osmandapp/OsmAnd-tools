package net.osmand.server.api.repo;

import java.io.Serializable;
import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import net.osmand.server.api.repo.DataMissingSearchRepository.DataMissingSearchFeedback;
import net.osmand.server.api.repo.DataMissingSearchRepository.DataMissingSearchFeedbackKey;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DataMissingSearchRepository extends JpaRepository<DataMissingSearchFeedback, DataMissingSearchFeedbackKey> {

	@Entity
	@Table(name = "data_missing_search")
	@IdClass(DataMissingSearchFeedbackKey.class)
	public class DataMissingSearchFeedback {
	
		@Id
		@Column(nullable = false, length = 100)
		public String ip;
		
		@Column(nullable = false, length = 100)
		public String search;
		
		@Column(nullable = false, length = 100)
		public String location;
		
		@Id
		@Column(nullable = false)
		@Temporal(TemporalType.TIMESTAMP)
		public Date timestamp;
		
	}
	
	public class DataMissingSearchFeedbackKey implements Serializable {
		private static final long serialVersionUID = 1192225751597358995L;
		public String ip;
		public Date timestamp;
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((ip == null) ? 0 : ip.hashCode());
			result = prime * result + ((timestamp == null) ? 0 : timestamp.hashCode());
			return result;
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			DataMissingSearchFeedbackKey other = (DataMissingSearchFeedbackKey) obj;
			if (ip == null) {
				if (other.ip != null) {
					return false;
				}
			} else if (!ip.equals(other.ip)) {
				return false;
			}
			if (timestamp == null) {
				if (other.timestamp != null) {
					return false;
				}
			} else if (!timestamp.equals(other.timestamp)) {
				return false;
			}
			return true;
		}
		
		
	}


}
