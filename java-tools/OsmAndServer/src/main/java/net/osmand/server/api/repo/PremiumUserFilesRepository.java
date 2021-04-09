package net.osmand.server.api.repo;

import java.util.Date;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import net.osmand.server.api.repo.PremiumUserFilesRepository.UserFile;

@Repository
public interface PremiumUserFilesRepository extends JpaRepository<UserFile, Long> {
	
	UserFile findTopByUseridAndNameAndTypeOrderByUpdatetimeDesc(int userid, String name, String type);
	
    @Entity(name = "UserFile")
    @Table(name = "user_files")
    class UserFile {

    	@Id
        @GeneratedValue(strategy = GenerationType.IDENTITY)
        public long id;
        
        @Column(name = "userid")
        public int userid;
        
        @Column(name = "deviceid")
        public int deviceid;

        @Column(name = "type")
        public String type;
        
        @Column(name = "name")
        public String name;

        @Column(name = "filesize")
        public int filesize;
        
        @Column(name = "updatetime")
        @Temporal(TemporalType.TIMESTAMP)
        public Date updatetime;
        
//      @Fetch(FetchMode.JOIN)
        @Column(name = "data", columnDefinition="bytea")
        public byte[] data;
//        @Lob
//        public Blob data;

    }

    
    // COALESCE(length(u.data), -1))
	@Query("select new net.osmand.server.api.repo.PremiumUserFilesRepository$UserFileNoData(u.id, u.userid, u.deviceid, u.type, u.name, u.updatetime, u.filesize, length(u.data) ) "
			+ " from UserFile u "
			+ " where u.userid = :userid  and (:name is null or u.name = :name) and (:type is null or u.type  = :type ) "
			+ " order by updatetime desc")
	List<UserFileNoData> listFilesByUserid(@Param(value = "userid") int userid, @Param(value = "name") String name, @Param(value = "type") String type);
	
	class UserFileNoData {
		public int userid;
		public long id;
        public int deviceid;
        public int filesize;
        public String type;
        public String name;
        public Date updatetime;
        public long updatetimems;
		public int zipSize;
		public UserFileNoData(long id, int userid, int deviceid, String type, String name, Date updatetime, int filesize, int zipSize) {
			this.userid = userid;
			this.id = id;
			this.deviceid = deviceid;
			this.type = type;
			this.name = name;
			this.updatetime = updatetime;
			this.filesize = filesize;
			this.zipSize = zipSize;
			this.updatetimems = updatetime == null ? 0 : updatetime.getTime(); 
		}
	}
	

}
