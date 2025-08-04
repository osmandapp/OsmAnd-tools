package net.osmand.server.api.test.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "dataset")
public class Dataset {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    @Column(nullable = false, unique = true)
    private String name;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private DatasetSource type; // e.g., "Overpass", "CSV"

    @Column(nullable = false, columnDefinition = "TEXT")
    private String source; // Overpass query or file path

    @Column(columnDefinition = "TEXT")
    private String addressExpression; // SQL expression to calculate address

    @Column(columnDefinition = "TEXT")
    private String columns; // CSV column names

    @Column(columnDefinition = "TEXT")
    private String error;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private DatasetConfigStatus sourceStatus = DatasetConfigStatus.UNKNOWN;

    @Column
    private Integer sizeLimit = 10000;

    @Column
    private Integer total;

    @Column(nullable = false, updatable = false)
    private LocalDateTime created = LocalDateTime.now();

    @Column(nullable = false)
    private LocalDateTime updated = LocalDateTime.now();

    public String getError() {
        return error;
    }

    public void setError(String error) {
        if (error != null) {
            sourceStatus = DatasetConfigStatus.ERROR;
        }
        this.error = error;
    }

    public Integer getTotal() {
        return total;
    }

    public void setTotal(Integer total) {
        this.total = total;
    }

    public String getAddressExpression() {
        return addressExpression;
    }

    public void setAddressExpression(String addressExpression) {
        this.addressExpression = addressExpression;
    }

    // Getters and Setters
    public String getColumns() {
        return columns;
    }

    public void setColumns(String columns) {
        this.columns = columns;
    }

    public Integer getSizeLimit() {
        return sizeLimit;
    }

    public void setSizeLimit(Integer sizeLimit) {
        this.sizeLimit = sizeLimit;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public DatasetSource getType() {
        return type;
    }

    public void setType(DatasetSource type) {
        this.type = type;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public DatasetConfigStatus getSourceStatus() {
        return sourceStatus;
    }

    public void setSourceStatus(DatasetConfigStatus status) {
        if (DatasetConfigStatus.OK.equals(sourceStatus)) {
            error = null;
        }
        this.sourceStatus = status;
    }

    public LocalDateTime getCreated() {
        return created;
    }

    public void setCreated(LocalDateTime created) {
        this.created = created;
    }

    public LocalDateTime getUpdated() {
        return updated;
    }

    public void setUpdated(LocalDateTime updated) {
        this.updated = updated;
    }
}
