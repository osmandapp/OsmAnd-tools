package net.osmand.server.api.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "dataset")
public class Dataset {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String name;

    @Column(nullable = false)
    private String type; // e.g., "Overpass", "CSV"

    @Column(nullable = false, columnDefinition = "TEXT")
    private String source; // Overpass query or file path

    @Column(columnDefinition = "TEXT")
    private String addressExpression; // SQL expression to calculate address

    @Column(columnDefinition = "TEXT")
    private String columns; // CSV column names

    @Column(columnDefinition = "TEXT")
    private String error;

    @Column(nullable = false)
    private String sourceStatus = "NEW";

    @Column(nullable = true)
    private Integer sizeLimit = 10000;

    @Column(nullable = false, updatable = false)
    private LocalDateTime created = LocalDateTime.now();

    @Column(nullable = false)
    private LocalDateTime updated = LocalDateTime.now();

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
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

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getSourceStatus() {
        return sourceStatus;
    }

    public void setSourceStatus(String status) {
        if (DatasetType.OK.name().equals(sourceStatus)) {
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
