package net.osmand.server.index;

public class RegionIndex extends AbstractIndex {

    private final IndexAttributes attributes = new IndexAttributes();

    public RegionIndex() {
        super(IndexNames.REGION);
    }

    @Override
    public String getIndexName() {
        return indexName;
    }

    @Override
    public IndexAttributes getAttributes() {
        return this.attributes;
    }
}
