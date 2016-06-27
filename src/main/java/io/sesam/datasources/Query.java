package io.sesam.datasources;

import java.util.List;

public class Query implements Source {

    private final String query;
    private final List<String> primaryKeys;
    private final String updatedColumn;

    public Query(String query, List<String> primaryKeys, String updatedColumn) {
        this.query = query;
        this.primaryKeys = primaryKeys;
        this.updatedColumn = updatedColumn;
    }
    
    @Override
    public String getQuery(String since) {
        return query;
    }

    @Override
    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public String getUpdatedColumn() {
        return updatedColumn;
    }

}
