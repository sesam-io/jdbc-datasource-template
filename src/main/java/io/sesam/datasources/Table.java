package io.sesam.datasources;

import java.util.List;

public class Table implements Source {

    private final List<String> primaryKeys;
    private final String queryFull;
    private final String updatedColumn;

    public Table(String tableName, List<String> primaryKeys, String updatedColumn) {
        this.primaryKeys = primaryKeys;
        this.updatedColumn = updatedColumn;
        this.queryFull = "select * from " + tableName;
    }
    
    @Override
    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    @Override
    public String getQuery(String since) {
        return queryFull;
    }

    @Override
    public String getUpdatedColumn() {
        return updatedColumn;
    }

}
