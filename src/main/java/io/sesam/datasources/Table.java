package io.sesam.datasources;

import java.util.List;

public class Table implements Source {

    private final String queryFull;
    private final String queryInc;
    private final List<String> primaryKeys;
    private final String updatedColumn;

    public Table(String tableName, List<String> primaryKeys, String updatedColumn) {
        this.queryFull = "select * from " + tableName;
        if (updatedColumn != null) {
            this.queryInc = queryFull + " where " + updatedColumn + " > ?";
        } else {
            this.queryInc = queryFull;
        }
        this.primaryKeys = primaryKeys;
        this.updatedColumn = updatedColumn;
    }

    @Override
    public String getQuery(String since) {
        if (since != null) {
            return queryInc;
        } else {
            return queryFull;
        }
    }
    
    @Override
    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    @Override
    public String getUpdatedColumn() {
        return updatedColumn;
    }

}
