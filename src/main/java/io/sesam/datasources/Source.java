package io.sesam.datasources;

import java.util.List;

public interface Source {

    public List<String> getPrimaryKeys();
    
    public String getQuery(String since);

    public String getUpdatedColumn();
    
}
