package io.sesam.datasources;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.stream.JsonWriter;
import com.zaxxer.hikari.HikariDataSource;

public class DataSystem implements AutoCloseable {

    private final HikariDataSource ds;
    private final Map<String, Source> sources;

    public DataSystem(HikariDataSource ds, Map<String,Source> sources) {
        this.ds = ds;
        this.sources = sources;
    }
    
    @Override
    public void close() throws Exception {
        ds.close();
    }

    public void writeEntities(JsonWriter jw, String sourceId, String since) throws SQLException, IOException {
        Source source = this.sources.get(sourceId);
        if (source == null) {
            throw new RuntimeException("Unknown source: " + sourceId);
        }
        String query = source.getQuery(since);
        Connection conn = ds.getConnection();
        try {
            PreparedStatement stmt = conn.prepareStatement(query);
            try {
                ResultSet rs = stmt.executeQuery();
                try {
                    ResultSetMetaData rsmd = rs.getMetaData();
                    Map<String,Integer> colIndexes = new HashMap<>();
                    int columnCount = rsmd.getColumnCount();
                    String[] colNames = new String[columnCount];
                    int[] colTypes = new int[columnCount];
                    for(int i=0; i < columnCount; i++) {
                        colNames[i]  = rsmd.getColumnName(i+1);
                        colTypes[i]  = rsmd.getColumnType(i+1);
                        colIndexes.put(colNames[i], i+1);
                    }
                    
                    List<String> primaryKeys = source.getPrimaryKeys();
                    int[] pkIndexes = new int[primaryKeys.size()];
                    for (int i=0; i < pkIndexes.length; i++) {
                        String primaryKey = primaryKeys.get(i);
                        Integer ix = colIndexes.get(primaryKey);
                        if (ix == null) {
                            throw new RuntimeException("Not able to find primary-key: " + primaryKey);
                        }
                        pkIndexes[i] = ix;
                    }
                    
                    int updatedIndex = 0;
                    String updatedColumn = source.getUpdatedColumn();
                    if (updatedColumn != null) {
                        if (!colIndexes.containsKey(updatedColumn)) {
                            throw new RuntimeException("Not able to find updated-column: " + updatedColumn);
                        }
                        updatedIndex = colIndexes.get(updatedColumn);
                    }
                    while (rs.next()) {
                        jw.beginObject();
                        
                        jw.name("_id");
                        StringWriter sw = new StringWriter();
                        for (int i=0; i < pkIndexes.length; i++) {
                            sw.append(rs.getString(pkIndexes[i]));
                        }
                        jw.value(sw.toString());
                        
                        if (updatedIndex > 0) {
                            jw.name("_updated");
                            jw.value(rs.getString(updatedIndex));
                        }
                        writeRow(jw, colNames, colTypes, rs);
                        jw.endObject();
                    }
                } finally {
                    rs.close();
                }
            } finally {
                stmt.close();
            }
        } finally {
            conn.close();;
        }
    }


    private void writeRow(JsonWriter jw, String[] colNames, int[] colTypes, ResultSet rs) throws SQLException, IOException {
        for(int i=1; i < colNames.length+1; i++) {
            jw.name(colNames[i-1]);
            switch (colTypes[i-1]) {
            case java.sql.Types.ARRAY:
//              Array array = rs.getArray(i);
//              array.getResultSet();
//              obj.addProperty(columnName, array);
                break;
            case java.sql.Types.BIGINT:
                jw.value((Number)rs.getObject(i)); 
                break;
            case java.sql.Types.BINARY:
                break;
            case java.sql.Types.BIT:
                jw.value(rs.getBoolean(i)); 
                break;
            case java.sql.Types.BLOB:
//              Blob blob = rs.getBlob(i);
//              String value2 = null;
//              jw.value(value2); 
              break;
            case java.sql.Types.BOOLEAN:
                jw.value(rs.getBoolean(i)); 
                break;
            case java.sql.Types.CHAR:
                break;
            case java.sql.Types.CLOB:
                break;
            case java.sql.Types.DATALINK:
                break;
            case java.sql.Types.DATE:
                Date date = rs.getDate(i);
                String value1 = "~t" + DateTimeFormatter.ISO_LOCAL_DATE.format(date.toLocalDate());
                jw.value(value1); 
                break;
            case java.sql.Types.DECIMAL:
                jw.value(rs.getBigDecimal(i)); 
                break;
            case java.sql.Types.DISTINCT:
                break;
            case java.sql.Types.DOUBLE:
                jw.value(rs.getDouble(i)); 
                break;
            case java.sql.Types.FLOAT:
                jw.value(rs.getFloat(i)); 
                break;
            case java.sql.Types.INTEGER:
                jw.value(rs.getInt(i)); 
                break;
            case java.sql.Types.JAVA_OBJECT:
                break;
            case java.sql.Types.LONGNVARCHAR:
                break;
            case java.sql.Types.LONGVARBINARY:
                break;
            case java.sql.Types.LONGVARCHAR:
                break;
            case java.sql.Types.NCHAR:
                jw.value(rs.getNString(i)); 
                break;
            case java.sql.Types.NULL:
                jw.nullValue(); 
                break;
            case java.sql.Types.NUMERIC:
                jw.value(rs.getBigDecimal(i)); 
                break;
            case java.sql.Types.NVARCHAR:
                jw.value(rs.getNString(i)); 
                break;
            case java.sql.Types.OTHER:
                break;
            case java.sql.Types.REAL:
                break;
            case java.sql.Types.REF:
                break;
            case java.sql.Types.REF_CURSOR:
                break;
            case java.sql.Types.ROWID:
                break;
            case java.sql.Types.SMALLINT:
                jw.value(rs.getInt(i)); 
                break;
            case java.sql.Types.SQLXML:
                jw.value(rs.getString(i)); 
                break;
            case java.sql.Types.STRUCT:
                break;
            case java.sql.Types.TIME:
                break;
            case java.sql.Types.TIME_WITH_TIMEZONE:
                break;
            case java.sql.Types.TIMESTAMP:
                Timestamp timestamp;
                timestamp = rs.getTimestamp(i);
                String value = "~t" + DateTimeFormatter.ISO_INSTANT.format(timestamp.toInstant());
                jw.value(value); 
                break;
            case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:
                timestamp = rs.getTimestamp(i);
                value = "~t" + DateTimeFormatter.ISO_INSTANT.format(timestamp.toInstant());
                jw.value(value); 
                break;
            case java.sql.Types.TINYINT:
                jw.value(rs.getInt(i)); 
                break;
             case java.sql.Types.VARBINARY:
                jw.value(rs.getString(i)); 
                break;
             case java.sql.Types.VARCHAR:
                jw.value(rs.getString(i)); 
                break;
            default:
                throw new SQLException("Unsupported column type: " + colTypes[i-1]);
            }
        }
    }

    public boolean isValidSource(String sourceId) {
        return sources.containsKey(sourceId);
    }

}
