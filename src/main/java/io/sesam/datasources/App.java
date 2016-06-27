package io.sesam.datasources;

import java.io.Writer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.stream.JsonWriter;

import spark.Spark;

public class App {

    static Logger log = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws Exception {
        Mapper mapper = Mapper.load(args[0]);
        
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                Spark.stop();
                mapper.close();
            }   
        }); 

        Spark.get("/:system/:table", (req, res) -> {
            res.type("application/json; charset=utf-8");
            String system = req.params("system");
            String table = req.params("table");
            String since = req.queryParams("since");
            
            Writer writer = res.raw().getWriter();
            JsonWriter jsonWriter = new JsonWriter(writer);
            try {
                mapper.writeEntities(jsonWriter, system, table, since);
            } finally {
                jsonWriter.flush();
            }
            return "";
        });
        
        Spark.exception(Exception.class, (exception, request, response) -> {
            log.error("Got exception", exception);
        });
    }

}