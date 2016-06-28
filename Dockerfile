FROM java:8-jre-alpine

ADD target/jdbc-datasource-template-1.0-SNAPSHOT.jar /srv/

ENTRYPOINT ["java", "-jar", "/srv/jdbc-datasource-template-1.0-SNAPSHOT.jar"]
CMD ["/config.json"]


