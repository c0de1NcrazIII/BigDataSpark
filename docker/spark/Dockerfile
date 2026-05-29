FROM apache/spark:3.5.3

USER root

RUN curl -fsSL -o /opt/spark/jars/postgresql-42.7.3.jar \
        https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar \
    && curl -fsSL -o /opt/spark/jars/clickhouse-jdbc-0.6.3.jar \
        https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.3/clickhouse-jdbc-0.6.3.jar \
    && curl -fsSL -o /opt/spark/jars/httpclient5-5.3.1.jar \
        https://repo1.maven.org/maven2/org/apache/httpcomponents/client5/httpclient5/5.3.1/httpclient5-5.3.1.jar \
    && curl -fsSL -o /opt/spark/jars/httpcore5-5.2.4.jar \
        https://repo1.maven.org/maven2/org/apache/httpcomponents/core5/httpcore5/5.2.4/httpcore5-5.2.4.jar \
    && curl -fsSL -o /opt/spark/jars/httpcore5-h2-5.2.4.jar \
        https://repo1.maven.org/maven2/org/apache/httpcomponents/core5/httpcore5-h2/5.2.4/httpcore5-h2-5.2.4.jar

USER spark
