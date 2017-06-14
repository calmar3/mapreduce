FROM    matnar/hadoop
COPY    hbase /hbase
COPY 	flume /flume
ENV HBASE_HOME=/hbase/bin FLUME_CONF=/flume/conf FLUME_HOME=/flume/bin flume=/flume/bin/flume-ng
RUN cp /usr/local/hadoop/share/hadoop/common/*.jar /flume/lib
RUN cp /usr/local/hadoop/share/hadoop/common/lib/*.jar /flume/lib
RUN cp /usr/local/hadoop/share/hadoop/hdfs/*.jar /flume/lib
RUN cp /usr/local/hadoop/share/hadoop/hdfs/lib/*.jar /flume/lib
