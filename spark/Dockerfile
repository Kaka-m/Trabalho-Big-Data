# IMPORTANTE: este Dockerfile deve ser buildado com o contexto na RAIZ
# do repositorio (veja docker-compose.yml: build.context = .), pois ele
# precisa copiar arquivos de hadoop/conf que ficam fora da pasta spark/.

FROM apache/spark:3.5.1-python3

USER root

# happybase para gravar no HBase via Thrift
RUN pip install --no-cache-dir happybase

# Configuracao do HDFS, para o Spark falar com o namenode do projeto
COPY hadoop/conf/core-site.xml /opt/spark/conf/core-site.xml
COPY hadoop/conf/hdfs-site.xml /opt/spark/conf/hdfs-site.xml

# Configuracao do Hive Metastore (thrift) usado pelo enableHiveSupport()
COPY spark/hive-site.xml /opt/spark/conf/hive-site.xml

COPY spark/job/etl_insights.py /opt/spark-app/etl_insights.py

# Mantem o container vivo para rodarmos spark-submit via docker exec
ENTRYPOINT ["tail", "-f", "/dev/null"]