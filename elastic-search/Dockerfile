FROM docker.elastic.co/elasticsearch/elasticsearch:8.13.3

COPY entrypoint.sh /usr/share/elasticsearch/entrypoint.sh

ENTRYPOINT ["/usr/share/elasticsearch/entrypoint.sh"]

EXPOSE 9200 9300