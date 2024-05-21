FROM docker.elastic.co/kibana/kibana:8.13.3

WORKDIR /usr/share/kibana/saved_objects/

COPY data/ .

COPY entrypoint.sh /usr/share/kibana/entrypoint.sh

ENTRYPOINT ["/usr/share/kibana/entrypoint.sh"]

EXPOSE 5601