FROM apache/nifi:1.27.0

RUN echo "java.arg.debug=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005" \
    >> /opt/nifi/nifi-current/conf/bootstrap.conf

