FROM apache/hadoop:3.3.6

COPY --chown=hadoop:users config/*.xml /opt/hadoop/it-config/
COPY --chown=hadoop:users scripts/ /opt/hadoop/it-scripts/

RUN curl -fsSL --retry 3 -o /tmp/jdk21.tar.gz https://api.adoptium.net/v3/binary/latest/21/ga/linux/x64/jdk/hotspot/normal/eclipse \
  && mkdir -p /opt/jdk21 \
  && tar -xzf /tmp/jdk21.tar.gz -C /opt/jdk21 --strip-components=1 \
  && rm -f /tmp/jdk21.tar.gz \
  && sed -i 's/\r$//' /opt/hadoop/it-scripts/*.sh \
  && chmod +x /opt/hadoop/it-scripts/*.sh
