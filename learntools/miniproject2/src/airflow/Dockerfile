FROM astrocrpublic.azurecr.io/runtime:3.1-11

# Install Java for PySpark
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Auto-detect Java path (works for both arm64 and amd64)
RUN JAVA_BIN=$(readlink -f /usr/bin/java) && \
    echo "export JAVA_HOME=${JAVA_BIN%/bin/java}" >> /etc/profile.d/java.sh && \
    echo "export PATH=\$JAVA_HOME/bin:\$PATH" >> /etc/profile.d/java.sh && \
    # Create a consistent symlink for JAVA_HOME
    ln -s ${JAVA_BIN%/bin/java} /usr/lib/jvm/default-java

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER astro
