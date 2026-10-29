FROM clojure:temurin-21-tools-deps
WORKDIR /app
COPY deps.edn /app/deps.edn
RUN clj -P
COPY src /app/src
CMD ["clj", "-M", "-m", "strategy-engine.core"]
