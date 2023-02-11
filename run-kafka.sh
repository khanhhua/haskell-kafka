docker run --name kafka-dev \
    -p 3030:3030 \
    -p 9092:9092 \
    -p 2181:2181 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://:9092 \
    -e KAFKA_LISTENERS=PLAINTEXT://:9092 \
    dougdonohoe/fast-data-dev
