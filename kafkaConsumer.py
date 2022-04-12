from kafka import KafkaConsumer
import ssl,os,sys

sasl_mechanism = 'PLAIN'
security_protocol = 'SASL_SSL'

# Create a new context using system defaults, disable all but TLS1.2
context = ssl.create_default_context()
context.options &= ssl.OP_NO_TLSv1
context.options &= ssl.OP_NO_TLSv1_1

consumer = KafkaConsumer(
    "hell",
    auto_offset_reset="earliest",
    bootstrap_servers="pkc-ymrq7.us-east-2.aws.confluent.cloud:9092",
    sasl_plain_username = '3ISEHMPBVSF3KJ4O'
                         #os.getenv('KAFKA_USERNAME')
                         ,
    sasl_plain_password = '' 
                         #os.getenv('KAFKA_PASSWORD')
                         ,
                         group_id="1",
    security_protocol = security_protocol,
    ssl_context = context,
    sasl_mechanism = sasl_mechanism,
)

# Call poll twice. First call will just assign partitions for our
# consumer without actually returning anything
print(consumer.topics)
for _ in range(2):
    raw_msgs = consumer.poll(timeout_ms=1000)
    for tp, msgs in raw_msgs.items():
        for msg in msgs:
            print("Received: {}".format(msg.value))

# Commit offsets so we won't get the same messages again

consumer.commit()
