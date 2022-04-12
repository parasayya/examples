from kafka import KafkaConsumer, KafkaProducer
import ssl,os,sys

from requests import session

sasl_mechanism = 'PLAIN'
security_protocol = 'SASL_SSL'

# Create a new context using system defaults, disable all but TLS1.2
context = ssl.create_default_context()
context.options &= ssl.OP_NO_TLSv1
context.options &= ssl.OP_NO_TLSv1_1

producer = KafkaProducer(bootstrap_servers ='pkc-ymrq7.us-east-2.aws.confluent.cloud:9092' 
                            #os.getenv('bootstrap_servers')
                            ,
                         sasl_plain_username = '3ISEHMPBVSF3KJ4O'
                         #os.getenv('KAFKA_USERNAME')
                         ,
                         sasl_plain_password = '' 
                         #os.getenv('KAFKA_PASSWORD')
                         ,
                         security_protocol = security_protocol,
                         ssl_context = context,
                         sasl_mechanism = sasl_mechanism,
                         retries=5,
                         )

def send_message(message):

    try:
        producer.send('hell'
        #os.getenv('KAFKA_TOPIC')
        , message.encode('utf-8'))
        producer.flush()
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise


send_message('hello  8te8 2ye89')
