import argparse
import sys
from kafka import KafkaConsumer


def inspect(topic, bootstrap_servers, num_messages=500):
    """
        Connects to a topic and inspect its messages to infer the schema
    Returns the inferred schema
    """
    print(f"Inspecting topic:'{topic}'")
    print(f"Connecting to Kafka brokers: {', '.join(bootstrap_servers)}")

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            value_deserializer=lambda v: v.decode("utf-8")
        )
        # final schema = {}
        messages_inspected = 0
        
        for message in consumer:
            if messages_inspected >= num_messages:
                break

                print("Analysing messages...")

    except Exception as e:
        sys.stderr.write(
            f"Error connecting to or inspecting topic '{topic}': {e}\n")
        return None


def inspect(topic, bootstrap_servers, num_messages=500): 
    """
	Connects to a topic and inspect its messages to infer the schema
    Returns the inferred schema 
    """
    print(f"Inspecting topic:'{topic}'")
    print(f"Connecting to Kafka brokers: {', '.join(bootstrap_servers)}")


def main():
    parser = argparse.ArgumentParser(
		description = """
		A simple Kafka Schema Inspector.
		Connects to a Kafka topic, samples a number of messages,
		and infers the schema.
		"""
   )
    
	# Topic
    parser.add_argument("topic", help = "The Kafka topic to inspect.")
    
	# Optional arguments
    parser.add_argument(
		"-b", "--brokers",
		nargs = "+",
		default = ['localhost:9092'],
		help = "A list of Kafka bootstrap servers. Default: localhost:9092"        
	)
    parser.add_argument(
        "-n",
        "--num-messages",
        type=int,
        default=500,
        help="The number of messages to sample for schema inference. Default: 500")
    args = parser.parse_args()
    inspect(args.topic, args.brokers, args.num_messages)

if __name__ == "__main__":
    main()
