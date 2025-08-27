import argparse
import sys
from kafka import KafkaConsumer
import json

def infer_schema_from_json(data_json):
    """
    Infers the schema for a single JSON object.
    """
    if isinstance(data_json, dict):
        return {k: infer_schema_from_json(v) for k, v in data_json.items()}
    elif isinstance(data_json, list):
        if not data_json:
            return []
        # Return schema of the first element in the list.
        return [infer_schema_from_json(data_json[0])]
    elif isinstance(data_json, str):
        return "string"
    elif isinstance(data_json, int):
        return "integer"
    elif isinstance(data_json, float):
        return "float"
    elif isinstance(data_json, bool):
        return "boolean"
    elif data_json is None:
        return "null"
    else:
        return str(type(data_json).__name__)
    
    
def consolidate_schemas(schema1, schema2):
    """
    Consolidates 2 schemas into a single one.
    This function handles inconsistencies by marking fields as mixed.
    """
    consolidated_schema = {}
    
    # Add fields from schema1
    for key, value in schema1.items():
        consolidated_schema[key] = value

    # Add or update fields from schema2
    for key, value in schema2.items():
        if key not in consolidated_schema:
            consolidated_schema[key] = value
        elif consolidated_schema[key] != value:
            # If the types are different, mark as mixed.
            consolidated_schema[key] = f"mixed({consolidated_schema[key]}, {value})"
            
    return consolidated_schema


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
            value_deserializer=lambda v: v.decode('utf-8')
        )

        final_schema = {}
        messages_inspected = 0
        
        for message in consumer:
            if messages_inspected >= num_messages:
                break
            
            try:
                data_json = json.loads(message.value)
                current_schema = infer_schema_from_json(data_json)
                final_schema = consolidate_schemas(final_schema, current_schema)
                
                messages_inspected += 1
            
            except json.JSONDecodeError:
                sys.stderr.write(f"Warning: Non-JSON message found. Skipping: {message.value[:50]}\n")
            except Exception as e:
                sys.stderr.write(f"Error processing message: {e}\n")

        consumer.close()
        print(f"Finished inspecting {messages_inspected} messages.")
        return final_schema

    except Exception as e:
        sys.stderr.write(f"Error connecting to or inspecting topic '{topic}': {e}\n")
        return None


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