import argparse

def main():
    parser = argparse.ArgumentParser(
		description = """
		A simple Kafka Schema Inspector.
		Connects to a Kafka topic, samples a number of messages,
		and infers the schema.
		"""
   )
    parser.add_argument("topic", help = "The Kafka topic to inspect.")
    parser.add_argument(
		"-b", "--brokers",
		nargs = "+",
		default=['localhost:9092'],
		help = "A list of Kafka bootstrap servers. Default: localhost:9092"        
	)
    parser.add_argument(
		"-n", "--num-messages",
		type = int,
		default = 500,
		help = "The number of messages to sample for schema inference. Default: 500"        
	)
    args = parser.parse_args()
    
    print(args)

if __name__ == "__main__":
      main()