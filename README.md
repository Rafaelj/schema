
A script to infer the json schema of a Kafka topic

# How to Use

Prerequisites
First, you'll need to install the required Python library.

```Bash
pip install kafka-python
```

# Running the Script
You can run the script from your command line. The only required argument is the name of the Kafka topic you want to inspect.

```Bash
python kafka_schema_inspector.py <topic_name>
```

Example Output
The output will be a JSON object representing the inferred schema.

```JSON
{
  "order_id": "integer",
  "customer_id": "string",
  "items": [
    {
      "product_id": "string",
      "quantity": "integer"
    }
  ],
  "timestamp": "string"
}
```

# Options

The script includes optional arguments to customize its behavior.

| Argument | Shorthand | Description | Default Value | 
| ---------| --------- | ------------| --------------| 
| --topic  |	None |	The name of the Kafka topic to inspect. (Required) |	None | 
| --brokers	| -b |	A list of Kafka bootstrap servers to connect to. |	['localhost:9092'] | 
| --num-messages | -n	The number of messages to sample for schema inference. A larger sample may be more accurate. | 	500| 

## Example
To inspect the topic my_transactions on a remote cluster and sample 1000 messages, you would use:

```Bash
python kafka_schema_inspector.py my_transactions --brokers kafka1.prod:9092 kafka2.prod:9092 -n 1000
```