"""
Author: Sammie Bever
Date: February 9, 2023 
Class: Streaming Data 
Assignment: Module 06 

This program creates a consumer to go with the bbq_producer file.

To exit program, press CTRL+C.

Questions:
- is start_listening the same as start_consuming??
- need to add queue_delete code in main() function
- can I set auto_ack as a constant?
- current issue: when I run the producer file on it's own, it counts the messages in RabbitMQ properly. But when I start running the consumer file,
    it clears the queues back to a 0 count.
"""
########################################################

# import python modules
import pika
import sys
import time

########################################################

# define variables/constants/options
host = "localhost"
csv_file = "smoker-temps.csv"
smoker_queue = "01-smoker"
foodA_queue = "02-food-A"
foodB_queue = "03-food-B"
show_offer = True # (RabbitMQ Server option - T=on, F=off)
# auto_ack = True

########################################################

# define functions

## define a callback function to be called when a message is received
# need one callback per queue
def smoker_callback(ch, method, properties, body):
    """ Define behavior on getting a message."""
    # decode the binary message body to a string
    print(f" [x] Received {body.decode()} on 01-smoker")
    # simulate work by sleeping for the number of dots in the message
    time.sleep(body.count(b"."))
    # when done with task, tell the user
    print(" [x] Done.")
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    # ch.basic_ack(delivery_tag=method.delivery_tag)

def foodA_callback(ch, method, properties, body):
    """ Define behavior on getting a message."""
    # decode the binary message body to a string
    print(f" [x] Received {body.decode()} on 02-food-A")
    # simulate work by sleeping for the number of dots in the message
    time.sleep(body.count(b"."))
    # when done with task, tell the user
    print(" [x] Done.")
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    # ch.basic_ack(delivery_tag=method.delivery_tag)

def foodB_callback(ch, method, properties, body):
    """ Define behavior on getting a message."""
    # decode the binary message body to a string
    print(f" [x] Received {body.decode()} on 03-food-B")
    # simulate work by sleeping for the number of dots in the message
    time.sleep(body.count(b"."))
    # when done with task, tell the user
    print(" [x] Done.")
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    # ch.basic_ack(delivery_tag=method.delivery_tag)

## define a main function to run the program
## create a connection object
# need to update the task_queue for each version
def main(host: str, qn: str):
    """ Continuously listen for task messages on a named queue."""

    # when a statement can go wrong, use a try-except block
    try:
        # try this code, if it works, keep going
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host))

    # except, if there's an error, do this
    except Exception as e:
        print()
        print("ERROR: connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={host}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        # need one channel per consumer
        channel = connection.channel()

        # add queue_delete() code

        # use the channel to declare a durable queue (1 for each queue name)
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        channel.queue_declare(queue=smoker_queue, durable=True)
        channel.queue_declare(queue=foodA_queue, durable=True)
        channel.queue_declare(queue=foodB_queue, durable=True)

        # The QoS level controls the # of messages
        # that can be in-flight (unacknowledged by the consumer)
        # at any given time.
        # Set the prefetch count to one to limit the number of messages
        # being consumed and processed concurrently.
        # This helps prevent a worker from becoming overwhelmed
        # and improve the overall system performance. 
        # prefetch_count = Per consumer limit of unaknowledged messages      
        channel.basic_qos(prefetch_count=1) 

        # configure the channel to listen on a specific queue,  
        # use the callback function named callback,
        # and do not auto-acknowledge the message (let the callback handle it)
        channel.basic_consume(queue=smoker_queue, on_message_callback=smoker_callback, auto_ack=True)
        channel.basic_consume(queue=foodA_queue, on_message_callback=foodA_callback, auto_ack=True)
        channel.basic_consume(queue=foodB_queue, on_message_callback=foodB_callback, auto_ack=True)

        # print a message to the console for the user
        print(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        # does this need to be start_listening??
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        print()
        print("ERROR: something went wrong.")
        print(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        print(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        print("\nClosing connection. Goodbye.\n")
        connection.close()

########################################################

# Run program

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    main(host, smoker_queue)
    main(host, foodA_queue)
    main(host, foodB_queue)