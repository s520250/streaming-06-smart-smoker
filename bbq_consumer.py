"""
Author: Sammie Bever
Date: February 9, 2023 
Class: Streaming Data 
Assignment: Module 06 

This program creates a consumer with 3 callbacks to go with the bbq_producer file.

To exit program, press CTRL+C.

Questions:
- need to add queue_delete code in main() function??
- current issue: when I run the producer file on it's own, it counts the messages in RabbitMQ properly. But when I start running the consumer file,
    it clears the queues back to a 0 count. Is this because of the auto_ack setting? If the consumer file is running, they seem to be acknowledged
    and cleared from the queue immediately, whether or not the consumer file has noted that it "receieved" the message or not.
- Update screenshot to include updated smoker alert phrasing.
- update sleeptime in producer file to 30 secs
"""
########################################################

# import python modules
import pika
import sys
import time
from collections import deque

########################################################

# define variables/constants/options
host = "localhost"
csv_file = "smoker-temps.csv"
smoker_queue = "01-smoker"
foodA_queue = "02-food-A"
foodB_queue = "03-food-B"
show_offer = True # (RabbitMQ Server option - T=on, F=off)

# set alert limits
smoker_alert_limit = 15 # temp decrease of this amount sends a smoker alert
food_stall_alert_limit = 1 # temp change of this amount sends a food stall alert

# Time windowing - Create deques to store that last n messages
smoker_deque = deque(maxlen=5)  # limited to 5 items (the 5 most recent readings)
foodA_deque = deque(maxlen=20) # limited to 20 items (the 20 most recent readings)
foodB_deque = deque(maxlen=20) # limited to 20 items (the 20 most recent readings)

########################################################

# define callback functions (called when message is received - 1 per queue)

def smoker_callback(ch, method, properties, body):
    """ Define behavior on getting a message in the 01-smoker queue.
    Monitor smoker temperature. Send an alert if the smoker temp decreases by 15 F or more in 2.5 min (or 5 readings). """

    # receive & decode the binary message body to a string
    print(f" [x] Received {body.decode()} on 01-smoker")
    # simulate work
    time.sleep(1)
    # removed basic_ack

    # create smoker deque to store x amount of recent messages
    # add new message to our smoker deque
    smoker_deque.append(body.decode())
    # first item in deque (oldest)
    smoker_deque_item1 = smoker_deque[0]
    # split the oldest message in the deque by the delimiter ", " and put data into list form
    # the first list item [0] is our date and timestamp from 5 messages ago (2.5 mins ago)
    # the second list item [1] is the temp from 5 messages ago (2.5 mins ago)
    smoker_deque_item1_split = smoker_deque_item1.split(", ")
    # change to float and remove last 2 characters from string, which is the ']' character of our message
    smoker_deque_item1_temp = float(smoker_deque_item1_split[1][:-1])

    # smoker current temp/current message code
    smoker_current_timetemp = body.decode()
    # split the current message by the delimiter ", " and put data into list form
    # the first list item [0] is our current date and timestamp being read in this message
    # the second list item [1] is our current temp being read in this message
    smoker_current_timetemp_split = smoker_current_timetemp.split(", ")
    # change temp to float and remove last 1 characters from string, which is the ']' character of our message
    smoker_current_temp = float(smoker_current_timetemp_split[1][:-1])

    # calculate smoker temperature change and round to 1 decimal point
    smoker_temp_change = round(smoker_deque_item1_temp - smoker_current_temp, 1)

    # set up smoker alert
    if smoker_temp_change >= smoker_alert_limit:
        print(f">>> Smoker alert! The temperature of the smoker has decreased by 15 F or more in 2.5 min (or 5 readings). \n          Smoker temp decrease = {smoker_temp_change} degrees F = {smoker_deque_item1_temp} - {smoker_current_temp}")


def foodA_callback(ch, method, properties, body):
    """ Define behavior on getting a message in the 02-food-A queue.
    Monitor food A temperature. Send an alert if the temp of food A changes (+/-) 1 F or less in 10 min (or 20 readings). """
    # receive & decode the binary message body to a string
    print(f" [x] Received {body.decode()} on 02-food-A")
    # simulate work
    time.sleep(1)
    # removed basic_ack

    # create food A deque to store x amount of recent messages
    # add new message to our food A deque
    foodA_deque.append(body.decode())
    # first item in deque (oldest)
    foodA_deque_item1 = foodA_deque[0]
    # split the oldest message in the deque by the delimiter ", " and put data into list form
    # the first list item [0] is our date and timestamp from 20 messages ago (10 mins ago)
    # the second list item [1] is the temp from 20 messages ago (10 mins ago)
    foodA_deque_item1_split = foodA_deque_item1.split(", ")
    # change to float and remove last 2 characters from string, which is the ']' character of our message
    foodA_deque_item1_temp = float(foodA_deque_item1_split[1][:-1])

    # food current temp/current message code
    foodA_current_timetemp = body.decode()
    # split the current message by the delimiter ", " and put data into list form
    # the first list item [0] is our current date and timestamp being read in this message
    # the second list item [1] is our current temp being read in this message
    foodA_current_timetemp_split = foodA_current_timetemp.split(", ")
    # change temp to float and remove last 1 characters from string, which is the ']' character of our message
    foodA_current_temp = float(foodA_current_timetemp_split[1][:-1])

    # calculate food temperature change and round to 1 decimal point
    foodA_temp_change = round(foodA_current_temp - foodA_deque_item1_temp, 1)

    # set up food stall alert - any temp change (+/-)
    if abs(foodA_temp_change) <= food_stall_alert_limit:
        print(f">>> Food stall alert! The temperature of food A has changed 1 F or less in 10 min (or 20 readings). \n          Food A temp change = {foodA_temp_change} degrees F = {foodA_current_temp} - {foodA_deque_item1_temp}")

def foodB_callback(ch, method, properties, body):
    """ Define behavior on getting a message in the 03-food-B queue.
    Monitor food B temperature. Send an alert if the temp of food B changes (+/-) 1 F or less in 10 min (or 20 readings). """
    # receive & decode the binary message body to a string
    print(f" [x] Received {body.decode()} on 03-food-B")
    # simulate work
    time.sleep(1)
    # removed basic_ack

    # create food B deque to store x amount of recent messages
    # add new message to our food B deque
    foodB_deque.append(body.decode())
    # first item in deque (oldest)
    foodB_deque_item1 = foodB_deque[0]
    # split the oldest message in the deque by the delimiter ", " and put data into list form
    # the first list item [0] is our date and timestamp from 20 messages ago (10 mins ago)
    # the second list item [1] is the temp from 20 messages ago (10 mins ago)
    foodB_deque_item1_split = foodB_deque_item1.split(", ")
    # change to float and remove last 2 characters from string, which is the ']' character of our message
    foodB_deque_item1_temp = float(foodB_deque_item1_split[1][:-1])

    # food current temp/current message code
    foodB_current_timetemp = body.decode()
    # split the current message by the delimiter ", " and put data into list form
    # the first list item [0] is our current date and timestamp being read in this message
    # the second list item [1] is our current temp being read in this message
    foodB_current_timetemp_split = foodB_current_timetemp.split(", ")
    # change temp to float and remove last 1 characters from string, which is the ']' character of our message
    foodB_current_temp = float(foodB_current_timetemp_split[1][:-1])

    # calculate food temperature change and round to 1 decimal point
    foodB_temp_change = round(foodB_current_temp - foodB_deque_item1_temp, 1)

    # set up food stall alert - any temp change (+/-)
    if abs(foodB_temp_change) <= food_stall_alert_limit:
        print(f">>> Food stall alert! The temperature of food B has changed 1 F or less in 10 min (or 20 readings). \n          Food B temp change = {foodB_temp_change} degrees F = {foodB_current_temp} - {foodB_deque_item1_temp}")

########################################################

# define a main function to run the program

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

        # use the channel to declare a durable queue (1 per queue)
        # a durable queue will survive a RabbitMQ server restart and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        channel.queue_declare(queue=smoker_queue, durable=True)
        channel.queue_declare(queue=foodA_queue, durable=True)
        channel.queue_declare(queue=foodB_queue, durable=True)

        # The QoS level controls the # of messages that can be in-flight (unacknowledged by the consumer) at any given time.
        # Set the prefetch count to one to limit the number of messages being consumed and processed concurrently.
        # This helps prevent a worker from becoming overwhelmed and improve the overall system performance. 
        # prefetch_count = Per consumer limit of unaknowledged messages      
        channel.basic_qos(prefetch_count=1) 

        # configure the channel to listen on a specific queue,  
        # use the callback function named callback,
        # we use the auto_ack for this assignment
        channel.basic_consume(queue=smoker_queue, on_message_callback=smoker_callback, auto_ack=True)
        channel.basic_consume(queue=foodA_queue, on_message_callback=foodA_callback, auto_ack=True)
        channel.basic_consume(queue=foodB_queue, on_message_callback=foodB_callback, auto_ack=True)

        # print a message to the console for the user
        print(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
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