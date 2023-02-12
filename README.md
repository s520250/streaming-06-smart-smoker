# streaming-06-smart-smoker
Continuing the project from module 05 + consumer

Author: Sammie Bever
Date: February 9, 2023 
Class: Streaming Data 
Assignment: Module 06

This program uses producers, task queues (RabbitMQ), and consumers. 
It reads data from the smoker-temps.csv file for smart smokers.

Creating a producer, consumer, using 3 task_queues, and 3 callbacks.

# Instructions on how to run the program
## Before you begin, adjust your settings in Visual Studio and set-up your conda environment
1. View / Command Palette - then Python: Select Interpreter
2. Select your conda environment. 

## Adjust your code as needed
1. Update your constants in the bbq_producer.py file
2. Update your constants in the the bbq_consumer.py file, including alert limits and deque lengths
3. Update your sleep time, if desired

## Execute the Producer
1. Open Anaconda Prompt Terminal
2. Run bbq_producer.py file (say y to monitor RabbitMQ queues)
3. Run bbq_consumer.py file

# Producer Assignment Details
## Using a Barbeque Smoker
When running a barbeque smoker, we monitor the temperatures of the smoker and the food to ensure everything turns out tasty. Over long cooks, the following events can happen:

## The smoker temperature can suddenly decline.
The food temperature doesn't change. At some point, the food will hit a temperature where moisture evaporates. It will stay close to this temperature for an extended period of time while the moisture evaporates (much like humans sweat to regulate temperature). We say the temperature has stalled.

## Sensors
We have temperature sensors track temperatures and record them to generate a history of both (a) the smoker and (b) the food over time. These readings are an example of time-series data, and are considered streaming data or data in motion.

## Streaming Data
Our thermometer records three temperatures every thirty seconds (two readings every minute). The three temperatures are:

the temperature of the smoker itself.
the temperature of the first of two foods, Food A.
the temperature for the second of two foods, Food B.
 
## Significant Events
We want know if:

The smoker temperature decreases by more than 15 degrees F in 2.5 minutes (smoker alert!)
Any food temperature changes less than 1 degree F in 10 minutes (food stall!)

## Smart System
We will use Python to:

Simulate a streaming series of temperature readings from our smart smoker and two foods.
Create a producer to send these temperature readings to RabbitMQ.
Create three consumer processes, each one monitoring one of the temperature streams. 
Perform calculations to determine if a significant event has occurred.
 
## Optional: Alert Notifications
Optionally, we can have our consumers send us an email or a text when a significant event occurs. 
You'll need some way to send outgoing emails. I use my main Gmail account - other options are possible. 

# Consumer Assignment Details
## Consumer goals 
Condition To monitor

If smoker temp decreases by 15 F or more in 2.5 min (or 5 readings)  --> smoker alert!
If food temp change in temp is 1 F or less in 10 min (or 20 readings)  --> food stall alert!
 
## Windowing
For more on windowing, read https://softwaremill.com/windowing-in-big-data-streams-spark-flink-kafka-akka/Links to an external site.
Smoker time window = 2.5 mins
Food time window = 10 mins
How many temperature readings are in the smoker time window? At one reading every 1/2 minute, the smoker deque max length is 5 (2.5 min * 1 reading/0.5 min)
How many temperature readings are in the food time window? At one reading every 1/2 minute, the food deque max length is 20 (10 min * 1 reading/0.5 min) 
 
## Deque
For more abut deques, read https://docs.python.org/3/library/collections.html#collections.deque Links to an external site.(only the description of the deque class)
We want to create a deque of limited size (to hold just the last n readings) - it'll act like a continuous queue
The deque will hold only the number of readings we need for the time window of interest. 

Code example:
from collections import deque
smoker_deque = deque(maxlen=5)  # limited to 5 items (the 5 most recent readings)

# References
## Producer
- Convert string to float (with blank cells in csv) - https://codedamn.com/news/programming/fix-valueerror
- auto-ack: https://www.rabbitmq.com/confirms.html
- SyntaxError: positional argument follows keyword argument - https://www.geeksforgeeks.org/how-to-fix-syntaxerror-positional-argument-follows-keyword-argument-in-python/
- how to tell python to do nothing: https://realpython.com/python-pass/

## Consumer
- deques: https://docs.python.org/3/library/collections.html#collections.deque
- split string: https://www.w3schools.com/python/ref_string_split.asp
- remove last characters from string: https://careerkarma.com/blog/python-remove-character-from-string/#:~:text=You%20can%20remove%20a%20character,the%20string%20without%20a%20replacement.
- round floats to 1 decimal place: https://stackoverflow.com/questions/3400965/getting-only-1-decimal-place
- how to split a new line: https://www.freecodecamp.org/news/python-new-line-and-how-to-python-print-without-a-newline/

# Screenshots of PRODUCER program running
Sometimes my screenshots don't come through to GitHub using the PNG file, so I also use a GitHub link to the photos in my repo

## Running code in Anaconda Prompt Terminal
1 producer

GitHub Link - 
![Bever Example GitHub](https://github.com/s520250/streaming-05-smart-smoker/blob/main/Screenshot_BBQ_Producer_AnacondaTerminal.PNG)

Using file name (PNG) -
![Bever Example PNG](Screenshot_BBQ_Producer_AnacondaTerminal.PNG)

## RabbitMQ Server
3 task queues

GitHub Link - 
![RabbitMQ Server GitHub](https://github.com/s520250/streaming-05-smart-smoker/blob/main/Screenshot_BBQ_Producer_RabbitMQ.PNG)

Using file name (PNG) -
![RabbitMQ Server PNG](Screenshot_BBQ_Producer_RabbitMQ.PNG)

# Screenshots of CONSUMER program running
Sometimes my screenshots don't come through to GitHub using the PNG file, so I also use a GitHub link to the photos in my repo

1 consumer, 3 callbacks

## Running Producer & Consumer code in Anaconda Prompt Terminal
GitHub Link - 
![Bever Producer & Consumer GitHub]()

Using file name (PNG) -
![Bever Producer & Consumer PNG](Screenshot_BBQ_Producer&Consumer1.PNG)

## RabbitMQ Server
Messages are being published and delivered at the same time. None are unacked since auto_ack is turned on.

GitHub Link - 
![Consumer RabbitMQ Server GitHub]()

Using file name (PNG) -
![Consumer RabbitMQ Server PNG](Screenshot_BBQ_Consumer_RabbitMQ.PNG)