# PUBSUB
pubsub is a communication/streaming service where exchange of messages happens without producer knowing who sender might be

The project tries to implement a kafka-like pubsub based model/arch functionality

Some of the main components involved:

- Broker: A single instance or a node which is responsible for accepting the incoming messages, storing them and directing them to specific client/consumers.
- Topics: Different categories in which messages might be organized, and can have more then one partition within it.
- Producers: Produces a message to a particular topic and to a particular partition.
- Consumers: Consumer subscribe to a particular topic, during this a partitions might be assigned to it to which it will be listening to incase on any message received there.
    - The partition in this are *auto-balancing* that is all consumer takes the equal amount of load of listening to the partitions

![arch](https://github.com/Saumya40-codes/pubsub/assets/115284013/f71c763e-0bbd-4ef3-90a4-557404573de9)

## Use case/example

Consider following simple stock prediction architecture

![image](https://github.com/Saumya40-codes/pubsub/assets/115284013/10a70e6e-10f3-43fd-a85d-adab557d40ae)

Now only those consumer will receive message who are subscribed to particular partition in a topic to which consumer is sending message to. 
On new consumer, auto load-balancing occurs (if existing consumer has more then one partiton)
