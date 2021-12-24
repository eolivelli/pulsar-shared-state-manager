# Simple Pulsar based Database

This project is a proof-of-concept about how to implement a very simple state storage backed by a Pulsar topic.

You will find it very useful if you need to implement:
- Shared configuration among Pulsar based services
- Shared metadata information among Pulsar based services

# How it works

Every instance of your application has a local copy of the "**state**".

We are using a **non-partitioned** Pulsar topic as write-ahead-log.


# Reading from the state

- When you read you can read from the local version or require to the library to update to the latest modification
- You cannot access directly the state

When you start a new instance the local state is reconstructed from the Pulsar topic.

We are reading from the Pulsar Topic using the Reader interface, so with a Non-Durable subscription, so
reading is very cheap, no need for Durable Cursor.

# Mutating the state (writes)
When you want to update the state you provide a function that generates a list of modifications to be applied to the state.

The library writes the modification to the Pulsar topic using an **Exclusive Producer** and then applies them to the local copy.

# Trade-offs, future-works

- Currently, we are not supporting any kind of checkpointing, so you must configure retention to keep the data of the commit-log 
forever.
- There is no way to leverage Compaction in Pulsar, because the library is unaware of the meaning of the mutations to the state,
in the future we could support this by associating a key to each mutation.
- The Exclusive Producer is a cool feature, but we need to open a new Producer per each mutation, this **may** be slow, 
essentially it is like using pessimistic locking, in the future new kinds of Exclusive Producers may be able to support optimistic locking,
like keeping the ownership of the topic and relying on some fencing mechanism.


# Getting started

There is a demo implementation of a simple Key-Value store backed by a Pulsar Topic.
You can find it in the [test folder](./src/test/java/org/apache/pulsar/db/PulsarMapImplTest.java)

This is the [Pulsar Map](./src/main/java/org/apache/pulsar/db/PulsarMap.java) API

This is how it looks like to implement a Map with SharedStatementManager [PulsarMapImpl](src/main/java/org/apache/pulsar/db/impl/PulsarMapImpl.java)

An example of usage


```
try (PulsarMap<String, Integer> map = PulsarMap.build(
   PulsarSharedStatementManagerImpl .builder()
     .withPulsarClient(pulsarBroker.getPulsarClient())
     .withTopic("persistent://public/default/mymap"),
     StandardSerDe.STRING,
     StandardSerDe.INTEGER);) {

     map.put("a", 1).get();
}
```


