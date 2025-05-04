const { Kafka } = require('kafkajs');
const {MongoClient} = require('mongodb');
const uri = "mongodb://localhost:27017";
const dbname="kafka_db";
const collectionName="kafka_collection";

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092']
  });
const run = async () => {
  const client = new MongoClient(uri);
  await client.connect();
  const db = client.db(dbname);
  const collection = db.collection(collectionName);

const consumer = kafka.consumer({ groupId: 'test-group' });
await consumer.connect();
await consumer.subscribe({ topic: 'test-topic', fromBeginning: true });


await consumer.run({

  eachMessage: async ({ topic, partition, message }) => {
    const content={
    value: message.value.toString(),
    };
    
    await collection.insertOne(content);
    console.log("📥 Message inséré :", content);
  },
});
};

run().catch(console.error);





