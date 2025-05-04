const express = require('express');
const { MongoClient } = require('mongodb');

const app = express();
const port = 3000;

const mongoUri = 'mongodb://localhost:27017'; // ou ton URI MongoDB Atlas
const dbName = 'kafka_db';
const collectionName = 'kafka_collection';

app.get('/messages', async (req, res) => {
  const client = new MongoClient(mongoUri);

  try {
    await client.connect();
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    const messages = await collection.find({}).toArray();
    res.json(messages);
  } catch (err) {
    console.error("Erreur MongoDB :", err);
    res.status(500).send("Erreur serveur");
  } finally {
    await client.close();
  }
});

app.listen(port, () => {
  console.log(`🚀 Serveur REST lancé sur http://localhost:${port}`);
});
