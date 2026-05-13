import express from 'express';
import { Kafka } from 'kafkajs';

const app = express();
app.use(express.json());

const kafkaBroker = process.env.KAFKA_BROKER || 'localhost:9092';
const port = process.env.PORT || 3000;

const kafka = new Kafka({
  clientId: 'node-producer',
  brokers: [kafkaBroker],
});

const producer = kafka.producer();

const initKafka = async () => {
  await producer.connect();
  console.log('Kafka Producer connected to', kafkaBroker);
};

app.post('/transactions', async (req, res) => {
  try {
    const { userId, amount, type } = req.body;
    
    // Simulate a financial transaction event
    const transactionEvent = {
      id: Math.random().toString(36).substring(7),
      userId,
      amount,
      type,
      timestamp: new Date().toISOString()
    };

    // Publish to Kafka topic 'financial-transactions'
    await producer.send({
      topic: 'financial-transactions',
      messages: [
        { value: JSON.stringify(transactionEvent) },
      ],
    });

    // Return immediately to the user without waiting for the actual processing
    res.status(202).json({
      message: 'Transaction received and is being processed.',
      transaction: transactionEvent
    });
  } catch (error) {
    console.error('Error publishing to Kafka', error);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

app.listen(port, async () => {
  console.log(`Server running on port ${port}`);
  await initKafka();
});

// Handle graceful shutdown
process.on('SIGINT', async () => {
  await producer.disconnect();
  process.exit(0);
});
