const express = require('express');
const amqp = require('amqplib');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(express.json());

const RABBITMQ_URL = 'amqp://localhost';
const EXCHANGE_NAME = 'messages_exchange';
const ROUTING_KEY = 'messages_key';

let channel, connection;

async function connect() {
    connection = await amqp.connect(RABBITMQ_URL);
    channel = await connection.createChannel();
    await channel.assertExchange(EXCHANGE_NAME, 'topic', { durable: true });
}

app.post('/send', async (req, res) => {
    const message = req.body.message;
    const correlationId = uuidv4();

    const replyQueue = await channel.assertQueue('', { exclusive: true });
    //wait response message from service B
    channel.consume(replyQueue.queue, (msg) => {
        if (msg.properties.correlationId === correlationId) {
            console.log('reply:', msg.content.toString());
            res.send(`reply: ${msg.content.toString()}`);
        }
    }, { noAck: true });

    //send  message to exchange
    channel.publish(EXCHANGE_NAME, ROUTING_KEY, Buffer.from(message), {
        replyTo: replyQueue.queue,
        correlationId,
    });
    //res.send(`reply: ok`);
});

connect().then(() => {
    app.listen(3000, () => {
        console.log('Service A listening on port 3000');
    });
});
