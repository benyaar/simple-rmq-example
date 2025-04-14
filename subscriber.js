const express = require('express');
const amqp = require('amqplib');

const app = express();

const RABBITMQ_URL = 'amqp://localhost';
const EXCHANGE_NAME = 'messages_exchange';
const ROUTING_KEY = 'messages_key';

async function start() {
    const connection = await amqp.connect(RABBITMQ_URL);
    const channel = await connection.createChannel();

    await channel.assertExchange(EXCHANGE_NAME, 'topic', { durable: true });

    const { queue } = await channel.assertQueue('', { exclusive: true });
    await channel.bindQueue(queue, EXCHANGE_NAME, ROUTING_KEY);

    console.log('Waiting for messages...');
    //wait messages from exchange
    channel.consume(queue, async (msg) => {
        const content = msg.content.toString();
        console.log('Received:', content);

        const reply = `Response  "${content}"`;
        await new Promise((res)=>{
            setTimeout(() => {
                res()
            }, 5000);
        })
        //if need response to service A, send message directly to exclusive queue
        if (msg.properties.replyTo) {
            channel.sendToQueue(
                msg.properties.replyTo,
                Buffer.from(reply),
                { correlationId: msg.properties.correlationId }
            );
        }        

        channel.ack(msg);
    });
    
    app.listen(3001, () => {
        console.log('Service B listening on port 3001');
    });
}

start();
