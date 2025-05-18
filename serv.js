const express = require('express');
const amqp = require('amqplib');
const bodyParser = require('body-parser');

const app = express();
app.use(bodyParser.json());

let channel, connection;
const RETRY_LIMIT = 3;

async function connectQueue() {
    try {
        connection = await amqp.connect('amqp://localhost'); 
        channel = await connection.createChannel();
        await channel.assertQueue('notificationQueue');
        console.log('Connected to RabbitMQ');
    } catch (error) {
        console.error('Error connecting to RabbitMQ:', error);
    }
}
connectQueue();


app.post('/notifications', async (req, res) => {
    const { type, message, userId } = req.body;

    if (!type || !message || !userId) {
        return res.status(400).json({ error: 'Type, message, and userId are required.' });
    }

    const notification = { type, message, userId, retries: 0 };
    channel.sendToQueue('notificationQueue', Buffer.from(JSON.stringify(notification)));

    res.status(200).json({ message: 'Notification sent successfully.' });
});


app.get('/users/:id/notifications', (req, res) => {
    const userId = req.params.id;
    res.json({ userId, notifications: [Sample notification for user ${userId}] });
});


async function consumeNotifications() {
    channel.consume('notificationQueue', (msg) => {
        const notification = JSON.parse(msg.content.toString());
        console.log('Processing Notification:', notification);

        try {
            setTimeout(() => {
                console.log('Notification sent:', notification);
                channel.ack(msg);
            }, 1000);
        } catch (error) {
            notification.retries += 1;
            if (notification.retries <= RETRY_LIMIT) {
                console.log(Retrying notification: ${notification.retries});
                channel.sendToQueue('notificationQueue', Buffer.from(JSON.stringify(notification)));
            } else {
                console.error('Failed Notification:', notification);
                channel.ack(msg);
            }
        }
    });
}

consumeNotifications();

app.listen(3000, () => {
    console.log('Notification service running on port 3000');
});