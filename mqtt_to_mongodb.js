const mqtt = require('mqtt');
const { MongoClient } = require('mongodb');

// MQTT Broker settings
const MQTT_BROKER = 'tcp://216.48.181.211:1883';  // Replace with your broker URL
const MQTT_USERNAME = 'cateina';                   // MQTT username
const MQTT_PASSWORD = 'Cateina@1234';              // MQTT password

// MongoDB settings
const MONGO_URI = 'mongodb+srv://sagarjadhav:sagar123@cluster0.7rxrj0k.mongodb.net/IotDb?retryWrites=true&w=majority&appName=Cluster0'; // Replace with your MongoDB URI
const DB_NAME = 'IotDb';
const COLLECTION_NAME = 'details';

// Connect to MongoDB
const mongoClient = new MongoClient(MONGO_URI, { useNewUrlParser: true, useUnifiedTopology: true });
let collection;

mongoClient.connect()
    .then(client => {
        console.log('Connected to MongoDB');
        const db = client.db(DB_NAME);
        collection = db.collection(COLLECTION_NAME);
    })
    .catch(err => console.error('MongoDB connection error:', err));

// Connect to MQTT broker
const mqttClient = mqtt.connect(MQTT_BROKER, {
    username: MQTT_USERNAME,
    password: MQTT_PASSWORD
});

mqttClient.on('connect', () => {
    console.log(`Connected to MQTT broker at ${MQTT_BROKER}`);
    mqttClient.subscribe('#', (err) => { // Subscribe to all topics
        if (err) {
            console.error('Subscription error:', err);
        } else {
            console.log('Subscribed to all MQTT topics');
        }
    });
});

mqttClient.on('message', (topic, message) => {
    // Log the incoming topic and message
    console.log(`Received message on topic "${topic}":`, message.toString());

    // Parse the message and insert into MongoDB
    try {
        const data = JSON.parse(message.toString());
        console.log('Data:',Object.keys(data).length);      
        // Check if data is an object
        if (typeof data === 'object' && data !== null && Object.keys(data).length>1) {
            //data.topic = topic; // Optionally store the topic with the message
            collection.insertOne(data)
                .then(() => console.log(`Data inserted into MongoDB1 from topic "${topic}":`, data))
                .catch(err => console.error('Insertion error:', err));
        } else {
            console.error('Invalid data format. Expected an object:', data);
        }
    } catch (err) {
        console.error('Message parsing error:', err);
    }
});
