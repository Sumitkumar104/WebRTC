import { Kafka } from "kafkajs";


// create an kafka  instance
const kafka = new Kafka({
    clientId: "my-app",
    brokers: ["localhost:9092"]        // provide the broker address run in local host
})


const producer = kafka.producer();        // create producer instance
const consumer = kafka.consumer({ groupId: "my-app3" });    // create consumer instance with group id


async function main() 
{

    await producer.connect();
    await producer.send({
        topic: "quickstart-events",
        messages: [{
            value: "hi there"
        }]
    })

    await consumer.connect();

    // first consumer subscribe to the topic and then run the consumer on each message
    await consumer.subscribe({
        topic: "quickstart-events", fromBeginning: true
    })

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log({
                offset: message.offset,
                value: message?.value?.toString(),
            })
        },
    })
}


main();