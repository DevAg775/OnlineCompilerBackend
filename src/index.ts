import amqplib,{ Channel, Connection } from 'amqplib'
import cors from 'cors'
import express from 'express'
import { v4 as uuidv4 } from 'uuid'

const app = express()
const PORT = 3001

// CORS configuration
const corsOptions = {
    origin: '*',
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization'],
    credentials: true,
    optionsSuccessStatus: 204
};

// Apply CORS before other middleware
app.use(cors(corsOptions));
app.use(express.json());

let channel: Channel, connection:any;

connect()
async function connect(){
    try{
        const amqpServer = 'amqp://localhost:5672' //amqp means Advanced Message Queuing Protocol and here i have provided the link for the amqp server also rabbitmq have default port as 5672
        connection =await amqplib.connect(amqpServer) //establising connection to amqp server 
        channel = await connection.createChannel() //creating channel for amqp server 
        await channel.assertQueue('CodeSender') // here i am using the channel we have created and putting in queue name as CodeSender 
    }
    catch(err){
        console.log(`unable to connect to RabbitMq please make sure that the server url is correct or server is active  ${err}`)
    }
}
app.post('/api/compile',async (req,res)=>{
    const data = req.body;
    const executionId = uuidv4();
    channel.sendToQueue('CodeSender',Buffer.from(JSON.stringify({
        ...data,
        executionId,
        date: new Date(),
    })))
    res.status(200).json({
        msg: `Code sent to queue`,
        executionId
    })
})

app.listen(PORT, () => {
    console.log(`Server running on ${PORT}`)
  })