import amqplib, { Channel, Connection } from 'amqplib'
import cors from 'cors'
import express from 'express'
import mongoose, { Schema, Document } from 'mongoose'
import { v4 as uuidv4 } from 'uuid'
import dotenv from 'dotenv'
dotenv.config()

const app = express()
const PORT = 3001

// ─── CORS ────────────────────────────────────────────────────────────────────
const corsOptions = {
    origin: '*',
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization'],
    credentials: true,
    optionsSuccessStatus: 204
};
app.use(cors(corsOptions));
app.use(express.json());

// ─── MongoDB ─────────────────────────────────────────────────────────────────

// 1. Execution ka shape define karna (TypeScript ke liye)
interface IExecution extends Document {
    executionId: string
    code: string
    language: string
    status: 'pending' | 'running' | 'completed' | 'failed'
    output: string | null
    exitCode: number | null
    error: string | null
    createdAt: Date
    completedAt: Date | null
}

// 2. Schema — MongoDB ko batao document kaisa dikhega
const ExecutionSchema = new Schema<IExecution>({
    executionId: { type: String, required: true, unique: true },
    code: { type: String, required: true },
    language: { type: String, required: true },
    status: { type: String, enum: ['pending', 'running', 'completed', 'failed'], default: 'pending' },
    output: { type: String, default: null },
    exitCode: { type: Number, default: null },
    error: { type: String, default: null },
    createdAt: { type: Date, default: Date.now },
    completedAt: { type: Date, default: null }
})

// 3. Model — is schema se queries chalayenge
export const Execution = mongoose.model<IExecution>('Execution', ExecutionSchema)

// 4. MongoDB se connect karo
async function connectMongo() {
    try {
        await mongoose.connect(process.env.MONGODB_URI|| '')
        console.log('MongoDB connected')
    } catch (err) {
        console.error('MongoDB connection failed', err)
        process.exit(1) // MongoDB na chale toh server band kar do
    }
}

// ─── RabbitMQ ─────────────────────────────────────────────────────────────────
let channel: Channel, connection: any;

async function connectRabbitMQ() {
    try {
        const amqpServer = 'amqp://localhost:5672'
        connection = await amqplib.connect(amqpServer)
        channel = await connection.createChannel()
        await channel.assertQueue('CodeSender')
        console.log('RabbitMQ connected')
    } catch (err) {
        console.error(`RabbitMQ connection failed ${err}`)
    }
}

// ─── Routes ───────────────────────────────────────────────────────────────────

// Job submit karo
app.post('/api/compile', async (req, res) => {
    try {
        const data = req.body;
        const executionId = uuidv4();

        // MongoDB mein "pending" record banao
        await Execution.create({
            executionId,
            code: data.code,
            language: data.language || data.Lang,
            status: 'pending',
            createdAt: new Date()
        })
        console.log(`Execution record created: ${executionId}`)

        // RabbitMQ mein job push karo (same as before)
        channel.sendToQueue('CodeSender', Buffer.from(JSON.stringify({
            ...data,
            executionId,
            date: new Date(),
        })))

        res.status(200).json({
            msg: `Code sent to queue`,
            executionId
        })
    } catch (err) {
        console.error('Error in /api/compile:', err)
        res.status(500).json({ error: 'Internal server error' })
    }
})

// Execution ka status/result fetch karo
app.get('/api/execution/:id', async (req, res) => {
    try {
        const execution = await Execution.findOne({ executionId: req.params.id })
        if (!execution) {
            res.status(404).json({ error: 'Execution not found' })
        }
        res.status(200).json(execution)
    } catch (err) {
        console.error('Error fetching execution:', err)
        res.status(500).json({ error: 'Internal server error' })
    }
})

// ─── Start ────────────────────────────────────────────────────────────────────
async function start() {
    await connectMongo()      // pehle MongoDB
    await connectRabbitMQ()   // phir RabbitMQ
    app.listen(PORT, () => {
        console.log(`Server running on ${PORT}`)
    })
}

start()