import amqplib, { Channel, Connection } from 'amqplib'
import cors from 'cors'
import express from 'express'
import mongoose, { Schema, Document } from 'mongoose'
import { v4 as uuidv4 } from 'uuid'
import Redis from 'ioredis'
import crypto from 'crypto'
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
        await mongoose.connect(process.env.MONGODB_URI || '')
        console.log('MongoDB connected')
    } catch (err) {
        console.error('MongoDB connection failed', err)
        process.exit(1) // MongoDB na chale toh server band kar do
    }
}

// ─── RabbitMQ ─────────────────────────────────────────────────────────────────
let channel: Channel | null, connection: any;

async function connectRabbitMQ() {
    try {
        const amqpServer = process.env.RABBITMQ_URL || 'amqp://admin:admin123@3.110.108.63:5672'
        console.log('Connecting to RabbitMQ:', amqpServer)
        connection = await amqplib.connect(amqpServer)
        channel = await connection.createChannel()
        await channel!.assertQueue('CodeSender')
        console.log('RabbitMQ connected')
        connection.on('error', () => { console.log('RabbitMQ error, reconnecting...'); channel = null; setTimeout(connectRabbitMQ, 5000); })
        connection.on('close', () => { console.log('RabbitMQ closed, reconnecting...'); channel = null; setTimeout(connectRabbitMQ, 5000); })
    } catch (err: any) {
        console.error('RabbitMQ connection failed:', err.message)
        channel = null
        setTimeout(connectRabbitMQ, 5000)
    }
}

//─── Reddis ───────────────────────────────────────────────────────────────────

const redis = new Redis(process.env.REDIS_URL || '')
redis.on('connect', () => console.log('Redis connected'))
redis.on('error', (err) => console.error('Redis error', err))

// ─── Routes ───────────────────────────────────────────────────────────────────

// Job submit karo
app.post('/api/compile', async (req, res) => {
    try {
        const data = req.body;

        // Generate cache key from code + language
        const cacheKey = crypto
            .createHash('md5')
            .update(data.code + (data.language || data.Lang))
            .digest('hex')

        // Check Redis cache first
        const cached = await redis.get(cacheKey)
        if (cached) {
            console.log(`Cache HIT for key: ${cacheKey}`)
            res.status(200).json({
                msg: 'Result from cache',
                cached: true,
                result: JSON.parse(cached)
            })
            return
        }

        console.log(`Cache MISS for key: ${cacheKey}`)
        const executionId = uuidv4();

        // Save to MongoDB
        await Execution.create({
            executionId,
            code: data.code,
            language: data.language || data.Lang,
            status: 'pending',
            createdAt: new Date()
        })

        // Push to RabbitMQ with cacheKey so worker can cache result
        channel!.sendToQueue('CodeSender', Buffer.from(JSON.stringify({
            ...data,
            executionId,
            cacheKey,
            date: new Date(),
        })))

        res.status(200).json({
            msg: 'Code sent to queue',
            cached: false,
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