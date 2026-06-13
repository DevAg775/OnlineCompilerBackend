"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.Execution = void 0;
const amqplib_1 = __importDefault(require("amqplib"));
const cors_1 = __importDefault(require("cors"));
const express_1 = __importDefault(require("express"));
const mongoose_1 = __importStar(require("mongoose"));
const uuid_1 = require("uuid");
const ioredis_1 = __importDefault(require("ioredis"));
const crypto_1 = __importDefault(require("crypto"));
const dotenv_1 = __importDefault(require("dotenv"));
dotenv_1.default.config();
const app = (0, express_1.default)();
const PORT = 3001;
// ─── CORS ────────────────────────────────────────────────────────────────────
const corsOptions = {
    origin: '*',
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization'],
    credentials: true,
    optionsSuccessStatus: 204
};
app.use((0, cors_1.default)(corsOptions));
app.use(express_1.default.json());
// 2. Schema — MongoDB ko batao document kaisa dikhega
const ExecutionSchema = new mongoose_1.Schema({
    executionId: { type: String, required: true, unique: true },
    code: { type: String, required: true },
    language: { type: String, required: true },
    status: { type: String, enum: ['pending', 'running', 'completed', 'failed'], default: 'pending' },
    output: { type: String, default: null },
    exitCode: { type: Number, default: null },
    error: { type: String, default: null },
    createdAt: { type: Date, default: Date.now },
    completedAt: { type: Date, default: null }
});
// 3. Model — is schema se queries chalayenge
exports.Execution = mongoose_1.default.model('Execution', ExecutionSchema);
// 4. MongoDB se connect karo
function connectMongo() {
    return __awaiter(this, void 0, void 0, function* () {
        try {
            yield mongoose_1.default.connect(process.env.MONGODB_URI || '');
            console.log('MongoDB connected');
        }
        catch (err) {
            console.error('MongoDB connection failed', err);
            process.exit(1); // MongoDB na chale toh server band kar do
        }
    });
}
// ─── RabbitMQ ─────────────────────────────────────────────────────────────────
let channel, connection;
function connectRabbitMQ() {
    return __awaiter(this, void 0, void 0, function* () {
        try {
            const amqpServer = process.env.RABBITMQ_URL || 'amqp://localhost:5672';
            connection = yield amqplib_1.default.connect(amqpServer);
            channel = yield connection.createChannel();
            yield channel.assertQueue('CodeSender');
            console.log('RabbitMQ connected');
        }
        catch (err) {
            console.error(`RabbitMQ connection failed ${err}`);
        }
    });
}
//─── Reddis ───────────────────────────────────────────────────────────────────
const redis = new ioredis_1.default(process.env.REDIS_URL || '');
redis.on('connect', () => console.log('Redis connected'));
redis.on('error', (err) => console.error('Redis error', err));
// ─── Routes ───────────────────────────────────────────────────────────────────
// Job submit karo
app.post('/api/compile', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const data = req.body;
        // Generate cache key from code + language
        const cacheKey = crypto_1.default
            .createHash('md5')
            .update(data.code + (data.language || data.Lang))
            .digest('hex');
        // Check Redis cache first
        const cached = yield redis.get(cacheKey);
        if (cached) {
            console.log(`Cache HIT for key: ${cacheKey}`);
            res.status(200).json({
                msg: 'Result from cache',
                cached: true,
                result: JSON.parse(cached)
            });
            return;
        }
        console.log(`Cache MISS for key: ${cacheKey}`);
        const executionId = (0, uuid_1.v4)();
        // Save to MongoDB
        yield exports.Execution.create({
            executionId,
            code: data.code,
            language: data.language || data.Lang,
            status: 'pending',
            createdAt: new Date()
        });
        // Push to RabbitMQ with cacheKey so worker can cache result
        channel.sendToQueue('CodeSender', Buffer.from(JSON.stringify(Object.assign(Object.assign({}, data), { executionId,
            cacheKey, date: new Date() }))));
        res.status(200).json({
            msg: 'Code sent to queue',
            cached: false,
            executionId
        });
    }
    catch (err) {
        console.error('Error in /api/compile:', err);
        res.status(500).json({ error: 'Internal server error' });
    }
}));
// Execution ka status/result fetch karo
app.get('/api/execution/:id', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const execution = yield exports.Execution.findOne({ executionId: req.params.id });
        if (!execution) {
            res.status(404).json({ error: 'Execution not found' });
        }
        res.status(200).json(execution);
    }
    catch (err) {
        console.error('Error fetching execution:', err);
        res.status(500).json({ error: 'Internal server error' });
    }
}));
// ─── Start ────────────────────────────────────────────────────────────────────
function start() {
    return __awaiter(this, void 0, void 0, function* () {
        yield connectMongo(); // pehle MongoDB
        yield connectRabbitMQ(); // phir RabbitMQ
        app.listen(PORT, () => {
            console.log(`Server running on ${PORT}`);
        });
    });
}
start();
