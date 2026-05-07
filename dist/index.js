"use strict";
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
const amqplib_1 = __importDefault(require("amqplib"));
const cors_1 = __importDefault(require("cors"));
const express_1 = __importDefault(require("express"));
const uuid_1 = require("uuid");
const app = (0, express_1.default)();
const PORT = 3001;
// CORS configuration
const corsOptions = {
    origin: '*',
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization'],
    credentials: true,
    optionsSuccessStatus: 204
};
// Apply CORS before other middleware
app.use((0, cors_1.default)(corsOptions));
app.use(express_1.default.json());
let channel, connection;
connect();
function connect() {
    return __awaiter(this, void 0, void 0, function* () {
        try {
            const amqpServer = 'amqp://localhost:5672'; //amqp means Advanced Message Queuing Protocol and here i have provided the link for the amqp server also rabbitmq have default port as 5672
            connection = yield amqplib_1.default.connect(amqpServer); //establising connection to amqp server 
            channel = yield connection.createChannel(); //creating channel for amqp server 
            yield channel.assertQueue('CodeSender'); // here i am using the channel we have created and putting in queue name as CodeSender 
        }
        catch (err) {
            console.log(`unable to connect to RabbitMq please make sure that the server url is correct or server is active  ${err}`);
        }
    });
}
app.post('/api/compile', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const data = req.body;
    const executionId = (0, uuid_1.v4)();
    channel.sendToQueue('CodeSender', Buffer.from(JSON.stringify(Object.assign(Object.assign({}, data), { executionId, date: new Date() }))));
    res.status(200).json({
        msg: `Code sent to queue`,
        executionId
    });
}));
app.listen(PORT, () => {
    console.log(`Server running on ${PORT}`);
});
