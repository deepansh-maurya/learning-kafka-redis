import http, { IncomingMessage, ServerResponse } from "http";
//@ts-ignore
import { Kafka, Producer, Consumer, KafkaMessage } from "kafkajs";
//@ts-ignore
import { createClient, RedisClientType } from "redis";
/**
 * Singleton class to start HTTP server and handling kafka pubsub and redis in memory cache
 */
export class HttpServer {
    private topic = "testing-kafka"
    private producer: Producer | undefined;
    private consumer: Consumer | undefined
    private port = 500;
    private redis_instance: RedisClientType
    private static _instance: HttpServer;
    private kafka_instance: Kafka;
    private server_instance: http.Server | undefined = undefined;

    static get instance() {
        if (!this._instance) {
            this._instance = new HttpServer();
        }
        return this._instance;
    }

    private constructor() {
        this.kafka_instance = new Kafka({
            clientId: "real-time-page-view",
            brokers: ["localhost:9092"],
        });
    }

    private async connect_producer_and_consumer() {
        this.producer = this.kafka_instance.producer();
        this.consumer = this.kafka_instance.consumer()
        await this.producer?.connect();
        await this.consumer?.connect()
        await this.consumer?.subscribe({ topic: this.topic, fromBeginning: true })
    }

    async initialize_server() {
        if (this.server_instance) return;

        this.server_instance = http
            .createServer(this.callback.bind(this))
            .listen(this.port, () => {
                console.log(`Server running at http://localhost:${this.port}`);
            });

        this.redis_instance = await createClient()
            .on('error', (err: any) => console.log('Redis Client Error', err))
            .connect();

        await this.connect_producer_and_consumer();
        await this.consumer_service()

    }

    private async consumer_service() {
        this.consumer?.run({
            eachMessage: async ({ topic, partition, message }: { topic: string, partition: number, message: KafkaMessage }) => {
                const { key, value } = message

                const keyStr = key?.toString() || ""
                const valueNum = value.toString() || "0"

                const views = parseInt(await this.redis_instance.get(keyStr))

                await this.redis_instance.set(keyStr, views + valueNum)

                if (parseInt(views + valueNum) == 0) await this.redis_instance.del(key)
                else await this.redis_instance.expire(key, 60)

            }
        })
    }

    private async callback(req: IncomingMessage, res: ServerResponse) {
        if (req.method === "POST" && req.url === "/") {
            let message = "";
            req.on("data", (chunk: Buffer) => {
                message += chunk.toString();
            });
            req.on("end", async () => {
                const parsed = JSON.parse(message);
                await this.producer?.send({
                    topic: this.topic,
                    messages: [{ value: JSON.stringify(parsed) }],
                });
                res.writeHead(200);
                res.end("Message sent to Kafka");
            });
        } else {
            res.writeHead(404);
            res.end("Not Found");
        }
    }
}

// Start server
(async () => {
    await HttpServer.instance.initialize_server();
})();
