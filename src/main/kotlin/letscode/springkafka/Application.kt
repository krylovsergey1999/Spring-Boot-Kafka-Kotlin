package letscode.springkafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import java.io.Closeable
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.function.Consumer
import kotlin.concurrent.thread

@SpringBootApplication
class Application

fun main(args: Array<String>) {
    runApplication<Application>(*args)

    val topic = "spring-kafka-demo"

    val producer = ProducerCustom(topic)
    thread {
        (1..100).forEach { i ->
            producer.send(i.toString(), "Hello from MyProducer!")
            TimeUnit.SECONDS.sleep(5)
        }
    }

    val consumer = ConsumerCustom(topic)
    consumer.consume(Consumer { record ->
        println("Got key: ${record.key()}, value: ${record.value()}")
    })

    TimeUnit.MINUTES.sleep(10)
    producer.close()
    consumer.close()
}

class ConsumerCustom(private val topic: String) : Closeable {
    private val consumer = getConsumer()

    private fun getConsumer(): KafkaConsumer<String, String> {
        val props = Properties()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        props[ConsumerConfig.GROUP_ID_CONFIG] = "groupId" // некий ИД consumer
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name

        val kafkaConsumer = KafkaConsumer<String, String>(props)
        kafkaConsumer.subscribe(listOf(topic))
        return kafkaConsumer
    }

    fun consume(recordConsumer: Consumer<ConsumerRecord<String, String>>) {
        // new Thread(lambda).start()
        // Это будет в отдельном потоке
        thread {
            while (true) {
                val records = consumer.poll(Duration.ofSeconds(1))
                records.forEach { record -> recordConsumer.accept(record) }
            }
        }
    }

    override fun close() {
        consumer.close()
    }
}

class ProducerCustom(private val topic: String) : Closeable {
    private val producer = getProducer()

    private fun getProducer(): KafkaProducer<String, String> {
        val props = Properties()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        props[ProducerConfig.CLIENT_ID_CONFIG] = "clientId"
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name

        return KafkaProducer<String, String>(props)
    }

    fun send(key: String, value: String) {
        producer
                .send(ProducerRecord(topic, key, value))
                .get()
    }

    override fun close() {
        producer.close()
    }
}