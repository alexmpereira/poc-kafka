<?php

$conf = new RdKafka\Conf();

$kafkaBroker = getenv('KAFKA_BROKER') ?: 'localhost:9092';

$conf->set('metadata.broker.list', $kafkaBroker);
$conf->set('group.id', 'php-consumer-group');

// Start from the earliest message if no offset is found
$conf->set('auto.offset.reset', 'earliest');

// Emit EOF event when we reach the end of a partition
$conf->set('enable.partition.eof', 'true');

$consumer = new RdKafka\KafkaConsumer($conf);

// Subscribe to the topic published by Node.js
$consumer->subscribe(['financial-transactions']);

echo "Connected to Kafka Broker: $kafkaBroker\n";
echo "Waiting for partition assignment...\n";

while (true) {
    // Consume messages with a 120s timeout
    $message = $consumer->consume(120 * 1000);
    
    switch ($message->err) {
        case RD_KAFKA_RESP_ERR_NO_ERROR:
            echo "\n======================================\n";
            echo "[".date('Y-m-d H:i:s')."] Event Received!\n";
            
            $payload = json_decode($message->payload, true);
            echo "-> Transaction ID: " . $payload['id'] . "\n";
            echo "-> User ID: " . $payload['userId'] . "\n";
            echo "-> Amount: $ " . number_format($payload['amount'], 2) . "\n";
            echo "-> Type: " . $payload['type'] . "\n";
            
            // Simulating heavy asynchronous processing
            echo "⏳ Processing transaction (fraud analysis, balance check)...\n";
            sleep(2);
            
            echo "✅ Transaction successfully processed!\n";
            echo "======================================\n";
            break;
            
        case RD_KAFKA_RESP_ERR__PARTITION_EOF:
            // Reached the end of the partition, keep polling
            break;
            
        case RD_KAFKA_RESP_ERR__TIMED_OUT:
            // Keep polling after timeout
            break;
            
        default:
            echo "Error: " . $message->errstr() . "\n";
            break;
    }
}
