<?php

require __DIR__ . '/vendor/autoload.php';

use Amp\Loop;
use Amp\Parallel\Worker;
use Amp\Parallel\Worker\DefaultPool;
use Google\Cloud\PubSub\PubSubClient;

$pubSub = new PubSubClient();

$topic = $pubSub->topic('amphp-tryout');
if (!$topic->exists()) {
    echo "Creating topic\n";
    $pubSub->createTopic('amphp-tryout');
}

$subscription = $pubSub->subscription('my_subscription', 'amphp-tryout');
if (!$subscription->exists()) {
    echo "Creating subscription\n";
    $subscription->create();
}

function printDebug(string $str): void
{
    if (array_key_exists('debug', getopt('', ['debug']))) {
        print($str);
    }
}

Loop::run(function () {
    $pullInProgress = false;
    $messagesToAck = [];
    $processedMessageCount = 0;
    $startTime = time();

    $pool = new DefaultPool;

    $pull = function () use (&$processedMessageCount, $pool, &$pullInProgress, &$messagesToAck) {
        if ($pullInProgress) {
            printDebug("Skipping pull since one is already in progress\n");
            return;
        }

        if (count($messagesToAck) > 1000) {
            printDebug("Skipping pull since there are " . count($messagesToAck) . " messages being processed\n");
            return;
        }

        printDebug("Scheduling a pull\n");

        $pullInProgress = true;

        $response = yield $pool->enqueue(new Worker\CallableTask('pullFromPubsub', []));

        $pullInProgress = false;

        printDebug("Pulled " . count($response['messages']) . " messages in {$response['duration']} s\n");

        /** @var \Google\Cloud\PubSub\Message $message */
        foreach ($response['messages'] as $message) {
            yield $pool->enqueue(new Worker\CallableTask('processPulledMessage', [$message]));
            $messagesToAck[] = $message;
            $processedMessageCount = $processedMessageCount+1;
        }
    };
    Loop::repeat($msInterval = 100, $pull);

    Loop::repeat($msInterval = 1000, function () use ($pool, &$messagesToAck) {
        if ($messagesToAck === []) {
            return;
        }

        printDebug("Scheduling an Ack for " . count($messagesToAck) . " messages\n");

        yield $pool->enqueue(new Worker\CallableTask('ackPubsubMessages', [$messagesToAck]));
        $messagesToAck = [];
    });

    Loop::repeat($msInterval = 500, function () use ($pool) {
        yield $pool->enqueue(new Worker\CallableTask('publishPubsubMessages', []));
    });

    Loop::repeat($msInterval = 1500, function () use (&$startTime, &$processedMessageCount) {
        $secondsRunning = time() - $startTime;
        $messagesPerSecond = round($processedMessageCount / $secondsRunning, 2);
        print("Processed {$processedMessageCount} messages in total ($messagesPerSecond messages/s)\n");
    });
});
