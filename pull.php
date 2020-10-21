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

Loop::run(function() {
    $pullInProgress = false;
    $messagesToAck = [];

    $pool = new DefaultPool;

    $pull = function () use ($pool, &$pullInProgress, &$messagesToAck) {
        if ($pullInProgress) {
            echo "Pull already in progress\n";
            return;
        }
        echo "Scheduling a pull\n";

        $pullInProgress = true;

        $response = yield $pool->enqueue(new Worker\CallableTask('pullFromPubsub', []));

        $pullInProgress = false;

        echo "Pulled " . count($response['messages']) . " messages in {$response['duration']} s\n";

        foreach ($response['messages'] as $message) {
            yield $pool->enqueue(new Worker\CallableTask('processPulledMessage', [$message]));
            $messagesToAck[] = $message;
        }
    };
    Loop::repeat($msInterval = 1000, $pull);

    Loop::repeat($msInterval = 1000, function () use ($pool, &$messagesToAck) {
        if ($messagesToAck === []) {
            return;
        }

        echo "Scheduling an Ack for " . count($messagesToAck) . " messages\n";

        yield $pool->enqueue(new Worker\CallableTask('ackPubsubMessages', [$messagesToAck]));
        $messagesToAck = [];
    });

    Loop::repeat($msInterval = 500, function () use ($pool, &$messagesToAck) {
        yield $pool->enqueue(new Worker\CallableTask('publishPubsubMessages', []));
    });
});
