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
    $messagesToProcess = [];

    $pool = new DefaultPool;

    $pull = function () use ($pool, &$pullInProgress, &$messagesToProcess) {
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
            $messagesToProcess[] = $message;
        }
    };
    Loop::repeat($msInterval = 1000, $pull);

    Loop::repeat($msInterval = 1000, function () use ($pool, &$messagesToProcess) {
        if ($messagesToProcess === []) {
            return;
        }

        echo "Scheduling an Ack for " . count($messagesToProcess) . " messages\n";

        yield $pool->enqueue(new Worker\CallableTask('ackPubsubMessages', [$messagesToProcess]));
        $messagesToProcess = [];
    });

    Loop::repeat($msInterval = 1000, function () use ($pool, &$messagesToProcess) {
        yield $pool->enqueue(new Worker\CallableTask('publishPubsubMessages', []));
    });
});
