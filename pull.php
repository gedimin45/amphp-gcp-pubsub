<?php

require __DIR__ . '/vendor/autoload.php';

use Amp\Loop;
use Amp\Parallel\Worker;
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

$topic->publish([
    'data' => 'My new message.',
    'attributes' => [
        'location' => 'Detroit'
    ]
]);

//// when there are < x unacknowledged messages, every X seconds
//$messages = $subscription->pull();
//
//$promises = [];
//$messagesToAck = [];
//
//foreach ($messages as $message) {
//    Worker\enqueueCallable('processPulledMessage', $message)->onResolve(function ($error, $data) use ($message) {
//        $messagesToAck[] = $message;
//        return print($data . "\n");
//    });
//}
//
//$responses = Promise\wait(Promise\all($promises));
//
//foreach ($responses as $url => $response) {
//    \printf("Read %d bytes from %s\n", \strlen($response), $url);
//}

Loop::run(function() use ($subscription) {
    $messagesToAck = [];

    Loop::repeat($msInterval = 1000, function () use (&$messagesToAck, $subscription) {
        echo "Scheduling a pull\n";
        Worker\enqueueCallable('pullFromPubsub')->onResolve(function (?\Throwable $error, $response) use (&$messagesToAck) {
            if ($error instanceof \Throwable) {
                print("Encountered error: {$error->getMessage()}.\n");
                if ($error instanceof Amp\Parallel\Worker\TaskFailureError) {
                    print($error->getOriginalMessage() . "\n");
                }
                return;
            }
            $response = unserialize($response);
            echo "Pulled " . count($response['messages']) . " messages in {$response['duration']} s\n";

            foreach ($response['messages'] as $message) {
                Worker\enqueueCallable('processPulledMessage', $message)->onResolve(function (?\Throwable $error, $data) use ($message, &$messagesToAck) {
                    if ($error instanceof \Throwable) {
                        print("Encountered error: {$error->getMessage()}.\n");
                        return;
                    }
                    $messagesToAck[] = $message;
                    print($data . "\n");
                });
            }
        });
    });

    Loop::repeat($msInterval = 1000, function () use (&$messagesToAck, $subscription) {
        if ($messagesToAck === []) {
            return;
        }

        echo "Scheduling an Ack for " . count($messagesToAck) . " messages\n";

        Worker\enqueueCallable('ackPubsubMessages', $messagesToAck)->onResolve(function (?\Throwable $error, $data) use (&$messagesToAck) {
            $messagesToAck = [];
        });
    });
});
