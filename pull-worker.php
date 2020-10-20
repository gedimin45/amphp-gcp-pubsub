<?php

use Google\Cloud\PubSub\PubSubClient;

function processPulledMessage(\Google\Cloud\PubSub\Message $message)
{
    return "Processing message ID {$message->id()} with data: {$message->data()}";
}

function pullFromPubsub()
{
    $pubSub = new PubSubClient();
    $subscription = $pubSub->subscription('my_subscription', 'amphp-tryout');

    $pullStartedAt = microtime(true);
    $messagesToProcess = $subscription->pull();
    $pullDuration = microtime(true) - $pullStartedAt;

    return serialize([
        'duration' => $pullDuration,
        'messages' => $messagesToProcess,
    ]);
}

function ackPubsubMessages($messages)
{
    $pubSub = new PubSubClient();
    $subscription = $pubSub->subscription('my_subscription', 'amphp-tryout');
    $subscription->acknowledgeBatch($messages);
}
