# OUFBroker
OUFBroker is a messaging broker written in C# using [zeromq](https://zeromq.org/).

## Features
- Supports queue mode (unicast) as well as Pub/Sub (multicast);
- REST API provides various actions on the broker and on the connected nodes;

## Usage

```c#
var sentBytes = new byte[] { 0x66 };
var receivedBytes = new List<byte>();

using var consumer1 = new OUFNode(Helpers.GetNodeSettings("consumer1"), _logger);
using var consumer2 = new OUFNode(Helpers.GetNodeSettings("consumer2"), _logger);
using var producer = new OUFNode(Helpers.GetNodeSettings("producer"), _logger);

(await consumer1.SubscribeAsync(Casting.Uni, "address", null)).DataReceived += (s, d) => Task.Run(() => receivedBytes.AddRange(d));
(await consumer2.SubscribeAsync(Casting.Uni, "address", null)).DataReceived += (s, d) => Task.Run(() => receivedBytes.AddRange(d));

await consumer1.ConnectAsync();
await consumer2.ConnectAsync();

await producer.ConnectAsync();

var res = await producer.SendAsync(Casting.Uni, "address", null, null, sentBytes, true);

await Task.Delay(100);

Assert.AreEqual(AckStatus.Success, res);
Assert.AreEqual(sentBytes, receivedBytes.ToArray());
```
