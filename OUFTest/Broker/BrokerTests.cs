using Microsoft.Extensions.Options;
using NUnit.Framework;
using OUFLib.Locker;
using OUFLib.Logging;
using OUFLib.Messaging.Broker;
using OUFLib.Messaging.Node;
using OUFLib.Model;
using OUFLib.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace OUFTest.Broker
{
    public class BrokerTests
    {
        private IOptions<OUFBrokerSettings> _brokerSettings;
        private IOptions<OUFLoggerSettings> _loggerSettings;

        private IOUFBroker _broker;
        private IOUFLogger _logger;
        private IOUFLocker _locker;

        #region Setup/Teardown
        [SetUp]
        public async Task Setup()
        {
            await Task.CompletedTask;

            _loggerSettings = Options.Create(new OUFLoggerSettings());
            _brokerSettings = Options.Create(new OUFBrokerSettings()
            {
                Router_EndPoint = Constants.ROUTER_BIND_ADDRESS,
                Publisher_EndPoint = Constants.PUBLISHER_BIND_ADDRESS
            });

            _logger = new OUFLogger(_loggerSettings);
            _locker = new OUFLocker(_logger);

            _broker = new OUFLib.Messaging.Broker.OUFBroker(_brokerSettings, _logger, _locker);
            _broker.Run();
        }

        [TearDown]
        public async Task TearDown()
        {
            await Task.CompletedTask;

            _broker?.Dispose();
        }
        #endregion

        [Test]
        public async Task BasicNode()
        {
            using var node = new OUFNode(Helpers.GetNodeSettings("node1"), _logger);
            var connected = await node.ConnectAsync();

            Assert.IsTrue(connected);

            Assert.IsNotEmpty(_broker.Nodes);
            Assert.AreEqual(node.Name, _broker.Nodes.First().Name);
        }

        [Test]
        public async Task ConsumerProducer()
        {
            using var consumer = new OUFNode(Helpers.GetNodeSettings("consumer"), _logger);
            using var producer = new OUFNode(Helpers.GetNodeSettings("producer"), _logger);

            await consumer.SubscribeAsync(Casting.Uni, "address", null);

            await consumer.ConnectAsync();
            await producer.ConnectAsync();

            var res = await producer.SendAsync(Casting.Uni, "address", null, null, Encoding.ASCII.GetBytes("payload"), true);

            Assert.AreEqual(AckStatus.Success, res);
        }

        [Test]
        public async Task NoConsumer()
        {
            using var consumer = new OUFNode(Helpers.GetNodeSettings("consumer"), _logger);
            using var producer = new OUFNode(Helpers.GetNodeSettings("producer"), _logger);

            await consumer.SubscribeAsync(Casting.Uni, "address", null);

            await consumer.ConnectAsync();
            await producer.ConnectAsync();

            var res = await producer.SendAsync(Casting.Uni, "address_with_no_subscriber", null, null, Encoding.ASCII.GetBytes("payload"), true);

            Assert.AreEqual(AckStatus.Failed, res);
        }

        [Test]
        public async Task NoAck()
        {
            using var producer = new OUFNode(Helpers.GetNodeSettings("producer"), _logger);
            await producer.ConnectAsync();

            var res = await producer.SendAsync(Casting.Uni, "address_with_no_consumer", null, null, Encoding.ASCII.GetBytes("payload"), false);

            Assert.AreEqual(AckStatus.Disabled, res);
        }

        [Test]
        public async Task Unicast()
        {
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
        }

        [Test]
        public async Task UnicastWithTarget()
        {
            var sentBytes = new byte[] { 0x66 };
            var receivedBytes = new List<byte>();

            using var consumer1 = new OUFNode(Helpers.GetNodeSettings("consumer1"), _logger);
            using var consumer2 = new OUFNode(Helpers.GetNodeSettings("consumer2"), _logger);
            using var producer = new OUFNode(Helpers.GetNodeSettings("producer"), _logger);

            (await consumer1.SubscribeAsync(Casting.Uni, "address1", null)).DataReceived += (s, d) => Task.Run(() => receivedBytes.AddRange(d));
            (await consumer2.SubscribeAsync(Casting.Uni, "address2", null)).DataReceived += (s, d) => Task.Run(() => receivedBytes.AddRange(d));

            await consumer1.ConnectAsync();
            await consumer2.ConnectAsync();

            await producer.ConnectAsync();

            var res1 = await producer.SendAsync(Casting.Uni, "address1", "consumer1", null, sentBytes, true);

            await Task.Delay(100);
            Assert.AreEqual(AckStatus.Success, res1);
            Assert.AreEqual(sentBytes, receivedBytes.ToArray());


            var res2 = await producer.SendAsync(Casting.Uni, "address1", "consumer2", null, sentBytes, true);

            await Task.Delay(100);
            Assert.AreEqual(AckStatus.Failed, res2);
        }

        [Test]
        public async Task Multicast()
        {
            var sentBytes = new byte[] { 0x66 };
            var receivedBytes1 = new List<byte>();
            var receivedBytes2 = new List<byte>();

            using var consumer1 = new OUFNode(Helpers.GetNodeSettings("consumer1"), _logger);
            using var consumer2 = new OUFNode(Helpers.GetNodeSettings("consumer2"), _logger);
            using var producer = new OUFNode(Helpers.GetNodeSettings("producer"), _logger);

            (await consumer1.SubscribeAsync(Casting.Multi, "address", null)).DataReceived += (s, d) => Task.Run(() => receivedBytes1.AddRange(d));
            (await consumer2.SubscribeAsync(Casting.Multi, "address", null)).DataReceived += (s, d) => Task.Run(() => receivedBytes2.AddRange(d));

            await consumer1.ConnectAsync();
            await consumer2.ConnectAsync();

            await producer.ConnectAsync();

            var res = await producer.SendAsync(Casting.Multi, "address", null, null, sentBytes, true);

            await Task.Delay(100);

            Assert.AreEqual(AckStatus.Success, res);
            Assert.AreEqual(sentBytes, receivedBytes1.ToArray());
            Assert.AreEqual(sentBytes, receivedBytes2.ToArray());
        }

        [Test]
        public async Task Subscriptions()
        {
            const string nodeName = "consumer";
            const string address = "address";

            using var consumer = new OUFNode(Helpers.GetNodeSettings(nodeName), _logger);

            (await consumer.SubscribeAsync(Casting.Uni, address, null)).DataReceived += (s, d) => Task.CompletedTask;
            await consumer .ConnectAsync();

            await Task.Delay(100);

            var node = _broker.Nodes.FirstOrDefault(n => n.Name == nodeName);

            Assert.NotNull(node);
            Assert.AreEqual(1, node.Subscriptions.Count);
            Assert.AreEqual(address, node.Subscriptions.Select(s => s.Address).First());
        }

        [Test]
        public async Task SelectorMatch()
        {
            var sentBytes = new byte[] { 0x66 };
            var receivedBytes = new List<byte>();

            using var consumer = new OUFNode(Helpers.GetNodeSettings("consumer"), _logger);
            using var producer = new OUFNode(Helpers.GetNodeSettings("producer"), _logger);

            (await consumer.SubscribeAsync(Casting.Multi, "address", "boolean(/KEY[.='1'])")).DataReceived += (s, d) => Task.Run(() => receivedBytes.AddRange(d));

            await consumer .ConnectAsync();
            await producer.ConnectAsync();

            var res = await producer.SendAsync(Casting.Multi, "address", null, new Headers(new KeyValuePair<string, string>("KEY", "1")), sentBytes, true);

            await Task.Delay(100);

            Assert.AreEqual(AckStatus.Success, res);
            Assert.AreEqual(sentBytes, receivedBytes.ToArray());
        }


        [Test]
        public async Task SelectorMisMatch()
        {
            var sentBytes = new byte[] { 0x66 };
            var receivedBytes = new List<byte>();

            using var consumer = new OUFNode(Helpers.GetNodeSettings("consumer"), _logger);
            using var producer = new OUFNode(Helpers.GetNodeSettings("producer"), _logger);

            (await consumer.SubscribeAsync(Casting.Multi, "address", "boolean(/KEY[.='0'])")).DataReceived += (s, d) => Task.Run(() => receivedBytes.AddRange(d));

            await consumer .ConnectAsync();
            await producer.ConnectAsync();

            var res = await producer.SendAsync(Casting.Multi, "address", null, new Headers(new KeyValuePair<string, string>("KEY", "1")) {}, sentBytes, true);

            await Task.Delay(100);

            Assert.AreEqual(AckStatus.Success, res);
            Assert.AreEqual(Array.Empty<byte>(), receivedBytes.ToArray());
        }

        [Test]
        public async Task ReSubscribeUni()
        {
            var sentBytes = new byte[] { 0x66 };
            var receivedBytes = new List<byte>();

            using var consumer = new OUFNode(Helpers.GetNodeSettings("consumer"), _logger);
            using var producer = new OUFNode(Helpers.GetNodeSettings("producer"), _logger);

            (await consumer.SubscribeAsync(Casting.Uni, "address", null)).DataReceived += (s, d) => Task.Run(() => receivedBytes.AddRange(d));

            await consumer.ConnectAsync();
            await producer.ConnectAsync();

            var res1 = await producer.SendAsync(Casting.Uni, "address", null, null, sentBytes, true);

            Assert.AreEqual(AckStatus.Success, res1);
            await Task.Delay(100);
            Assert.AreEqual(sentBytes, receivedBytes.ToArray());

            await consumer.UnsubscribeAsync(Casting.Uni, "address");
            await Task.Delay(100);

            var res2 = await producer .SendAsync(Casting.Uni, "address", null, null, sentBytes, true);
            Assert.AreEqual(AckStatus.Failed, res2);
            Assert.AreEqual(1, receivedBytes.Count);

            (await consumer.SubscribeAsync(Casting.Uni, "address", null)).DataReceived += (s, d) => Task.Run(() => receivedBytes.AddRange(d)) ;
            var res3 = await producer.SendAsync(Casting.Uni, "address", null, null, sentBytes, true);
            Assert.AreEqual(AckStatus.Success, res3);
            await Task.Delay(100);

            Assert.AreEqual(2, receivedBytes.Count);
        }

        [Test]
        public async Task ReSubscribeMulti()
        {
            var sentBytes = new byte[] { 0x66 };
            var receivedBytes = new List<byte>();

            using var consumer = new OUFNode(Helpers.GetNodeSettings("consumer"), _logger);
            using var producer = new OUFNode(Helpers.GetNodeSettings("producer"), _logger);

            (await consumer.SubscribeAsync(Casting.Multi, "address", null)).DataReceived += (s, d) => Task.Run(() => receivedBytes.AddRange(d));

            await consumer.ConnectAsync();
            await producer.ConnectAsync();

            var res1 = await producer.SendAsync(Casting.Multi, "address", null, null, sentBytes, true);

            Assert.AreEqual(AckStatus.Success, res1);
            await Task.Delay(100);
            Assert.AreEqual(sentBytes, receivedBytes.ToArray());

            Assert.True(await consumer.UnsubscribeAsync(Casting.Multi, "address"));
            await Task.Delay(100);

            var res2 = await producer.SendAsync(Casting.Multi, "address", null, null, sentBytes, true);
            Assert.AreEqual(AckStatus.Success, res2);
            Assert.AreEqual(1, receivedBytes.Count);

            (await consumer.SubscribeAsync(Casting.Multi, "address", null)).DataReceived += (s, d) => Task.Run(() => receivedBytes.AddRange(d)) ;
            var res3 = await producer.SendAsync(Casting.Multi, "address", null, null, sentBytes, true);
            Assert.AreEqual(AckStatus.Success, res3);
            await Task.Delay(100);

            Assert.AreEqual(2, receivedBytes.Count);
        }

        [Test]
        public async Task MultipleSubscribersWithOneUnsubscribe()
        {
            var sentBytes = new byte[] { 0x66 };
            var receivedBytes1 = new List<byte>();
            var receivedBytes2 = new List<byte>();

            using var consumer1 = new OUFNode(Helpers.GetNodeSettings("consumer1"), _logger);
            using var consumer2 = new OUFNode(Helpers.GetNodeSettings("consumer2"), _logger);
            using var producer = new OUFNode(Helpers.GetNodeSettings("producer"), _logger);

            (await consumer1.SubscribeAsync(Casting.Multi, "address", null)).DataReceived += (s, d) => Task.Run(() => receivedBytes1.AddRange(d));
            (await consumer2.SubscribeAsync(Casting.Multi, "address", null)).DataReceived += (s, d) => Task.Run(() => receivedBytes2.AddRange(d)) ;

            await consumer1.ConnectAsync();
            await consumer2.ConnectAsync();
            await producer.ConnectAsync();

            var res1 = await producer.SendAsync(Casting.Multi, "address", null, null, sentBytes, true);

            Assert.AreEqual(AckStatus.Success, res1);
            await Task.Delay(100);
            Assert.AreEqual(sentBytes, receivedBytes1.ToArray());
            Assert.AreEqual(sentBytes, receivedBytes2.ToArray());

            Assert.True(await consumer1.UnsubscribeAsync(Casting.Multi, "address"));
            await Task.Delay(100);

            var res2 = await producer.SendAsync(Casting.Multi, "address", null, null, sentBytes, true);
            Assert.AreEqual(AckStatus.Success, res2);
            await Task.Delay(100);

            Assert.AreEqual(1, receivedBytes1.Count);
            Assert.AreEqual(2, receivedBytes2.Count);
        }

        [Test]
        public async Task MultipleSubscriptionsOnSameAddress()
        {
            var sentBytes = new byte[] { 0x66 };
            var receivedBytes1 = new List<byte>();
            var receivedBytes2 = new List<byte>();

            using var consumer = new OUFNode(Helpers.GetNodeSettings("consumer"), _logger);
            using var producer = new OUFNode(Helpers.GetNodeSettings("producer"), _logger);

            (await consumer.SubscribeAsync(Casting.Multi, "address", null)).DataReceived += (s, d) => Task.Run(() => receivedBytes1.AddRange(d));
            Assert.ThrowsAsync<ApplicationException>(async () => await consumer.SubscribeAsync(Casting.Multi, "address", null));

            (await consumer.SubscribeAsync(Casting.Uni, "address", null)).DataReceived += (s, d) => Task.Run(() => receivedBytes1.AddRange(d));
            Assert.ThrowsAsync<ApplicationException>(async () => await consumer.SubscribeAsync(Casting.Uni, "address", null));
        }

        [Test]
        public async Task TriggerAction()
        {
            string receivedStringProp = null;
            DateTime receivedDateTimeProp = DateTime.MinValue;
            int receivedIntProp = int.MinValue;

            var testaction = new OUFAction()
            {
                Name = "TestAction",
                Arguments = new Dictionary<string, Property>()
                        {
                            { "stringProp", new Property(typeof(string), "default") },
                            { "datetimeProp", new Property(typeof(DateTime), new DateTime(2000, 12, 31)) },
                            { "intProp", new Property(typeof(int), 0) }
                        }
            };

            testaction.Triggered += (action, args) =>
            {
                receivedStringProp = action.GetArgumentValue<string>(args, "stringProp");
                receivedDateTimeProp = action.GetArgumentValue<DateTime>(args, "datetimeProp");
                receivedIntProp = action.GetArgumentValue<int>(args, "intProp");
            };

            var info = new NodeInformation(true, "details", new List<OUFAction>() { testaction });

            using var consumer = new OUFNode(Helpers.GetNodeSettings("consumer", info), _logger);

            var connected = await consumer.ConnectAsync();

            Assert.IsTrue(connected);

            Dictionary<string, Property> arguments = new()
            {
                {
                    nameof(OUFAction),
                    new Property(
                        typeof(OUFAction),
                        new OUFAction()
                        {
                            Name = "testaction",
                            Arguments = new Dictionary<string, Property>()
                            {
                                { "stringProp", new Property(typeof(string), "someString") },
                                { "datetimeProp", new Property(typeof(DateTime), DateTime.Today) },
                                { "intProp", new Property(typeof(int), 6) }
                            }
                        }
                    )
                }
            };

            var res = await _broker.ExecuteAsync("consumer",
                NodeTask.TriggerAction,
                arguments
            );

            Assert.AreEqual(AckStatus.Success, res.status);

            await Task.Delay(200);

            Assert.AreEqual("someString", receivedStringProp);
            Assert.AreEqual(DateTime.Today, receivedDateTimeProp);
            Assert.AreEqual(6, receivedIntProp);
        }

        [Test]
        public async Task GetLogs()
        {
            using var consumer = new OUFNode(Helpers.GetNodeSettings("consumer"), _logger);

            await consumer.ConnectAsync();

            var res = await _broker.ExecuteAsync("consumer",
                NodeTask.GetLogs,
                new Dictionary<string, Property>() { { "asof", new Property(typeof(DateTime), DateTime.Today) } }
            );

            Assert.AreEqual(AckStatus.Success, res.status);
            Assert.NotNull(res.data);
        }

        [Test]
        public async Task Status()
        {
            using var consumer = new OUFNode(Helpers.GetNodeSettings("consumer"), _logger);

            await consumer.ConnectAsync();

            var res1 = await _broker.ExecuteAsync("consumer",
                NodeTask.GetStatus,
                null
            );

            Assert.AreEqual(AckStatus.Success, res1.status);

            var status1 = Serializer.Deserialize<NodeStatus>(res1.data);
            Assert.Greater(status1.MemoryUsage, 0);
        }
    }
}