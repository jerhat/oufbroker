using Microsoft.Extensions.Options;
using NUnit.Framework;
using OUFLib.Logging;
using OUFLib.Messaging.Node;
using OUFLib.Model;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace OUFTest.Memory
{
    public class MemoryTests
    {
        private IOptions<OUFLoggerSettings> _loggerSettings;

        private static readonly string _oufbrokerDll = "OUFBroker.dll";

        private static readonly long NB_CONSUMERS = 20;
        private static readonly long NB_PRODUCERS = 10;
        private static readonly long NB_QUEUES = 10;
        private static readonly long NB_TOPICS = 10;
        private static readonly long NB_MESSAGES = 500;

        private static readonly double MEMORY_MAX_INCREASE_PCT = 200 / 100d;

        private static readonly List<string> _queues = new();
        private static readonly List<string> _topics = new();

        private static readonly int PAYLOAD_SIZE = 1024;
        private static readonly int TIMEOUT_PER_MB_ms = 100;

        private static readonly Random _random = new();
        private static byte[] _payload;

        private static int _initialMemoryMb;

        private static long _expectedUniReceived;
        private static long _expectedMultiReceived;

        private static long _expectedUniSent;
        private static long _expectedMultiSent;

        private static int _nbUniReceived = 0;
        private static int _nbMultiReceived = 0;

        private static int _nbUniSent = 0;
        private static int _nbMultiSent = 0;

        private static CancellationTokenSource _cancellationTokenSource = new();

        private SemaphoreSlim _semaphore = new(0);

        private IOUFLogger _logger;
        private TimeSpan _timeout;

        Process _brokerProcess;

        #region Setup/Teardown
        [SetUp]
        public async Task Setup()
        {
            if (!File.Exists(_oufbrokerDll))
            {
                throw new FileNotFoundException(_oufbrokerDll);
            }

            Environment.SetEnvironmentVariable("ROUTER_ENDPOINT", "tcp://*:61616");
            Environment.SetEnvironmentVariable("PUBLISHER_ENDPOINT", "tcp://*:61617");
            Environment.SetEnvironmentVariable("API_KEY", Constants.API_KEY);
            Environment.SetEnvironmentVariable("ASPNETCORE_URLS", "http://127.0.0.1:8443");

            _brokerProcess = new Process()
            {
                StartInfo = new ProcessStartInfo("dotnet", _oufbrokerDll)
                {
                    RedirectStandardOutput = true,
                    CreateNoWindow = true,
                    UseShellExecute = false
                },
                EnableRaisingEvents = true
            };

            _brokerProcess.OutputDataReceived += BrokerProcessOutputDataReceived;

            _brokerProcess.Exited += BrokerProcessExited;

            _brokerProcess.Start();
            _brokerProcess.BeginOutputReadLine();

            _loggerSettings = Options.Create(new OUFLoggerSettings());

            _logger = new OUFLogger(_loggerSettings);

            _payload = Enumerable.Repeat((byte)0, PAYLOAD_SIZE).ToArray();

            for (int i = 0; i < NB_QUEUES; i++)
            {
                _queues.Add($"Q{i}");
            }

            for (int i = 0; i < NB_TOPICS; i++)
            {
                _topics.Add($"T{i}");
            }

            _timeout = TimeSpan.FromMilliseconds(TIMEOUT_PER_MB_ms * NB_MESSAGES * NB_PRODUCERS * NB_TOPICS * PAYLOAD_SIZE / 1024 / 1024);

            _expectedUniReceived = NB_PRODUCERS * NB_MESSAGES * NB_QUEUES;
            _expectedMultiReceived = NB_PRODUCERS * NB_MESSAGES * NB_TOPICS * NB_CONSUMERS;

            _expectedUniSent = NB_PRODUCERS * NB_MESSAGES * NB_QUEUES;
            _expectedMultiSent = NB_PRODUCERS * NB_MESSAGES * NB_TOPICS;

            _logger.Info($"Timeout set to {(int)_timeout.TotalSeconds} seconds");

            await Task.Factory.StartNew(() => LogStatus(_cancellationTokenSource.Token), TaskCreationOptions.LongRunning);
        }

        private void BrokerProcessOutputDataReceived(object sender, DataReceivedEventArgs e)
        {
            _logger.Info($"*** BROKER ***  {e.Data}");
        }

        private void BrokerProcessExited(object sender, EventArgs e)
        {
            _logger.Info($"Broker process exited");
        }

        [TearDown]
        public void TearDown()
        {
            _brokerProcess.Kill();
        }
        #endregion

        [Test]
        public async Task MemoryStressTest()
        {
            await Task.CompletedTask;

            List<Task<bool>> consumerTasks = new();

            for (int i = 0; i < NB_CONSUMERS; i++)
            {
                consumerTasks.Add(AddConsumer($"c{i}"));
            }

            Task.WaitAll(consumerTasks.ToArray());

            Assert.True(consumerTasks.TrueForAll(t => t.Result), "{0} consumers could not connect", consumerTasks.Where(t => !t.Result).Count());

            await Helpers.GC("http://127.0.0.1:8443");
            _initialMemoryMb = _brokerProcess.GetProcessMemoryMb();

            var maxAllowedMemory = (int)(_initialMemoryMb * (1 + MEMORY_MAX_INCREASE_PCT));

            _logger.Info("Initial memory: {0}Mb, Max Allowed Final Memory: {1}Mb (+{2:P0})", _initialMemoryMb, maxAllowedMemory, MEMORY_MAX_INCREASE_PCT);

            List<Task<bool>> producerTasks = new();

            for (int i = 0; i < NB_PRODUCERS; i++)
            {
                producerTasks.Add(StartProducer($"p{i}"));
            }

            Task.WaitAll(producerTasks.ToArray());

            Assert.True(producerTasks.TrueForAll(t => t.Result), "{0} producers could not connect", producerTasks.Where(t => !t.Result).Count());

            if (!await _semaphore.WaitAsync(_timeout))
            {
                _logger.Error($"{nameof(MemoryStressTest)} timeout");

                _cancellationTokenSource.Cancel();

                Assert.AreEqual(_expectedUniReceived, _nbUniReceived, "Received {0} unicast, expected {1}", _nbUniReceived, _expectedUniReceived);
                Assert.AreEqual(_expectedMultiReceived, _nbMultiReceived, "Received {0} multicast, expected {1}", _nbMultiReceived, _expectedMultiReceived);
            }

            _logger.Info(string.Format("Received {0} unicast, expected {1}", _nbUniReceived, _expectedUniReceived));
            _logger.Info(string.Format("Received {0} multicast, expected {1}", _nbMultiReceived, _expectedMultiReceived));

            var finalMemoryBeforeGC = _brokerProcess.GetProcessMemoryMb();

            _logger.Info($"Final Memory before GC: {finalMemoryBeforeGC}Mb");

            await Helpers.GC("http://127.0.0.1:8443");

            var finalMemoryAfterGC = _brokerProcess.GetProcessMemoryMb();

            _logger.Info($"Final Memory after GC: {finalMemoryAfterGC}Mb");

            Assert.Less(finalMemoryAfterGC, maxAllowedMemory, "Memory usage is too high, {0} found but should be no more than {1}", finalMemoryAfterGC, maxAllowedMemory);
        }

        private async Task LogStatus(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                await Task.Delay(10000, token);
                _logger.Info($"[UNI] {_nbUniSent}/{_expectedUniSent} sent, {_nbUniReceived}/{_expectedUniReceived} received - [MULTI] {_nbMultiSent}/{_expectedMultiSent} sent, {_nbMultiReceived}/{_expectedMultiReceived} received");
            }
        }

        private async Task<bool> AddConsumer(string name)
        {
            OUFNode node = new OUFNode(Helpers.GetNodeSettings(name), _logger);

            var connected = await node.ConnectAsync();

            if (!connected)
                return false;

            _queues.ForEach(async q =>
            {
                (await node.SubscribeAsync(Casting.Uni, q, null)).DataReceived += (a, p) => Task.Run(() => CheckResults(ref _nbUniReceived));
            });

            _topics.ForEach(async t =>
            {
                (await node.SubscribeAsync(Casting.Multi, t, null)).DataReceived += (a, p) => Task.Run(() => CheckResults(ref _nbMultiReceived));
            });

            return true;
        }

        private void CheckResults(ref int received)
        {
            Interlocked.Increment(ref received);

            if (_expectedUniReceived == _nbUniReceived && _expectedMultiReceived == _nbMultiReceived)
            {
                _semaphore.Release();
            }
        }

        private async Task<bool> StartProducer(string name)
        {
            OUFNode node = new(Helpers.GetNodeSettings(name), _logger);
            var connected = await node.ConnectAsync();

            if (!connected)
                return false;

            List<Task<bool>> tasks = new();

            for (int i = 0; i < NB_MESSAGES; i++)
            {
                foreach (var q in _queues)
                {
                    await SendMessage(q, Casting.Uni);
                }

                foreach (var t in _topics)
                {
                    await SendMessage(t, Casting.Multi);
                }
            }

            Task.WaitAll(tasks.ToArray());

            Assert.True(tasks.TrueForAll(t => t.Result), "{0} messages could not be sent by producer {1}", tasks.Where(t => !t.Result).Count(), name);

            return true;

            async Task<bool> SendMessage(string address, Casting casting)
            {
                var target = $"c{_random.Next(0, (int)NB_CONSUMERS)}";
                var res = await node.SendAsync(casting, address, target, null, _payload, true);

                if (res == AckStatus.Success)
                {
                    switch (casting)
                    {
                        case Casting.Uni:
                            Interlocked.Increment(ref _nbUniSent);
                            break;
                        case Casting.Multi:
                            Interlocked.Increment(ref _nbMultiSent);
                            break;
                    }
                    return true;
                }
                else
                {
                    _logger.Error($"Could not send {casting} message to {address}: {res}");
                    return false;
                }
            }
        }
    }
}