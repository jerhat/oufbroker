using Microsoft.Extensions.Options;
using NUnit.Framework;
using OUFLib.Locker;
using OUFLib.Logging;
using OUFLib.Messaging.Broker;
using OUFLib.Messaging.Node;
using OUFLib.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace OUFTest.Locker
{
    public class LockerTests
    {
        private IOptions<OUFBrokerSettings> _brokerSettings;
        private IOptions<OUFLoggerSettings> _loggerSettings;

        private IOUFBroker _broker;
        private IOUFLogger _logger;
        private IOUFLocker _locker;


        #region Setup/Teardown
        [SetUp]
        public void Setup()
        {
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
        public void TearDown()
        {
            _broker?.Dispose();
        }
        #endregion

        [Test]
        public async Task LockerNoReentry()
        {
            using var consumer = new OUFNode(Helpers.GetNodeSettings("consumer"), _logger);

            await consumer.ConnectAsync();

            bool res1 = false, res2 = false;

            var task1 = Task.Run(async () => res1 = await consumer.TryAcquireLockAsync("resource1", TimeSpan.Zero, TimeSpan.MaxValue, false));
            var task2 = Task.Run(async () => res2 = await consumer.TryAcquireLockAsync("resource1", TimeSpan.Zero, TimeSpan.MaxValue, false));

            Task.WaitAll(new[] { task1, task2 });

            Assert.True(res1 ^ res2);

            consumer.Disconnect();

            await Task.CompletedTask;
        }

        [Test]
        public async Task LockerReentry()
        {
            using var consumer = new OUFNode(Helpers.GetNodeSettings("consumer"), _logger);

            await consumer.ConnectAsync();

            bool res1 = false, res2 = false;

            var task1 = Task.Run(async () => res1 = await consumer.TryAcquireLockAsync("resource1", TimeSpan.Zero, TimeSpan.MaxValue, true));
            var task2 = Task.Run(async () => res2 = await consumer.TryAcquireLockAsync("resource1", TimeSpan.Zero, TimeSpan.MaxValue, true));

            Task.WaitAll(new[] { task1, task2 });

            Assert.True(res1 && res2);

            await Task.CompletedTask;
        }


        [Test]
        public async Task AutoUnlock()
        {
            var unlockTimespan = TimeSpan.FromSeconds(1);

            using var consumer1 = new OUFNode(Helpers.GetNodeSettings("consumer1"), _logger);
            using var consumer2 = new OUFNode(Helpers.GetNodeSettings("consumer2"), _logger);

            await consumer1.ConnectAsync();
            await consumer2.ConnectAsync();

            bool res1 = await consumer1.TryAcquireLockAsync("resource", TimeSpan.Zero, unlockTimespan, false);

            Assert.True(res1);

            await Task.Delay((int)unlockTimespan.TotalMilliseconds + 500);

            bool res2 = await consumer2.TryAcquireLockAsync("resource", TimeSpan.Zero, TimeSpan.MaxValue, false);

            Assert.True(res2);
        }


        [Test]
        public async Task LockBlast()
        {
            int nbNodes = 10;
            int nbLocksAcquired = 0;

            List<Task> lockTasks = new();
            List<Task> unlockTasks = new();

            for (var i = 0; i < nbNodes; i++)
            {
                var node = new OUFNode(Helpers.GetNodeSettings($"node{i}"), _logger);
                var connected = await node.ConnectAsync();

                Assert.True(connected);

                lockTasks.Add(Task.Run(async () =>
                {
                    if (await node.TryAcquireLockAsync("resource", TimeSpan.Zero, TimeSpan.MaxValue, false))
                    {
                        Interlocked.Increment(ref nbLocksAcquired);
                    }

                    node.Disconnect();
                }));
            }

            Task.WaitAll(lockTasks.ToArray());

            Assert.AreEqual(1, nbLocksAcquired);

            await Task.CompletedTask;
        }


        [Test]
        public async Task AcquisitionTimespan()
        {
            var acquisitionTimespan = TimeSpan.FromSeconds(5);

            using var consumer1 = new OUFNode(Helpers.GetNodeSettings("consumer1"), _logger);
            using var consumer2 = new OUFNode(Helpers.GetNodeSettings("consumer2"), _logger);

            await consumer1.ConnectAsync();
            await consumer2.ConnectAsync();

            bool res1 = await consumer1.TryAcquireLockAsync("resource", TimeSpan.Zero, TimeSpan.MaxValue, false);
            Assert.True(res1);

            bool res2 = false;
            var t = Task.Run(async () => res2 = await consumer2.TryAcquireLockAsync("resource", acquisitionTimespan, TimeSpan.MaxValue, false));

            await Task.Delay((int)(acquisitionTimespan.TotalMilliseconds / 2));

            await consumer1.ReleaseLockAsync("resource");

            await t;

            Assert.True(res2);
        }


        [Test]
        public async Task NoAutoUnlockWhenProperlyUnlocked()
        {

            using var consumer1 = new OUFNode(Helpers.GetNodeSettings("consumer1"), _logger);
            using var consumer2 = new OUFNode(Helpers.GetNodeSettings("consumer2"), _logger);

            await consumer1.ConnectAsync();
            await consumer2.ConnectAsync();

            Assert.True(await consumer1.TryAcquireLockAsync("resource", TimeSpan.Zero, TimeSpan.FromSeconds(2), false));

            await consumer1.ReleaseLockAsync("resource");

            await Task.Delay(TimeSpan.FromSeconds(1));

            Assert.True(await consumer1.TryAcquireLockAsync("resource", TimeSpan.Zero, TimeSpan.MaxValue, false));

            await Task.Delay(TimeSpan.FromSeconds(2));

            //consumer2 should not be allowed to lock the resource, since consumer1 still holds the lock
            Assert.False(await consumer2.TryAcquireLockAsync("resource", TimeSpan.Zero, TimeSpan.MaxValue, false));
        }
    }
}
