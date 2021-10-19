using NetMQ;
using NetMQ.Monitoring;
using NetMQ.Sockets;
using OUFLib.Logging;
using OUFLib.Model;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace OUFLib.Messaging.Node
{
    internal interface IOUFSubscriber : IDisposable
    {
        void Connect();
        bool IsConnected { get; }
        Subscription Subscribe(string address, string selector);
        bool Unsubscribe(string address);

        event Action<IOUFSubscriber> Connected;
        event Action<IOUFSubscriber> Disconnected;
    }

    internal class OUFSubscriber : IOUFSubscriber
    {
        private SubscriberSocket _socket;

        private NetMQPoller _poller;
        private NetMQMonitor _monitor;

        private readonly string _brokerAddress;

        private readonly ConcurrentDictionary<string, Subscription> _subscriptions;

        private readonly IOUFLogger _logger;
        private readonly string _id;

        public event Action<IOUFSubscriber> Connected;
        public event Action<IOUFSubscriber> Disconnected;

        public bool IsConnected { get; private set; }

        public OUFSubscriber(string brokerAddress, string id, IOUFLogger logger)
        {
            _brokerAddress = brokerAddress;
            _id = id;

            _subscriptions = new ConcurrentDictionary<string, Subscription>();

            _logger = logger;
        }

        public void Connect()
        {
            if (IsConnected)
                return;

            // if the socket exists dispose it and re-create one
            if (_socket is not null)
            {
                _socket.Unbind(_brokerAddress);
            }

            _socket = new SubscriberSocket();

            // hook up the received message processing method before the socket is connected
            _socket.ReceiveReady += MessageReceived;

            _poller = new NetMQPoller() { _socket };

            if (_monitor is not null)
            {
                _monitor.DetachFromPoller();
                _monitor.Dispose();
            }

            _monitor = new NetMQMonitor(_socket, $"inproc://MONITOR.MULTI.{_id}.{Guid.NewGuid()}", SocketEvents.Connected | SocketEvents.Disconnected);

            _monitor.AttachToPoller(_poller);

            _monitor.Connected += (s, e) => MonitorConnected();
            _monitor.Disconnected += (s, e) => MonitorDisconnected();

            _socket.Connect(_brokerAddress);

            _poller.RunAsync(nameof(OUFSubscriber));
        }

        public Subscription Subscribe(string address, string selector)
        {
            if (_subscriptions.ContainsKey(address))
                throw new ApplicationException($"Address '{address}' already subscribed!");

            var subscription = new Subscription(Casting.Multi, address, selector);

            if (!subscription.IsValid())
            {
                _logger?.Error($"{nameof(Subscribe)}: invalid subscription '{subscription}'");
                return subscription;
            }

            lock (_subscriptions)
            {
                subscription = _subscriptions.GetOrAdd(address, subscription);
                subscription.UpdateSelector(selector);

                if (IsConnected)
                {
                    SendSubscription(subscription);
                }
            }

            return subscription;
        }

        private void SendSubscription(Subscription subscription)
        {
            _socket.Subscribe(subscription.Address);
        }

        public bool Unsubscribe(string address)
        {
            _socket.Unsubscribe(address);
            return _subscriptions.Remove(address, out _);
        }

        private void MonitorDisconnected()
        {
            IsConnected = false;
            _logger?.Info($"Subscriber '{_id}' disconnected from broker at '{_brokerAddress}'");
            Disconnected?.Invoke(this);
        }

        private void MonitorConnected()
        {
            _logger?.Info($"Publisher '{_id}' connected to broker at '{_brokerAddress}'");

            lock (_subscriptions)
            {
                foreach (var subscription in _subscriptions.Values)
                {
                    SendSubscription(subscription);
                }

                IsConnected = true;
            }

            Connected?.Invoke(this);
        }

        private void MessageReceived(object sender, NetMQSocketEventArgs args)
        {
            ///     BROKER  -> NODE:  [address][selector][payload]
            NetMQMessage request;

            try
            {
                // a message has arrived process it
                request = _socket.ReceiveMultipartMessage();
            }
            catch (Exception e)
            {
                _logger?.Error($"{nameof(OUFSubscriber)}: {nameof(MessageReceived)}: {e}");
                return;
            }

            Task.Run(async () =>
            {
                _logger?.Debug($"Multicast received '{request}'");

                var (address, headers, payload) = Unwrap(request);

                if (!_subscriptions.TryGetValue(address, out var subscription))
                {
                    _logger?.Warn($"{nameof(MessageReceived)}: no related subscription found for '{address}'");
                    return;
                }

                if (subscription.Match(Casting.Multi, address, headers, out var e))
                {
                    await subscription.Raise(address, payload);
                }
                else
                {
                    if (null != e)
                    {
                        _logger?.Error($"{nameof(OUFSubscriber)}: error matching selector {e}");
                    }
                }
            });
        }
       
        /// <summary>
        /// Check the message envelope for errors
        /// and get the command embedded.
        /// The message will be altered!
        /// </summary>
        /// <param name="request">NetMQMessage received</param>
        /// <returns>the received command</returns>
        private (string address, Headers headers, byte[] payload) Unwrap(NetMQMessage request)
        {
            ///     BROKER  -> NODE:  [SOME.ADDRESS][e][DESKID=1][SOME.PAYLOAD]
            
            var expectedFrameCount = 4;

            if (request.FrameCount != expectedFrameCount)
            {
                var message = $"Malformed request received. Expected '{expectedFrameCount}' frames, got '{request.FrameCount}'";
                _logger?.Error($"{nameof(OUFSubscriber)}: {message}");
                _logger?.Debug(request.ToString());

                throw new ApplicationException(message);
            }

            var address = request.Pop().ConvertToString();

            request.Pop();

            var headersFrame = request.Pop();
            var headers = new Headers(headersFrame.ConvertToString());

            var payload = request.Pop().ToByteArray();

            return (address, headers, payload);
        }

        public void Dispose()
        {
            try { _socket?.Unbind(_brokerAddress); } catch { }
            try { _monitor?.DetachFromPoller(); } catch { }

            try { _poller?.RemoveAndDispose(_socket); } catch { }
            try { _monitor?.Stop(); } catch { }
            try { _poller?.Stop(); } catch { }

            try { _monitor?.Dispose(); } catch { }
            try { _poller?.Dispose(); } catch { }

            GC.SuppressFinalize(this);
        }
    }
}
