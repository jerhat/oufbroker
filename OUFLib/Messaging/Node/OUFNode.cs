using Microsoft.Extensions.Options;
using NetMQ;
using OUFLib.Locker;
using OUFLib.Logging;
using OUFLib.Model;
using System;
using System.Threading;
using System.Threading.Tasks;
using System.Web;

namespace OUFLib.Messaging.Node
{
    public delegate NodeStatus GetNodeStatus();

    public class OUFNodeSettings
    {
        public string Name { get; set; }
        public string Router_EndPoint { get; set; }
        public string Publisher_EndPoint { get; set; }
        public NodeInformation NodeInfo { get; set; } = new NodeInformation();
        public GetNodeStatus GetStatus { get; set; } = () => new NodeStatus();
    }

    public interface IOUFNode : IDisposable
    {
        string Name { get; }

        Task<bool> ConnectAsync();
        void Disconnect();

        event Action<IOUFNode> Connected;
        event Action<IOUFNode> Disconnected;
        event Action<IOUFNode> Killed;

        bool IsConnected { get; }

        /// <summary>
        /// Send a payload to a node.
        /// </summary>
        /// <param name="casting">Unicast or Multicast.</param>
        /// <param name="address">The address which the payload will be sent to.</param>
        /// <param name="targetNode">The target node. Only available for Unicast mode. If null then payload will be sent to a single node</param>
        /// <param name="headers">Message headers</param>
        /// <param name="payload">The payload</param>
        /// <param name="ack">Whether to wait for an acknowledgement or not.</param>
        /// <returns>Success if payload could be delivered. Failed if no node could be contacted. Timeout if timed out. Disabled if ack is false.</returns>
        Task<AckStatus> SendAsync(Casting casting, string address, string targetNode, Headers headers, byte[] payload, bool ack);
        Task<AckStatus> ProbeAsync(string node);

        Task<Subscription> SubscribeAsync(Casting casting, string address, string selector);
        Task<bool> UnsubscribeAsync(Casting casting, string address);

        Task<bool> TryAcquireLockAsync(string resource, TimeSpan acquisitionTimeout, TimeSpan autoUnlockTimeout, bool allowSelfReentry);
        Task ReleaseLockAsync(string resource);

        void Cleanup();
    }

    public class OUFNode : IOUFNode
    {
        private readonly IOUFDealer _dealer;
        private readonly IOUFSubscriber _subscriber;

        private readonly IOptions<OUFNodeSettings> _settings;

        private readonly IOUFLogger _logger;

        public event Action<IOUFNode> Connected;
        public event Action<IOUFNode> Disconnected;
        public event Action<IOUFNode> Killed;

        private readonly SemaphoreSlim _connectedSemaphore;

        public string Name { get; private set; }

        private volatile bool _isConnected;

        public OUFNode(IOptions<OUFNodeSettings> settings, IOUFLogger logger)
        {
            _logger = logger;

            if (string.IsNullOrWhiteSpace(settings?.Value.Publisher_EndPoint))
                throw new ApplicationException($"{nameof(OUFNodeSettings.Publisher_EndPoint)} must be specified");

            if (string.IsNullOrWhiteSpace(settings?.Value.Router_EndPoint))
                throw new ApplicationException($"{nameof(OUFNodeSettings.Router_EndPoint)} must be specified");

            _settings = settings;

            if (null == _settings?.Value?.Name)
                throw new ApplicationException($"{nameof(OUFNode)}: name is required!");

            Name = HttpUtility.UrlEncode(_settings.Value.Name);

            _dealer = new OUFDealer(_settings.Value.Router_EndPoint, Name, _settings.Value.NodeInfo, _settings.Value.GetStatus, _logger);
            _dealer.Connected += DealerConnected;
            _dealer.Disconnected += DealerDisconnected;
            _dealer.Killed += DealerKilled;

            _subscriber = new OUFSubscriber(_settings.Value.Publisher_EndPoint, Name, _logger);
            _subscriber.Connected += SubscriberConnected;
            _subscriber.Disconnected += SubscriberDisconnected;

            _connectedSemaphore = new SemaphoreSlim(0);
        }

        public bool IsConnected
        {
            get => _isConnected;
            set
            {
                _isConnected = value;

                if (value)
                {
                    Connected?.Invoke(this);
                    _connectedSemaphore.Release();
                }
                else
                {
                    Disconnected?.Invoke(this);
                }
            }
        }

        public async Task<bool> ConnectAsync()
        {
            _dealer.Connect();
            _subscriber.Connect();

            var connected = await _connectedSemaphore.WaitAsync(Constants.NODE_CONNECTION_TIMEOUT_ms);

            if (connected)
            {
                return true;
            }
            else
            {
                _logger.Error($"{nameof(OUFNode)} - {nameof(ConnectAsync)} timeout for node '{Name}'");
                return false;
            }
        }

        public void Disconnect()
        {
            
        }

        private void DealerKilled(IOUFDealer dealer)
        {
            Killed?.Invoke(this);
        }

        public async Task<AckStatus> SendAsync(Casting casting, string address, string targetNode, Headers headers, byte[] payload, bool ack)
        {
            return await _dealer.CastAsync(casting, address, targetNode, headers, payload, ack);
        }

        public async Task<Subscription> SubscribeAsync(Casting casting, string address, string selector)
        {
            switch (casting)
            {
                case Casting.Uni:
                    return await _dealer.SubscribeAsync(casting, address, selector);
                case Casting.Multi:
                    await _dealer.SubscribeAsync(casting, address, selector);
                    return _subscriber.Subscribe(address, selector);
                default:
                    _logger?.Error($"Node {Name}: unknown casting '{casting}' provided to {nameof(SubscribeAsync)}");
                    return default;
            }
        }

        public async Task<bool> UnsubscribeAsync(Casting casting, string address)
        {
            switch (casting)
            {
                case Casting.Uni:
                    return await _dealer.UnsubscribeAsync(casting, address);
                case Casting.Multi:
                    var ok = await _dealer.UnsubscribeAsync(casting, address);
                    return ok && _subscriber.Unsubscribe(address);
                default:
                    _logger?.Error($"Node {Name}: unknown casting '{casting}' provided to {nameof(UnsubscribeAsync)}");
                    return false;
            }
        }

        public async Task<AckStatus> ProbeAsync(string node)
        {
            return await _dealer.ProbeAsync(node);
        }

        public async Task<bool> TryAcquireLockAsync(string resource, TimeSpan acquisitionTimeout, TimeSpan autoUnlockTimeout, bool allowSelfReentry)
        {
            var lockInfo = new Lock()
            {
                OwnerId = Name,
                AcquisitionTimeout = acquisitionTimeout,
                AutoUnlockTimeout = autoUnlockTimeout,
                AllowSelfReentry = allowSelfReentry
            };

            return await _dealer.AcquireLockAsync(resource, lockInfo) == AckStatus.Success;
        }

        public async Task ReleaseLockAsync(string resource)
        {
            await _dealer.ReleaseLockAsync(resource, Name);
        }

        private void SubscriberDisconnected(IOUFSubscriber subscriber)
        {
            IsConnected = false;
        }

        private void SubscriberConnected(IOUFSubscriber subscriber)
        {
            if (_dealer.IsConnected && !_isConnected)
            {
                IsConnected = true;
            }
        }

        private void DealerDisconnected(IOUFDealer dealer)
        {
            IsConnected = false;
        }

        private void DealerConnected(IOUFDealer dealer)
        {
            if (_subscriber.IsConnected && !_isConnected)
            {
                IsConnected = true;
            }
        }

        public void Dispose()
        {
            _dealer.Connected -= DealerConnected;
            _dealer.Disconnected -= DealerDisconnected;
            _dealer.Killed -= DealerKilled;
            _dealer.Dispose();

            _subscriber.Connected -= SubscriberConnected;
            _subscriber.Disconnected -= SubscriberDisconnected;
            _subscriber.Dispose();

            GC.SuppressFinalize(this);
        }

        public void Cleanup()
        {
            NetMQConfig.Cleanup(false);
        }
    }
}
