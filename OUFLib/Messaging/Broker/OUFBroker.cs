using Microsoft.Extensions.Options;
using NetMQ;
using OUFLib.Locker;
using OUFLib.Logging;
using OUFLib.Model;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace OUFLib.Messaging.Broker
{

    public class OUFBrokerSettings
    {
        public string Router_EndPoint { get; set; }
        public string Publisher_EndPoint { get; set; }
    }

    public interface IOUFBroker : IDisposable
    {
        ICollection<Model.Node> Nodes { get; }
        void Run();
        Task<(AckStatus status, byte[] data)> ExecuteAsync(string nodeName, NodeTask action, Dictionary<string, Property> arguments);
        OUFBrokerSettings Settings { get; }
    }


    public class OUFBroker : IOUFBroker
    {
        private readonly IOUFLogger _logger;
        private readonly OUFBrokerSettings _settings;

        private readonly IOUFRouter _router;
        private readonly IOUFPublisher _publisher;
        public ICollection<Model.Node> Nodes => _router.Nodes;

        public OUFBrokerSettings Settings => _settings;

        public OUFBroker(IOptions<OUFBrokerSettings> settings, IOUFLogger logger, IOUFLocker locker)
        {
            _settings = settings?.Value;
            _logger = logger;

            _router = new OUFRouter(_settings.Router_EndPoint, _logger, locker);
            _publisher = new OUFPublisher(_settings.Publisher_EndPoint, _logger);
        }

        public void Run()
        {
            _router.MulticastMessageReceived += MulticastMessageReceived;
            _router.Run();
            _publisher.Run();
        }

        private void MulticastMessageReceived(NetMQMessage message)
        {
            _publisher.Send(message);
        }

        public async Task<(AckStatus status, byte[] data)> ExecuteAsync(string nodeName, NodeTask action, Dictionary<string, Property> arguments)
        {
            return await _router.ExecuteAsync(nodeName, action, arguments);
        }

        public void Dispose()
        {
            _router.MulticastMessageReceived -= MulticastMessageReceived;

            _router.Dispose();
            _publisher.Dispose();

#if TEST
#else
            NetMQConfig.Cleanup(false);
#endif
            GC.SuppressFinalize(this);
        }
    }
}
