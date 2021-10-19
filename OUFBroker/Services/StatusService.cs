using OUFLib.Logging;
using OUFLib.Messaging.Broker;
using OUFLib.Model;
using System;
using System.Diagnostics;
using System.Reflection;

namespace OUFBroker.Services
{
    public interface IStatusService
    {
        public BrokerInformation Status { get; }
    }

    public class StatusService : IStatusService
    {
        private readonly IOUFBroker _broker;
        private readonly IOUFLogger _logger;

        public BrokerInformation Status { get; }

        public StatusService(IOUFBroker broker, IOUFLogger logger)
        {
            _logger = logger;
            _broker = broker;

            Status = new BrokerInformation()
            {
                StartedAt = DateTime.UtcNow,
                Version = FileVersionInfo.GetVersionInfo(Assembly.GetExecutingAssembly().Location).ProductVersion ?? "unknown",
                RouterEndPoint = _broker.Settings.Router_EndPoint,
                PublisherEndPoint = _broker.Settings.Publisher_EndPoint
            };
        }
    }
}
