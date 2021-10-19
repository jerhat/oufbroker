using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using OUFLib.Logging;
using OUFLib.Messaging.Broker;
using System;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace OUFBroker.Services
{
    public class AppInitializer : IHostedService
    {
        private readonly IOUFBroker _broker;
        private readonly IOUFLogger _logger;
        private readonly IConfiguration _configuration;

        public AppInitializer(IOUFBroker broker, IOUFLogger logger, IConfiguration configuration)
        {
            _broker = broker;
            _logger = logger;
            _configuration = configuration;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.Info($"Starting Version {Assembly.GetExecutingAssembly().GetName().Version}");
            _logger.Info($"API listening on {_configuration.GetValue<string>("ASPNETCORE_URLS")}");

            _broker.Run();
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
