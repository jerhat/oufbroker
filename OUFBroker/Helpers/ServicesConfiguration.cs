using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using OUFBroker.Services;
using OUFLib.Locker;
using OUFLib.Logging;
using OUFLib.Messaging.Broker;

namespace OUFBroker.Helpers
{
    public static class ServicesConfiguration
    {
        public static IServiceCollection ConfigureServices(this IServiceCollection services, IConfiguration Configuration)
        {
            services.Configure<OUFBrokerSettings>(options => Configuration.Bind(options));
            services.Configure<OUFLoggerSettings>(options => Configuration.Bind(options));

            services.AddSingleton<IOUFLogger, OUFLogger>();
            services.AddHostedService<AppInitializer>();

            services.AddSingleton<IOUFBroker, OUFLib.Messaging.Broker.OUFBroker>();
            services.AddSingleton<IOUFLocker, OUFLocker>();
            services.AddSingleton<IStatusService, StatusService>(); 

            return services;
        }
    }
}
