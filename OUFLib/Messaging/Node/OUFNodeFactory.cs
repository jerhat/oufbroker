using Microsoft.Extensions.Options;
using OUFLib.Logging;

namespace OUFLib.Messaging.Node
{
    public interface IOUFNodeFactory
    {
        IOUFNode CreateInstance(OUFNodeSettings settings);
    }

    public class OUFNodeFactory : IOUFNodeFactory
    {
        private readonly IOUFLogger _logger;

        public OUFNodeFactory(IOUFLogger logger)
        {
            _logger = logger;
        }

        public IOUFNode CreateInstance(OUFNodeSettings settings)
        {
            return new OUFNode(Options.Create(settings), _logger);
        }
    }
}
