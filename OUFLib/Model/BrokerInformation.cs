using System;

namespace OUFLib.Model
{
    public class BrokerInformation
    {
        public string RouterEndPoint { get; set; }
        public string PublisherEndPoint { get; set; }
        public DateTime StartedAt { get; set; }
        public string Version { get; set; }
        public long? MemoryUsage => System.Diagnostics.Process.GetCurrentProcess()?.WorkingSet64;
    }
}
