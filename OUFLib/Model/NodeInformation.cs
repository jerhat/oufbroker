using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection;

namespace OUFLib.Model
{
    public class NodeInformation
    {
        public string Version { get; set; } = GetVersion();
        public string Machine { get; set; } = Environment.MachineName;
        public string Build { get; set; } = GetBuild();
        public string IP { get; set; } = GetIPs();

        public bool Persistent { get; set; }
        public object Details { get; set; }
        public List<OUFAction> Actions { get; set; }
        
        public NodeInformation() { }

        public NodeInformation(bool persistent, string details, List<OUFAction> actions)
        {
            Persistent = persistent;
            Details = details;
            Actions = actions;
        }

        private static string GetVersion()
        {
            return Assembly.GetEntryAssembly()?.GetName()?.Version?.ToString();
        }

        private static string GetBuild()
        {
#if DEBUG
            return "Debug";
#else
            return "Release";
#endif
        }

        private static string GetIPs()
        {
            try
            {
                var host = Dns.GetHostEntry(Dns.GetHostName());
                return string.Join("-", host.AddressList.Where(ip => ip.AddressFamily == AddressFamily.InterNetwork).Select(ip => ip.ToString()));
            }
            catch
            {
                return null;
            }
        }
    }
}
