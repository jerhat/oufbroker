using Microsoft.Extensions.Options;
using OUFLib.Messaging.Node;
using OUFLib.Model;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace OUFTest
{
    internal static class Helpers
    {
        private static readonly HttpClient _client;

        static Helpers()
        {
            _client = new HttpClient();
            _client.DefaultRequestHeaders.Add("X-API-KEY", Constants.API_KEY);
        }


        internal static IOptions<OUFNodeSettings> GetNodeSettings(string name, NodeInformation info = null)
        {
            return Options.Create(new OUFNodeSettings()
            {
                Name = name,
                Router_EndPoint = Constants.ROUTER_BIND_ADDRESS,
                Publisher_EndPoint = Constants.PUBLISHER_BIND_ADDRESS,
                NodeInfo = info,
            });
        }

        internal static int GetProcessMemoryMb(this Process p)
        {
            p.Refresh();
            return (int)(p.WorkingSet64 / 1024d / 1024d);
        }

        internal static async Task<bool> GC(string baseurl)
        {
            try
            {
                var response = await _client.PostAsync($"{baseurl}/api/gc", null);
                return response.IsSuccessStatusCode;
            }
            catch
            {
                return false;
            }
        }
    }
}
