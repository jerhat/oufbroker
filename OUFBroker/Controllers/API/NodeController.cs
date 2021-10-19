using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.ModelBinding;
using OUFBroker.Services;
using OUFLib.Logging;
using OUFLib.Messaging.Broker;
using OUFLib.Model;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OUFBroker.Controllers.API
{
    [ApiController]
    [Area("api")]
    [Route("[area]/[controller]")]
    public class NodesController : ControllerBase
    {
        private readonly IOUFLogger _logger;
        private readonly IOUFBroker _broker;

        public NodesController(IOUFBroker broker, IOUFLogger logger)
        {
            _logger = logger;
            _broker = broker;
        }

        /// <summary>
        /// Get the list of nodes
        /// </summary>
        [HttpGet]
        public IActionResult Get()
        {
            return new OkObjectResult(_broker.Nodes.Select(x => x.Name));
        }

        /// <summary>
        /// Trigger an action on a node
        /// </summary>
        /// <param name="nodeName">The name of the node</param>
        /// /// <param name="action">The action to trigger</param>
        [HttpPost("{nodeName}/[action]")]
        [Authorize(AuthenticationSchemes = Constants.API_KEY_POLICY_NAME)]
        public async Task<IActionResult> Action(string nodeName, [FromBody] OUFAction action)
        {
            Dictionary<string, Property> arguments = new()
            {
                { nameof(OUFAction), new Property(typeof(OUFAction), action) }
            };

            var result = await _broker.ExecuteAsync(nodeName, NodeTask.TriggerAction, arguments);

            return result.status switch
            {
                AckStatus.Success => Ok(),
                _ => new StatusCodeResult(StatusCodes.Status404NotFound),
            };
        }

        /// <summary>
        /// Kill a node. The node won't be connected anymore.
        /// </summary>
        /// <param name="nodeName">The name of the node</param>
        [HttpPost("{nodeName}/[action]")]
        [Authorize(AuthenticationSchemes = Constants.API_KEY_POLICY_NAME)]
        public async Task<IActionResult> Kill(string nodeName)
        {
            var result = await _broker.ExecuteAsync(nodeName, NodeTask.Kill, null);

            return result.status switch
            {
                AckStatus.Success => Ok(),
                _ => new StatusCodeResult(StatusCodes.Status404NotFound),
            };
        }

        /// <summary>
        /// Disconnect a node. The node will reconnect.
        /// </summary>
        /// <param name="nodeName">The name of the node</param>
        [HttpPost("{nodeName}/[action]")]
        [Authorize(AuthenticationSchemes = Constants.API_KEY_POLICY_NAME)]
        public async Task<IActionResult> Disconnect(string nodeName)
        {
            var result = await _broker.ExecuteAsync(nodeName, NodeTask.Disconnect, null);

            return result.status switch
            {
                AckStatus.Success => Ok(),
                _ => new StatusCodeResult(StatusCodes.Status404NotFound),
            };
        }

        /// <summary>
        /// Get a node's logs.
        /// </summary>
        /// <param name="nodeName">The name of the node</param>
        /// <param name="asof">The date of the logs, e.g. "2021-12-31". Default is Today</param>
        [HttpGet("{nodeName}/[action]")]
        public async Task<IActionResult> Logs(string nodeName, DateTime? asof = null)
        {
            if (null == asof)
            {
                asof = DateTime.UtcNow.Date;
            }

            Dictionary<string, Property> arguments = new()
            {
                {  "asof", new Property(typeof(DateTime), asof.Value ) }
            };

            var result = await _broker.ExecuteAsync(nodeName, NodeTask.GetLogs, arguments);

            if (result.status == AckStatus.Success)
            {
                return File(result.data, "test/plain", $"{nodeName}-logs.{asof.Value:yyyyMMdd}.txt");
            }
            else
            {
                return new StatusCodeResult(StatusCodes.Status404NotFound);
            }
        }

        /// <summary>
        /// Get the status of a node.
        /// </summary>
        /// <param name="nodeName">The name of the node</param>
        [HttpGet("{nodeName}/[action]")]
        public async Task<IActionResult> Status(string nodeName)
        {
            var result = await _broker.ExecuteAsync(nodeName, NodeTask.GetStatus, null);

            var status = null != result.data ? Encoding.ASCII.GetString(result.data) : "";

            return result.status switch
            {
                AckStatus.Success => new OkObjectResult(status),
                _ => new StatusCodeResult(StatusCodes.Status404NotFound),
            };
        }

        /// <summary>
        /// Get a specific node
        /// </summary>
        /// <param name="nodeName">The name of the node</param>
        [HttpGet("{nodeName}")]
        public IActionResult Get(string nodeName)
        {
            var node = _broker?.Nodes?.FirstOrDefault(x => x.Name == nodeName);

            if (null == node)
            {
                return NotFound();
            }
            else
            {
                return new OkObjectResult(node);
            }
        }

        /// <summary>
        /// Get a specific node information 
        /// </summary>
        /// <param name="nodeName">The name of the node</param>
        [HttpGet("{nodeName}/[action]")]
        public IActionResult Info(string nodeName)
        {
            var node = _broker?.Nodes?.FirstOrDefault(x => x.Name == nodeName);

            if (null == node?.Information)
            {
                return NotFound();
            }
            else
            {
                return new OkObjectResult(node.Information);
            }
        }

        /// <summary>
        /// Get the list of available actions for a node
        /// </summary>
        /// <param name="nodeName">The name of the node</param>
        /// <returns>The list of available actions</returns>
        [HttpGet("{nodeName}/[action]")]
        public IActionResult Actions(string nodeName)
        {
            var node = _broker?.Nodes?.FirstOrDefault(x => x.Name == nodeName);

            if (null == node?.Information?.Actions)
            {
                return NotFound();
            }
            else
            {
                return new OkObjectResult(node.Information?.Actions);
            }
        }

        /// <summary>
        /// Get the list of subscriptions of a node
        /// </summary>
        /// <param name="nodeName">The name of the node</param>
        /// <returns></returns>
        [HttpGet("{nodeName}/[action]")]
        public IActionResult Subscriptions(string nodeName)
        {
            var node = _broker?.Nodes?.FirstOrDefault(x => x.Name == nodeName);

            if (null == node?.Subscriptions)
            {
                return NotFound();
            }
            else
            {
                return new OkObjectResult(node.Subscriptions);
            }
        }
    }
}
