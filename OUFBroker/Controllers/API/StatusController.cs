using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using OUFBroker.Services;
using OUFLib.Logging;
using System;
using System.Threading.Tasks;

namespace OUFBroker.Controllers.API
{
    [ApiController]
    [Area("api")]
    [Route("[area]")]
    public class StatusController : ControllerBase
    {
        private readonly IOUFLogger _logger;
        private readonly IStatusService _statusService;

        public StatusController(IStatusService statusService, IOUFLogger logger)
        {
            _logger = logger;
            _statusService = statusService;
        }

        /// <summary>
        /// Get the broker status
        /// </summary>
        /// <returns></returns>
        [HttpGet("[controller]")]
        public IActionResult Get()
        {
            return Ok(_statusService.Status);
        }

        /// <summary>
        /// Get the broker's logs.
        /// </summary>
        /// <param name="asof">The date of the logs, e.g. "2021-12-31". Default is Today</param>
        [HttpGet("[action]")]
        public IActionResult Logs(DateTime? asof = null)
        {
            if (null == asof)
            {
                asof = DateTime.UtcNow.Date;
            }

            var data = _logger.GetLogFile(asof.Value);

            if (null == data)
            {
                return new StatusCodeResult(StatusCodes.Status404NotFound);
            }
            else
            {
                return File(data, "test/plain", $"broker-logs.{asof.Value:yyyyMMdd}.txt");
            }
        }

        /// <summary>
        /// Trigger the garbage collector on the broker
        /// </summary>
        [HttpPost("[action]")]
        [Authorize(AuthenticationSchemes = Constants.API_KEY_POLICY_NAME)]
        public async Task<IActionResult> GC()
        {
            System.GC.Collect(System.GC.MaxGeneration, GCCollectionMode.Forced, blocking: true);

            System.GC.WaitForPendingFinalizers();

            await Task.CompletedTask;

            return Ok();
        }
    }
}
