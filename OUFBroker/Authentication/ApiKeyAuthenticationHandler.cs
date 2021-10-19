using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using System.Text.Encodings.Web;
using System.Threading.Tasks;

namespace OUFBroker.Authentication
{
    public class ApiKeyAuthenticationHandler : AuthenticationHandler<ApiKeyAuthenticationOptions>
    {

        private readonly string _apiKey;

        public ApiKeyAuthenticationHandler(
            IOptionsMonitor<ApiKeyAuthenticationOptions> options,
            ILoggerFactory logger,
            UrlEncoder encoder,
            ISystemClock clock,
            IConfiguration configuration) : base(options, logger, encoder, clock)
        {
            _apiKey = configuration.GetValue<string>("API_KEY");

            if (string.IsNullOrWhiteSpace(_apiKey))
                throw new Exception("API_KEY is not defined");
        }

        protected override async Task<AuthenticateResult> HandleAuthenticateAsync()
        {
            if (!Request.Headers.TryGetValue(Constants.API_KEY_HEADER_NAME, out var apiKeyHeaderValues))
            {
                return AuthenticateResult.Fail($"No {Constants.API_KEY_HEADER_NAME} provided");
            }

            var providedApiKey = apiKeyHeaderValues.FirstOrDefault();

            if (apiKeyHeaderValues.Count == 0 || string.IsNullOrWhiteSpace(providedApiKey))
            {
                return AuthenticateResult.Fail($"No {Constants.API_KEY_HEADER_NAME} provided");
            }

            if (!_apiKey.Equals(providedApiKey, StringComparison.InvariantCultureIgnoreCase))
            {
                return AuthenticateResult.Fail($"API KEY mismatch");
            }

            var claims = new List<Claim> { new Claim(ClaimTypes.Name, "AuthenticatedUser") };

            var identity = new ClaimsIdentity(claims, Options.AuthenticationType);
            var identities = new List<ClaimsIdentity> { identity };
            var principal = new ClaimsPrincipal(identities);
            var ticket = new AuthenticationTicket(principal, Options.Scheme);

            await Task.CompletedTask;

            return AuthenticateResult.Success(ticket);
        }
    }
}
