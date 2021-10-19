using Microsoft.AspNetCore.Authentication;

namespace OUFBroker.Authentication
{
    public class ApiKeyAuthenticationOptions : AuthenticationSchemeOptions
    {
        public const string DefaultScheme = Constants.API_KEY_POLICY_NAME;
        public string Scheme => DefaultScheme;
        public string AuthenticationType = DefaultScheme;
    }
}
