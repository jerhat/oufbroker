using System;
using System.Threading.Tasks;
using System.Xml.XPath;

namespace OUFLib.Model
{
    public delegate Task DataReceivedEvent(string address, byte[] payload);

    public sealed record Subscription
    {

        private string _selector;

        public Subscription() { }

        public Casting Casting { get; set; }
        public string Address { get; set; }

        public string Selector
        {
            get => _selector;
            set
            {
                _selector = value;

                if (string.IsNullOrWhiteSpace(_selector))
                {
                    _selectorExpression = null;
                }
                else
                {
                    try
                    {
                        _selectorExpression = XPathExpression.Compile(_selector);
                    }
                    catch (Exception e)
                    {
                        throw new ArgumentException($"Subscription failed because selector '{_selector}' is invalid: {e.Message}");
                    }
                }
            }
        }

        private XPathExpression _selectorExpression;

        public event DataReceivedEvent DataReceived;

        public Subscription(Casting casting, string address) : this(casting, address, null)
        { }

        public Subscription(Casting casting, string address, string selector)
        {
            Casting = casting;
            Address = address;
            Selector = selector ?? "";
        }

        public void UpdateSelector(string selector)
        {
            Selector = selector ?? "";
        }

        public async Task Raise(string address, byte[] payload)
        {
            if (null == DataReceived)
                return;

            await DataReceived.Invoke(address, payload);
        }

        public bool Match(Casting casting, string address, Headers headers, out Exception error)
        {
            error = null;

            if (casting != Casting)
                return false;

            if (string.IsNullOrWhiteSpace(address))
                return false;

            if (!address.Equals(Address, StringComparison.InvariantCultureIgnoreCase))
                return false;

            if (null == headers)
                return true;

            if (null == _selectorExpression) // no selector, it's a match
                return true;

            try
            {
                return headers.IsMatch(_selectorExpression);
            }
            catch (Exception e)
            {
                error = e;
                return false;
            }
        }

        public bool IsValid()
        {
            return !string.IsNullOrWhiteSpace(Address) && !Address.StartsWith("@");
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Address);
        }

        public override string ToString()
        {
            return $"{Address}@{Casting}" + (string.IsNullOrEmpty(Selector) ? "" : $"[{Selector}]");
        }

        public bool Equals(Subscription other)
        {
            if (other is not Subscription subscription) return false;
            return Casting == subscription.Casting && Address == subscription.Address;
        }
    }
}
