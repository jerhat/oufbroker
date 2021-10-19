using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml.Linq;
using System.Xml.XPath;

namespace OUFLib.Model
{
    public class Headers
    {
        public readonly XElement _xElement;

        private readonly XPathNavigator _navigator;

        public Headers(params KeyValuePair<string, string>[] source)
        {
            _xElement = ToXElement(source);
            _navigator = _xElement?.CreateNavigator();
        }

        public Headers(string xml)
        {
            if (string.IsNullOrEmpty(xml))
                return;

            _xElement = XDocument.Parse(xml)?.Root;

            _xElement?.Remove();

            _navigator = _xElement?.CreateNavigator();
        }

        public override string ToString()
        {
            return _xElement?.ToString();
        }

        public static XElement ToXElement(IEnumerable<KeyValuePair<string, string>> source)
        {
            if (source?.Any() != true)
                return null;

            return new XElement("root", source.Select(kv => new XElement(kv.Key, kv.Value)));
        }

        public bool IsMatch(XPathExpression expression)
        {
            return (bool?)_navigator?.Evaluate(expression) == true;
        }
    }
}
