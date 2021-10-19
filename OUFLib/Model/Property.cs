using OUFLib.Serialization.Converters;
using System;
using System.Text.Json.Serialization;

namespace OUFLib.Model
{
    public class Property
    {
        // needed for serizalization
        public Property() { }

        public Property(Type type) : this(type, null)
        {
        }

        public Property(Type type, object value)
        {
            Type = type;
            Value = value;
        }

        public Type Type { get; set; }
        public object Value { get; set; }

        public override string ToString()
        {
            return Value?.ToString();
        }
    }
}
