using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace OUFLib.Serialization
{
    public class SerializationOptions
    {
        private static IEnumerable<JsonConverter> _converters
        {
            get
            {
                foreach (var type in Assembly.GetAssembly(typeof(SerializationOptions)).GetTypes().Where(t => typeof(JsonConverter).IsAssignableFrom(t)))
                    yield return Activator.CreateInstance(type) as JsonConverter;
            }
        }

        public static void SetOptions(JsonSerializerOptions options)
        {
            foreach (var converter in _converters)
                options.Converters.Add(converter);


            options.Converters.Add(new JsonStringEnumConverter(JsonNamingPolicy.CamelCase));
            options.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
        }
    }
}
