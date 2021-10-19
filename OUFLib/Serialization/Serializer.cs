using System;
using System.Text.Json;

namespace OUFLib.Serialization
{
    public class Serializer
    {
        private static JsonSerializerOptions _options;

        private static JsonSerializerOptions GetSerializerOptions()
        {
            var options = new JsonSerializerOptions();
            
            SerializationOptions.SetOptions(options);

            return new JsonSerializerOptions(options);
        }

        static Serializer()
        {
            _options = GetSerializerOptions();
        }

        public static byte[] ToJsonBytes(object obj)
        {
            try
            {
                return JsonSerializer.SerializeToUtf8Bytes(obj, _options);
            }
            catch (Exception)
            {
                return default;
            }
        }

        public static string ToJson(object obj)
        {
            try
            {
                return JsonSerializer.Serialize(obj, _options);
            }
            catch (Exception)
            {
                return default;
            }
        }

        public static string ToString(object obj)
        {
            try
            {
                return JsonSerializer.Serialize(obj, _options);
            }
            catch (Exception)
            {
                return default;
            }
        }

        public static T Deserialize<T>(byte[] bytes)
        {
            try
            {
                return JsonSerializer.Deserialize<T>(bytes, _options);
            }
            catch
            {
                return default;
            }
        }

        public static T Deserialize<T>(string json)
        {
            try
            {
                return JsonSerializer.Deserialize<T>(json, _options);
            }
            catch
            {
                return default;
            }
        }
    }
}
