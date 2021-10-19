using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OUFLib.Helpers
{
    internal static class AttributeHelper
    {
        private static ConcurrentDictionary<object, Type[]> _cache = new();

        internal static bool HasAttribute<TAttribute>(object obj) where TAttribute : Attribute
        {
            if (null == obj)
                return false;

            if (_cache.TryGetValue(obj, out var attributes))
            {
                return attributes?.Contains(typeof(TAttribute)) == true;
            }

            var enumType = obj.GetType();
            var memberInfos = enumType.GetMember(obj.ToString());
            var enumValueMemberInfo = memberInfos.FirstOrDefault(m => m.DeclaringType == enumType);

            attributes = enumValueMemberInfo.GetCustomAttributes(true).Select(x => x.GetType()).ToArray();

            _cache[obj] = attributes;

            return attributes?.Contains(typeof(TAttribute)) == true;
        }
    }
}
