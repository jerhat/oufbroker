using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace OUFLib.Model
{
    public delegate void ActionTriggeredEvent(OUFAction action, Dictionary<string, Property> arguments);

    public class OUFAction
    {
        public string Name { get; set; }

        public Dictionary<string, Property> Arguments { get; set; }

        public event ActionTriggeredEvent Triggered;

        public void Trigger(Dictionary<string, Property> arguments)
        {
            Task.Run(() => Triggered?.Invoke(this, arguments));
        }

        public T GetArgumentValue<T>(Dictionary<string, Property> arguments, string argumentName)
        {
            if (null == arguments)
                return default;

            if (!arguments.TryGetValue(argumentName, out Property argument))
            {
                // argument not found, returning default value from job's Arguments definition
                if (!this.Arguments.TryGetValue(argumentName, out argument))
                {
                    return default;
                }
            }

            if (argument.Value is System.Text.Json.JsonElement jsonElement)
            {
                return (T)Convert.ChangeType(jsonElement.ToString(), argument.Type);
            }
            else
            {
                return (T)argument.Value;
            }
        }
    }
}
