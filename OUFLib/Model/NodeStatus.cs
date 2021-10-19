namespace OUFLib.Model
{
    public class NodeStatus
    {
        public long? MemoryUsage => System.Diagnostics.Process.GetCurrentProcess()?.WorkingSet64;
    }
}
