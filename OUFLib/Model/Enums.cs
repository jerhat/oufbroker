using System.Runtime.Serialization;

namespace OUFLib.Model
{
    public enum OUFState : ushort { Unknown = 0, Sleeping, Frozen, Started };

    public enum Command
    {
        Ready = 0x02,
        Subscribe = 0x03,
        Unsubscribe = 0x04,
        Unicast = 0x05,
        Multicast = 0x06,
        Heartbeat = 0x07,
        Ack = 0x08,
        Probe = 0x10,
        Execute = 0x12,
        Lock = 0x13,
        Unlock = 0x14
    }

    public enum NodeTask
    {
        GetLogs = 0x00,
        Kill = 0x01,
        Disconnect = 0x02,
        TriggerAction = 0x03,
        [IgnoreDataMember]
        GetStatus = 0x04
    }

    public enum Casting
    {
        Uni,
        Multi
    }

    public enum AckStatus
    {
        Disabled = 0x00,
        Timeout = 0x01,
        Pending = 0x02,
        Success = 0x03,
        Failed = 0x04
    }
}
