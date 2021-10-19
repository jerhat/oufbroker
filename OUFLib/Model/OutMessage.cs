using NetMQ;
using System.Collections.Generic;

namespace OUFLib.Model
{
    internal class OutMessage
    {
        internal NetMQFrame Sender { get; }
        internal NetMQMessage Message { get; }
        internal List<Node> Destinations { get; }
        internal byte[] CorrelationId { get; }

        internal OutMessage(NetMQFrame sender, NetMQMessage message, List<Node> destination, byte[] correlationId)
        {
            Sender = sender;
            Message = message;
            Destinations = destination;
            CorrelationId = correlationId;
        }

        internal OutMessage(NetMQFrame sender, NetMQMessage message, Node destination, byte[] correlationId) : this(sender, message, new List<Node> { destination }, correlationId)
        {
        }

        internal AckMessage CreateAckMessage(AckStatus status) => new(Sender, CorrelationId, status);
    }
}
