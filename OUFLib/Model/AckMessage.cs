using NetMQ;
using System.Linq;

namespace OUFLib.Model
{
    internal class AckMessage
    {
        internal AckStatus Status { get; }
        internal NetMQFrame Sender { get; }
        internal byte[] CorrelationId { get; }

        internal AckMessage(NetMQFrame sender, byte[] correlationId, AckStatus status)
        {
            Sender = sender;
            CorrelationId = correlationId;
            Status = status;
        }

        internal NetMQMessage BuildMQMessage()
        {
            if (!NeedsAck)
            {
                return null;
            }

            var ackMessage = new NetMQMessage();

            ackMessage.Push(new byte[] { (byte)Status });
            ackMessage.Push(CorrelationId);
            ackMessage.Push(new byte[] { (byte)Command.Ack });
            ackMessage.PushEmptyFrame();
            ackMessage.Push(Sender);

            return ackMessage;
        }

        private bool NeedsAck => CorrelationId?.Any() == true;
    }
}
