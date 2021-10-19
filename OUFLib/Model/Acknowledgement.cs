using System;
using System.Threading;

namespace OUFLib.Model
{
    internal class Acknowledgement
    {
        internal Guid CorrelationId { get; private set; }
        internal SemaphoreSlim Semaphore { get; private set; }
        internal AckStatus Status { get; private set; }
        internal DateTime Creation { get; private set; }

        internal byte[] Payload { get; private set; }

        internal static Acknowledgement Create()
        {
            return new Acknowledgement()
            {
                CorrelationId = Guid.NewGuid(),
                Semaphore = new SemaphoreSlim(0),
                Status = AckStatus.Pending,
                Creation = DateTime.UtcNow
            };
        }

        internal void SetStatus (AckStatus status)
        {
            Status = status;
        }

        internal void SetPayload(byte[] payload)
        {
            Payload = payload;
        }

        internal byte[] GetBytes()
        {
            return CorrelationId.ToByteArray();
        }

        private Acknowledgement()
        {
        }
    }
}
