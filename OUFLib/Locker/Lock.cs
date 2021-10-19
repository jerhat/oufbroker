using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace OUFLib.Locker
{
    public class Lock : IDisposable
    {
        private int _refCounter;

        public string OwnerId { get; set; }
        public TimeSpan AcquisitionTimeout { get; set; }
        public TimeSpan AutoUnlockTimeout { get; set; }
        public bool AllowSelfReentry { get; set; }

        public Lock()
        {
            _refCounter = 1;
        }

        public Lock(string ownerId, bool allowSelfReentry) : this()
        {
            OwnerId = ownerId;
            AllowSelfReentry = allowSelfReentry;
        }
        

        // Trying to keep RefCounter to be positive value
        // Once it is equal to 0, the item can be disposed, so we lost the race
        public bool Pin()
        {
            // Spinning while one of the following cases happen
            while (true)
            {
                int refCounter = _refCounter;

                // 1. The item is released by its previous owner, so it can be deleted from the repository
                if (refCounter == 0)
                {
                    return false;
                }

                // 2. We added our reference to the counter, so while we release it, noone can delete it
                if (Interlocked.CompareExchange(ref _refCounter, refCounter + 1, refCounter) == refCounter)
                {
                    return true;
                }
            }
        }

        public bool Unpin()
        {
            var value = Interlocked.Decrement(ref _refCounter);
            if (value == 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public override string ToString()
        {
            return $"{nameof(Lock)} (OwnerId='{OwnerId}')";
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
        }
    }
}
