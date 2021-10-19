using OUFLib.Logging;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace OUFLib.Locker
{
    public interface IOUFLocker : IDisposable
    {
        /// <summary>
        /// Release the lock on a resource. Resource can only be unlocked if it was locked by the same requestor.
        /// </summary>
        /// <param name="resource">the name of the resource to release.</param>
        /// <param name="ownerId">the id of the lock owner, i.e. id that was passed when locking the resource.</param>
        /// <returns>true if the resource could be unlocked, i.e. no more locks was present on the resource. False otherwise.</returns>
        bool ReleaseLock(string resource, string requestorId);

        /// <summary>
        /// Acquire a lock on a specific resource.
        /// </summary>
        /// <param name="resource">The resource to lock.</param>
        /// <param name="requestorId">Id requesting the lock.</param>
        /// <param name="acquisitionTimeout">Time after which the aquisition will fail if the resource is already locked. Default is null, i.e. lock will will fail if resource is not available right away.</param>
        /// <param name="autoUnlockTimeout">Time after which the lock will be autoamtically released. Default is null, i.e. auto-unlock is disabled.</param>
        /// <param name="allowSelfReentry">Whether to allow self re-entry, i.e whether to allow locking a resource that was previously locked by the same requestor. Default is false.</param>
        /// <returns>true if the resource could be locked, false otherwise.</returns>
        Task<bool> TryAcquireLockAsync(string resource, string requestorId, TimeSpan? acquisitionTimeout = null, TimeSpan? autoUnlockTimeout = null, bool allowSelfReentry = false);
    }

    public class OUFLocker : IOUFLocker
    {
        private readonly ConcurrentDictionary<string, Lock> _locks = new();
        private readonly ConcurrentDictionary<string, Timer> _autoUnlockTimers = new();
        private readonly IOUFLogger _logger;

        public OUFLocker(IOUFLogger logger)
        {
            _logger = logger;
        }

        public async Task<bool> TryAcquireLockAsync(string resource, string requestorId, TimeSpan? acquisitionTimeout = null, TimeSpan? autoUnlockTimeout = null, bool allowSelfReentry = false)
        {
            DateTime expiry = DateTime.UtcNow.Add(acquisitionTimeout ?? TimeSpan.Zero);

            while (null == acquisitionTimeout || acquisitionTimeout == TimeSpan.Zero || DateTime.UtcNow < expiry)
            {
                var newItem = new Lock(requestorId, allowSelfReentry);

                var item = _locks.GetOrAdd(resource, newItem);

                // Item was just added. The items are created with refCount = 1, so we don't need to pin it
                if (newItem == item)
                {
                    if (null != autoUnlockTimeout && autoUnlockTimeout != TimeSpan.MaxValue && autoUnlockTimeout != Timeout.InfiniteTimeSpan && autoUnlockTimeout > TimeSpan.Zero)
                    {
                        var timer = new Timer(AutoUnlock, (resource, requestorId), (int)autoUnlockTimeout.Value.TotalMilliseconds, Timeout.Infinite);
                        _autoUnlockTimers[resource] = timer;
                        _logger.Info($"Scheduling auto-unlock of resource '{resource}' in {autoUnlockTimeout.Value.TotalMilliseconds} ms");
                    }

                    return true;
                }
                // Item was already in dictionary. Trying to pin it
                else
                {
                    if (item.OwnerId == requestorId)
                    {
                        if (item.AllowSelfReentry && allowSelfReentry)
                        {
                            // Pinned successfully other thread will not delete it
                            if (item.Pin())
                            {
                                return true;
                            }
                        }
                    }
                }

                if (acquisitionTimeout == TimeSpan.Zero || null == acquisitionTimeout)
                    return false;

                // The item was just successfully unpinned. and is about to be deleted
                // We got to the moment it is still in dictionary, but already unpinned.
                // Going to the next round

                await Task.Delay(10);
                continue;
            }

            return false;
        }

        private void AutoUnlock(object state)
        {
            var (resource, requestorId) = ((string resource, string requestorId))state;

            _logger.Info($"{nameof(AutoUnlock)} of resource '{resource}', requestorId '{requestorId}' triggered");

            ReleaseLock(resource, requestorId);
        }

        public bool ReleaseLock(string resource, string requestorId)
        {
            try
            {
                if (!_locks.TryGetValue(resource, out var @lock))
                {
                    _logger.Warn($"{nameof(ReleaseLock)}: resource '{resource}' not found");
                    return false;
                }

                if (@lock.OwnerId != requestorId)
                {
                    _logger.Warn($"{nameof(ReleaseLock)}: resource '{resource}' was locked by '{@lock.OwnerId}'. Requestor '{requestorId}' cannot unlock");
                    return true;
                }

                if (@lock.Unpin())
                {
                    // Here we are free do delete it. Even if some of the threads refer to the item, they would skip it and wait until we delete it
                    if (_locks.TryRemove(resource, out var _))
                    {
                        _logger.Info($"{nameof(ReleaseLock)}: resource '{resource}' unlocked by '{requestorId}'");
                        return true;
                    }
                }

                return false;
            }
            finally
            {
                var timerRemoved = _autoUnlockTimers.TryRemove(resource, out var timer);

                if (!timerRemoved)
                {
                    _logger.Warn($"{nameof(ReleaseLock)}: resource '{resource}' could not remove its auto-unlock timer");
                }

                timer.Dispose();
            }
        }

        
        public void Dispose()
        {
            _locks.Clear();
            GC.SuppressFinalize(this);
        }
    }
}
