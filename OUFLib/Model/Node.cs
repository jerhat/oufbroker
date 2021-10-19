using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;

namespace OUFLib.Model
{
    public class Node
    {
        public string Name { get; }

        [JsonIgnore]
        public byte[] Id { get; }

        public DateTime LastSeen { get; set; }

        public DateTime StartedAt { get; private set; }

        public bool IsExpired { get; private set; }

        public NodeInformation Information { get; }

        public ICollection<Subscription> Subscriptions => _subscriptions.Keys;

        [JsonIgnore]
        public bool Persistent => Information?.Persistent == true;

        private readonly ConcurrentDictionary<Subscription, byte> _subscriptions = new();

        public Node([NotNull] byte[] idFrame, NodeInformation nodeInfo)
        {
            Id = idFrame;
            Name = Encoding.ASCII.GetString(idFrame);

            Information = nodeInfo;
            StartedAt = DateTime.UtcNow;
            LastSeen = DateTime.UtcNow;
            IsExpired = false;
        }

        public void Subscribe(Subscription subscription)
        {
            _subscriptions.AddOrUpdate(subscription, default(byte), (subscription, _) => _);
        }

        public void Unsubscribe(Subscription subscription)
        {
            var matches = _subscriptions.Keys.Where(x => x.Address == subscription.Address && x.Casting == subscription.Casting);

            foreach (var match in matches)
            {
                _subscriptions.TryRemove(match, out var _);
            }
        }

        public bool HasExpired()
        {
            if (!IsExpired && LastSeen.AddMilliseconds(Constants.HEARTBEAT_LIVENESS * Constants.HEARTBEAT_INTERVAL_ms) < DateTime.UtcNow)
            {
                IsExpired = true;
                return true;
            }
            else
            {
                return false;
            }
        }

        public override int GetHashCode()
        {
            return Name.GetHashCode();
        }

        public override string ToString()
        {
            return $"{nameof(Node)} '{Name}'";
        }

        public override bool Equals(object obj)
        {
            if (obj is null)
                return false;

            var other = obj as Node;

            return other is not null && Name == other.Name;
        }
    }
}
