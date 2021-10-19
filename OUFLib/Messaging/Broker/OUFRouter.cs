using NetMQ;
using NetMQ.Monitoring;
using NetMQ.Sockets;
using OUFLib.Helpers;
using OUFLib.Locker;
using OUFLib.Logging;
using OUFLib.Model;
using OUFLib.Serialization;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace OUFLib.Messaging.Broker
{
    internal interface IOUFRouter : IDisposable
    {
        ICollection<Model.Node> Nodes { get; }
        void Run();
        event Action<NetMQMessage> MulticastMessageReceived;

        Task<(AckStatus status, byte[] data)> ExecuteAsync(string nodeName, NodeTask action, Dictionary<string, Property> arguments);
    }

    internal class OUFRouter : IOUFRouter
    {
        private readonly IOUFLogger _logger;
        private readonly IOUFLocker _locker;

        private readonly ConcurrentDictionary<string, Model.Node> _nodes = new();
        private readonly ConcurrentDictionary<Guid, Acknowledgement> _acks;

        private volatile bool _isRunning;
        private volatile bool _isBound;

        internal readonly NetMQSocket _socket;
        private readonly NetMQPoller _poller;
        private readonly NetMQMonitor _monitor;
        private readonly NetMQQueue<OutMessage> _outQueue;
        private readonly NetMQQueue<NetMQMessage> _ackQueue;

        private readonly NetMQTimer _routineTimer;

        private readonly string _endPoint;

        public ICollection<Model.Node> Nodes => _nodes.Values;

        public event Action<NetMQMessage> MulticastMessageReceived;

        internal OUFRouter(string endpoint, IOUFLogger logger, IOUFLocker locker)
        {
            _logger = logger;
            _locker = locker;

            _endPoint = endpoint;

            if (string.IsNullOrWhiteSpace(_endPoint))
                throw new ArgumentNullException(nameof(_endPoint), "A endpoint where the router binds to must be given!");

            _acks = new ConcurrentDictionary<Guid, Acknowledgement>();

            _routineTimer = new NetMQTimer(Constants.BROKER_ROUTINE_INTERVAL_ms);
            _routineTimer.Elapsed += (s, e) => Routine();

            _socket = new RouterSocket();
            _socket.Options.RouterMandatory = true;
            _socket.Options.RouterHandover = true;

            _outQueue = new NetMQQueue<OutMessage>();
            _outQueue.ReceiveReady += OutEnqueued;

            _ackQueue = new NetMQQueue<NetMQMessage>();
            _ackQueue.ReceiveReady += AckEnqueued;

            _poller = new NetMQPoller() { _socket, _outQueue, _ackQueue, _routineTimer };

            _monitor = new NetMQMonitor(_socket, $"inproc://MONITOR.ROUTER.{Guid.NewGuid()}", SocketEvents.Accepted | SocketEvents.Disconnected);

            _monitor.Accepted += MonitorAccepted;
            _monitor.Disconnected += MonitorDisconnected;

            _monitor.AttachToPoller(_poller);
        }

        private void Routine()
        {
            foreach (var node in _nodes.Values.ToList())
            {
                if (node.HasExpired())
                {
                    _logger?.Info($"{node} expired");

                    _ = ExecuteAsync(node.Name, NodeTask.Disconnect, null);

                    if (!node.Persistent)
                    {
                        RemoveNode(node);
                    }
                }
            }
        }

        private void OutEnqueued(object sender, NetMQQueueEventArgs<OutMessage> e)
        {
            var outMessage = e.Queue?.Dequeue();

            var message = outMessage.Message;

            var ackStatus = AckStatus.Failed;

            foreach (var node in outMessage.Destinations)
            {
                if (null == node)
                    continue;

                message.Push(node.Id);

                try
                {
                    _socket.SendMultipartMessage(message);

                    ackStatus = AckStatus.Success;
                    break;

                }
                catch (Exception exception)
                {
                    var name = message?.First?.ConvertToString() ?? "??";
                    _logger?.Warn($"Unable to deliver message to '{name}': {exception}");
                }
                finally
                {
                    message.Pop();
                }
            }

            HandleAck(outMessage.CreateAckMessage(ackStatus));
        }

        public async Task<(AckStatus status, byte[] data)> ExecuteAsync(string nodeName, NodeTask action, Dictionary<string, Property> arguments)
        {
            ///     BROKER -> NODE:  [nodeidentity][e][EXECUTE][CORRELATIONID][ACTION][ARGUMENTS]

            if (!AttributeHelper.HasAttribute<IgnoreDataMemberAttribute>(action))
            {
                _logger?.Info($"{nameof(ExecuteAsync)} action '{action}' on node '{nodeName}'");
            }

            var destination = GetNode(nodeName);

            if (null == destination)
            {
                _logger?.Warn($"Node '{nodeName}' not found");
                return (AckStatus.Failed, default);
            }

            var acknowledgement = Acknowledgement.Create();
            _acks.GetOrAdd(acknowledgement.CorrelationId, acknowledgement);

            var msg = new NetMQMessage();

            // set action
            msg.Push(null == arguments ? Array.Empty<byte>() : Serializer.ToJsonBytes(arguments));

            // set action
            msg.Push(new[] { (byte)action });

            msg.Push(acknowledgement.CorrelationId.ToByteArray());

            msg.Push(new[] { (byte)Command.Execute });

            // set empty frame as separator
            msg.Push(NetMQFrame.Empty);

            msg.Push(destination.Id);

            try
            {
                _ackQueue.Enqueue(msg);
            }
            catch (TerminatingException e)
            {
                _logger?.Warn($"{nameof(OUFRouter)}: {nameof(ExecuteAsync)}: {e}");
            }

            var acked = await acknowledgement.Semaphore.WaitAsync(Constants.ACK_TIMEOUT_ms);

            if (acked)
            {
                return (acknowledgement.Status, acknowledgement.Payload);
            }
            else
            {
                _acks.Remove(acknowledgement.CorrelationId, out _);
                return (AckStatus.Timeout, default);
            }
        }

        private void HandleAck(AckMessage ackMessage)
        {
            var message = ackMessage.BuildMQMessage();

            ///     BROKER  -> NODE:  [node1][e][ACK][CORRELATIONID][ACKSTATUS]
            if (null != message)
            {
                try
                {
                    _ackQueue.Enqueue(message);
                }
                catch (TerminatingException e)
                {
                    _logger?.Warn($"{nameof(OUFRouter)}: {nameof(HandleAck)}: {e}");
                }
            }
        }

        private void AckEnqueued(object sender, NetMQQueueEventArgs<NetMQMessage> e)
        {
            var message = e.Queue?.Dequeue();

            try
            {
                _socket.SendMultipartMessage(message);
            }
            catch (HostUnreachableException)
            {
                var name = message?.First?.ConvertToString() ?? "??";
                _logger?.Warn($"Unable to deliver ack message to '{name}': host is gone");
            }
            catch (Exception exception)
            {
                var name = message?.First?.ConvertToString() ?? "??";
                _logger?.Warn($"Unable to deliver ack message to '{name}': {exception}");
            }
            finally
            {
                message.Pop();
            }
        }

        private void MonitorDisconnected(object sender, NetMQMonitorSocketEventArgs e)
        {
            _logger?.Info($"Disconnection occured");
        }

        private void MonitorAccepted(object sender, NetMQMonitorSocketEventArgs e)
        {
            _logger?.Info($"Router accepting new connection");
        }

        private Model.Node GetNode(string nodeName)
        {
            return _nodes.TryGetValue(nodeName, out var node) ? node : null;
        }

        private IEnumerable<Model.Node> GetSubscribedNodes(string targetNode, string address, Headers headers)
        {
            var errorNodes = new Dictionary<Model.Node, List<(Subscription subscription, Exception e)>>();

            var sourceNodes = _nodes.Values.Where(n => string.IsNullOrEmpty(targetNode) || n.Name == targetNode);

            foreach (var node in sourceNodes)
            {
                if (node.IsExpired)
                    continue;

                foreach (var subscription in node.Subscriptions)
                {
                    if (subscription.Match(Casting.Uni, address, headers, out var e))
                    {
                        if (null != e)
                        {
                            if (!errorNodes.ContainsKey(node))
                            {
                                errorNodes[node] = new List<(Subscription, Exception)>() { (subscription, e) };
                            }
                            else
                            {
                                errorNodes[node].Add((subscription, e));
                            }
                        }

                        yield return node;
                        break;
                    }
                }
            }

            foreach (var errorNode in errorNodes)
            {
                _logger.Warn($"{nameof(GetSubscribedNodes)} - {errorNode.Key}: one or more error found when matching selector:");
                errorNode.Value.ForEach(x => _logger.Warn($"{x.subscription}: {x.e}"));
            }
        }

        private Model.Node AddNode(Model.Node node)
        {
            return _nodes.AddOrUpdate(node.Name, node, (name, _) => node);
        }

        private bool RemoveNode(Model.Node node)
        {
            return _nodes.TryRemove(node.Name, out var _);
        }

        private void Bind()
        {
            if (_isBound)
                return;

            try
            {
                _socket.Bind(_endPoint);
                _logger?.Info($"{nameof(OUFRouter)} bound to '{_endPoint}");
            }
            catch (Exception e)
            {
                var error = $"The bind operation failed on '{_endPoint}'";
                error += $"\nMessage: {e.Message}";
                throw new ApplicationException(error);
            }

            _isBound = true;

            _logger?.Info($"{nameof(OUFRouter)} is active");
        }

        public void Run()
        {
            if (_isRunning)
                throw new InvalidOperationException("Can't start same router more than once!");

            if (!_isBound)
                Bind();

            _isRunning = true;

            _socket.ReceiveReady += MessageReceived;

            _logger?.Info($"{nameof(OUFRouter)} started to listen for incoming messages");

            _poller.RunAsync(nameof(OUFRouter));

            _isRunning = false;
        }

        private void MessageReceived(object sender, NetMQSocketEventArgs args)
        {
            NetMQMessage msg;

            try
            {
                msg = args.Socket.ReceiveMultipartMessage();
            }
            catch (Exception e)
            {
                _logger?.Error($"{nameof(OUFRouter)}: {nameof(MessageReceived)}: {e}");
                return;
            }

            Task.Run(async () =>
            {
                _logger?.Debug($"Received: {msg}");

                var senderFrame = msg.Pop();
                _ = msg.Pop();

                var commandFrame = msg.Pop();

                if (commandFrame.BufferSize > 1)
                    throw new ApplicationException("The Command frame had more than one byte!");

                var command = (Command)commandFrame.Buffer[0];

                switch (command)
                {
                    case Command.Heartbeat:
                        ProcessHeartbeatMessage(senderFrame);
                        break;
                    case Command.Ready:
                        ProcessReadyMessage(senderFrame, msg);
                        break;
                    case Command.Subscribe:
                        ProcessSubscribeMessage(senderFrame, msg);
                        break;
                    case Command.Unsubscribe:
                        ProcessUnsubscribeMessage(senderFrame, msg);
                        break;
                    case Command.Multicast:
                        ProcessMulticastMessage(senderFrame, msg);
                        break;
                    case Command.Unicast:
                        ProcessUnicastMessage(senderFrame, msg);
                        break;
                    case Command.Probe:
                        ProcessProbeMessage(senderFrame, msg);
                        break;
                    case Command.Ack:
                        ProcessAckMessage(senderFrame, msg);
                        break;
                    case Command.Lock:
                        await ProcessLockMessage(senderFrame, msg);
                        break;
                    case Command.Unlock:
                        ProcessUnlockMessage(senderFrame, msg);
                        break;
                    default:
                        _logger?.Error($"Received message with invalid protocol command '{command}'");
                        break;
                }
            });
        }

        private void ProcessUnlockMessage(NetMQFrame senderFrame, NetMQMessage request)
        {
            var (correlationId, resource, requestorId) = UnwrapLock<string>(request);
            var lockReleased = _locker.ReleaseLock(resource, requestorId);

            var ackMessage = new AckMessage(senderFrame, correlationId.ToByteArray(), lockReleased ? AckStatus.Success : AckStatus.Failed);
            HandleAck(ackMessage);
        }

        private async Task ProcessLockMessage(NetMQFrame senderFrame, NetMQMessage request)
        {
            var (correlationId, resource, receivedLockInfo) = UnwrapLock<Lock>(request);
            var lockTaken = await _locker.TryAcquireLockAsync(resource, receivedLockInfo.OwnerId, receivedLockInfo.AcquisitionTimeout, receivedLockInfo.AutoUnlockTimeout, receivedLockInfo.AllowSelfReentry);

            var ackMessage = new AckMessage(senderFrame, correlationId.ToByteArray(), lockTaken ? AckStatus.Success : AckStatus.Failed);
            HandleAck(ackMessage);
        }

        private (Guid correlationId, string resource, T lockInfo) UnwrapLock<T>(NetMQMessage request)
        {
            // [CORRELATIONID][RESOURCE][e][e][PAYLOAD]
            var expectedFrameCount = 5;

            if (request.FrameCount != expectedFrameCount)
            {
                var message = $"{nameof(UnwrapLock)}: malformed request received. Expected '{expectedFrameCount}' frames, got '{request.FrameCount}'";
                _logger?.Error($"{nameof(OUFRouter)}: {message}");
                _logger?.Debug(request.ToString());

                throw new ApplicationException(message);
            }

            var correlationFrame = request.Pop();
            if (correlationFrame.BufferSize != 16)
                throw new ApplicationException($"CorrelationId must be 16 frames!");

            var correlationId = new Guid(correlationFrame.Buffer);

            var resourceFrame = request.Pop();
            var resource = resourceFrame.ConvertToString();

            // drop targetNode
            request.Pop();

            // drop headers
            request.Pop();

            var payloadFrame = request.Pop();

            var payload = Serializer.Deserialize<T>(payloadFrame.Buffer);

            return (correlationId, resource, payload);
        }

        private void ProcessAckMessage(NetMQFrame sender, NetMQMessage message)
        {
            var (correlationId, ackStatus, payload) = UnwrapAck(message);

            if (_acks.TryRemove(correlationId, out var acknowlegment))
            {
                acknowlegment.SetStatus(ackStatus);
                acknowlegment.SetPayload(payload);
                acknowlegment.Semaphore.Release();
            }
        }

        private (Guid correlationId, AckStatus ackStatus, byte[] payload) UnwrapAck(NetMQMessage request)
        {
            ///     BROKER  -> NODE:  [CORRELATIONID][ACKSTATUS][PAYLOAD]

            var expectedFrameCount = 3;

            if (request.FrameCount != expectedFrameCount)
            {
                var message = $"{nameof(UnwrapAck)}: malformed request received. Expected '{expectedFrameCount}' frames, got '{request.FrameCount}'";
                _logger?.Error($"{nameof(OUFRouter)}: {message}");
                _logger?.Debug(request.ToString());

                throw new ApplicationException(message);
            }

            var correlationFrame = request.Pop();
            if (correlationFrame.BufferSize != 16)
                throw new ApplicationException($"CorrelationId must be 16 frames!");

            var correlationId = new Guid(correlationFrame.Buffer);

            var ackStatusFrame = request.Pop();
            if (ackStatusFrame.BufferSize > 1)
                throw new ApplicationException("AckStatus must be one byte not multiple!");

            var ackStatus = (AckStatus)ackStatusFrame.Buffer[0];

            var payload = request.Pop().Buffer;

            return (correlationId, ackStatus, payload);
        }

        private void ProcessProbeMessage(NetMQFrame senderFrame, NetMQMessage message)
        {
            var nodeName = senderFrame.ConvertToString();

            if (message.IsEmpty)
            {
                _logger?.Error($"Empty message from '{nodeName}' discarded");
            }

            // [CORRELATIONID][e][NODETOPROBE][e][e]

            var correlationId = message.Pop().Buffer;

            message.Pop();

            var probedNodeFrame = message.Pop();
            var probedNode = probedNodeFrame.ConvertToString();

            if (!_nodes.TryGetValue(probedNode, out var destination))
            {
                _logger.Warn($"Node '{probedNode}' not found");
            }

            /// [destination][e][PROBE]

            var probeMessage = new NetMQMessage();
            probeMessage.Push(new byte[] { (byte)Command.Probe });
            probeMessage.PushEmptyFrame();

            var outMessage = new OutMessage(senderFrame, probeMessage, destination, correlationId);

            try
            {
                _outQueue.Enqueue(outMessage);
            }
            catch (TerminatingException e)
            {
                _logger?.Warn($"{nameof(OUFRouter)}: {nameof(ProcessProbeMessage)}: {e}");
            }
        }

        private void ProcessUnicastMessage(NetMQFrame sender, NetMQMessage message)
        {
            // [CORRELATIONID][ADDRESS][TARGETNODE][HEADERS][PAYLOAD]

            var correlationId = message.Pop().Buffer;

            var addressFrame = message.Pop();
            var address = addressFrame.ConvertToString();

            var targetNodeFrame = message.Pop();
            var targetNode = targetNodeFrame.IsEmpty ? null : targetNodeFrame.ConvertToString();

            var headersFrame = message.Pop();
            var headers = new Headers(headersFrame.ConvertToString());

            var destinations = GetSubscribedNodes(targetNode, address, headers);

            if (destinations?.Any() != true)
            {
                _logger.Warn($"No subscriber to '{address}' (targetNode '{targetNode}'), message will not be routed");
            }

            ///                         [node1][e][UNICAST][CORRELATIONID][SOME.ADDRESS][jeremyb][DESKID=1][SOME.PAYLOAD]
            ///     BROKER  -> NODE:    [node1][e][UNICAST][0000-0000-0000][SOME.ADDRESS][jeremyb][DESKID=1][SOME.PAYLOAD]

            message.Push(headersFrame);
            message.Push(addressFrame);
            message.Push(new byte[] { (byte)Command.Unicast });
            message.PushEmptyFrame();

            var outMessage = new OutMessage(sender, message, destinations.ToList(), correlationId);

            try
            {
                _outQueue.Enqueue(outMessage);
            }
            catch (TerminatingException e)
            {
                _logger?.Warn($"{nameof(OUFRouter)}: {nameof(ProcessUnicastMessage)}: {e}");
            }
        }

        private void ProcessMulticastMessage(NetMQFrame sender, NetMQMessage message)
        {
            /// [CORRELATIONID][ADDRESS][SELECTOR][PAYLOAD]

            var correlationId = message.Pop().Buffer;
            var ackMessage = new AckMessage(sender, correlationId, AckStatus.Success);

            /// [ADDRESS][SELECTOR][PAYLOAD]
            MulticastMessageReceived?.Invoke(message);

            HandleAck(ackMessage);
        }

        private void ProcessHeartbeatMessage(NetMQFrame sender)
        {
            var nodeName = sender.ConvertToString();

            var node = GetNode(nodeName);

            if (null == node)
            {
                _logger?.Warn($"Received Heartbeat from unknown node '{nodeName}'");
                return;
            }

            _logger?.Debug($"Received Heartbeat from '{nodeName}'");
            node.LastSeen = DateTime.UtcNow;
        }

        private bool ProcessReadyMessage(NetMQFrame senderFrame, NetMQMessage message)
        {
            /// [CORRELATIONID][e][e][e][NODEINFO]

            var nodeName = senderFrame.ConvertToString();

            var node = GetNode(nodeName);

            if (null != node)
            {
                _logger?.Info($"Node '{node.Name}' already present. Overwriting");
            }

            byte[] correlationId = message.Pop().Buffer;

            message.Pop();
            message.Pop();
            message.Pop();

            var nodeInfoFrame = message.Pop();

            var nodeInfo = nodeInfoFrame.IsEmpty ? null : Serializer.Deserialize<NodeInformation>(nodeInfoFrame.Buffer);

            node = new Model.Node(senderFrame.Buffer, nodeInfo);

            var added = AddNode(node);

            _logger?.Info($"READY processed. Node '{added.Name}' added");

            var ackMessage = new AckMessage(senderFrame, correlationId, AckStatus.Success);
            HandleAck(ackMessage);

            return true;
        }

        private bool ProcessUnsubscribeMessage(NetMQFrame senderFrame, NetMQMessage message)
        {
            var nodeName = senderFrame.ConvertToString();

            if (message.IsEmpty)
            {
                _logger?.Error($"Empty message from '{nodeName}' discarded");
            }

            var node = GetNode(nodeName);

            if (null == node)
            {
                _logger?.Error($"Unsubscribe from unknown node '{nodeName}'");
                return false;
            }

            byte[] correlationId = message.Pop().Buffer;
            var ackMessage = new AckMessage(senderFrame, correlationId, AckStatus.Success);
            HandleAck(ackMessage);

            message.Pop();
            message.Pop();
            message.Pop();

            var subscriptionBytes = message.Pop().Buffer;
            var subscription = Serializer.Deserialize<Subscription>(subscriptionBytes);

            node.Unsubscribe(subscription);

            _logger?.Info($"UNSUBSCRIBE processed. Node '{nodeName}' unsubscribed from '{subscription}'");
            return true;
        }

        private bool ProcessSubscribeMessage(NetMQFrame senderFrame, NetMQMessage message)
        {
            var nodeName = senderFrame.ConvertToString();

            if (message.IsEmpty)
            {
                _logger?.Error($"Empty message from '{nodeName}' discarded");
            }

            var node = GetNode(nodeName);

            if (null == node)
            {
                _logger?.Error($"Subscribe from unknown node '{nodeName}'");
                return false;
            }

            byte[] correlationId = message.Pop().Buffer;
            var ackMessage = new AckMessage(senderFrame, correlationId, AckStatus.Success);
            HandleAck(ackMessage);

            message.Pop();
            message.Pop();
            message.Pop();

            var subscriptionBytes = message.Pop().Buffer;
            var subscription = Serializer.Deserialize<Subscription>(subscriptionBytes);

            node.Subscribe(subscription);

            _logger?.Info($"SUBSCRIBE processed. Node '{nodeName}' subscribed to '{subscription}'");
            return true;
        }

        #region IDisposable
        public void Dispose()
        {
            _locker.Dispose();
            _routineTimer.Enable = false;

            try
            {
                _socket.Unbind(_endPoint);
                _logger.Info($"{nameof(OUFRouter)}: Unbind successful");
            }
            catch
            {
                _logger.Warn($"{nameof(OUFRouter)}: Unbind failed");
            }

            try { _monitor.DetachFromPoller(); } catch { }
            try { _poller.RemoveAndDispose(_outQueue); } catch { }
            try { _poller.RemoveAndDispose(_ackQueue); } catch { }
            try { _poller.RemoveAndDispose(_socket); } catch { }
            try { _monitor.Stop(); } catch { }
            try { _poller.Stop(); } catch { }
            try { _outQueue.Dispose(); } catch { }
            try { _ackQueue.Dispose(); } catch { }

            try { _poller.Dispose(); } catch { }
            try { _monitor.Dispose(); } catch { }

            GC.SuppressFinalize(this);
        }
        #endregion IDisposable
    }
}
