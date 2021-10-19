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
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace OUFLib.Messaging.Node
{
    internal interface IOUFDealer : IDisposable
    {
        Task<AckStatus> CastAsync(Casting casting, string address, string targetNode, Headers headers, byte[] payload, bool ack);
        bool IsConnected { get; }
        void Connect();
        Task<Subscription> SubscribeAsync(Casting casting, string address, string selector);
        Task<bool> UnsubscribeAsync(Casting casting, string address);
        Task<AckStatus> ProbeAsync(string node);

        Task<AckStatus> ReleaseLockAsync(string resource, string requestorId);
        Task<AckStatus> AcquireLockAsync(string resource, Lock lockInfo);

        event Action<IOUFDealer> Connected;
        event Action<IOUFDealer> Disconnected;
        event Action<IOUFDealer> Killed;
    }

    internal class OUFDealer : IOUFDealer
    {
        private NetMQSocket _socket;

        private NetMQPoller _poller;
        private NetMQMonitor _monitor;
        private NetMQQueue<NetMQMessage> _outQueue;
        private NetMQTimer _heartBeatTimer;

        private readonly string _brokerAddress;
        private readonly NodeInformation _nodeInfo;
        private readonly string _id;

        private readonly ConcurrentDictionary<string, Subscription> _uniSubscriptions;
        private readonly ConcurrentDictionary<string, Subscription> _multiSubscriptions;

        private readonly ConcurrentDictionary<Guid, Acknowledgement> _acks;

        private readonly IOUFLogger _logger;

        public event Action<IOUFDealer> Connected;
        public event Action<IOUFDealer> Disconnected;
        public event Action<IOUFDealer> Killed;

        private GetNodeStatus _getStatus;

        public bool IsConnected { get; private set; }

        internal OUFDealer(string brokerAddress, string id, NodeInformation nodeInfo, GetNodeStatus getStatus, IOUFLogger logger)
        {
            _brokerAddress = brokerAddress;
            _id = id;
            _nodeInfo = nodeInfo;

            _uniSubscriptions = new ConcurrentDictionary<string, Subscription>();
            _multiSubscriptions = new ConcurrentDictionary<string, Subscription>();
            _acks = new ConcurrentDictionary<Guid, Acknowledgement>();

            _getStatus = getStatus;

            _logger = logger;
        }

        public void Connect()
        {
            if (IsConnected)
                return;

            // if the socket exists dispose it and re-create one
            if (_socket is not null)
            {
                _socket.Unbind(_brokerAddress);
            }

            _socket = new DealerSocket();

            // set identity
            _socket.Options.Identity = Encoding.ASCII.GetBytes(_id);

            // hook up the received message processing method before the socket is connected
            _socket.ReceiveReady += MessageReceived;

            _outQueue = new NetMQQueue<NetMQMessage>();
            _outQueue.ReceiveReady += Enqueued;

            _heartBeatTimer = new NetMQTimer(Constants.HEARTBEAT_INTERVAL_ms);
            _heartBeatTimer.Elapsed += HeartBeatTimer_Elapsed;

            _poller = new NetMQPoller() { _socket, _outQueue, _heartBeatTimer };

            if (_monitor is not null)
            {
                _monitor.DetachFromPoller();
                _monitor.Dispose();
            }

            _monitor = new NetMQMonitor(_socket, $"inproc://MONITOR.UNI.{_id}.{Guid.NewGuid()}", SocketEvents.Connected | SocketEvents.Disconnected);

            _monitor.AttachToPoller(_poller);

            _monitor.Connected += async (s, e) => await MonitorConnected();
            _monitor.Disconnected += (s, e) => MonitorDisconnected();

            _socket.Connect(_brokerAddress);

            _poller.RunAsync(nameof(OUFDealer));
        }

        public void Disconnect()
        {
            _logger?.Info($"Node '{_id}' disconnecting");

            MonitorDisconnected();
            Dispose();
        }

        private ConcurrentDictionary<string, Subscription> GetSubscriptions(Casting casting)
        {
            return casting switch
            {
                Casting.Uni => _uniSubscriptions,
                Casting.Multi => _multiSubscriptions,
                _ => null,
            };
        }

        public async Task<Subscription> SubscribeAsync(Casting casting, string address, string selector)
        {
            var subscription = new Subscription(casting, address, selector);

            if (!subscription.IsValid())
                throw new ApplicationException($"Invalid subscription '{subscription}'!");

            var subscriptions = GetSubscriptions(casting);

            if (subscriptions.ContainsKey(address))
                throw new ApplicationException($"Address '{address}' already subscribed for casting '{casting}'!");

            subscription = subscriptions.GetOrAdd(address, subscription);
            subscription.UpdateSelector(selector);

            if (IsConnected)
            {
                await SendSubscriptionAsync(subscription);
            }

            return subscription;
        }

        public async Task<AckStatus> ProbeAsync(string node)
        {
            _logger?.Info($"Probing {node}...");

            if (!IsConnected)
            {
                _logger.Error($"{nameof(ProbeAsync)}: not connected");
                return AckStatus.Failed;
            }

            if (string.IsNullOrWhiteSpace(node))
            {
                _logger.Warn($"{nameof(ProbeAsync): null node}");
                return AckStatus.Failed;
            }

            return await SendAsync(Command.Probe, null, node, null, null, true);
        }

        public async Task<AckStatus> ReleaseLockAsync(string resource, string requestor)
        {
            _logger?.Info($"{nameof(ReleaseLockAsync)} resource '{resource}', requestor '{requestor}'...");

            if (!IsConnected)
            {
                _logger.Error($"{nameof(ReleaseLockAsync)}: not connected");
                return AckStatus.Failed;
            }

            if (string.IsNullOrWhiteSpace(resource))
            {
                _logger.Warn($"{nameof(ReleaseLockAsync): null resource}");
                return AckStatus.Failed;
            }

            return await SendAsync(Command.Unlock, resource, null, null, Serializer.ToJsonBytes(requestor), true);
        }

        public async Task<AckStatus> AcquireLockAsync(string resource, Lock lockInfo)
        {
            _logger?.Info($"{nameof(AcquireLockAsync)} for resource '{resource}', requestor '{lockInfo.OwnerId}'...");

            if (!IsConnected)
            {
                _logger.Error($"{nameof(AcquireLockAsync)}: not connected");
                return AckStatus.Failed;
            }

            if (string.IsNullOrWhiteSpace(resource))
            {
                _logger.Warn($"{nameof(AcquireLockAsync): null resource}");
                return AckStatus.Failed;
            }

            return await SendAsync(Command.Lock, resource, null, null, Serializer.ToJsonBytes(lockInfo), true);
        }

        private async Task SendSubscriptionAsync(Subscription subscription)
        {
            _logger?.Info($"Subscribing to '{subscription}'");

            await SendAsync(Command.Subscribe, null, null, null, Serializer.ToJsonBytes(subscription), true);
        }

        public async Task<bool> UnsubscribeAsync(Casting casting, string address)
        {
            var subscription = new Subscription(casting, address);

            var res = await SendAsync(Command.Unsubscribe, null, null, null, Serializer.ToJsonBytes(subscription), true);

            if (res == AckStatus.Success)
            {
                return GetSubscriptions(casting).Remove(address, out _);
            }
            else
            {
                _logger.Warn($"{nameof(UnsubscribeAsync)}: subscription to '{address}' could not be removed");
                return false;
            }
        }

        private void HeartBeatTimer_Elapsed(object sender, NetMQTimerEventArgs e)
        {
            if (!IsConnected)
                return;

            Send(Command.Heartbeat);
        }

        private void MonitorDisconnected()
        {
            IsConnected = false;
            _logger?.Info($"Dealer '{_id}' disconnected from broker at '{_brokerAddress}'");

            Disconnected?.Invoke(this);
        }

        private async Task MonitorConnected()
        {
            _logger?.Info($"Dealer '{_id}' connected to broker at '{_brokerAddress}'");

            var ack = await SendAsync(Command.Ready, null, null, null, Serializer.ToJsonBytes(_nodeInfo), true);

            foreach (var subscription in _uniSubscriptions.Values.Union(_multiSubscriptions.Values))
            {
                await SendSubscriptionAsync(subscription);
            }

            IsConnected = true;

            Connected?.Invoke(this);
        }

        private void Enqueued(object sender, NetMQQueueEventArgs<NetMQMessage> e)
        {
            var message = e.Queue?.Dequeue();
            _socket.SendMultipartMessage(message);
        }

        public async Task<AckStatus> CastAsync(Casting casting, string address, string targetNode, Headers headers, byte[] payload, bool ack)
        {
            if (!IsConnected)
            {
                _logger.Error($"{nameof(CastAsync)}: not connected");
                return AckStatus.Failed;
            }

            Command command;

            switch (casting)
            {
                case Casting.Multi:
                    command = Command.Multicast;
                    break;
                case Casting.Uni:
                    command = Command.Unicast;
                    break;
                default:
                    _logger.Error($"Unsupport casting '{casting}'");
                    return AckStatus.Failed;
            }

            return await SendAsync(command, address, targetNode, headers?.ToString(), payload, ack);
        }

        /// <summary>
        /// Send a message to broker
        /// </summary>
        /// <param name="command">command</param>
        /// <param name="payload">the payload to be sent</param>
        private void Send(Command command)
        {
            var msg = new NetMQMessage();

            // set command
            msg.Push(new[] { (byte)command });

            // set empty frame as separator
            msg.Push(NetMQFrame.Empty);

            _logger?.Debug($"Node '{_id}' sending command '{command}' to broker");

            try
            {
                _outQueue.Enqueue(msg);
            }
            catch (TerminatingException e)
            {
                _logger?.Warn($"{nameof(OUFDealer)}: {nameof(Send)}: {e}");
            }
        }

        /// <summary>
        /// Send a message to broker
        /// </summary>
        private async Task<AckStatus> SendAsync(Command command, [NotNull] string address, string targetNode, string headersXml, byte[] payload, bool ack)
        {
            ///     NODE  -> BROKER:  [nodeidentity][e][address][targetNode][headers][payload]
            ///     
            ///     NODE  -> BROKER:  [node1][e][READY]
            ///     NODE  -> BROKER:  [node1][e][SUBSCRIBE][CORRELATIONID][ADDRESS][e][deskid=1][PAYLOAD]
            ///     NODE  -> BROKER:  [node1][e][UNICAST][CORRELATIONID][ADDRESS][jeremyb][HEADERS][PAYLOAD]

            var res = AckStatus.Disabled;

            Acknowledgement acknowledgement = null;

            if (ack)
            {
                acknowledgement = Acknowledgement.Create();
                _acks.GetOrAdd(acknowledgement.CorrelationId, acknowledgement);
            }

            var msg = new NetMQMessage();

            // last frame is the payload
            msg.Push(payload ?? Array.Empty<byte>());

            // then headers
            msg.Push(headersXml ?? string.Empty);

            // then targetNode
            msg.Push(targetNode ?? string.Empty);

            // then address
            msg.Push(address ?? string.Empty);

            msg.Push(ack ? acknowledgement.CorrelationId.ToByteArray() : Array.Empty<byte>());

            // set command
            msg.Push(new[] { (byte)command });

            // set empty frame as separator
            msg.Push(NetMQFrame.Empty);

            _logger?.Debug($"Node '{_id}' sending command '{command}' to broker");

            _outQueue?.Enqueue(msg);

            if (!ack)
            {
                return res;
            }

            _logger.Debug($"{nameof(SendAsync)}: waiting for ack, correlationId='{acknowledgement.CorrelationId}', current semaphore count='{acknowledgement.Semaphore.CurrentCount}'");

            var acked = await acknowledgement.Semaphore.WaitAsync(Constants.ACK_TIMEOUT_ms);

            _logger.Debug($"{nameof(SendAsync)}: processing ack for correlationId='{acknowledgement.CorrelationId}'");

            if (acked)
            {
                res = acknowledgement.Status;
            }
            else
            {
                _acks.Remove(acknowledgement.CorrelationId, out _);
                res = AckStatus.Timeout;
            }

            return res;
        }

        private void MessageReceived(object sender, NetMQSocketEventArgs args)
        {
            ///     BROKER  -> NODE:  [nodeidentity][e][command][address][headers][payload]
            ///     
            ///     BROKER  -> NODE:  [node1][e][UNICAST][SOME.ADDRESS][DESKID=1][SOME.PAYLOAD]
            ///     BROKER  -> NODE:  [node1][e][MULTICAST][SOME.ADDRESS][DESKID=1][SOME.PAYLOAD]
            ///     BROKER  -> NODE:  [node1][e][ACK][CORRELATIONID][ACKSTATUS]
            ///     BROKER  -> NODE:  [node1][e][TRIGGER][JOBDATA]

            NetMQMessage request;

            try
            {
                request = _socket.ReceiveMultipartMessage();
            }
            catch (Exception e)
            {
                _logger?.Error($"{nameof(OUFDealer)}: {nameof(MessageReceived)}: {e}");
                return;
            }

            Task.Run(async () =>
            {
                _logger?.Debug($"Node '{_id}' received '{request}'");

                // pop first empty frame
                request.Pop();

                var commandFrame = request.Pop();

                if (commandFrame.BufferSize > 1)
                    throw new ApplicationException("Command must be one byte not multiple!");

                var command = (Command)commandFrame.Buffer[0];

                switch (command)
                {
                    case Command.Unicast:
                        {
                            var (address, payload) = UnwrapCast(request);
                            await ProcessUnicastReceived(address, payload);
                            break;
                        }
                    case Command.Ack:
                        {
                            var (correlationId, ackStatus) = UnwrapAck(request);
                            ProcessAckReceived(correlationId, ackStatus);
                            break;
                        }
                    case Command.Probe:
                        _logger?.Info($"Probed!");
                        break;
                    case Command.Execute:
                        {
                            var (correlationId, action, arguments) = UnwrapExecute(request);
                            ProcessExecuteReceived(correlationId, action, arguments);
                            break;
                        }
                    default:
                        _logger?.Info($"Node '{_id}' invalid command received!");
                        break;
                }
            });
        }

        private void ProcessExecuteReceived(Guid correlationId, NodeTask action, Dictionary<string, Property> arguments)
        {
            if (!AttributeHelper.HasAttribute<IgnoreDataMemberAttribute>(action))
            {
                _logger?.Info($"{nameof(ProcessExecuteReceived)} action '{action}' received");
            }

            var payload = GetExecutionPayload(action, arguments, out var postAction);

            var response = new NetMQMessage();

            response.Push(payload);
            response.Push(new byte[] { (byte)AckStatus.Success });
            response.Push(correlationId.ToByteArray());
            response.Push(new byte[] { (byte)Command.Ack });
            response.PushEmptyFrame();

            try
            {
                _outQueue.Enqueue(response);
            }
            catch (TerminatingException e)
            {
                _logger?.Warn($"{nameof(OUFDealer)}: {nameof(ProcessExecuteReceived)}: {e}");
            }

            if (null != postAction)
            {
                Task.Run(postAction);
            }
        }

        private byte[] GetExecutionPayload(NodeTask action, Dictionary<string, Property> arguments, out Action postAction)
        {
            postAction = null;

            switch (action)
            {
                case NodeTask.Disconnect:
                    postAction = ProcessDisconnectReceived;
                    break;
                case NodeTask.Kill:
                    postAction = ProcessKilledReceived;
                    break;
                case NodeTask.TriggerAction:
                    var oufActionArg = arguments.GetValueOrDefault(nameof(OUFAction))?.Value;
                    var oufaction = Serializer.Deserialize<OUFAction>(Encoding.ASCII.GetBytes((oufActionArg?.ToString())));
                    ProcessTriggerAction(oufaction);
                    break;
                case NodeTask.GetLogs:
                    return GetLogs(arguments);
                case NodeTask.GetStatus:
                    return GetStatus();
                default:
                    _logger?.Error($"{nameof(GetExecutionPayload)}: unknown action '{action}'");
                    break;
            }

            return Array.Empty<byte>();
        }

        private byte[] GetStatus()
        {
            NodeStatus status = _getStatus?.Invoke();

            if (null == status)
                return Array.Empty<byte>();

            return Serializer.ToJsonBytes(status);
        }

        private void ProcessTriggerAction(OUFAction receivedAction)
        {
            if (null == receivedAction)
            {
                _logger?.Error($"{nameof(ProcessTriggerAction)}: null action received");
                return;
            }

            var nodeAction = _nodeInfo?.Actions.FirstOrDefault(x => x.Name?.Equals(receivedAction.Name, StringComparison.InvariantCultureIgnoreCase) == true);

            if (null == nodeAction)
            {
                _logger?.Error($"{nameof(ProcessTriggerAction)}: unknown action '{receivedAction?.Name}'");
                return;
            }

            string prettyargs = string.Join(";", receivedAction.Arguments?.Select(x => $"{x.Key}=>{x.Value}") ?? Array.Empty<string>());

            _logger?.Info($"{nameof(ProcessTriggerAction)}: '{receivedAction.Name}' args '{prettyargs}'");

            nodeAction.Trigger(receivedAction.Arguments);
        }

        private byte[] GetLogs(Dictionary<string, Property> arguments)
        {
            var asof = ((System.Text.Json.JsonElement)(arguments.GetValueOrDefault("asof").Value)).GetDateTime();

            return _logger.GetLogFile(asof);
        }

        private (Guid correlationId, NodeTask action, Dictionary<string, Property> arguments) UnwrapExecute(NetMQMessage message)
        {
            ///     BROKER -> NODE:  [CORRELATIONID][ACTION][ARGUMENTS]

            if (message.IsEmpty)
            {
                _logger?.Error($"{nameof(UnwrapExecute)}: empty message!");
                return default;
            }

            var correlationFrame = message.Pop();
            if (correlationFrame.BufferSize != 16)
                throw new ApplicationException($"CorrelationId must be 16 frames!");

            var correlationId = new Guid(correlationFrame.Buffer);

            var actionFrame = message.Pop();
            if (actionFrame.BufferSize > 1)
                throw new ApplicationException("Action must be one byte not multiple!");

            var action = (NodeTask)actionFrame.Buffer[0];

            var argumentFrame = message.Pop(); ;
            var arguments = argumentFrame.IsEmpty ? new Dictionary<string, Property>() : Serializer.Deserialize<Dictionary<string, Property>>(argumentFrame.Buffer);

            if (null == arguments)
            {
                _logger?.Error($"{nameof(UnwrapExecute)}: could not deserialize arguments!");
                return default;
            }

            return (correlationId, action, arguments);
        }

        private void ProcessKilledReceived()
        {
            _logger?.Info($"Node '{_id}': KILL command received!");

            Disconnect();
            Killed?.Invoke(this);
        }

        private void ProcessDisconnectReceived()
        {
            Disconnect();
            Connect();
        }

        private void ProcessAckReceived(Guid correlationId,  AckStatus ackStatus)
        {
            _logger?.Debug($"{nameof(ProcessAckReceived)} {correlationId} {ackStatus}");

            if (_acks.TryRemove(correlationId, out var acknowlegment))
            {
                acknowlegment.SetStatus(ackStatus);
                var previousCount = acknowlegment.Semaphore.Release();

                _logger?.Debug($"{nameof(ProcessAckReceived)} {correlationId} {ackStatus} semaphore released, previous count='{previousCount}', new count='{acknowlegment.Semaphore.CurrentCount}'");
            }
        }

        private async Task ProcessUnicastReceived(string address, byte[] payload)
        {
            if (!_uniSubscriptions.TryGetValue(address, out var subscription))
            {
                _logger?.Warn($"{nameof(ProcessUnicastReceived)}: no related subscription found for '{address}'");
                return;
            }

            await subscription.Raise(address, payload);
        }

        /// <summary>
        /// Check the message envelope for errors
        /// and get the command embedded.
        /// The message will be altered!
        /// </summary>
        /// <param name="request">NetMQMessage received</param>
        /// <returns>the received command</returns>
        private (string address, byte[] payload) UnwrapCast(NetMQMessage request)
        {
            ///     BROKER  -> NODE:  [SOME.ADDRESS][DESKID=1][SOME.PAYLOAD]
            ///     BROKER  -> NODE:  [SOME.ADDRESS][DESKID=1][SOME.PAYLOAD]

            var expectedFrameCount = 3;

            if (request.FrameCount != expectedFrameCount)
            {
                var message = $"{nameof(UnwrapCast)}: malformed request received. Expected '{expectedFrameCount}' frames, got '{request.FrameCount}'";
                _logger?.Error($"{nameof(OUFDealer)}: {message}");
                _logger?.Debug(request.ToString());

                throw new ApplicationException(message);
            }

            var address = request.Pop().ConvertToString();

            // discard headers
            _ = request.Pop();
            
            var payload = request.IsEmpty ? null : request.Pop().ToByteArray();

            return (address, payload);
        }

        /// <summary>
        /// Check the message envelope for errors
        /// and get the command embedded.
        /// The message will be altered!
        /// </summary>
        /// <param name="request">NetMQMessage received</param>
        /// <returns>the received command</returns>
        private (Guid correlationId, AckStatus ackStatus) UnwrapAck(NetMQMessage request)
        {
            ///     BROKER  -> NODE:  [CORRELATIONID][ACKSTATUS]

            var expectedFrameCount = 2;

            if (request.FrameCount != expectedFrameCount)
            {
                var message = $"{nameof(UnwrapAck)}: malformed request received. Expected '{expectedFrameCount}' frames, got '{request.FrameCount}'";
                _logger?.Error($"{nameof(OUFDealer)}: {message}");
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

            return (correlationId, ackStatus);
        }

        public void Dispose()
        {
            if (null != _socket)
                _socket.ReceiveReady -= MessageReceived;

            if (null != _outQueue)
                _outQueue.ReceiveReady -= Enqueued;

            if (null != _heartBeatTimer)
                _heartBeatTimer.Elapsed -= HeartBeatTimer_Elapsed;

            if (null != _monitor)
            {
                _monitor.Connected -= async (s, e) => await MonitorConnected();
                _monitor.Disconnected -= (s, e) => MonitorDisconnected();
            }

            try { _socket?.Unbind(_brokerAddress); } catch { }

            try { _monitor?.DetachFromPoller(); } catch { }

            try { _poller?.RemoveAndDispose(_outQueue); } catch { }
            try { _poller?.RemoveAndDispose(_socket); } catch { }

            try { _monitor?.Stop(); } catch { }
            try { _poller?.Stop(); } catch { }

            try { _outQueue?.Dispose(); } catch { }
            
            try { _monitor?.Dispose(); } catch { }
            try { _poller?.Dispose(); } catch { }

            _socket = null;
            _monitor = null;
            _outQueue = null;
            _poller = null;

            GC.SuppressFinalize(this);
        }
    }
}
