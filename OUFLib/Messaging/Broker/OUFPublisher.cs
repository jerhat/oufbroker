using NetMQ;
using NetMQ.Monitoring;
using NetMQ.Sockets;
using OUFLib.Logging;
using System;
using System.Reflection;

namespace OUFLib.Messaging.Broker
{
    internal interface IOUFPublisher : IDisposable
    {
        void Run();
        void Send(NetMQMessage message);
    }

    internal class OUFPublisher : IOUFPublisher
    {
        private readonly IOUFLogger _logger;

        private volatile bool _isRunning;
        private volatile bool _isBound;

        internal readonly NetMQSocket _socket;
        private readonly NetMQPoller _poller;
        private readonly NetMQMonitor _monitor;
        private readonly NetMQQueue<NetMQMessage> _outQueue;

        private readonly string _endPoint;

        internal OUFPublisher(string endpoint, IOUFLogger logger)
        {
            _logger = logger;

            _endPoint = endpoint;

            if (string.IsNullOrWhiteSpace(_endPoint))
                throw new ArgumentNullException(nameof(_endPoint), "A endpoint where the publisher binds to must be given!");

            _socket = new PublisherSocket();

            _outQueue = new NetMQQueue<NetMQMessage>();
            _outQueue.ReceiveReady += Enqueued;

            _poller = new NetMQPoller() { _socket, _outQueue };

            _monitor = new NetMQMonitor(_socket, $"inproc://MONITOR.PUBLISHER.{Guid.NewGuid()}", SocketEvents.Accepted | SocketEvents.Disconnected);

            _monitor.Accepted += MonitorAccepted;
            _monitor.Disconnected += MonitorDisconnected;

            _monitor.AttachToPoller(_poller);
        }

        public void Send(NetMQMessage message)
        {
            try
            {
                _outQueue.Enqueue(message);
            }
            catch (TerminatingException e)
            {
                _logger?.Warn($"{nameof(OUFPublisher)}: {nameof(Send)}: {e}");
            }
        }

        private void Enqueued(object sender, NetMQQueueEventArgs<NetMQMessage> e)
        {
            var message = e.Queue?.Dequeue();

            try
            {
                _socket.SendMultipartMessage(message);
            }
            catch (Exception exception)
            {
                _logger?.Warn($"Unable to deliver message: {exception}");
            }
        }

        private void MonitorDisconnected(object sender, NetMQMonitorSocketEventArgs e)
        {
            _logger?.Info($"Disconnection occured");
        }

        private void MonitorAccepted(object sender, NetMQMonitorSocketEventArgs e)
        {
            _logger?.Info($"Publisher accepting new connection");
        }

        private void Bind()
        {
            if (_isBound)
                return;

            try
            {
                _socket.Bind(_endPoint);
            }
            catch (Exception e)
            {
                var error = $"The bind operation failed on {_endPoint}";
                error += $"\nMessage: {e.Message}";
                throw new ApplicationException(error);
            }

            _isBound = true;

            _logger?.Info($"{nameof(OUFPublisher)} is active on {_endPoint}");
        }

        public void Run()
        {
            if (_isRunning)
                throw new InvalidOperationException("Can't start same publisher more than once!");

            if (!_isBound)
                Bind();

            _isRunning = true;

            _logger?.Info($"{nameof(OUFPublisher)} started to listen for incoming messages");

            _poller.RunAsync(nameof(OUFPublisher));

            _isRunning = false;
        }

        #region IDisposable
        public void Dispose()
        {
            try { _socket.Unbind(_endPoint); } catch { }

            try { _monitor.DetachFromPoller(); } catch { }

            try { _poller.RemoveAndDispose(_outQueue); } catch { }
            try { _poller.RemoveAndDispose(_socket); } catch { }

            try { _monitor.Stop(); } catch { }
            try { _poller.Stop(); } catch { }

            try { _monitor.Dispose(); } catch { }
            try { _outQueue.Dispose(); } catch { }

            try { _poller.Dispose(); } catch { }

            GC.SuppressFinalize(this);
        }
        #endregion IDisposable
    }
}
