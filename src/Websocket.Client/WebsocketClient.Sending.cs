using Dubu;

using System;
using System.Net.Http;
using System.Net.WebSockets;
using System.Threading.Channels;
using System.Threading.Tasks;

using Websocket.Client.Logging;

namespace Websocket.Client
{
    public partial class WebsocketClient
    {
        private DuTaskQueueLoop<string> _messagesTextToSendQueue { get; init; }
        private DuTaskQueueLoop<ArraySegment<byte>> _messagesBinaryToSendQueue { get; init; }

        private async Task send_method(string message)
        {
            try
            {
                await SendInternalSynchronized(message).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                Logger.Error(e, L($"Failed to send text message: '{message}'. Error: {e.Message}"));
            }
        }


        private async Task send_method(ArraySegment<byte> message)
        {
            try
            {
                await SendInternalSynchronized(message).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                Logger.Error(e, L($"Failed to send text message: '{message}'. Error: {e.Message}"));
            }
        }


        /// <summary>
        /// Send text message to the websocket channel. 
        /// It inserts the message to the queue and actual sending is done on an other thread
        /// </summary>
        /// <param name="message">Text message to be sent</param>
        public void Send(string message)
        {
            Validations.Validations.ValidateInput(message, nameof(message));

            _messagesTextToSendQueue.add(message);
        }

        /// <summary>
        /// Send binary message to the websocket channel. 
        /// It inserts the message to the queue and actual sending is done on an other thread
        /// </summary>
        /// <param name="message">Binary message to be sent</param>
        public void Send(byte[] message)
        {
            Validations.Validations.ValidateInput(message, nameof(message));

            _messagesBinaryToSendQueue.add(new ArraySegment<byte>(message));
        }

        /// <summary>
        /// Send binary message to the websocket channel. 
        /// It inserts the message to the queue and actual sending is done on an other thread
        /// </summary>
        /// <param name="message">Binary message to be sent</param>
        public void Send(ArraySegment<byte> message)
        {
            Validations.Validations.ValidateInput(message, nameof(message));

            _messagesBinaryToSendQueue.add(message);
        }

        /// <summary>
        /// Send text message to the websocket channel. 
        /// It doesn't use a sending queue, 
        /// beware of issue while sending two messages in the exact same time 
        /// on the full .NET Framework platform
        /// </summary>
        /// <param name="message">Message to be sent</param>
        public Task SendInstant(string message)
        {
            Validations.Validations.ValidateInput(message, nameof(message));

            return SendInternalSynchronized(message);
        }

        /// <summary>
        /// Send binary message to the websocket channel. 
        /// It doesn't use a sending queue, 
        /// beware of issue while sending two messages in the exact same time 
        /// on the full .NET Framework platform
        /// </summary>
        /// <param name="message">Message to be sent</param>
        public Task SendInstant(byte[] message)
        {
            return SendInternalSynchronized(new ArraySegment<byte>(message));
        }

        /// <summary>
        /// Stream/publish fake message (via 'MessageReceived' observable).
        /// Use for testing purposes to simulate a server message. 
        /// </summary>
        /// <param name="message">Message to be stream</param>
        public void StreamFakeMessage(ResponseMessage message)
        {
            Validations.Validations.ValidateInput(message, nameof(message));

            MessageReceived.publish(message);
        }


        private void StartBackgroundThreadForSendingText()
        {
            // _ = Task.Factory.StartNew(_ => SendTextFromQueue(), TaskCreationOptions.LongRunning, _cancellationTotal.Token);
        }

        private void StartBackgroundThreadForSendingBinary()
        {
            //_ = Task.Factory.StartNew(_ => SendBinaryFromQueue(), TaskCreationOptions.LongRunning, _cancellationTotal.Token);
        }

        private async Task SendInternalSynchronized(string message)
        {
            using (await _locker.LockAsync())
            {
                await SendInternal(message);
            }
        }

        private async Task SendInternal(string message)
        {
            if (!IsClientConnected())
            {
                Logger.Debug(L($"Client is not connected to server, cannot send:  {message}"));
                return;
            }

            Logger.Trace(L($"Sending:  {message}"));
            var buffer = GetEncoding().GetBytes(message);
            var messageSegment = new ArraySegment<byte>(buffer);
            await _client
                .SendAsync(messageSegment, WebSocketMessageType.Text, true, _cancellation.Token)
                .ConfigureAwait(false);
        }

        private async Task SendInternalSynchronized(ArraySegment<byte> message)
        {
            using (await _locker.LockAsync())
            {
                await SendInternal(message);
            }
        }

        private async Task SendInternal(ArraySegment<byte> message)
        {
            if (!IsClientConnected())
            {
                Logger.Debug(L($"Client is not connected to server, cannot send binary, length: {message.Count}"));
                return;
            }

            Logger.Trace(L($"Sending binary, length: {message.Count}"));

            await _client
                .SendAsync(message, WebSocketMessageType.Binary, true, _cancellation.Token)
                .ConfigureAwait(false);
        }
    }
}
