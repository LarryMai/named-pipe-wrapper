using Microsoft.Win32.SafeHandles;
using NamedPipeWrapper.IO;
using NamedPipeWrapper.Threading;
using System;
using System.Collections.Generic;
using System.IO.Pipes;
using System.Runtime.InteropServices;
using System.Security.AccessControl;
using System.Security.Principal;

namespace NamedPipeWrapper
{
    /// <summary>
    /// Wraps a <see cref="NamedPipeServerStream"/> and provides multiple simultaneous client connection handling.
    /// </summary>
    /// <typeparam name="TReadWrite">Reference type to read from and write to the named pipe</typeparam>
    public class NamedPipeServer<TReadWrite> : Server<TReadWrite, TReadWrite> where TReadWrite : class
    {
        /// <summary>
        /// Constructs a new <c>NamedPipeServer</c> object that listens for client connections on the given <paramref name="pipeName"/>.
        /// </summary>
        /// <param name="pipeName">Name of the pipe to listen on</param>
        public NamedPipeServer(string pipeName)
            : base(pipeName)
        {
        }
    }

    /// <summary>
    /// Wraps a <see cref="NamedPipeServerStream"/> and provides multiple simultaneous client connection handling.
    /// </summary>
    /// <typeparam name="TRead">Reference type to read from the named pipe</typeparam>
    /// <typeparam name="TWrite">Reference type to write to the named pipe</typeparam>
    public class Server<TRead, TWrite>
        where TRead : class
        where TWrite : class
    {
        /// <summary>
        /// Invoked whenever a client connects to the server.
        /// </summary>
        public event ConnectionEventHandler<TRead, TWrite> ClientConnected;

        /// <summary>
        /// Invoked whenever a client disconnects from the server.
        /// </summary>
        public event ConnectionEventHandler<TRead, TWrite> ClientDisconnected;

        /// <summary>
        /// Invoked whenever a client sends a message to the server.
        /// </summary>
        public event ConnectionMessageEventHandler<TRead, TWrite> ClientMessage;

        /// <summary>
        /// Invoked whenever an exception is thrown during a read or write operation.
        /// </summary>
        public event PipeExceptionEventHandler Error;

        private readonly string _pipeName;
        private readonly List<NamedPipeConnection<TRead, TWrite>> _connections = new List<NamedPipeConnection<TRead, TWrite>>();

        private int _nextPipeId;

        private volatile bool _shouldKeepRunning;
        private volatile bool _isRunning;

        /// <summary>
        /// Constructs a new <c>NamedPipeServer</c> object that listens for client connections on the given <paramref name="pipeName"/>.
        /// </summary>
        /// <param name="pipeName">Name of the pipe to listen on</param>
        public Server(string pipeName)
        {
            _pipeName = pipeName;
        }

        /// <summary>
        /// Begins listening for client connections in a separate background thread.
        /// This method returns immediately.
        /// </summary>
        public void Start()
        {
            _shouldKeepRunning = true;
            var worker = new Worker();
            worker.Error += OnError;
            worker.DoWork(ListenSync);
        }

        /// <summary>
        /// Sends a message to all connected clients asynchronously.
        /// This method returns immediately, possibly before the message has been sent to all clients.
        /// </summary>
        /// <param name="message"></param>
        public void PushMessage(TWrite message)
        {
            lock (_connections)
            {
                foreach (var client in _connections)
                {
                    client.PushMessage(message);
                }
            }
        }

        /// <summary>
        /// push message to the given client.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="clientName"></param>
        public void PushMessage(TWrite message, string clientName)
        {
            lock (_connections)
            {
                foreach (var client in _connections)
                {
                    if (client.Name == clientName)
                        client.PushMessage(message);
                }
            }
        }

        /// <summary>
        /// Closes all open client connections and stops listening for new ones.
        /// </summary>
        public void Stop()
        {
            _shouldKeepRunning = false;

            lock (_connections)
            {
                foreach (var client in _connections.ToArray())
                {
                    client.Close();
                }
            }

            // If background thread is still listening for a client to connect,
            // initiate a dummy connection that will allow the thread to exit.
            //dummy connection will use the local server name.
            var dummyClient = new NamedPipeClient<TRead, TWrite>(_pipeName, ".");
            dummyClient.Start();
            dummyClient.WaitForConnection(TimeSpan.FromSeconds(2));
            dummyClient.Stop();
            dummyClient.WaitForDisconnection(TimeSpan.FromSeconds(2));
        }

        #region Private methods

        private void ListenSync()
        {
            _isRunning = true;
            while (_shouldKeepRunning)
            {
                WaitForConnection(_pipeName);
            }
            _isRunning = false;
        }

        private void WaitForConnection(string pipeName)
        {
            NamedPipeServerStream handshakePipe = null;
            NamedPipeServerStream dataPipe = null;
            NamedPipeConnection<TRead, TWrite> connection = null;

            var connectionPipeName = GetNextConnectionPipeName(pipeName);

            try
            {
                // Send the client the name of the data pipe to use
                handshakePipe = PipeServerFactory.CreateAndConnectPipe(pipeName);
                var handshakeWrapper = new PipeStreamWrapper<string, string>(handshakePipe);
                handshakeWrapper.WriteObject(connectionPipeName);
                handshakeWrapper.WaitForPipeDrain();
                handshakeWrapper.Close();

                // Wait for the client to connect to the data pipe
                dataPipe = PipeServerFactory.CreatePipe(connectionPipeName);
                dataPipe.WaitForConnection();

                // Add the client's connection to the list of connections
                connection = ConnectionFactory.CreateConnection<TRead, TWrite>(dataPipe);
                connection.ReceiveMessage += ClientOnReceiveMessage;
                connection.Disconnected += ClientOnDisconnected;
                connection.Error += ConnectionOnError;
                connection.Open();

                lock (_connections)
                {
                    _connections.Add(connection);
                }

                ClientOnConnected(connection);
            }
            // Catch the IOException that is raised if the pipe is broken or disconnected.
            catch (Exception e)
            {
                Console.Error.WriteLine("Named pipe is broken or disconnected: {0}", e);

                Cleanup(handshakePipe);
                Cleanup(dataPipe);

                ClientOnDisconnected(connection);
            }
        }

        private void ClientOnConnected(NamedPipeConnection<TRead, TWrite> connection)
        {
            if (ClientConnected != null)
                ClientConnected(connection);
        }

        private void ClientOnReceiveMessage(NamedPipeConnection<TRead, TWrite> connection, TRead message)
        {
            if (ClientMessage != null)
                ClientMessage(connection, message);
        }

        private void ClientOnDisconnected(NamedPipeConnection<TRead, TWrite> connection)
        {
            if (connection == null)
                return;

            lock (_connections)
            {
                _connections.Remove(connection);
            }

            if (ClientDisconnected != null)
                ClientDisconnected(connection);
        }

        /// <summary>
        ///     Invoked on the UI thread.
        /// </summary>
        private void ConnectionOnError(NamedPipeConnection<TRead, TWrite> connection, Exception exception)
        {
            OnError(exception);
        }

        /// <summary>
        ///     Invoked on the UI thread.
        /// </summary>
        /// <param name="exception"></param>
        private void OnError(Exception exception)
        {
            if (Error != null)
                Error(exception);
        }

        private string GetNextConnectionPipeName(string pipeName)
        {
            return string.Format("{0}_{1}", pipeName, ++_nextPipeId);
        }

        private static void Cleanup(NamedPipeServerStream pipe)
        {
            if (pipe == null) return;
            using (var x = pipe)
            {
                x.Close();
            }
        }

        #endregion
    }

    static class PipeServerFactory
    {
        public static NamedPipeServerStream CreateAndConnectPipe(string pipeName)
        {
            var pipe = CreatePipe(pipeName);
            pipe.WaitForConnection();
            return pipe;
        }

        public static NamedPipeServerStream CreatePipe(string pipeName)
        {
          
          
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                PipeSecurity pipeSecurity = new PipeSecurity();

                WindowsIdentity identity = WindowsIdentity.GetCurrent();
                WindowsPrincipal principal = new WindowsPrincipal(identity);

                if (principal.IsInRole(WindowsBuiltInRole.Administrator))
                {
                    // Allow the Administrators group full access to the pipe.
                    pipeSecurity.AddAccessRule(new PipeAccessRule(
                        new SecurityIdentifier(WellKnownSidType.BuiltinAdministratorsSid, null).Translate(typeof(NTAccount)),
                        PipeAccessRights.FullControl, AccessControlType.Allow));
                }
                else
                {
                    // Allow the current user read/write access to the pipe.
                    pipeSecurity.AddAccessRule(new PipeAccessRule(
                        WindowsIdentity.GetCurrent().User,
                        PipeAccessRights.ReadWrite, AccessControlType.Allow));
                }

                // Unfortunately, .NET Core does not support passing in a PipeSecurity object into the constructor for
                // NamedPipeServerStream so we are creating native Named Pipes and securing them using native APIs. The
                // issue on .NET Core regarding Named Pipe security is here: https://github.com/dotnet/corefx/issues/30170
                // 99% of this code was borrowed from PowerShell here:
                // https://github.com/PowerShell/PowerShell/blob/master/src/System.Management.Automation/engine/remoting/common/RemoteSessionNamedPipe.cs#L124-L256
                return NamedPipeNative.CreateNamedPipe(pipeName, pipeSecurity);
            }
            else
            {
                // This handles the Unix case since PipeSecurity is not supported on Unix.
                // Instead, we use chmod in Start-EditorServices.ps1
                 
                return new NamedPipeServerStream(pipeName, PipeDirection.InOut, 1
              , PipeTransmissionMode.Byte, PipeOptions.Asynchronous | PipeOptions.WriteThrough, 0, 0);
            }
        }
    }

    /// <summary>
    /// Native API for Named Pipes
    /// This code was borrowed from PowerShell here:
    /// https://github.com/PowerShell/PowerShell/blob/master/src/System.Management.Automation/engine/remoting/common/RemoteSessionNamedPipe.cs#L124-L256
    /// </summary>
    internal static class NamedPipeNative
    {
        #region Pipe constants

        // Pipe open mode
        internal const uint PIPE_ACCESS_DUPLEX = 0x00000003;

        // Pipe modes
        internal const uint PIPE_TYPE_BYTE = 0x00000000;
        internal const uint FILE_FLAG_OVERLAPPED = 0x40000000;
        internal const uint FILE_FLAG_FIRST_PIPE_INSTANCE = 0x00080000;
        internal const uint PIPE_READMODE_BYTE = 0x00000000;

        #endregion

        #region Data structures

        [StructLayout(LayoutKind.Sequential)]
        internal class SECURITY_ATTRIBUTES
        {
            /// <summary>
            /// The size, in bytes, of this structure. Set this value to the size of the SECURITY_ATTRIBUTES structure.
            /// </summary>
            public int NLength;

            /// <summary>
            /// A pointer to a security descriptor for the object that controls the sharing of it.
            /// </summary>
            public IntPtr LPSecurityDescriptor = IntPtr.Zero;

            /// <summary>
            /// A Boolean value that specifies whether the returned handle is inherited when a new process is created.
            /// </summary>
            public bool InheritHandle;

            /// <summary>
            /// Initializes a new instance of the SECURITY_ATTRIBUTES class
            /// </summary>
            public SECURITY_ATTRIBUTES()
            {
                this.NLength = 12;
            }
        }

        #endregion

        #region Pipe methods

        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
        internal static extern SafePipeHandle CreateNamedPipe(
           string lpName,
           uint dwOpenMode,
           uint dwPipeMode,
           uint nMaxInstances,
           uint nOutBufferSize,
           uint nInBufferSize,
           uint nDefaultTimeOut,
           SECURITY_ATTRIBUTES securityAttributes);

        internal static SECURITY_ATTRIBUTES GetSecurityAttributes(GCHandle securityDescriptorPinnedHandle, bool inheritHandle = false)
        {
            SECURITY_ATTRIBUTES securityAttributes = new NamedPipeNative.SECURITY_ATTRIBUTES();
            securityAttributes.InheritHandle = inheritHandle;
            securityAttributes.NLength = (int)Marshal.SizeOf(securityAttributes);
            securityAttributes.LPSecurityDescriptor = securityDescriptorPinnedHandle.AddrOfPinnedObject();
            return securityAttributes;
        }

        /// <summary>
        /// Helper method to create a PowerShell transport named pipe via native API, along
        /// with a returned .Net NamedPipeServerStream object wrapping the named pipe.
        /// </summary>
        /// <param name="pipeName">Named pipe core name.</param>
        /// <param name="securityDesc"></param>
        /// <returns>NamedPipeServerStream</returns>
        internal static NamedPipeServerStream CreateNamedPipe(
            string pipeName,
            PipeSecurity pipeSecurity)

        {
            string fullPipeName = @"\\.\pipe\" + pipeName;
            CommonSecurityDescriptor securityDesc = new CommonSecurityDescriptor(false, false, pipeSecurity.GetSecurityDescriptorBinaryForm(), 0);

            // Create optional security attributes based on provided PipeSecurity.
            NamedPipeNative.SECURITY_ATTRIBUTES securityAttributes = null;
            GCHandle? securityDescHandle = null;
            if (securityDesc != null)
            {
                byte[] securityDescBuffer = new byte[securityDesc.BinaryLength];
                securityDesc.GetBinaryForm(securityDescBuffer, 0);

                securityDescHandle = GCHandle.Alloc(securityDescBuffer, GCHandleType.Pinned);
                securityAttributes = NamedPipeNative.GetSecurityAttributes(securityDescHandle.Value);
            }

            // Create named pipe.
            SafePipeHandle pipeHandle = NamedPipeNative.CreateNamedPipe(
                fullPipeName,
                NamedPipeNative.PIPE_ACCESS_DUPLEX | NamedPipeNative.FILE_FLAG_FIRST_PIPE_INSTANCE | NamedPipeNative.FILE_FLAG_OVERLAPPED,
                NamedPipeNative.PIPE_TYPE_BYTE | NamedPipeNative.PIPE_READMODE_BYTE,
                1,
                1024,
                1024,
                0,
                securityAttributes);

            int lastError = Marshal.GetLastWin32Error();
            if (securityDescHandle != null)
            {
                securityDescHandle.Value.Free();
            }

            if (pipeHandle.IsInvalid)
            {
                throw new InvalidOperationException();
            }
            // Create the .Net NamedPipeServerStream wrapper.
            try
            {
                return new NamedPipeServerStream(
                    PipeDirection.InOut,
                    true,                       // IsAsync
                    false,                      // IsConnected
                    pipeHandle);
            }
            catch (Exception)
            {
                pipeHandle.Dispose();
                throw;
            }
        }
        #endregion
    }
}
