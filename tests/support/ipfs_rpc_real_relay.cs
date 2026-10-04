// Host-loopback raw TCP bridge for Docker internal networks that do not publish
// ports. Only docker exec into an exact run-owned node; no HTTP interpretation.
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

public sealed class IpfsRpcRealRelay : IDisposable
{
    private readonly TcpListener listener;
    private readonly CancellationTokenSource stop = new CancellationTokenSource();
    private readonly ConcurrentDictionary<int, Process> children = new ConcurrentDictionary<int, Process>();
    private readonly ConcurrentBag<Task> connections = new ConcurrentBag<Task>();
    private readonly object logLock = new object();
    private readonly string docker, node, log;
    private readonly Task accepting;
    public string Url { get; }

    public IpfsRpcRealRelay(string dockerPath, string ownedNode, string logPath)
    {
        if (!Regex.IsMatch(ownedNode, "^ipfs-s3-rpc-[a-f0-9]{32}-(source|target)$"))
            throw new ArgumentException("Not an exact owned node name.");
        docker = dockerPath; node = ownedNode; log = logPath;
        listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        Url = "http://127.0.0.1:" + ((IPEndPoint)listener.LocalEndpoint).Port;
        Write("loopback_listener=" + Url + " exec_node=" + node + " destination=127.0.0.1:5001");
        accepting = Accept();
    }

    private void Write(string text)
    {
        lock (logLock) File.AppendAllText(log, text + Environment.NewLine);
    }

    private async Task Accept()
    {
        try
        {
            while (!stop.IsCancellationRequested)
            {
                TcpClient client = await listener.AcceptTcpClientAsync(stop.Token);
                connections.Add(Forward(client));
            }
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
        catch (SocketException) when (stop.IsCancellationRequested) { }
    }

    private async Task Forward(TcpClient client)
    {
        using (client)
        using (var process = new Process())
        {
            var start = new ProcessStartInfo(docker) {
                UseShellExecute = false, CreateNoWindow = true,
                RedirectStandardInput = true, RedirectStandardOutput = true,
                RedirectStandardError = true
            };
            foreach (string argument in new[] { "exec", "-i", node, "nc", "127.0.0.1", "5001" })
                start.ArgumentList.Add(argument);
            process.StartInfo = start;
            int id = 0;
            try
            {
                if (!process.Start()) throw new IOException("Could not start owned exec relay.");
                id = process.Id;
                children.TryAdd(id, process);
                Write("exec_started=" + id);
                Task<string> errors = process.StandardError.ReadToEndAsync();
                using (NetworkStream socket = client.GetStream())
                {
                    Task upload = socket.CopyToAsync(process.StandardInput.BaseStream, stop.Token);
                    Task download = process.StandardOutput.BaseStream.CopyToAsync(socket, stop.Token);
                    await await Task.WhenAny(upload, download);
                }
                if (!process.HasExited) process.Kill(true);
                await process.WaitForExitAsync();
                string error = await errors;
                Write("exec_finished=" + id + " exit=" + process.ExitCode + " stderr=" + error.Trim());
            }
            catch (Exception error) when (error is IOException || error is SocketException || error is OperationCanceledException)
            {
                Write("connection_closed=" + id + " reason=" + error.GetType().Name);
            }
            finally
            {
                if (id != 0)
                {
                    if (!process.HasExited) process.Kill(true);
                    await process.WaitForExitAsync();
                    children.TryRemove(id, out _);
                }
            }
        }
    }

    public void Dispose()
    {
        stop.Cancel();
        listener.Stop();
        if (!accepting.Wait(TimeSpan.FromSeconds(10))) throw new TimeoutException("Relay accept loop did not stop.");
        foreach (Process child in children.Values)
            if (!child.HasExited) child.Kill(true);
        if (!Task.WaitAll(connections.ToArray(), TimeSpan.FromSeconds(10)))
            throw new TimeoutException("Owned exec relay connections did not stop.");
        if (!children.IsEmpty) throw new IOException("Owned exec relay processes remain.");
        Write("cleanup=PASS listener_stopped=true owned_exec_children=0");
        stop.Dispose();
    }
}
