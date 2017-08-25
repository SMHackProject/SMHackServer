namespace SMHackServer {
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Data.SQLite;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Serialization;
    using SMHackCore;

    public class ServerInterfaceImpl : ServerInterface {
        private readonly ConcurrentDictionary<string, SQLiteCommand> _commands =
            new ConcurrentDictionary<string, SQLiteCommand>();

        private readonly SQLiteConnection _dbConnection;

        private readonly JsonSerializerSettings _jsonSettings = new JsonSerializerSettings {
            ContractResolver = new IgnoredContractResolver()
        };

        private readonly Dictionary<int, Process> _processes = new Dictionary<int, Process>();

        public ServerInterfaceImpl(string connectstring, string pluginConfigPath) {
            PluginConfig = new PluginConfig(pluginConfigPath);
            PluginConfigDirectory = Path.GetDirectoryName(pluginConfigPath);
            _dbConnection = new SQLiteConnection(connectstring);
        }

        public override PluginConfig PluginConfig { get; }
        public override string PluginConfigDirectory { get; }

        public static void Main(string[] args) {
            if (args.Length < 3)
                return;
            var serverInterface = new ServerInterfaceImpl(args[0], args[1]);
            serverInterface.Run();
        }

        public void Run() {
            try {
                InitDatabase();
                ServerLog("Server Starting");
                var args = GetRest(Environment.CommandLine, 3);
                if (GetCommandLine(args, out var name, out var restargs))
                    DoCreateAndInject(name, restargs);
                else
                    throw new ArgumentException(args);
                DoLoop();
                ServerLog("Server Stopped");
            } catch (Exception e) {
                ServerLog($"Server Stopped, Exception : \n{e}");
            }
        }

        public override void DoInject(int id) {
            DecoHook(id);
            base.DoInject(id);
        }

        public override void Connect(int id, string image) {
            var process = FetchProcess(id);
            Console.WriteLine(
                "{0:yyyy-MM-dd HH:mm:ss.fff}[{1}-{2}]connect({3})",
                DateTime.Now,
                process.Id,
                process.ProcessName,
                image);
            ExecuteSql(
                "INSERT INTO ClientLog(pid, name, level, message) VALUES (?, ?, ?, json_object('image', ?))",
                process.Id,
                process.ProcessName,
                EventLevel.Connect,
                image);
        }

        public override void DoLog(params ClientLogPacket[] packets) {
            if (packets.Length == 0)
                return;
            lock (_dbConnection) {
                using (var transaction = _dbConnection.BeginTransaction()) {
                    foreach (var packet in packets) {
                        var process = FetchProcess(packet.Pid);
                        var serializeObject = JsonConvert.SerializeObject(packet, _jsonSettings);
                        Console.WriteLine(
                            "{0:yyyy-MM-dd HH:mm:ss.fff}[{1}-{2}]{3}",
                            packet.Time,
                            process.Id,
                            process.ProcessName,
                            serializeObject);
                        ExecuteSql(
                            "INSERT INTO ClientLog(date, pid, name, level, message) VALUES (?, ?, ?, ?, ?)",
                            packet.Time,
                            packet.Pid,
                            process.ProcessName,
                            packet.Message is Exception ? EventLevel.Exception : EventLevel.Message,
                            serializeObject);
                    }
                    transaction.Commit();
                }
            }
        }

        private void DoLoop() {
            Process.GetCurrentProcess().PriorityClass = ProcessPriorityClass.Idle;
            while (_processes.Count != 0)
                lock (_processes) {
                    Monitor.Wait(_processes);
                }
        }

        private void InitDatabase() {
            _dbConnection.Open();
            _dbConnection.EnableExtensions(true);
            _dbConnection.LoadExtension("SQLite.Interop.dll", "sqlite3_json_init");
            ExecuteSql(
                "CREATE TABLE IF NOT EXISTS ServerLog(" +
                "  id INTEGER PRIMARY KEY AUTOINCREMENT," +
                "  date TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
                "  message TEXT NOT NULL" +
                ")");
            ExecuteSql(
                "CREATE TABLE IF NOT EXISTS ClientLog(" +
                "  id INTEGER PRIMARY KEY AUTOINCREMENT," +
                "  date TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
                "  pid INTEGER NOT NULL," +
                "  name TEXT NOT NULL," +
                "  level INTEGER NOT NULL," +
                "  message JSON NOT NULL" +
                ")");
        }

        private SQLiteCommand FetchCommand(string stmt) {
            var ret = _commands.GetOrAdd(stmt, s => new SQLiteCommand(s, _dbConnection));
            ret.Parameters.Clear();
            return ret;
        }

        private void ExecuteSql(string stmt, params object[] param) {
            var cmd = FetchCommand(stmt);
            foreach (var o in param)
                cmd.Parameters.Add(new SQLiteParameter {Value = o});
            cmd.ExecuteNonQuery();
        }

        private void ServerLog(string data) {
            Console.WriteLine("{0:yyyy-MM-dd HH:mm:ss.fff} - {1}", DateTime.Now, data);
            ExecuteSql("INSERT INTO ServerLog (message) VALUES (?)", data);
        }

        private void DecoHook(int id) {
            var targetProcess = FetchProcess(id);
            ExecuteSql(
                "INSERT INTO ClientLog(pid, name, level, message) VALUES (?, ?, ?, json('{}'))",
                id,
                targetProcess.ProcessName,
                EventLevel.Hooked);
            targetProcess.EnableRaisingEvents = true;
            targetProcess.Exited += (sender, args) => {
                Console.WriteLine(
                    "{0:yyyy-MM-dd HH:mm:ss.fff}[{1}-{2}]disconnect",
                    DateTime.Now,
                    targetProcess.Id,
                    targetProcess.ProcessName);
                ExecuteSql(
                    "INSERT INTO ClientLog(pid, name, level, message) VALUES (?, ?, ?, json('{}'))",
                    id,
                    targetProcess.ProcessName,
                    EventLevel.Disconnect);
                lock (_processes) {
                    if (_processes.Remove(targetProcess.Id))
                        Monitor.Pulse(_processes);
                }
            };
        }

        private Process FetchProcess(int id) {
            lock (_processes) {
                Process process;
                if (_processes.TryGetValue(id, out process))
                    return process;
                process = Process.GetProcessById(id);
                _processes.Add(id, process);
                return process;
            }
        }

        private class IgnoredContractResolver : CamelCasePropertyNamesContractResolver {
            protected override IList<JsonProperty>
                CreateProperties(Type type, MemberSerialization memberSerialization) {
                return (from prop in base.CreateProperties(type, memberSerialization)
                        let atts = prop.AttributeProvider.GetAttributes(typeof(ClientIgnored), true)
                        where atts.Count == 0
                        select prop).ToList();
            }
        }

        private enum EventLevel {
            Connect,
            Hooked,
            Disconnect,
            Message,
            Exception
        }
    }
}