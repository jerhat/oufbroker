using Microsoft.Extensions.Options;
using NLog;
using NLog.Targets;
using OUFLib.Helpers;
using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;

namespace OUFLib.Logging
{
    public class OUFLoggerSettings
    {
        public string LogFolder { get; set; }
        public string LogFileName { get; set; }
        public LogLevel LogMinLevel { get; set; }
        public string LogLayout { get; set; }
        public int LogMaxArchiveFiles { get; set; }
        public string LogArchiveDateFormat { get; set; }

        public OUFLoggerSettings()
        {
            LogFolder = "${basedir}/logs";
            LogFileName = Assembly.GetEntryAssembly().GetName().Name;
            LogMinLevel = LogLevel.Info;
            LogLayout = @"${date:format=yyyyMMdd HH\:mm\:ss.fff}|${processname}|${processid}|${level}|${logger}|${message}";
            LogArchiveDateFormat = "yyyyMMdd";
            LogMaxArchiveFiles = 60;
        }
    }

    public interface IOUFLogger
    {
        byte[] GetLogFile(DateTime asof);
        ILogger Logger { get; }

        void Info(string message, params object[] args);
        void Info(Exception exception, string message);
        void Warn(string message, params object[] args);
        void Warn(Exception exception, string message);
        void Error(string message, params object[] args);
        void Error(Exception exception, string message);
        void Debug(string message, params object[] args);
        void Debug(Exception exception, string message);
    }

    public class OUFLogger : IOUFLogger
    {
        private readonly OUFLoggerSettings _settings;

        private static readonly ILogger _logger = LogManager.GetCurrentClassLogger();

        public OUFLogger(IOptions<OUFLoggerSettings> settings)
        {
            _settings = settings?.Value ?? new OUFLoggerSettings();

            InitLogger();
        }

        private void InitLogger()
        {
            var config = new NLog.Config.LoggingConfiguration();

            // Targets where to log to: File and Console
            var logfile = new FileTarget("logfile")
            {
                FileName = $"{_settings.LogFolder}/{_settings.LogFileName}.log",
                Layout = _settings.LogLayout,
                ArchiveEvery = FileArchivePeriod.Day,
                ArchiveNumbering = ArchiveNumberingMode.Date,
                ArchiveDateFormat = _settings.LogArchiveDateFormat,
                ArchiveFileName = $"{_settings.LogFolder}/{_settings.LogFileName}.{{#}}.zip",
                EnableArchiveFileCompression = true,
                MaxArchiveFiles = _settings.LogMaxArchiveFiles,
                ConcurrentWrites = true,
                Encoding =  Encoding.UTF8
            };

            var logconsole = new ColoredConsoleTarget("ColoredConsole")
            {
                Layout = _settings.LogLayout
            };

            // Rules for mapping loggers to targets            
            config.AddRule(_settings.LogMinLevel, LogLevel.Fatal, logconsole);
            config.AddRule(_settings.LogMinLevel, LogLevel.Fatal, logfile);

#if DEBUG || TEST
            var logdebugger = new DebuggerTarget();
            config.AddRule(_settings.LogMinLevel, LogLevel.Fatal, logdebugger);
#endif

            // Apply config           
            LogManager.Configuration = config;
        }

        public ILogger Logger => _logger;

        public byte[] GetLogFile(DateTime asof)
        {
            string filename = GetLogFilename(asof);

            if (string.IsNullOrWhiteSpace(filename))
            {
                _logger.Error($"{nameof(GetLogFile)}: could not get log filename asof {asof}");
                return null;
            }

            if (!File.Exists(filename))
            {
                _logger.Error($"{nameof(GetLogFile)}: log filename {filename} does not exist");
                return null;
            }


            if (ZipHelper.IsZip(filename))
            {
                return ZipHelper.UnZip(filename);
            }
            else
            {
                return File.ReadAllBytes(filename);
            }
        }

        private string GetLogFilename(DateTime asof)
        {
            if (LogManager.Configuration.AllTargets.FirstOrDefault(t => t is FileTarget) is not FileTarget fileTarget)
                return null;

            if (asof.Date == DateTime.Today.Date)
            {
                return fileTarget.FileName?.Render(new LogEventInfo { Level = LogLevel.Info });
            }
            else
            {
                return fileTarget.ArchiveFileName?.Render(new LogEventInfo { Level = LogLevel.Info })
                    ?.Replace("{#}", asof.ToString(_settings.LogArchiveDateFormat));
            }
        }

        public void Info(string message, params object[] args)
        {
            _logger.Info(message, args);
        }

        public void Warn(string message, params object[] args)
        {
            _logger.Warn(message, args);
        }

        public void Error(string message, params object[] args)
        {
            _logger.Error(message, args);
        }

        public void Debug(string message, params object[] args)
        {
            _logger.Debug(message, args);
        }

        public void Info(Exception exception, string message)
        {
            _logger.Info(exception, message);
        }

        public void Warn(Exception exception, string message)
        {
            _logger.Warn(exception, message);
        }

        public void Error(Exception exception, string message)
        {
            _logger.Error(exception, message);
        }

        public void Debug(Exception exception, string message)
        {
            _logger.Debug(exception, message);
        }
    }
}
