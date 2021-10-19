using System;

namespace OUFLib.Scheduler
{
    public interface IOUFScheduledJob
    {
        string Name { get; set; }
        string Cron { get; set; }
        DateTimeOffset? NextExecution { get; set; }
        DateTimeOffset? PreviousExecution { get; set; }
    }

    public class OUFScheduledJob : IOUFScheduledJob
    {
        public string Name { get; set; }
        public string Cron { get; set; }
        public DateTimeOffset? NextExecution { get; set; }
        public DateTimeOffset? PreviousExecution { get; set; }
    }
}
