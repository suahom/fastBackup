using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace notZip去重备份程序
{
    public class ProgressReporter
    {
        private int totalFiles;
        private int processedFiles;
        private Stopwatch stopwatch;

        public ProgressReporter(int totalFiles)
        {
            this.totalFiles = totalFiles;
            this.processedFiles = 0;
            this.stopwatch = Stopwatch.StartNew();
        }

        public void ReportProgress()
        {
            processedFiles++;
            double percentage = (double)processedFiles / totalFiles * 100;
            TimeSpan elapsed = stopwatch.Elapsed;
            TimeSpan estimated = TimeSpan.FromTicks((long)(elapsed.Ticks / (percentage / 100)));
            TimeSpan remaining = estimated - elapsed;

            Console.Write($"\r进度: {percentage:F2}% ({processedFiles}/{totalFiles}) | 已用时间: {elapsed:hh\\:mm\\:ss} | 预计剩余时间: {remaining:hh\\:mm\\:ss}");
        }

        public void Complete()
        {
            stopwatch.Stop();
            Console.WriteLine($"\n完成! 总耗时: {stopwatch.Elapsed:hh\\:mm\\:ss}");
        }
    }
}
