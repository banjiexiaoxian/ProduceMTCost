using Microsoft.Analytics.Interfaces;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace ProduceMTCost
{
    [SqlUserDefinedReducer(IsRecursive = true)]
    public class ParseRawLog : IReducer
    {

        public override IEnumerable<IRow> Reduce(IRowset input, IUpdatableRow output)

        {
            string lastTrackingId = null;
            string trackingId = null;
            long totalProcessAPICallCount = 0;
            long totalZeroTrackingIdCount = 0;
            List<LogRecord> oneAPICallLogs = new List<LogRecord>();
            foreach (IRow row in input.Rows)
            {

                LogRecord oneRecord = new LogRecord();
                //line structure
                oneRecord.TimeStamp = row.Get<DateTime>("timestamp");
                oneRecord.parentStageId = row.Get<string>("parentstageid");
                oneRecord.stageId = row.Get<string>("stageid");
                oneRecord.success = row.Get<Boolean>("success");
                oneRecord.userId = long.Parse(row.Get<string>("userid"));
                oneRecord.componentName = row.Get<string>("componentname");
                trackingId = oneRecord.trackingId = row.Get<string>("trackingid");
                oneRecord.codeBlockName = row.Get<string>("codeblockname");
                oneRecord.startTime = row.Get<DateTime>("starttime");
                oneRecord.endTime = row.Get<DateTime>("endtime");
                oneRecord.elapsedTime = row.Get<Double>("elapsedtime");
                oneRecord.scaleDimension = long.Parse(row.Get<string>("scaledimension"));
                oneRecord.requestUrl = row.Get<string>("requesturl");
                if (trackingId == "00000000-0000-0000-0000-000000000000")
                {
                    ++totalZeroTrackingIdCount;
                    continue;
                }

                if (lastTrackingId == null)
                {
                    lastTrackingId = trackingId;
                }
                if (lastTrackingId == trackingId)
                {
                    oneAPICallLogs.Add(oneRecord);
                }
                else
                {
                    CalculateOneAPICallCost calculator = new CalculateOneAPICallCost(oneAPICallLogs, lastTrackingId, oneAPICallLogs.First().userId, oneAPICallLogs.First().startTime);
                    string[] rs = calculator.Calculate().Split('\t');

                    output.Set<string>("StartTime", rs[0]);

                    output.Set<string>("TrackingId", rs[1]);
                    output.Set<long>("UserId", long.Parse(rs[2]));
                    output.Set<double>("MTCost", double.Parse(rs[3]));
                    output.Set<double>("E2ECost", double.Parse(rs[4]));
                    output.Set<long>("TotalLogCount", long.Parse(rs[5]));
                    output.Set<long>("TotalIgnoreRecordCount", long.Parse(rs[6]));
                    output.Set<long>("TotalSameStageIdCount", long.Parse(rs[7]));
                    output.Set<long>("TotalNotFoundParentStageIdCount", long.Parse(rs[8]));
                    output.Set<long>("TotalIgnoreDecoratorCount", long.Parse(rs[9]));
                    output.Set<long>("TotalNetStageNegativeCount", long.Parse(rs[10]));
                    //PrintStagePerfInfo(stagePerfInfoWriter, calculator.GetStagePerformanceInformation(), calculator.userId);
                    ++totalProcessAPICallCount;

                    oneAPICallLogs.Clear();

                    //output.Set<string>("trackingId", trackingId);
                    //output.Set<long>("totalProcessAPICallCount", totalProcessAPICallCount);
                    //output.Set<long>("totalZeroTrackingIdCount", totalZeroTrackingIdCount);
                    //output.Set<string>("lastTrackingId", lastTrackingId);
                    lastTrackingId = trackingId;
                    oneAPICallLogs.Add(oneRecord);
                    yield return output.AsReadOnly();
                }
            }
            CalculateOneAPICallCost thelastCompute = new CalculateOneAPICallCost(oneAPICallLogs, lastTrackingId, oneAPICallLogs.First().userId, oneAPICallLogs.First().startTime);



            string[] thelastRs = thelastCompute.Calculate().Split('\t');
            output.Set<string>("StartTime", thelastRs[0]);
            output.Set<string>("TrackingId", thelastRs[1]);
            output.Set<long>("UserId", long.Parse(thelastRs[2]));
            output.Set<double>("MTCost", double.Parse(thelastRs[3]));
            output.Set<double>("E2ECost", double.Parse(thelastRs[4]));
            output.Set<long>("TotalLogCount", long.Parse(thelastRs[5]));
            output.Set<long>("TotalIgnoreRecordCount", long.Parse(thelastRs[6]));
            output.Set<long>("TotalSameStageIdCount", long.Parse(thelastRs[7]));
            output.Set<long>("TotalNotFoundParentStageIdCount", long.Parse(thelastRs[8]));
            output.Set<long>("TotalIgnoreDecoratorCount", long.Parse(thelastRs[9]));
            output.Set<long>("TotalNetStageNegativeCount", long.Parse(thelastRs[10]));
            ++totalProcessAPICallCount;
            yield return output.AsReadOnly();

            //outputWriter.WriteLine(c.Calculate());
            //PrintStagePerfInfo(stagePerfInfoWriter, c.GetStagePerformanceInformation(), c.userId);




            //Console.WriteLine("Processing={0}, API={1}, ZeroTrackingId={2}", totalProcessingCount, totalProcessAPICallCount, totalZeroTrackingIdCount);

            //CalculateOneAPICallCost.OutputSlowStageStatistics(this.apiCriticalStageStatisticOutputFilePath);




        }


    }

    class LogRecord
    {
        public DateTime TimeStamp { get; set; }
        public string parentStageId { get; set; }
        public string stageId { get; set; }
        public bool success { get; set; }
        public long userId { get; set; }
        public string componentName { get; set; }
        public string trackingId { get; set; }
        public string codeBlockName { get; set; }
        public DateTime startTime { get; set; }
        public DateTime endTime { get; set; }
        public double elapsedTime { get; set; }
        public long scaleDimension { get; set; }
        public string requestUrl { get; set; }
    }

    class APITreeNode
    {
        public LogRecord Record { get; set; }
        public Dictionary<string, APITreeNode> ChildStages = new Dictionary<string, APITreeNode>();
        public Dictionary<int, List<APITreeNode>> GroupedChildStages = new Dictionary<int, List<APITreeNode>>();
        public double NetStageCost { get; set; }
        public bool IsLeaf { get; set; }

        public int StageExecutionLevel { get; set; }
        public int StageChildrenCount { get; set; }
        public int CallGroupIndex { get; set; }
        public bool IsCritical { get; set; }

        public double ChildrenCriticalTotalCost { get; set; }
    }




    class CalculateOneAPICallCost
    {
        public static Dictionary<string, List<Tuple<string, double>>> SlowCriticalStageInfos = new Dictionary<string, List<Tuple<string, double>>>();

        private List<LogRecord> totalLogs = null;
        private string trackingId = null;
        public long userId = -1;
        public DateTime startTime = DateTime.MinValue;
        private APITreeNode root = null;
        private long totalIgnoreRecordCount = 0;
        private long totalSameStageIdCount = 0;
        private long totalNotFoundParentStageIdCount = 0;
        private long totalIgnoreDecoratorCount = 0;
        private long totalNetStageNegativeCount = 0;
        private double mtCost = 0;
        private double e2eCost = 0;


        private List<APITreeNode> sortedStages = new List<APITreeNode>();
        public CalculateOneAPICallCost(List<LogRecord> oneAPILog, string trackingId, long userId, DateTime startTime)
        {
            this.totalLogs = oneAPILog;
            this.trackingId = trackingId;

            this.userId = userId;
            this.startTime = startTime;
        }


        public string Calculate()
        {
            this.ConstructGraphInMemory();
            if (this.root == null)
            {
                return string.Format("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}\t{9}\t{10}",
                    -1,
                    this.trackingId,
                    -1,
                    -1,
                    -1,
                    this.totalLogs.Count(),
                    this.totalIgnoreRecordCount,
                    this.totalSameStageIdCount,
                    this.totalNotFoundParentStageIdCount,
                    this.totalIgnoreDecoratorCount,
                    this.totalNetStageNegativeCount);
            }

            TagStageRecursive(this.root, 0, true);
            CalculateMTCost(this.root);
            this.e2eCost = this.root.Record.elapsedTime;

            CalculateStageNetCostByLayer(this.root);
            SortStage();

            //if (this.mtCost > 1000 && this.totalNetStageNegativeCount == 0 && ((DateTime.Now.ToUniversalTime() - this.startTime).TotalDays < 30))
            //{
            //    PrintDebug(Path.Combine(slowDirectory, string.Format("{0}_{1}.txt", this.root.Record.startTime.ToString("yyyyMMdd"), this.trackingId)));
            //}

            return string.Format("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}\t{9}\t{10}",
                this.root.Record.startTime.ToString("o"),
                this.root.Record.trackingId,
                this.root.Record.userId,
                this.mtCost,
                this.e2eCost,
                this.totalLogs.Count(),
                this.totalIgnoreRecordCount,
                this.totalSameStageIdCount,
                this.totalNotFoundParentStageIdCount,
                this.totalIgnoreDecoratorCount,
                this.totalNetStageNegativeCount);
        }

        //public List<StagePerfInfo> GetStagePerformanceInformation()
        //{
        //    List<StagePerfInfo> stagePerfs = new List<StagePerfInfo>();

        //    foreach (var one in this.sortedStages)
        //    {
        //        StagePerfInfo oneStagePerf = new StagePerfInfo();
        //        oneStagePerf.codeBlockName = one.Record.codeBlockName;
        //        oneStagePerf.elapsedTime = one.Record.elapsedTime;
        //        oneStagePerf.NetCost = one.NetStageCost;
        //        oneStagePerf.scaleDimension = one.Record.scaleDimension;
        //        oneStagePerf.shortBlockName = NormalizeCodeBlockName(one.Record.codeBlockName);
        //        oneStagePerf.startTime = one.Record.startTime;
        //        oneStagePerf.success = one.Record.success;

        //        stagePerfs.Add(oneStagePerf);
        //    }

        //    return stagePerfs;
        //}

        public static void OutputSlowStageStatistics(string output)
        {

            using (StreamWriter writer = new StreamWriter(output))
            {
                writer.WriteLine("StageName\tNormalizedName\tTakeMoreThan100MSCount\tMin\tAverage\tMax\tTop10TrackingId");

                foreach (var key in SlowCriticalStageInfos.Keys)
                {
                    List<Tuple<string, double>> data = SlowCriticalStageInfos[key];

                    double average = data.Select(x => x.Item2).Average();
                    double max = data.Select(x => x.Item2).Max();
                    double min = data.Select(x => x.Item2).Min();

                    data.Sort((x, y) =>
                    {
                        if (x.Item2 < y.Item2)
                        {
                            return 1;
                        }
                        else if (x.Item2 > y.Item2)
                        {
                            return -1;
                        }
                        else
                        {
                            return 0;
                        }
                    });

                    List<string> top10 = data.Take(10).Select(x => x.Item1).ToList();

                    writer.WriteLine("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}", key, NormalizeCodeBlockName(key), data.Count(), min, average, max, String.Join(",", top10));
                }
            }
        }

        public static void ClearStaticStageInfo()
        {
            SlowCriticalStageInfos = new Dictionary<string, List<Tuple<string, double>>>();
        }

        private void PrintDebug(string debugFilePath)
        {
            using (StreamWriter output = new StreamWriter(debugFilePath))
            {
                output.WriteLine("API Call Info");
                output.WriteLine("startTime={0}", this.root.Record.startTime.ToString("o"));
                output.WriteLine("trackingId={0}", this.root.Record.trackingId);
                output.WriteLine("userId={0}", this.root.Record.userId);
                output.WriteLine("mtCost={0}", this.mtCost);
                output.WriteLine("e2eCost={0}", this.e2eCost);
                output.WriteLine("totalLogs={0}", this.totalLogs.Count());
                output.WriteLine("totalIgnoreRecordCount={0}", this.totalIgnoreRecordCount);
                output.WriteLine("totalSameStageIdCount={0}", this.totalSameStageIdCount);
                output.WriteLine("totalNotFoundParentStageIdCount={0}", this.totalNotFoundParentStageIdCount);
                output.WriteLine("totalIgnoreDecoratorCount={0}", this.totalIgnoreDecoratorCount);
                output.WriteLine("totalNetStageNegativeCount={0}", this.totalNetStageNegativeCount);

                output.WriteLine("\r\n\r\nCritical Execution Flow");
                PrintExecutionFlow(this.root, output, true);

                output.WriteLine("\r\n\r\nExecution Flow");
                PrintExecutionFlow(this.root, output, false);

                output.WriteLine("\r\n\r\nStage Net Cost");
                output.WriteLine("CodeBlockName\tNetStageCost\tIsLeafStage\tStageExecutionLevel\tStageChildrenCount\tIsStageSuccess\tStageId\tScaleDimension\tIsCritical");

                foreach (var oneStage in this.sortedStages)
                {
                    output.WriteLine("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}",
                        oneStage.Record.codeBlockName,
                        oneStage.NetStageCost,
                        oneStage.IsLeaf,
                        oneStage.StageExecutionLevel,
                        oneStage.StageChildrenCount,
                        oneStage.Record.success,
                        oneStage.Record.stageId,
                        oneStage.Record.scaleDimension,
                        oneStage.IsCritical);

                    UpdateSlowCriticalStageInfo(oneStage);
                }
            }
        }

        private static string NormalizeCodeBlockName(string codeBlockName)
        {
            string normalizedName = string.Empty;
            int lastDot = codeBlockName.LastIndexOf(".");

            if (lastDot < 0)
            {
                normalizedName = codeBlockName;
            }
            else
            {
                normalizedName = codeBlockName.Substring(lastDot + 1);
            }

            return normalizedName;
        }

        private void UpdateSlowCriticalStageInfo(APITreeNode oneStage)
        {
            if (!oneStage.IsCritical)
            {
                return;
            }

            if ((DateTime.Now.ToUniversalTime() - oneStage.Record.startTime).TotalDays > 30)
            {
                return;
            }

            if (oneStage.NetStageCost < 100)
            {
                return;
            }

            if (!SlowCriticalStageInfos.ContainsKey(oneStage.Record.codeBlockName))
            {
                SlowCriticalStageInfos.Add(oneStage.Record.codeBlockName, new List<Tuple<string, double>>());
            }

            SlowCriticalStageInfos[oneStage.Record.codeBlockName].Add(new Tuple<string, double>(oneStage.Record.trackingId, oneStage.NetStageCost));
        }

        private void CalculateStageNetCostByLayer(APITreeNode node)
        {
            this.sortedStages.Add(node);

            if (node.IsLeaf)
            {
                return;
            }

            foreach (var groupStages in node.GroupedChildStages.Values)
            {
                foreach (var oneStage in groupStages)
                {
                    CalculateStageNetCostByLayer(oneStage);
                }
            }
        }

        private void PrintExecutionFlow(APITreeNode node, StreamWriter writer, bool isOnlyCritical)
        {
            string prefix = new string('\t', node.StageExecutionLevel);

            if (node.IsCritical)
            {
                writer.WriteLine("{0}{1}, {2} , {3} , {4} , {5}, {6} ,CRITICAL",
                    prefix,
                    node.Record.codeBlockName,
                    node.Record.elapsedTime,
                    node.Record.startTime.ToString("o"),
                    node.Record.endTime.ToString("o"),
                    node.NetStageCost,
                    node.CallGroupIndex);
            }
            else
            {
                if (!isOnlyCritical)
                {
                    writer.WriteLine("{0}{1}, {2} , {3} , {4} , {5}, {6}",
                        prefix,
                        node.Record.codeBlockName,
                        node.Record.elapsedTime,
                        node.Record.startTime.ToString("o"),
                        node.Record.endTime.ToString("o"),
                        node.NetStageCost,
                        node.CallGroupIndex);
                }
            }

            if (node.IsLeaf)
            {
                return;
            }

            foreach (var groupStages in node.GroupedChildStages.Values)
            {
                foreach (var oneStage in groupStages)
                {
                    PrintExecutionFlow(oneStage, writer, isOnlyCritical);
                }
            }
        }

        private void ConstructGraphInMemory()
        {
            Dictionary<string, APITreeNode> topStages = new Dictionary<string, APITreeNode>();

            foreach (LogRecord one in this.totalLogs)
            {
                if (one.parentStageId == "00000000-0000-0000-0000-000000000000" && one.codeBlockName != "Astro.GetDataTablesAsync:")
                {
                    ++this.totalIgnoreRecordCount;
                    continue;
                }

                if (one.codeBlockName.Contains("Microsoft.Ads.UcmApi.Repository.ServiceClientLogDecorator"))
                {
                    ++this.totalIgnoreDecoratorCount;
                    continue;
                }

                if (topStages.ContainsKey(one.stageId))
                {
                    ++this.totalSameStageIdCount;
                    continue;
                }

                APITreeNode node = new APITreeNode() { Record = one };
                topStages.Add(one.stageId, node);
                if (node.Record.parentStageId == "00000000-0000-0000-0000-000000000000")
                {
                    this.root = node;
                }
            }

            foreach (string key in topStages.Keys)
            {
                LogRecord current = topStages[key].Record;
                string parent = current.parentStageId;

                if (parent == "00000000-0000-0000-0000-000000000000" && current.stageId == this.root.Record.stageId)
                {
                    continue;
                }

                if (!topStages.ContainsKey(parent))
                {
                    ++this.totalNotFoundParentStageIdCount;
                    continue;
                    //throw new Exception("can't find parent stage id " + parent);
                }

                APITreeNode parentNode = topStages[parent];
                parentNode.ChildStages.Add(key, topStages[key]);
            }
        }

        private void TagStageRecursive(APITreeNode node, int level, bool isCritical)
        {
            if (node == null)
            {
                return;
            }

            node.StageExecutionLevel = level;
            node.StageChildrenCount = node.ChildStages.Count;
            node.IsLeaf = (node.StageChildrenCount == 0);
            node.IsCritical = isCritical;

            if (node.IsLeaf)
            {
                node.ChildrenCriticalTotalCost = 0;
                node.NetStageCost = node.Record.elapsedTime - node.ChildrenCriticalTotalCost;
                if (node.NetStageCost < 0)
                {
                    ++this.totalNetStageNegativeCount;
                }
                return;
            }

            List<APITreeNode> groupedChildrenStages = CalculateStageCallGroupIndex(node.ChildStages.Values.ToList());
            foreach (var one in groupedChildrenStages)
            {
                if (!node.GroupedChildStages.ContainsKey(one.CallGroupIndex))
                {
                    node.GroupedChildStages.Add(one.CallGroupIndex, new List<APITreeNode>());
                }
                node.GroupedChildStages[one.CallGroupIndex].Add(one);
            }

            double childrenCriticalStageTotalCost = 0;
            foreach (var value in node.GroupedChildStages.Values)
            {
                double groupMaxCost = double.MinValue;
                string groupMaxStageId = "";

                foreach (var oneStage in value)
                {
                    if (oneStage.Record.elapsedTime > groupMaxCost)
                    {
                        groupMaxStageId = oneStage.Record.stageId;
                        groupMaxCost = oneStage.Record.elapsedTime;
                    }
                }

                childrenCriticalStageTotalCost += groupMaxCost;

                foreach (var oneStage in value)
                {
                    if (oneStage.Record.stageId == groupMaxStageId)
                    {
                        TagStageRecursive(oneStage, level + 1, isCritical);
                    }
                    else
                    {
                        TagStageRecursive(oneStage, level + 1, false);
                    }
                }
            }

            node.ChildrenCriticalTotalCost = childrenCriticalStageTotalCost;
            node.NetStageCost = node.Record.elapsedTime - node.ChildrenCriticalTotalCost;
            if (node.NetStageCost < 0)
            {
                ++this.totalNetStageNegativeCount;
            }
        }

        private void CalculateMTCost(APITreeNode node)
        {
            if (node == null || node.IsLeaf || !node.IsCritical)
            {
                return;
            }

            this.mtCost += node.NetStageCost;

            foreach (var groupStages in node.GroupedChildStages.Values)
            {
                foreach (var oneStage in groupStages)
                {
                    CalculateMTCost(oneStage);
                }
            }
        }

        private List<APITreeNode> CalculateStageCallGroupIndex(List<APITreeNode> childrenStage)
        {
            childrenStage.Sort((x, y) =>
            {
                if (x.Record.startTime < y.Record.startTime)
                {
                    return -1;
                }
                else if (x.Record.startTime > y.Record.startTime)
                {
                    return 1;
                }
                else
                {
                    return 0;
                }
            });

            int groupIndex = 0;
            DateTime groupEnd = DateTime.MinValue;
            DateTime groupStart = DateTime.MinValue;
            for (int i = 0; i < childrenStage.Count; ++i)
            {
                if (IsNewCallGroup(groupStart, groupEnd, childrenStage[i].Record.startTime))
                {
                    ++groupIndex;
                    childrenStage[i].CallGroupIndex = groupIndex;
                    groupEnd = childrenStage[i].Record.endTime;
                    groupStart = childrenStage[i].Record.startTime;
                }
                else
                {
                    childrenStage[i].CallGroupIndex = groupIndex;
                    if (childrenStage[i].Record.endTime > groupEnd)
                    {
                        groupEnd = childrenStage[i].Record.endTime;
                    }
                }
            }

            return childrenStage;
        }

        private bool IsNewCallGroup(DateTime currentGroupStart, DateTime currentGroupEnd, DateTime currentStageStart)
        {
            if (currentStageStart >= currentGroupEnd)
            {
                return true;
            }

            double overlap = (currentGroupEnd - currentStageStart).TotalMilliseconds;
            double currentGroupRange = (currentGroupEnd - currentGroupStart).TotalMilliseconds;
            if (currentGroupRange == 0)
            {
                return true;
            }
            else if (overlap / currentGroupRange <= 0.1)
            {
                return true;
            }

            return false;
        }

        private void SortStage()
        {
            this.sortedStages.Sort((x, y) =>
            {
                if (x.NetStageCost < y.NetStageCost)
                {
                    return 1;
                }
                else if (x.NetStageCost > y.NetStageCost)
                {
                    return -1;
                }
                else
                {
                    return 0;
                }
            });
        }

        //public static void Test1()
        //{
        //    List<APITreeNode> records = new List<APITreeNode>();
        //    records.Add(new APITreeNode() { Record = new LogRecord() { startTime = DateTime.Parse("2016-09-18T14:40:25.5580825+08:00"), endTime = DateTime.Parse("2016-09-18T14:40:25.6225880+08:00") } });
        //    records.Add(new APITreeNode() { Record = new LogRecord() { startTime = DateTime.Parse("2016-09-18T14:40:25.5580825+08:00"), endTime = DateTime.Parse("2016-09-18T14:40:25.6225880+08:00") } });
        //    records.Add(new APITreeNode() { Record = new LogRecord() { startTime = DateTime.Parse("2016-09-18T14:40:25.5580825+08:00"), endTime = DateTime.Parse("2016-09-18T14:40:25.6225880+08:00") } });
        //    records.Add(new APITreeNode() { Record = new LogRecord() { startTime = DateTime.Parse("2016-09-18T14:40:25.5580825+08:00"), endTime = DateTime.Parse("2016-09-18T14:40:25.6225880+08:00") } });
        //    records.Add(new APITreeNode() { Record = new LogRecord() { startTime = DateTime.Parse("2016-09-18T14:40:25.5580825+08:00"), endTime = DateTime.Parse("2016-09-18T14:40:25.6225880+08:00") } });
        //    records.Add(new APITreeNode() { Record = new LogRecord() { startTime = DateTime.Parse("2016-09-18T14:40:25.5580825+08:00"), endTime = DateTime.Parse("2016-09-18T14:40:25.6225880+08:00") } });
        //    records.Add(new APITreeNode() { Record = new LogRecord() { startTime = DateTime.Parse("2016-09-18T14:40:25.5580825+08:00"), endTime = DateTime.Parse("2016-09-18T14:40:25.6225880+08:00") } });

        //    records = CalculateStageCallGroupIndex(records);
        //    foreach (var one in records)
        //    {
        //        Console.WriteLine("{0}\t{1}\t{2}", one.CallGroupIndex, one.Record.startTime.ToString("o"), one.Record.endTime.ToString("o"));
        //    }
        //}

        //public static void Test2()
        //{
        //    List<APITreeNode> records = new List<APITreeNode>();
        //    records.Add(new APITreeNode() { Record = new LogRecord() { startTime = DateTime.Parse("2016-09-18T14:40:25.5580825+08:00"), endTime = DateTime.Parse("2016-09-18T14:40:25.6225880+08:00") } });
        //    records.Add(new APITreeNode() { Record = new LogRecord() { startTime = DateTime.Parse("2016-09-18T14:40:25.6225890+08:00"), endTime = DateTime.Parse("2016-09-18T14:40:26.6225880+08:00") } });
        //    records.Add(new APITreeNode() { Record = new LogRecord() { startTime = DateTime.Parse("2016-09-18T14:40:25.6225891+08:00"), endTime = DateTime.Parse("2016-09-18T14:40:25.6255880+08:00") } });
        //    records.Add(new APITreeNode() { Record = new LogRecord() { startTime = DateTime.Parse("2016-09-18T14:40:25.6225892+08:00"), endTime = DateTime.Parse("2016-09-18T14:40:27.8225880+08:00") } });
        //    records.Add(new APITreeNode() { Record = new LogRecord() { startTime = DateTime.Parse("2016-09-18T14:40:27.8225878+08:00"), endTime = DateTime.Parse("2016-09-18T14:40:29.6225880+08:00") } });
        //    records.Add(new APITreeNode() { Record = new LogRecord() { startTime = DateTime.Parse("2016-09-18T14:40:28.5580825+08:00"), endTime = DateTime.Parse("2016-09-18T14:40:30.6225880+08:00") } });
        //    records.Add(new APITreeNode() { Record = new LogRecord() { startTime = DateTime.Parse("2016-09-18T14:40:28.5580825+08:00"), endTime = DateTime.Parse("2016-09-18T14:40:29.7225880+08:00") } });

        //    records = CalculateStageCallGroupIndex(records);
        //    foreach (var one in records)
        //    {
        //        Console.WriteLine("{0}\t{1}\t{2}", one.CallGroupIndex, one.Record.startTime.ToString("o"), one.Record.endTime.ToString("o"));
        //    }
        //}

        //public static void Test3()
        //{
        //    List<APITreeNode> records = new List<APITreeNode>();
        //    records.Add(new APITreeNode() { Record = new LogRecord() { startTime = DateTime.Parse("2016-09-18T14:40:28.5580825+08:00"), endTime = DateTime.Parse("2016-09-18T14:40:29.7225880+08:00") } });
        //    records.Add(new APITreeNode() { Record = new LogRecord() { startTime = DateTime.Parse("2016-09-18T14:40:25.5580825+08:00"), endTime = DateTime.Parse("2016-09-18T14:40:25.6225880+08:00") } });
        //    records.Add(new APITreeNode() { Record = new LogRecord() { startTime = DateTime.Parse("2016-09-18T14:40:25.6225891+08:00"), endTime = DateTime.Parse("2016-09-18T14:40:25.6255880+08:00") } });
        //    records.Add(new APITreeNode() { Record = new LogRecord() { startTime = DateTime.Parse("2016-09-18T14:40:25.6225892+08:00"), endTime = DateTime.Parse("2016-09-18T14:40:27.8225880+08:00") } });
        //    records.Add(new APITreeNode() { Record = new LogRecord() { startTime = DateTime.Parse("2016-09-18T14:40:28.5580825+08:00"), endTime = DateTime.Parse("2016-09-18T14:40:29.6225880+08:00") } });
        //    records.Add(new APITreeNode() { Record = new LogRecord() { startTime = DateTime.Parse("2016-09-18T14:40:28.5580825+08:00"), endTime = DateTime.Parse("2016-09-18T14:40:30.6225880+08:00") } });
        //    records.Add(new APITreeNode() { Record = new LogRecord() { startTime = DateTime.Parse("2016-09-18T14:40:25.6225890+08:00"), endTime = DateTime.Parse("2016-09-18T14:40:26.6225880+08:00") } });


        //    records = CalculateStageCallGroupIndex(records);
        //    foreach (var one in records)
        //    {
        //        Console.WriteLine("{0}\t{1}\t{2}", one.CallGroupIndex, one.Record.startTime.ToString("o"), one.Record.endTime.ToString("o"));
        //    }
        //}

    }
    public class ParseVariable
    {
        public static string GetWeek(string inputTime)
        {
            if (inputTime == "-1")
            {
                return "-1";
            }
            DateTime formatInputTime = DateTime.Parse(inputTime).ToUniversalTime();
            DateTime weekSunday = formatInputTime.Date.AddDays(-(int)formatInputTime.DayOfWeek);
            return weekSunday.ToString("yyyy-MM-dd");
        }

        public static string GetDay(string inputTime)
        {
            if (inputTime == "-1")
            {
                return "-1";
            }
            DateTime formatInputTime = DateTime.Parse(inputTime).ToUniversalTime();
            return formatInputTime.Date.ToString("yyyy-MM-dd");
        }
    }


    class MTCostRecord
    {
        public string StartWeek { get; set; }
        public string StartDay { get; set; }
        public string StartTime { get; set; }
        public string TrackingId { get; set; }
        public long UserId { get; set; }
        public double MTCost { get; set; }
        public double E2ECost { get; set; }
        public long TotalLogCount { get; set; }
        public long TotalIgnoreRecordCount { get; set; }
        public long TotalSameStageIdCount { get; set; }
        public long TotalNotFoundParentStageIdCount { get; set; }
        public long TotalIgnoreDecoratorCount { get; set; }
        public long TotalNetStageNegativeCount { get; set; }
    }

    class RangeBucketDistribution
    {
        private static long[] mtCostseparator = new long[] {
            1000
        };

        private static long[] totalCostSeparator = new long[] {
            3000
        };

        public string Date { get; set; }
        public List<long> MTCostBucketCounts;
        public List<long> TotalCostBucketCounts;
        public long TotalCount;
        public double MTCostLt_1000;
        public double MTCostGt_1000;
        public double E2ECostLt_3000;
        public double E2ECostGt_3000;
        public RangeBucketDistribution(string date)
        {
            this.Date = date;
            this.MTCostBucketCounts = new List<long>();
            this.TotalCostBucketCounts = new List<long>();
            this.TotalCount = 0;
            this.MTCostLt_1000 = 0;
            this.MTCostGt_1000 = 0;
            this.E2ECostLt_3000 = 0;
            this.E2ECostGt_3000 = 0;
            for (int i = 0; i < mtCostseparator.Length + 1; ++i)
            {
                this.MTCostBucketCounts.Add(0);
            }

            for (int i = 0; i < totalCostSeparator.Length + 1; ++i)
            {
                this.TotalCostBucketCounts.Add(0);
            }
        }

        public void AddOne(double mtCost, double totalCost)
        {
            int i = 0;
            for (; i < mtCostseparator.Length; ++i)
            {
                if (mtCost < mtCostseparator[i])
                {
                    break;
                }
            }

            ++this.MTCostBucketCounts[i];

            i = 0;
            for (; i < totalCostSeparator.Length; ++i)
            {
                if (totalCost < totalCostSeparator[i])
                {
                    break;
                }
            }

            ++this.TotalCostBucketCounts[i];

            ++this.TotalCount;
        }

        public void GetResult()
        {

            this.MTCostLt_1000 = (this.MTCostBucketCounts[0] * 1.0) / this.TotalCount;
            this.MTCostGt_1000 = (this.MTCostBucketCounts[1] * 1.0) / this.TotalCount;
            this.E2ECostLt_3000 = (this.TotalCostBucketCounts[0] * 1.0) / this.TotalCount;
            this.E2ECostGt_3000 = (this.TotalCostBucketCounts[1] * 1.0) / this.TotalCount;

        }
    }

    class Trend
    {
        public string Date { get; set; }
        public long TotalCount = 0;

        public List<double> RoughMTCosts = new List<double>();
        public double MTCostAverageCost = 0;
        public double MTCostPercentileCost_75 = 0;
        public double MTCostPercentileCost_95 = 0;
        public double MTCostMax = 0;

        public List<double> TotalCosts = new List<double>();
        public double TotalCostAverageCost = 0;
        public double TotalCostPercentileCost_75 = 0;
        public double TotalCostPercentileCost_95 = 0;
        public double TotalCostMax = 0;

        public Trend(string date)
        {
            this.Date = date;
        }

        public void AddCost(double mtCost, double totalCost)
        {
            this.RoughMTCosts.Add(mtCost);
            this.TotalCosts.Add(totalCost);
            ++this.TotalCount;
        }

        public void Calculate()
        {
            CalculateMTCost();
            CalculateTotalCost();
        }

        private void CalculateMTCost()
        {
            var sorted = this.RoughMTCosts.OrderBy(x => x).ToList();

            double sum = 0;
            for (int i = 0; i < sorted.Count(); ++i)
            {
                sum += sorted[i];
            }

            this.MTCostAverageCost = sum / sorted.Count();
            this.MTCostPercentileCost_75 = sorted[(int)Math.Floor((75 / 100.0) * sorted.Count())];
            this.MTCostPercentileCost_95 = sorted[(int)Math.Floor((95 / 100.0) * sorted.Count())];
            this.MTCostMax = sorted.Last();
        }

        private void CalculateTotalCost()
        {
            var sorted = this.TotalCosts.OrderBy(x => x).ToList();

            double sum = 0;
            for (int i = 0; i < sorted.Count(); ++i)
            {
                sum += sorted[i];
            }

            this.TotalCostAverageCost = sum / sorted.Count();
            this.TotalCostPercentileCost_75 = sorted[(int)Math.Floor((75 / 100.0) * sorted.Count())];
            this.TotalCostPercentileCost_95 = sorted[(int)Math.Floor((95 / 100.0) * sorted.Count())];
            this.TotalCostMax = sorted.Last();
        }
    }




    [SqlUserDefinedReducer(IsRecursive = true)]
    public class OneAPIWeeklyAnalyser : IReducer
    {


        public override IEnumerable<IRow> Reduce(IRowset input, IUpdatableRow output)
        {
            String lastStartWeek = null;
            Trend trendRecord = null;
            RangeBucketDistribution distribution = null;
            //HashSet<long> AMUserIds = new HashSet<long>();
            long totalRecordCount = 0;
            long CannotGenerateValidGraphRecordCount = 0;
            long MTCostIsNegativeRecordCount = 0;
            long NetStageNegativeRecordCount = 0;
            //long NotAMUserRecordCount = 0;
            //long ValidAMUserRecordCount = 0;


            //string[] allLines = File.ReadAllLines("/AMUserIDs.txt");

            //foreach (var one in allLines)
            //{
            //    AMUserIds.Add(long.Parse(one));
            //}


            foreach (IRow row in input.Rows)
            {
                ++totalRecordCount;
                if (totalRecordCount % 10000 == 0)
                {
                    Console.WriteLine("Read {0} records", totalRecordCount);
                }


                MTCostRecord record = new MTCostRecord();
                record.StartWeek = row.Get<string>("StartWeek");
                record.StartDay = row.Get<string>("StartDay");
                record.StartTime = row.Get<string>("StartTime");
                record.TrackingId = row.Get<string>("TrackingId");
                record.UserId = row.Get<long>("UserId");
                record.MTCost = row.Get<double>("MTCost");
                record.E2ECost = row.Get<double>("E2ECost");
                record.TotalLogCount = row.Get<long>("TotalLogCount");
                record.TotalIgnoreRecordCount = row.Get<long>("TotalIgnoreRecordCount");
                record.TotalSameStageIdCount = row.Get<long>("TotalSameStageIdCount");
                record.TotalNotFoundParentStageIdCount = row.Get<long>("TotalNotFoundParentStageIdCount");
                record.TotalIgnoreDecoratorCount = row.Get<long>("TotalIgnoreDecoratorCount");
                record.TotalNetStageNegativeCount = row.Get<long>("TotalNetStageNegativeCount");


                //if (!AMUserIds.Contains(record.UserId))
                //{
                //    ++NotAMUserRecordCount;
                //    continue;
                //}

                //++ValidAMUserRecordCount;


                if (record.StartTime == "-1")
                {
                    ++CannotGenerateValidGraphRecordCount;
                    continue;
                }

                if (record.MTCost <= 0)
                {
                    ++MTCostIsNegativeRecordCount;
                    continue;
                }

                if (record.TotalNetStageNegativeCount > 0)
                {
                    ++NetStageNegativeRecordCount;
                    continue;
                }

                if (lastStartWeek == null)
                {
                    lastStartWeek = record.StartWeek;
                }

                if (lastStartWeek == record.StartWeek)
                {
                    if (trendRecord == null)
                    {
                        trendRecord = new Trend(record.StartWeek);
                    }

                    trendRecord.AddCost(record.MTCost, record.E2ECost);

                    if (distribution == null)
                    {
                        distribution = new RangeBucketDistribution(record.StartWeek);
                    }
                    distribution.AddOne(record.MTCost, record.E2ECost);
                }
                else
                {
                    trendRecord.Calculate();
                    distribution.GetResult();
                    //output the trend[record.StartWeek]
                    output.Set<string>("Date", trendRecord.Date);
                    output.Set<long>("TotalCount", distribution.TotalCount);
                    output.Set<double>("MTCostLt_1000", distribution.MTCostLt_1000);
                    output.Set<double>("MTCostGt_1000", distribution.MTCostGt_1000);
                    output.Set<double>("E2ECostLt_3000", distribution.E2ECostLt_3000);
                    output.Set<double>("E2ECostGt_3000", distribution.E2ECostGt_3000);
                    output.Set<double>("MTCostAverageCost", trendRecord.MTCostAverageCost);
                    output.Set<double>("MTCostPercentileCost_75", trendRecord.MTCostPercentileCost_75);
                    output.Set<double>("MTCostPercentileCost_95", trendRecord.MTCostPercentileCost_95);
                    output.Set<double>("MTCostMax", trendRecord.MTCostMax);
                    output.Set<double>("TotalCostAverageCost", trendRecord.TotalCostAverageCost);
                    output.Set<double>("TotalCostPercentileCost_75", trendRecord.TotalCostPercentileCost_75);
                    output.Set<double>("TotalCostPercentileCost_95", trendRecord.TotalCostPercentileCost_95);
                    output.Set<double>("TotalCostMax", trendRecord.TotalCostMax);

                    trendRecord = null;
                    distribution = null;
                    lastStartWeek = record.StartWeek;
                    yield return output.AsReadOnly();
                }


            }

            trendRecord.Calculate();
            distribution.GetResult();
            //output the trend[record.StartWeek]
            output.Set<string>("Date", trendRecord.Date);
            output.Set<long>("TotalCount", distribution.TotalCount);
            output.Set<double>("MTCostLt_1000", distribution.MTCostLt_1000);
            output.Set<double>("MTCostGt_1000", distribution.MTCostGt_1000);
            output.Set<double>("E2ECostLt_3000", distribution.E2ECostLt_3000);
            output.Set<double>("E2ECostGt_3000", distribution.E2ECostGt_3000);
            output.Set<double>("MTCostAverageCost", trendRecord.MTCostAverageCost);
            output.Set<double>("MTCostPercentileCost_75", trendRecord.MTCostPercentileCost_75);
            output.Set<double>("MTCostPercentileCost_95", trendRecord.MTCostPercentileCost_95);
            output.Set<double>("MTCostMax", trendRecord.MTCostMax);
            output.Set<double>("TotalCostAverageCost", trendRecord.TotalCostAverageCost);
            output.Set<double>("TotalCostPercentileCost_75", trendRecord.TotalCostPercentileCost_75);
            output.Set<double>("TotalCostPercentileCost_95", trendRecord.TotalCostPercentileCost_95);
            output.Set<double>("TotalCostMax", trendRecord.TotalCostMax);


            yield return output.AsReadOnly();


        }


    }

}

