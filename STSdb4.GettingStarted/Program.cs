using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using STSdb4.Data;
using STSdb4.Database;
using STSdb4.General.Communication;
using STSdb4.General.Comparers;
using STSdb4.General.Compression;
using STSdb4.General.Extensions;
using STSdb4.General.Persist;
using STSdb4.Remote;
using STSdb4.Storage;
using System.Threading;

namespace STSdb4.GettingStarted
{
    class Program
    {
        static void Main(string[] args)
        {
            Test();
        }

        /// <summary>
        /// Create a simple table
        /// </summary>
        private static void Example1()
        {
            //insert
            using (IStorageEngine engine = STSdb.FromFile("test.stsdb4"))
            {
                ITable<int, string> table = engine.OpenXTable<int, string>("table1");

                for (int i = 0; i < 1000; i++)
                {
                    table[i] = i.ToString();
                }

                engine.Commit();
            }

            //read
            using (IStorageEngine engine = STSdb.FromFile("test.stsdb4"))
            {
                ITable<int, string> table = engine.OpenXTable<int, string>("table1");

                foreach (var row in table) //table.Forward(), table.Backward()
                {
                    Console.WriteLine("{0} {1}", row.Key, row.Value);
                }
            }
        }

        /// <summary>
        /// Create a table with user type
        /// </summary>
        private static void Example2()
        {
            Random random = new Random();

            //insert
            using (IStorageEngine engine = STSdb.FromFile("test.stsdb4"))
            {
                ITable<long, Tick> table = engine.OpenXTable<long, Tick>("table2");

                for (int i = 0; i < 1000; i++)
                {
                    long key = random.Next();

                    Tick tick = new Tick();
                    tick.Type = TickType.Forex;
                    tick.Symbol = "";
                    tick.Timestamp = DateTime.Now;
                    tick.Bid = Math.Round(random.NextDouble(), 4);
                    tick.Ask = Math.Round(tick.Bid + 0.0001 * random.Next(2, 10), 4);
                    tick.Volume = i;
                    tick.Provider = "";

                    table[key] = tick;
                }

                engine.Commit();
            }

            ////read
            using (IStorageEngine engine = STSdb.FromFile("test.stsdb4"))
            {
                ITable<long, Tick> table = engine.OpenXTable<long, Tick>("table2");

                foreach (var row in table) //table.Forward(), table.Backward()
                {
                    Console.WriteLine("{0} {1}", row.Key, row.Value);
                }
            }
        }

        /// <summary>
        /// Create a client connection
        /// </summary>
        private static void Example3()
        {
            //insert
            using (IStorageEngine engine = STSdb.FromNetwork("localhost", 7182))
            {
                ITable<int, string> table = engine.OpenXTablePortable<int, string>("table");

                for (int i = 0; i < 1000; i++)
                {
                    table[i] = i.ToString();
                }

                engine.Commit();
            }

            //read
            using (IStorageEngine engine = STSdb.FromNetwork("localhost", 7182))
            {
                ITable<int, string> table = engine.OpenXTablePortable<int, string>("table");

                foreach (var row in table) //table.Forward(), table.Backward()
                {
                    Console.WriteLine("{0} {1}", row.Key, row.Value);
                }
            }
        }

        /// <summary>
        /// Create server
        /// </summary>
        private static void Example4()
        {
            using (IStorageEngine engine = STSdb.FromFile("test.stsdb4"))
            {
                var server = STSdb.CreateServer(engine, 7182);

                server.Start();

                //server is ready for connections

                server.Stop();
            }
        }

        private static void Test()
        {
            int count = 20000000;

            string fileName = "test.stsdb4";
            //File.Delete(fileName);

            Random random = new Random();
            Stopwatch sw = new Stopwatch();

            double progress = 0.0;

            //insert
            Console.WriteLine(String.Format("Inserting {0} records...", count));
            sw.Start();
            using (var engine = STSdb.FromFile(fileName))
            {
                var index = engine.OpenXTable<int, int>("table");

                for (int i = 0; i < count; i++)
                {
                    int key = random.Next(0, int.MaxValue);
                    int rec = i;

                    index[key] = rec;

                    double p = Math.Round(100.0 * (i + 1) / count);
                    if (p - progress >= 5)
                    {
                        Console.Write(String.Format("{0}% ", p));
                        progress = p;
                    }
                }

                engine.Commit();

                Console.WriteLine("{0} rec/sec", sw.GetSpeed(count));
                Console.WriteLine("~{0}MB", Math.Round(engine.DatabaseSize / (1024 * 1024.0)));
            }
            sw.Stop();

            progress = 0;

            //read
            Console.WriteLine("Reading...");
            sw.Reset();
            sw.Start();
            int c = 0;
            using (var engine = STSdb.FromFile(fileName))
            {
                var index = engine.OpenXTable<int, int>("table");

                int key = -1;
                foreach (var kv in index)
                {
                    if (key > kv.Key)
                        throw new Exception();

                    key = kv.Key;
                    c++;

                    double p = Math.Round(100.0 * c / count);
                    if (p - progress >= 5)
                    {
                        Console.Write(String.Format("{0}% ", p));
                        progress = p;
                    }
                }

                Console.WriteLine(String.Format("{0} records", c));
                Console.WriteLine("{0} rec/sec", sw.GetSpeed(count));
                Console.WriteLine("~{0}MB", Math.Round(engine.DatabaseSize / (1024 * 1024.0)));
            }
            sw.Stop();

            Thread.Sleep(100000);
        }
    }

    public class Tick
    {
        public TickType Type { get; set; }
        public string Symbol { get; set; }
        public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
        public double Ask { get; set; }
        public long Volume { get; set; }
        public string Provider { get; set; }

        public Tick()
        {
        }

        public Tick(TickType type, string symbol, DateTime time, double bid, double ask, long volume, string provider)
        {
            Type = type;
            Symbol = symbol;
            Timestamp = time;
            Bid = bid;
            Ask = ask;
            Volume = volume;
            Provider = provider;
        }

        public override string ToString()
        {
            return String.Format("{0};{1:yyyy-MM-dd HH:mm:ss};{2};{3};{4};{5}", Symbol, Timestamp, Bid, Ask, Volume, Provider);
        }
    }

    public enum TickType : byte
    {
        Forex,
        Futures,
        Stock
    }
}
