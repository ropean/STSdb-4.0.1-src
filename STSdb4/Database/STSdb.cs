using STSdb4.General.Communication;
using STSdb4.General.IO;
using STSdb4.Remote;
using STSdb4.Storage;
using STSdb4.WaterfallTree;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Management;
using System.Text;

namespace STSdb4.Database
{
    public static class STSdb
    {
        public static IStorageEngine FromHeap(IHeap heap)
        {
            return new StorageEngine(heap);
        }

        public static IStorageEngine FromStream(Stream stream, bool useCompression = false, AllocationStrategy strategy = AllocationStrategy.FromTheCurrentBlock)
        {
            IHeap heap = new Heap(stream, useCompression, strategy);

            return FromHeap(heap);
        }

        public static IStorageEngine FromMemory(bool useCompression = false, AllocationStrategy strategy = AllocationStrategy.FromTheCurrentBlock)
        {
            var stream = new MemoryStream();

            return STSdb.FromStream(stream, useCompression, strategy);
        }

        public static IStorageEngine FromFile(string fileName, bool useCompression = false, AllocationStrategy strategy = AllocationStrategy.FromTheCurrentBlock)
        {
            var stream = new OptimizedFileStream(fileName, FileMode.OpenOrCreate);

            return STSdb.FromStream(stream, useCompression, strategy);
        }

        public static IStorageEngine FromNetwork(string host, int port = 7182)
        {
            return new StorageEngineClient(host, port);
        }

        public static StorageEngineServer CreateServer(IStorageEngine engine, int port = 7182)
        {
            TcpServer server = new TcpServer(port);
            StorageEngineServer engineServer = new StorageEngineServer(engine, server);

            return engineServer;
        }
    }
}