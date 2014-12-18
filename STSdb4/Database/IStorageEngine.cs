using STSdb4.Data;
using STSdb4.WaterfallTree;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace STSdb4.Database
{
    public interface IStorageEngine : IEnumerable<IDescriptor>, IDisposable
    {
        /// <summary>
        /// Works with anonymous types.
        /// </summary>
        ITable<IData, IData> OpenXTable(string name, DataType keyDataType, DataType recordDataType);

        /// <summary>
        /// Works with the user types directly.
        /// </summary>
        ITable<TKey, TRecord> OpenXTable<TKey, TRecord>(string name);

        /// <summary>
        /// Works with portable types via custom transformers.
        /// </summary>
        ITable<TKey, TRecord> OpenXTablePortable<TKey, TRecord>(string name, DataType keyDataType, DataType recordDataType, ITransformer<TKey, IData> keyTransformer, ITransformer<TRecord, IData> recordTransformer);

        /// <summary>
        /// Works with portable types via default transformers.
        /// </summary>
        ITable<TKey, TRecord> OpenXTablePortable<TKey, TRecord>(string name);

        /// <summary>
        /// 
        /// </summary>
        XFile OpenXFile(string name);

        IDescriptor this[string name] { get; }
        IDescriptor Find(long id);

        void Delete(string name);
        void Rename(string name, string newName);
        bool Exists(string name);

        /// <summary>
        /// The number of tables & virtual files into the storage engine.
        /// </summary>
        int Count { get; }

        /// <summary>
        /// The size of the database in bytes.
        /// </summary>
        long DatabaseSize { get; }

        /// <summary>
        /// The number of nodes that are kept in memory.
        /// </summary>
        int CacheSize { get; set; }

        void Commit();
        void Close();
    }
}
