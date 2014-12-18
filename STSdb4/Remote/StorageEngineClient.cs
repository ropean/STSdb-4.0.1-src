﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Net.Sockets;
using System.IO;
using System.Collections.Concurrent;
using STSdb4.WaterfallTree;
using STSdb4.Database;
using STSdb4.Data;
using STSdb4.Remote;
using STSdb4.General.Extensions;
using STSdb4.Database.Operations;
using STSdb4.General.Communication;
using System.Collections;
using STSdb4.Remote.Commands;

namespace STSdb4.Remote
{
    public class StorageEngineClient : IStorageEngine
    {
        private int cacheSize;
        private ConcurrentDictionary<string, XTableRemote> indexes = new ConcurrentDictionary<string, XTableRemote>();

        public static readonly Descriptor StorageEngineDescriptor = new Descriptor(-1, "", DataType.Boolean, DataType.Boolean);
        public readonly ClientConnection ClientConnection;

        public StorageEngineClient(string machineName = "localhost", int port = 7182)
        {
            ClientConnection = new ClientConnection(machineName, port);
            ClientConnection.Start();
        }

        #region IStorageEngine

        public ITable<IData, IData> OpenXTable(string name, DataType keyType, DataType recordType)
        {
            var cmd = new StorageEngineOpenXIndexCommand(name, keyType, recordType);
            InternalExecute(cmd);

            var descriptor = new Descriptor(cmd.ID, name, keyType, recordType);

            var index = new XTableRemote(this, descriptor);
            indexes.TryAdd(name, index);

            return index;
        }

        public ITable<TKey, TRecord> OpenXTable<TKey, TRecord>(string name)
        {
            return OpenXTablePortable<TKey, TRecord>(name);
        }

        public ITable<TKey, TRecord> OpenXTablePortable<TKey, TRecord>(string name, DataType keyDataType, DataType recordDataType, ITransformer<TKey, IData> keyTransformer, ITransformer<TRecord, IData> recordTransformer)
        {
            var index = OpenXTable(name, keyDataType, recordDataType);

            return new XTablePortable<TKey, TRecord>(index, keyTransformer, recordTransformer);
        }

        public ITable<TKey, TRecord> OpenXTablePortable<TKey, TRecord>(string name)
        {
            var keyDataType = DataTypeUtils.BuildDataType(typeof(TKey));
            var recordDataType = DataTypeUtils.BuildDataType(typeof(TRecord));

            var keyTransformer = new DataTransformer<TKey>(typeof(TKey));
            var recordTransformer = new DataTransformer<TRecord>(typeof(TRecord));

            return OpenXTablePortable<TKey, TRecord>(name, keyDataType, recordDataType, null, null);
        }

        public XFile OpenXFile(string name)
        {
            throw new NotSupportedException();
        }

        public void Rename(string name, string newName)
        {
            InternalExecute(new StorageEngineRenameCommand(name, newName));
        }

        public IDescriptor this[string name]
        {
            get
            {
                return indexes[name].Descriptor;
            }
        }

        public void Delete(string name)
        {
            var cmd = new StorageEngineDeleteCommand(name);
            InternalExecute(cmd);
        }

        public bool Exists(string name)
        {
            var cmd = new StorageEngineExistsCommand(name);
            InternalExecute(cmd);

            return cmd.Exist;
        }

        public int Count
        {
            get
            {
                var cmd = new StorageEngineCountCommand();
                InternalExecute(cmd);

                return cmd.Count;
            }
        }

        public long DatabaseSize
        {
            get
            {
                var cmd = new StorageEngineDatabaseSizeCommand();
                InternalExecute(cmd);

                return cmd.Size;
            }
        }

        public IDescriptor Find(long id)
        {
            var cmd = new StorageEngineFindByIDCommand(null, id);
            InternalExecute(cmd);

            return cmd.Descriptor;
        }

        public IEnumerator<IDescriptor> GetEnumerator()
        {
            var cmd = new StorageEngineGetEnumeratorCommand();
            InternalExecute(cmd);

            return cmd.Descriptions.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Commit()
        {
            foreach (var index in indexes.Values)
                index.Flush();

            InternalExecute(new StorageEngineCommitCommand());
        }

        #endregion

        #region Server

        public CommandCollection Execute(IDescriptor descriptor, CommandCollection commands)
        {
            MemoryStream ms = new MemoryStream();
            BinaryWriter writer = new BinaryWriter(ms);

            Message message = new Message(descriptor, commands);
            message.Serialize(writer);

            Packet packet = new Packet(ms);
            ClientConnection.Send(packet);

            packet.Wait();

            BinaryReader reader = new BinaryReader(packet.Response);
            message = Message.Deserialize(reader, (id) => { return descriptor; });

            return message.Commands;
        }

        private void InternalExecute(ICommand command)
        {
            CommandCollection cmds = new CommandCollection(1);
            cmds.Add(command);

            var resultCommand = Execute(StorageEngineDescriptor, cmds)[0];
            SetResult(command, resultCommand);
        }

        private void SetResult(ICommand command, ICommand resultCommand)
        {
            switch (resultCommand.Code)
            {
                case CommandCode.STORAGE_ENGINE_COMMIT:
                    break;

                case CommandCode.STORAGE_ENGINE_OPEN_XTABLE:
                    {
                        ((StorageEngineOpenXIndexCommand)command).ID = ((StorageEngineOpenXIndexCommand)resultCommand).ID;
                        ((StorageEngineOpenXIndexCommand)command).CreateTime = ((StorageEngineOpenXIndexCommand)resultCommand).CreateTime;
                    }
                    break;

                case CommandCode.STORAGE_ENGINE_OPEN_XFILE:
                    ((StorageEngineOpenXFileCommand)command).ID = ((StorageEngineOpenXFileCommand)resultCommand).ID;
                    break;

                case CommandCode.STORAGE_ENGINE_EXISTS:
                    ((StorageEngineExistsCommand)command).Exist = ((StorageEngineExistsCommand)resultCommand).Exist;
                    break;

                case CommandCode.STORAGE_ENGINE_FIND_BY_ID:
                    ((StorageEngineFindByIDCommand)command).Descriptor = ((StorageEngineFindByIDCommand)resultCommand).Descriptor;
                    break;

                case CommandCode.STORAGE_ENGINE_FIND_BY_NAME:
                    ((StorageEngineFindByNameCommand)command).Descriptor = ((StorageEngineFindByNameCommand)resultCommand).Descriptor;
                    break;

                case CommandCode.STORAGE_ENGINE_DELETE:
                    break;

                case CommandCode.STORAGE_ENGINE_COUNT:
                    ((StorageEngineCountCommand)command).Count = ((StorageEngineCountCommand)resultCommand).Count;
                    break;

                case CommandCode.STORAGE_ENGINE_GET_ENUMERATOR:
                    ((StorageEngineGetEnumeratorCommand)command).Descriptions = ((StorageEngineGetEnumeratorCommand)resultCommand).Descriptions;
                    break;

                case CommandCode.STORAGE_ENGINE_DATABASE_SIZE:
                    ((StorageEngineDatabaseSizeCommand)command).Size = ((StorageEngineDatabaseSizeCommand)resultCommand).Size;
                    break;

                case CommandCode.STORAGE_ENGINE_GET_CACHE_SIZE:
                    ((StorageEngineGetCacheSizeCommand)command).CacheSize = ((StorageEngineGetCacheSizeCommand)resultCommand).CacheSize;
                    break;

                case CommandCode.EXCEPTION:
                    throw new Exception(((ExceptionCommand)resultCommand).Exception);

                default:
                    break;
            }
        }
        
        #endregion
 
        #region IDisposable Members

        private volatile bool disposed = false;

        private void Dispose(bool disposing)
        {
            if (!this.disposed)
            {
                if (disposing)
                {
                    ClientConnection.Stop();
                }

                disposed = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);

            GC.SuppressFinalize(this);
        }

        ~StorageEngineClient()
        {
            Dispose(false);
        }

        public void Close()
        {
            Dispose();
        }

        #endregion

        public int CacheSize
        {
            get
            {
                StorageEngineGetCacheSizeCommand command = new StorageEngineGetCacheSizeCommand(0);

                CommandCollection collection = new CommandCollection(1);
                collection.Add(command);

                StorageEngineGetCacheSizeCommand resultComamnd = (StorageEngineGetCacheSizeCommand)Execute(StorageEngineDescriptor, collection)[0];

                return resultComamnd.CacheSize;
            }
            set
            {
                cacheSize = value;
                StorageEngineSetCacheSizeCommand command = new StorageEngineSetCacheSizeCommand(cacheSize);

                CommandCollection collection = new CommandCollection(1);
                collection.Add(command);

                Execute(StorageEngineDescriptor, collection);
            }
        }
    }
}
