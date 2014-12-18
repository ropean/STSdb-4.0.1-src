using STSdb4.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace STSdb4.WaterfallTree
{
    /// <summary>
    /// Gives an opportunity for writting and reading of blocks on logical addresses (handles). Supports history for each block (versions).  
    /// </summary>
    public interface IHeap
    {
        /// <summary>
        /// Small user data (usually up to 256 bytes), atomic written with Commit()
        /// </summary>
        byte[] Tag { get; set; }

        /// <summary>
        /// Obtained handle is always unique
        /// </summary>
        long ObtainHandle();
        void Release(long handle);
        bool Exists(long handle);

        /// <summary>
        /// New block will be written always with version = CurrentVersion
        /// If new block is written to handle and the last block of this handle have same version with the new one, 
        /// occupied space by the last block will be freed
        /// Block can be read from specific version
        /// If block is require to be read form specific version and block form this version doesn`t exist,
        /// Read() return block with greatest existing version which less than the given
        /// </summary>
        void Write(long handle, byte[] buffer, int index, int count);
        byte[] Read(long handle);

        IEnumerable<KeyValuePair<long, byte[]>> GetLatest(long atVersion);

        //TODO: remove from interface
        KeyValuePair<long, Ptr>[] GetUsedSpace();

        /// <summary>
        /// After commit CurrentVersion is incremented. 
        /// </summary>
        void Commit();

        long Size { get; }

        bool UseCompression { get; }

        /// <summary>
        /// 
        /// </summary>
        void Close();

        long CurrentVersion { get; }
    }
}
