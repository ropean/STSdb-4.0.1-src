using System;
using System.IO;
using STSdb4.General.Extensions;
using STSdb4.Data;
using STSdb4.WaterfallTree;

namespace STSdb4.Database
{
    public class XStream : Stream
    {
        internal const int BLOCK_SIZE = 2 * 1024;

        private long position;

        public ITable<IData, IData> Table { get; private set; }

        internal XStream(ITable<IData, IData> table)
        {
            Table = table;
        }

        public IDescriptor Description
        {
            get { return Table.Descriptor; }
        }

        #region Stream Members

        public override void Write(byte[] buffer, int offset, int count)
        {
            while (count > 0)
            {
                int chunk = Math.Min(BLOCK_SIZE - (int)(position % BLOCK_SIZE), count);

                IData key = new Data<long>(position);
                IData record = new Data<byte[]>(buffer.Middle(offset, chunk));
                Table[key] = record;

                position += chunk;
                offset += chunk;
                count -= chunk;
            }
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (offset + count > buffer.Length)
                throw new ArgumentException("offset + count > buffer.Length");

            if (count == 0)
                return 0;

            long oldPosition = position;
            var fromKey = new Data<long>(position - position % BLOCK_SIZE);
            var toKey = new Data<long>(position + count- 1);
            int chunk;

            foreach (var kv in Table.Forward(fromKey, true, toKey, true))
            {
                Data<long> key = (Data<long>)kv.Key;
                Data<byte[]> rec = (Data<byte[]>)kv.Value;

                if (position >= key.Value)
                {
                    chunk = Math.Min(rec.Value.Length - (int)(position % BLOCK_SIZE), count);
                    Buffer.BlockCopy(rec.Value, (int)(position - key.Value), buffer, offset, chunk);
                }
                else
                {
                    chunk = (int)Math.Min(key.Value - position, (long)count);
                    Array.Clear(buffer, offset, chunk);
                }

                position += chunk;
                offset += chunk;
                count -= chunk;
            }

            if (count > 0)
            {
                Array.Clear(buffer, offset, count);
                position += count;
            }

            return (int)(position - oldPosition);
        }

        public override void Flush()
        {
            //do nothing
        }

        public override bool CanRead
        {
            get { return true; }
        }

        public override bool CanSeek
        {
            get { return true; }
        }

        public override bool CanWrite
        {
            get { return true; }
        }

        public override long Length
        {
            get
            {
                foreach (var row in Table.Backward())
                {
                    var key = (Data<long>)row.Key;
                    var rec = (Data<byte[]>)row.Value;

                    return key.Value + rec.Value.Length;
                }

                return 0;
            }
        }

        public override long Position
        {
            get { return position; }
            set { position = value; }
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            switch (origin)
            {
                case SeekOrigin.Begin:
                    position = offset;
                    break;
                case SeekOrigin.Current:
                    position += offset;
                    break;
                case SeekOrigin.End:
                    position = Length - 1 - offset;
                    break;
            }

            return position;
        }

        public override void SetLength(long value)
        {
            var length = Length;
            if (value == length)
                return;

            var oldPosition = this.position;
            try
            {
                if (value > length)
                {
                    Seek(value - 1, SeekOrigin.Begin);
                    Write(new byte[1] { 0 }, 0, 1);
                }
                else //if (value < length)
                {
                    Seek(value, SeekOrigin.Begin);
                    Zero(length - value);
                }
            }
            finally
            {
                Seek(oldPosition, SeekOrigin.Begin);
            }
        }

        #endregion

        public void Zero(long count)
        {
            var fromKey = new Data<long>(position);
            var toKey = new Data<long>(position + count - 1);
            Table.Delete(fromKey, toKey);

            position += count;
        }
    }
}
