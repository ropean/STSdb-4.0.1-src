﻿using STSdb4.General.Persist;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace STSdb4.Data
{
    public class DataPersist : IPersist<IData>
    {
        public Action<BinaryWriter, IData> write;
        public Func<BinaryReader, IData> read;

        public Expression<Action<BinaryWriter, IData>> LambdaWrite;
        public Expression<Func<BinaryReader, IData>> LambdaRead;

        public Type Type { get; private set; }

        public readonly Func<Type, MemberInfo, int> MembersOrder;

        public DataPersist(Type type, Func<Type, MemberInfo, int> membersOrder = null)
        {
            Type = type;

            MembersOrder = membersOrder;

            LambdaWrite = CreateWriteMethod();
            write = LambdaWrite.Compile();

            LambdaRead = CreateReadMethod();
            read = LambdaRead.Compile();
        }

        public void Write(BinaryWriter writer, IData item)
        {
            write(writer, item);
        }

        public IData Read(BinaryReader reader)
        {
            return read(reader);
        }

        private Expression<Action<BinaryWriter, IData>> CreateWriteMethod()
        {
            var writer = Expression.Parameter(typeof(BinaryWriter), "writer");
            var idata = Expression.Parameter(typeof(IData), "idata");

            var dataType = typeof(Data<>).MakeGenericType(Type);
            var dataValue = Expression.Variable(Type, "dataValue");

            var assign = Expression.Assign(dataValue, Expression.Convert(idata, dataType).Value());

            return Expression.Lambda<Action<BinaryWriter, IData>>(Expression.Block(new ParameterExpression[] { dataValue }, assign, PersistHelper.CreateWriteBody(dataValue, writer, MembersOrder, false)), writer, idata);
        }

        private Expression<Func<BinaryReader, IData>> CreateReadMethod()
        {
            var reader = Expression.Parameter(typeof(BinaryReader), "reader");

            var dataType = typeof(Data<>).MakeGenericType(Type);

            return Expression.Lambda<Func<BinaryReader, IData>>(
                    Expression.Label(Expression.Label(dataType), Expression.New(dataType.GetConstructor(new Type[] { Type }), PersistHelper.CreateReadBody(reader, Type, MembersOrder, false))),
                    reader
                );
        }
    }

    #region Examples

    //public class PersistDataTick : IPersist<IData>
    //{
    //    public class Tick
    //    {
    //        public string Symbol { get; set; }
    //        public DateTime Timestamp { get; set; }
    //        public double Bid { get; set; }
    //        public double Ask { get; set; }
    //        public long Volume { get; set; }
    //        public string Provider { get; set; }
    //    }

    //    public void Write(BinaryWriter writer, IData item)
    //    {
    //        Data2<Tick> data = (Data2<Tick>)item;

    //        if (data.Value.Symbol != null)
    //        {
    //            writer.Write(true);
    //            writer.Write(data.Value.Symbol);
    //        }
    //        else
    //            writer.Write(false);

    //        writer.Write(data.Value.Timestamp.Ticks);
    //        writer.Write(data.Value.Bid);
    //        writer.Write(data.Value.Ask);
    //        writer.Write(data.Value.Volume);

    //        if (data.Value.Provider != null)
    //        {
    //            writer.Write(true);
    //            writer.Write(data.Value.Provider);
    //        }
    //        else
    //            writer.Write(false);
    //    }

    //    public IData Read(BinaryReader reader)
    //    {
    //        var var1 = new Tick();

    //        var1.Symbol = reader.ReadBoolean() ? reader.ReadString() : null;
    //        var1.Timestamp = new DateTime(reader.ReadInt64());
    //        var1.Bid = reader.ReadDouble();
    //        var1.Ask = reader.ReadDouble();
    //        var1.Symbol = reader.ReadBoolean() ? reader.ReadString() : null;

    //        return new Data2<Tick>(var1);
    //    }
    //}

    #endregion
}
