using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using STSdb4.WaterfallTree;
using STSdb4.General.Collections;
using STSdb4.Data;
using STSdb4.General.Extensions;

namespace STSdb4.Database
{
    public class RecordSet : OrderedSet<IData, IData>, IDataContainer
    {
        public int MAX_RECORDS { get; set; }

        private RecordSet(IComparer<IData> comparer, IEqualityComparer<IData> equalityComparer, List<KeyValuePair<IData, IData>> list)
            : base(comparer, equalityComparer, list)
        {
        }

        private RecordSet(IComparer<IData> comparer, IEqualityComparer<IData> equalityComparer, SortedSet<KeyValuePair<IData, IData>> set)
            : base(comparer, equalityComparer, set)
        {
        }

        public RecordSet(IComparer<IData> comparer, IEqualityComparer<IData> equalityComparer)
            : base(comparer, equalityComparer, 4)
        {
        }

        private new RecordSet InternalSplit(int count)
        {
            RecordSet recordSet;

            if (list != null)
                recordSet = new RecordSet(Comparer, EqualityComparer, list.Split(count));
            else
            {
                if (dictionary != null)
                    TransformDictionaryToTree();

                recordSet = new RecordSet(Comparer, EqualityComparer, set.Split(count));
            }
     
            recordSet.MAX_RECORDS = MAX_RECORDS;

            return recordSet;
        }

        #region IData Members

        public double FillPercentage
        {
            get { return (Count / (float)MAX_RECORDS) * 100; }
        }

        public int Counter
        {
            get { return Count; }
        }

        public bool IsEmpty
        {
            get { return Count == 0; }
        }

        public IDataContainer Split(double percentage)
        {
            return InternalSplit((int)((percentage / 100) * MAX_RECORDS));
        }

        public void Merge(IDataContainer records)
        {
            InternalMerge((OrderedSet<IData, IData>)records);
        }

        public IData FirstKey
        {
            get { return First.Key; }
        }

        public IData LastKey
        {
            get { return Last.Key; }
        }

        #endregion
    }
}
