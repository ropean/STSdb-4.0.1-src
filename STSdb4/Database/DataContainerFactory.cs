using STSdb4.WaterfallTree;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace STSdb4.Database
{
    public class DataContainerFactory : IDataContainerFactory
    {
        public Locator Locator { get; private set; }
        
        public DataContainerFactory(Locator locator)
        {
            Locator = locator;
        }

        public IDataContainer Create()
        {
            const int MAX_RECORDS = 128 * 1024;

            var data = new RecordSet(Locator.KeyComparer, Locator.KeyEqualityComparer);
            data.MAX_RECORDS = MAX_RECORDS;

            return data;
        }
    }
}
