using Microsoft.ServiceFabric.Services.Remoting;
using Models;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Common
{
    public interface IBookstore : IService,ITransaction
    {
        Task<Dictionary<string,Book>> GetAvailableBooks();

        Task RecordPurchase(string bookId, uint count);

        Task<double> GetBookPrice(string bookId);
    }
}
