using Microsoft.ServiceFabric.Services.Remoting;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Common
{
    public interface ITransactionCoordinator : IService
    {
        Task ProcessBookTransaction(string title, int quantity, string client);
    }
}
