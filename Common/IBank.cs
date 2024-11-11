using Microsoft.ServiceFabric.Services.Remoting;
using Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Common
{
    public interface IBank : IService,ITransaction
    {
        Task<Dictionary<string,Client>> GetClients();

        Task InitiateMoneyTransfer(string userId,double amount);
    }
}
