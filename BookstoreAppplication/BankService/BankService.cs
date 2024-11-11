using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Common;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Remoting.FabricTransport.Runtime;
using Microsoft.ServiceFabric.Services.Remoting.V2.FabricTransport.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using Models;

namespace BankService
{
    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    internal sealed class BankService : StatefulService,IBank
    {
        private const string clientDict = "clients";
        private const string reservedMoneyDict = "reservedMoney";

        private IReliableDictionary<string, Client> _clients;
        private IReliableDictionary<string, double> reservedMoney;

        public BankService(StatefulServiceContext context)
            : base(context)
        { }

        public async Task ConfirmTransaction()
        {
            var stateManager = this.StateManager;


            _clients = await stateManager.GetOrAddAsync<IReliableDictionary<string, Client>>(clientDict);
            reservedMoney = await stateManager.GetOrAddAsync<IReliableDictionary<string, double>>(reservedMoneyDict);
            using (var tx = stateManager.CreateTransaction())
            {
                var reservedClientEnumerator = (await reservedMoney.CreateEnumerableAsync(tx)).GetAsyncEnumerator();

                while (await reservedClientEnumerator.MoveNextAsync(CancellationToken.None))
                {
                    string clientId = reservedClientEnumerator.Current.Key;
                    double reservedAmount = reservedClientEnumerator.Current.Value;
                    var clientResult = await _clients.TryGetValueAsync(tx, clientId);

                    if (!clientResult.HasValue)
                    {
                        throw new ArgumentException($"Klijent sa ID {clientId} ne postoji");
                    }

                   Client client = clientResult.Value;

                    client.Balance -= reservedAmount;

                    Debug.WriteLine($"{client.Name} now has {client.Balance}");

                    await _clients.SetAsync(tx, reservedClientEnumerator.Current.Key, client);
                }

                await reservedMoney.ClearAsync();

                await tx.CommitAsync();
            }
        }

        public async Task<Dictionary<string, Client>> GetClients()
        {
            var stateManager = this.StateManager;

            var clients = new Dictionary<string, Client>();


            _clients = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, Client>>(clientDict);

            using (var tx = stateManager.CreateTransaction())
            {
                var enumerator = (await _clients.CreateEnumerableAsync(tx)).GetAsyncEnumerator();

                while (await enumerator.MoveNextAsync(CancellationToken.None))
                {
                    clients.Add(enumerator.Current.Key, enumerator.Current.Value);
                }
            }

            return clients;
        }

        public async Task<bool> InitializeTransaction()
        {
            var stateManager = this.StateManager;
            

            _clients = await stateManager.GetOrAddAsync<IReliableDictionary<string, Client>>(clientDict);
            reservedMoney = await stateManager.GetOrAddAsync<IReliableDictionary<string, double>>(reservedMoneyDict);

            using (var tx = stateManager.CreateTransaction())
            {
                var reservedClientEnumerator = (await reservedMoney.CreateEnumerableAsync(tx)).GetAsyncEnumerator();

                while (await reservedClientEnumerator.MoveNextAsync(CancellationToken.None))
                {
                    string clientId = reservedClientEnumerator.Current.Key;

                    var clientResult = await _clients.TryGetValueAsync(tx, clientId);
                    if (!clientResult.HasValue)
                    {
                        throw new ArgumentException($"Klijent sa id {clientId} ne postoji");
                    }

                   Client client = clientResult.Value;

                    if (reservedClientEnumerator.Current.Value >= client.Balance)
                    {
                        return false;
                    }
                }

                return true;
            }
        }

        public async Task InitiateMoneyTransfer(string userId, double amount)
        {
            var stateManeger = this.StateManager;
            
            reservedMoney = await stateManeger.GetOrAddAsync<IReliableDictionary<string, double>>(reservedMoneyDict);

            using (var tx = stateManeger.CreateTransaction())
            {
                var reservedFunds = await reservedMoney.TryGetValueAsync(tx, userId);
                //double newReservedFunds = reservedFunds.HasValue ? reservedFunds.Value + amount : amount;

                double newReservedFunds;
                if (reservedFunds.HasValue)
                {
                    newReservedFunds = reservedFunds.Value + amount;
                }
                else
                {
                    newReservedFunds= amount;
                }
                await reservedMoney.SetAsync(tx, userId, newReservedFunds);

                await tx.CommitAsync();
            }
        }

        public async Task Rollback()
        {

            var stateManager = this.StateManager;
            
            reservedMoney = await stateManager.GetOrAddAsync<IReliableDictionary<string, double>>(reservedMoneyDict);

            using (var tx = stateManager.CreateTransaction())
            {


                try
                {
                    await reservedMoney.ClearAsync();
                    await tx.CommitAsync();
                }
                catch (Exception ex)
                {
                    Debug.WriteLine($"Greska prilikom rollback-a: {ex.Message}");
                }

            }
        }

        
        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {
            return new List<ServiceReplicaListener>
            {
                new ServiceReplicaListener(serviceContext =>
                    new FabricTransportServiceRemotingListener(
                        serviceContext,
                        this, new FabricTransportRemotingListenerSettings
                            {
                                ExceptionSerializationTechnique = FabricTransportRemotingListenerSettings.ExceptionSerialization.Default,
                            })
                    )
            };
        }

        /// <summary>
        /// This is the main entry point for your service replica.
        /// This method executes when this replica of your service becomes primary and has write status.
        /// </summary>
        /// <param name="cancellationToken">Canceled when Service Fabric needs to shut down this service replica.</param>
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            _clients = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, Client>>(clientDict);
            reservedMoney = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, double>>(reservedMoneyDict);

            using (var tx = this.StateManager.CreateTransaction())
            {
                var enumerator = (await _clients.CreateEnumerableAsync(tx)).GetAsyncEnumerator();


                if (!await enumerator.MoveNextAsync(cancellationToken))
                {

                    await _clients.AddAsync(tx, "client1", new Client { Name = "Luka", Balance = 2000 });
                    await _clients.AddAsync(tx, "client2", new Client { Name = "Vuk", Balance = 1000 });
                    await _clients.AddAsync(tx, "client3", new Client { Name = "Dijana", Balance = 3000 });
                }

                await reservedMoney.ClearAsync();

                await tx.CommitAsync();
            }
        }
    }
}
