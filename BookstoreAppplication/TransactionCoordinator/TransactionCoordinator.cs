using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Common;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Client;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Remoting.Client;
using Microsoft.ServiceFabric.Services.Remoting.FabricTransport;
using Microsoft.ServiceFabric.Services.Remoting.FabricTransport.Runtime;
using Microsoft.ServiceFabric.Services.Remoting.V2.FabricTransport.Client;
using Microsoft.ServiceFabric.Services.Remoting.V2.FabricTransport.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;

namespace TransactionCoordinator
{
    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    internal sealed class TransactionCoordinator : StatefulService,ITransactionCoordinator
    {
        private readonly IBookstore _bookstore;
        private readonly IBank _bank;

        public TransactionCoordinator(StatefulServiceContext context)
            : base(context)
        {
            var serviceProxyFactory1 = new ServiceProxyFactory((callbackClient) =>
            {
                return new FabricTransportServiceRemotingClientFactory(
                    new FabricTransportRemotingSettings
                    {
                        ExceptionDeserializationTechnique = FabricTransportRemotingSettings.ExceptionDeserialization.Default
                    }, callbackClient);
            });

            var serviceUri1 = new Uri("fabric:/BookstoreAppplication/BookstoreService");

            _bookstore = serviceProxyFactory1.CreateServiceProxy<IBookstore>(serviceUri1, new ServicePartitionKey(0));

            var serviceProxyFactory2 = new ServiceProxyFactory((callbackClient) =>
            {
                return new FabricTransportServiceRemotingClientFactory(
                    new FabricTransportRemotingSettings
                    {
                        ExceptionDeserializationTechnique = FabricTransportRemotingSettings.ExceptionDeserialization.Default
                    }, callbackClient);
            });

            var serviceUri2 = new Uri("fabric:/BookstoreAppplication/BankService");

            _bank = serviceProxyFactory2.CreateServiceProxy<IBank>(serviceUri2, new ServicePartitionKey(0));

        }


        public async Task ProcessBookTransaction(string title, int quantity, string client)
        {
            var availableBooks = await _bookstore.GetAvailableBooks();

            var selectedBook = availableBooks.FirstOrDefault(b => b.Value.Title.Equals(title,StringComparison.OrdinalIgnoreCase));

            var bookId= selectedBook.Key;

            double price = await _bookstore.GetBookPrice(bookId);

            var clients = await _bank.GetClients();

            var clientId = clients.FirstOrDefault(c => c.Value.Name.Equals(client)).Key;

            double amount = quantity * price;
            
            Debug.WriteLine($"amont:{amount} clientid {clientId} bookid {bookId}");


            try
            {
                await _bookstore.RecordPurchase(bookId, (uint)quantity);
                await _bank.InitiateMoneyTransfer(clientId, amount);

                
                bool BookstoreReady = await _bookstore.InitializeTransaction();
                bool BankReady = await _bank.InitializeTransaction();

                if(BookstoreReady && BankReady)
                {
                    await _bookstore.ConfirmTransaction();
                    await _bank.ConfirmTransaction();
                }
                else
                {
                    await _bookstore.Rollback();
                    await _bank.Rollback();
                }


            }catch (Exception ex) 
                {
                await _bookstore.Rollback();
                await _bank.Rollback();
                throw new InvalidOperationException("Doslo je do greske u procesu transakcije",ex);

            }



        }

        /// <summary>
        /// Optional override to create listeners (e.g., HTTP, Service Remoting, WCF, etc.) for this service replica to handle client or user requests.
        /// </summary>
        /// <remarks>
        /// For more information on service communication, see https://aka.ms/servicefabricservicecommunication
        /// </remarks>
        /// <returns>A collection of listeners.</returns>
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

        
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            // TODO: Replace the following sample code with your own logic 
            //       or remove this RunAsync override if it's not needed in your service.

            var myDictionary = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, long>>("myDictionary");

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                using (var tx = this.StateManager.CreateTransaction())
                {
                    var result = await myDictionary.TryGetValueAsync(tx, "Counter");

                    ServiceEventSource.Current.ServiceMessage(this.Context, "Current Counter Value: {0}",
                        result.HasValue ? result.Value.ToString() : "Value does not exist.");

                    await myDictionary.AddOrUpdateAsync(tx, "Counter", 0, (key, value) => ++value);

                    // If an exception is thrown before calling CommitAsync, the transaction aborts, all changes are 
                    // discarded, and nothing is saved to the secondary replicas.
                    await tx.CommitAsync();
                }

                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            }
        }
    }
}
