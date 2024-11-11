using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Fabric;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Common;
using Microsoft.ServiceFabric.Services.Client;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Remoting.Client;
using Microsoft.ServiceFabric.Services.Remoting.FabricTransport;
using Microsoft.ServiceFabric.Services.Remoting.FabricTransport.Runtime;
using Microsoft.ServiceFabric.Services.Remoting.V2.FabricTransport.Client;
using Microsoft.ServiceFabric.Services.Remoting.V2.FabricTransport.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;

namespace ValidationService
{
    /// <summary>
    /// An instance of this class is created for each service instance by the Service Fabric runtime.
    /// </summary>
    internal sealed class ValidationService : StatelessService, IValidation
    {

        private readonly ITransactionCoordinator transactionCoordinator;
        public ValidationService(StatelessServiceContext context)
            : base(context)
        {

            var serviceProxyFactory = new ServiceProxyFactory((callbackClient) =>
            {
                return new FabricTransportServiceRemotingClientFactory(
                    new FabricTransportRemotingSettings
                    {
                        ExceptionDeserializationTechnique = FabricTransportRemotingSettings.ExceptionDeserialization.Default
                    }, callbackClient);

            });
            var serviceUri = new Uri("fabric:/BookstoreAppplication/TransactionCoordinator");
            transactionCoordinator = serviceProxyFactory.CreateServiceProxy<ITransactionCoordinator>(serviceUri,new ServicePartitionKey(0));
        
        
        
        }

        public async Task BookValidationAsync(string title, int quantity, string client)
        {
            Debug.WriteLine($"Client - Book Title: {title}, Quantity: {quantity} and Client: {client}");

            if (string.IsNullOrEmpty(title))
            {
                throw new ArgumentException("Unesite validan naslov.Polje ne sme biti prazno");
            }

            if (quantity <= 0)
            {
                throw new ArgumentException("Kolicina mora biti veca od nule");
            }

            if (string.IsNullOrEmpty(client))
            {
                throw new ArgumentException("Unesite validno ime klijenta,polje ne sme da bude prazno.");
            }

            await transactionCoordinator.ProcessBookTransaction(title,quantity, client);
        }

        /// <summary>
        /// Optional override to create listeners (e.g., TCP, HTTP) for this service replica to handle client or user requests.
        /// </summary>
        /// <returns>A collection of listeners.</returns>
        protected override IEnumerable<ServiceInstanceListener> CreateServiceInstanceListeners()
        {
            return new List<ServiceInstanceListener>
            {
                new ServiceInstanceListener(serviceContext =>
                    new FabricTransportServiceRemotingListener(
                        serviceContext,
                        this,
                        new FabricTransportRemotingListenerSettings
                            {
                                ExceptionSerializationTechnique = FabricTransportRemotingListenerSettings.ExceptionSerialization.Default,
                            }),
                        "ServiceEndpointV2")
            };
        }

        /// <summary>
        /// This is the main entry point for your service instance.
        /// </summary>
        /// <param name="cancellationToken">Canceled when Service Fabric needs to shut down this service instance.</param>
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            // TODO: Replace the following sample code with your own logic 
            //       or remove this RunAsync override if it's not needed in your service.

            long iterations = 0;

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                ServiceEventSource.Current.ServiceMessage(this.Context, "Working-{0}", ++iterations);

                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            }
        }
    }
}
