using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Fabric;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Common;
using Microsoft.ServiceFabric.Data.Collections;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Remoting.FabricTransport.Runtime;
using Microsoft.ServiceFabric.Services.Remoting.V2.FabricTransport.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using Models;

namespace BookstoreService
{
    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    internal sealed class BookstoreService : StatefulService,IBookstore
    {
        private const string bookDict = "bookDict";
        private const string reservedBookDict = "reservedBooks";

        private IReliableDictionary<string, Book> books;
        private IReliableDictionary<string, uint> reservedBooks;


        public BookstoreService(StatefulServiceContext context)
            : base(context)
        { }

        public async Task ConfirmTransaction()
        {
            var stateManager = this.StateManager;
            Debug.WriteLine("hello bookstore");

            
            books = await stateManager.GetOrAddAsync<IReliableDictionary<string, Book>>(bookDict);
            reservedBooks = await stateManager.GetOrAddAsync<IReliableDictionary<string, uint>>(reservedBookDict);
            

            using (var tx = stateManager.CreateTransaction())
            {
                var reservedBookEnumerator = (await reservedBooks.CreateEnumerableAsync(tx)).GetAsyncEnumerator();

                while (await reservedBookEnumerator.MoveNextAsync(CancellationToken.None))
                {
                    string bookId = reservedBookEnumerator.Current.Key;
                    uint reservedQuantity = reservedBookEnumerator.Current.Value;

                    var book = await books.TryGetValueAsync(tx, bookId);
                    if (!book.HasValue)
                    {
                        throw new ArgumentException($"Knjiga sa id {bookId} ne postoji");
                    }

                    Book updateBook = book.Value;
                    updateBook.Quantity -= reservedQuantity;

                   

                    await books.SetAsync(tx, reservedBookEnumerator.Current.Key, updateBook);
                }

                await reservedBooks.ClearAsync();

                await tx.CommitAsync();

                
            }
        }

        public async Task<Dictionary<string, Book>> GetAvailableBooks()
        {
            var stateManager = this.StateManager;

            var availableBooks = new Dictionary<string, Book>();


            books = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, Book>>(bookDict);

            using (var tx = this.StateManager.CreateTransaction())
            {
                var enumerableBooks = (await books.CreateEnumerableAsync(tx)).GetAsyncEnumerator();

                while (await enumerableBooks.MoveNextAsync(CancellationToken.None))
                {
                    if (enumerableBooks.Current.Value.Quantity > 0)
                        availableBooks.Add(enumerableBooks.Current.Key, enumerableBooks.Current.Value);
                }
            }

            return availableBooks;


        }

        public async Task<double> GetBookPrice(string bookId)
        {
            var stateManager = this.StateManager;

            books = await stateManager.GetOrAddAsync<IReliableDictionary<string, Book>>(bookDict);
            double bookPrice = 0;
            using (var tx = stateManager.CreateTransaction())
            {
                var bookResult = await books.TryGetValueAsync(tx, bookId);
                if (!bookResult.HasValue)
                {
                    throw new ArgumentException($"Knjiga sa id-jem {bookId} ne postoji");
                }

                Book book = bookResult.Value;


                bookPrice = book.Price;
            }

            return bookPrice;
        }

        public async Task<bool> InitializeTransaction()
        {
            var stateManager = this.StateManager;

            books = await stateManager.GetOrAddAsync<IReliableDictionary<string, Book>>(bookDict);
            reservedBooks = await stateManager.GetOrAddAsync<IReliableDictionary<string, uint>>(reservedBookDict);

            using (var tx = stateManager.CreateTransaction())
            {
                var reservedBookEnumerator = (await reservedBooks.CreateEnumerableAsync(tx)).GetAsyncEnumerator();

                while (await reservedBookEnumerator.MoveNextAsync(CancellationToken.None))
                {
                    string bookId = reservedBookEnumerator.Current.Key;
                    var bookResult = await books.TryGetValueAsync(tx, bookId);
                    if (!bookResult.HasValue)
                    {
                        throw new ArgumentException($"Knjiga sa {bookId} ne postoji!");
                    }

                    Book book = bookResult.Value;
                    if(reservedBookEnumerator.Current.Value >= book.Quantity)
                    {
                        return false;
                    }
                }

                return true;
            }
        }

        public async Task RecordPurchase(string bookId, uint count)
        {
            var stateManeger = this.StateManager;
            reservedBooks = await stateManeger.GetOrAddAsync<IReliableDictionary<string, uint>>(reservedBookDict);

            using (var tx = stateManeger.CreateTransaction())
            {
                var reservedQuantity = await reservedBooks.TryGetValueAsync(tx, bookId);

                uint newReservedQuantity;
                if (reservedQuantity.HasValue)
                {
                    newReservedQuantity = reservedQuantity.Value + count;
                }
                else
                {
                    newReservedQuantity = count;
                }
                

                await reservedBooks.SetAsync(tx, bookId, newReservedQuantity);

                await tx.CommitAsync();
            }
        }

        public async Task Rollback()
        {
            var stateManager = this.StateManager;

            reservedBooks = await stateManager.GetOrAddAsync<IReliableDictionary<string, uint>>(reservedBookDict);

            using (var tx = stateManager.CreateTransaction())
            {
                try
                {
                    await reservedBooks.ClearAsync();
                    await tx.CommitAsync();
                }
                catch(Exception ex) 
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
            books = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, Book>>(bookDict);
            reservedBooks = await this.StateManager.GetOrAddAsync<IReliableDictionary<string, uint>>(reservedBookDict);

            using (var trans = this.StateManager.CreateTransaction())
            {
                var enumerator = (await books.CreateEnumerableAsync(trans)).GetAsyncEnumerator();
                bool isBooksEmpty = !await enumerator.MoveNextAsync(cancellationToken);
                if (isBooksEmpty)
                {
                    
                    await books.AddAsync(trans, "book1", new Book { Title = "Most", Quantity = 100, Price = 100 });
                    await books.AddAsync(trans, "book2", new Book { Title = "Frankenstajn", Quantity = 50, Price = 50 });
                    await books.AddAsync(trans, "book3", new Book { Title = "Orkanski visovi", Quantity = 30, Price = 30 });

                   
                }

                await reservedBooks.ClearAsync();

                await trans.CommitAsync();
            }
        }
    }
}
