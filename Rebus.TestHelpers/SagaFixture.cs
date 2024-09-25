using System;
using System.Collections.Generic;
using System.Linq;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Extensions;
using Rebus.Handlers;
using Rebus.Pipeline;
using Rebus.Retry;
using Rebus.Retry.Simple;
using Rebus.Sagas;
using Rebus.TestHelpers.Internals;
using Rebus.Transport.InMem;
using InMemorySagaStorage = Rebus.TestHelpers.Internals.InMemorySagaStorage;
// ReSharper disable UnusedTypeParameter
// ReSharper disable UnusedMember.Global

namespace Rebus.TestHelpers;

/// <summary>
/// Saga fixture factory class - can be used to create an appropriate <see cref="SagaFixture{TSagaHandler}"/> for a saga
/// handler to participate in white-box testing scenarios
/// </summary>
public static class SagaFixture
{
    internal static bool _loggingInfoHasBeenShown;

    /// <summary>
    /// Sets the global customizer, which will be called each time a <see cref="SagaFixture{TSagaHandler}"/> is created. This is a good way to configure a global default (e.g.
    /// by configuring the message serializer), which will then be used in all subsequent calls to the <see cref="SagaFixture{TSagaHandler}"/> factory methods.
    /// Can be cleared again by calling <see cref="ClearGlobalCustomizer"/>.
    /// </summary>
    public static void SetGlobalCustomizer(Func<RebusConfigurer, RebusConfigurer> configure)
    {
        GlobalCustomizer = configure ?? throw new ArgumentException(
            $"Please provide a customer callback when calling this method. If you want to clear the customizer, please call {nameof(ClearGlobalCustomizer)}() instead.");
    }

    public static void ClearGlobalCustomizer() => GlobalCustomizer = null;

    public static Func<RebusConfigurer, RebusConfigurer> GlobalCustomizer { get; private set; }

    /// <summary>
    /// Creates a saga fixture for the specified saga handler, which must have a default constructor. If the saga handler
    /// requires any parameters to be created, use the <see cref="For{TSagaHandler}(Func{TSagaHandler}, int, bool, Func{ISagaSerializer})"/> overload that
    /// accepts a factory function as a saga handler instance creator
    /// </summary>
    public static SagaFixture<TSagaHandler> For<TSagaHandler>(Func<ISagaSerializer> sagaSerializerFactory = null, int maxDeliveryAttempts = 5, bool secondLevelRetriesEnabled = false) where TSagaHandler : Saga, IHandleMessages, new()
    {
        TSagaHandler HandlerFactory()
        {
            try
            {
                return new TSagaHandler();
            }
            catch (Exception exception)
            {
                throw new ArgumentException($"Could not create new saga handler instance of type {typeof(TSagaHandler)}", exception);
            }
        }

        return For(HandlerFactory, sagaSerializerFactory: sagaSerializerFactory, maxDeliveryAttempts: maxDeliveryAttempts, secondLevelRetriesEnabled: secondLevelRetriesEnabled);
    }

    /// <summary>
    /// Creates a saga fixture for the specified saga handler, which will be instantiated by the given factory method
    /// </summary>
    public static SagaFixture<TSagaHandler> For<TSagaHandler>(Func<TSagaHandler> sagaHandlerFactory, int maxDeliveryAttempts = 5, bool secondLevelRetriesEnabled = false, Func<ISagaSerializer> sagaSerializerFactory = null) where TSagaHandler : Saga, IHandleMessages
    {
        if (sagaHandlerFactory == null) throw new ArgumentNullException(nameof(sagaHandlerFactory));

        var activator = new BuiltinHandlerActivator();

        activator.Register(sagaHandlerFactory);

        return CreateSagaFixture<TSagaHandler>(maxDeliveryAttempts, secondLevelRetriesEnabled, sagaSerializerFactory, activator);
    }

    /// <summary>
    /// Creates a saga fixture for the specified saga handler, which will be instantiated by the given factory method
    /// </summary>
    public static SagaFixture<TSagaHandler> For<TSagaHandler>(Func<IBus, TSagaHandler> sagaHandlerFactory, int maxDeliveryAttempts = 5, bool secondLevelRetriesEnabled = false, Func<ISagaSerializer> sagaSerializerFactory = null) where TSagaHandler : Saga, IHandleMessages
    {
        if (sagaHandlerFactory == null) throw new ArgumentNullException(nameof(sagaHandlerFactory));

        var activator = new BuiltinHandlerActivator();

        activator.Register((bus, _) => sagaHandlerFactory(bus));

        return CreateSagaFixture<TSagaHandler>(maxDeliveryAttempts, secondLevelRetriesEnabled, sagaSerializerFactory, activator);
    }

    static SagaFixture<TSagaHandler> CreateSagaFixture<TSagaHandler>(int maxDeliveryAttempts, bool secondLevelRetriesEnabled,
        Func<ISagaSerializer> sagaSerializerFactory, BuiltinHandlerActivator activator) where TSagaHandler : Saga, IHandleMessages
    {
        RebusConfigurer DefaultRebusConfigurerFactory()
        {
            return Configure.With(activator);
        }

        return For<TSagaHandler>(
            configurerFactory: DefaultRebusConfigurerFactory,
            maxDeliveryAttempts: maxDeliveryAttempts,
            secondLevelRetriesEnabled: secondLevelRetriesEnabled,
            sagaSerializerFactory: sagaSerializerFactory ?? DefaultSagaSerializerFactory
        );
    }

    /// <summary>
    /// Creates a saga fixture for the specified saga handler, which will be instantiated by the given factory method
    /// </summary>
    public static SagaFixture<TSagaHandler> For<TSagaHandler>(Func<RebusConfigurer> configurerFactory, int maxDeliveryAttempts = 5, bool secondLevelRetriesEnabled = false, Func<ISagaSerializer> sagaSerializerFactory = null) where TSagaHandler : Saga, IHandleMessages
    {
        if (!_loggingInfoHasBeenShown)
        {
            Console.WriteLine("Remember that the saga fixture collects all internal logs which you can access with fixture.LogEvents");
            _loggingInfoHasBeenShown = true;
        }

        return new SagaFixture<TSagaHandler>(
            configurerFactory: configurerFactory,
            maxDeliveryAttempts: maxDeliveryAttempts,
            secondLevelRetriesEnabled: secondLevelRetriesEnabled,
            sagaSerializerFactory: sagaSerializerFactory ?? DefaultSagaSerializerFactory
        );
    }

    static ISagaSerializer DefaultSagaSerializerFactory() => new NewtonSoftSagaSerializer();
}

/// <summary>
/// Saga fixture that wraps an in-mem Rebus that can be used to exercise sagas in automated tests
/// </summary>
public class SagaFixture<TSagaHandler> : IDisposable where TSagaHandler : Saga
{
    const string SagaInputQueueName = "sagafixture";

    readonly IBus _bus;
    readonly InMemorySagaStorage _inMemorySagaStorage;
    readonly LockStepper _lockStepper;
    readonly ExceptionCollector _exceptionCollector;
    readonly TestLoggerFactory _loggerFactory;

    SecondLevelDispatcher _secondLevelDispatcher;

    bool _disposed;

    /// <summary>
    /// Event that is raised whenever a message could be successfully correlated with a saga data instance. The instance
    /// is passed to the event handler
    /// </summary>
    public event Action<ISagaData> Correlated;

    /// <summary>
    /// Event that is raised whenever a message could NOT be successfully correlated with a saga data instance. The event is
    /// raised regardless of whether the incoming message is allowed to initiate a new saga or not.
    /// </summary>
    public event Action CouldNotCorrelate;

    /// <summary>
    /// Event that is raised when the incoming message resulted in creating a new saga data instance. The created instance
    /// is passed to the event handler.
    /// </summary>
    public event Action<ISagaData> Created;

    /// <summary>
    /// Event that is raised when the incoming message resulted in updating an existing saga data instance. The updated instance
    /// is passed to the event handler.
    /// </summary>
    public event Action<ISagaData> Updated;

    /// <summary>
    /// Event that is raised when the incoming message resulted in deleting an existing saga data instance. The deleted instance
    /// is passed to the event handler.
    /// </summary>
    public event Action<ISagaData> Deleted;

    /// <summary>
    /// Event raised when the saga fixture is disposed
    /// </summary>
    public event Action Disposed;

    internal SagaFixture(Func<RebusConfigurer> configurerFactory, int maxDeliveryAttempts, bool secondLevelRetriesEnabled, Func<ISagaSerializer> sagaSerializerFactory)
    {
        if (configurerFactory == null) throw new ArgumentNullException(nameof(configurerFactory));

        var network = new InMemNetwork();

        _inMemorySagaStorage = new InMemorySagaStorage(sagaSerializerFactory());
        _inMemorySagaStorage.Correlated += sagaData => Correlated?.Invoke(sagaData);
        _inMemorySagaStorage.CouldNotCorrelate += () => CouldNotCorrelate?.Invoke();
        _inMemorySagaStorage.Created += sagaData => Created?.Invoke(sagaData);
        _inMemorySagaStorage.Updated += sagaData => Updated?.Invoke(sagaData);
        _inMemorySagaStorage.Deleted += sagaData => Deleted?.Invoke(sagaData);

        _lockStepper = new LockStepper();

        _exceptionCollector = new ExceptionCollector();

        _loggerFactory = new TestLoggerFactory(new FakeRebusTime());

        var rebusConfigurer = configurerFactory();

        rebusConfigurer = rebusConfigurer
            .Logging(l => l.Use(_loggerFactory))
            .Transport(t => t.UseInMemoryTransport(network, SagaInputQueueName))
            .Sagas(s => s.Register(_ => _inMemorySagaStorage))
            .Options(o =>
            {
                o.SetNumberOfWorkers(1);
                o.SetMaxParallelism(1);

                o.RetryStrategy(
                    maxDeliveryAttempts: maxDeliveryAttempts,
                    secondLevelRetriesEnabled: secondLevelRetriesEnabled
                );

                o.Decorate<IPipeline>(c =>
                {
                    var pipeline = c.Get<IPipeline>();

                    return new PipelineStepConcatenator(pipeline)
                        .OnReceive(_lockStepper, PipelineAbsolutePosition.Front);
                });

                o.Decorate<IPipeline>(c =>
                {
                    var pipeline = c.Get<IPipeline>();

                    return new PipelineStepInjector(pipeline)
                        .OnReceive(_exceptionCollector, PipelineRelativePosition.After, typeof(DefaultRetryStep));
                });

                o.Decorate<IPipeline>(c =>
                {
                    var pipeline = c.Get<IPipeline>();

                    _secondLevelDispatcher = new SecondLevelDispatcher(c.Get<IErrorTracker>());

                    return new PipelineStepInjector(pipeline)
                        .OnReceive(_secondLevelDispatcher, PipelineRelativePosition.Before, typeof(DefaultRetryStep));
                });
            });

        var customizer = SagaFixture.GlobalCustomizer;

        if (customizer != null)
        {
            rebusConfigurer = customizer(rebusConfigurer);
        }

        _bus = rebusConfigurer.Start();
    }

    /// <summary>
    /// Gets all of the currently existing saga data instances
    /// </summary>
    public IEnumerable<ISagaData> Data => _inMemorySagaStorage.Instances;

    /// <summary>
    /// Gets all log events emitted by the internal Rebus instance
    /// </summary>
    public IEnumerable<LogEvent> LogEvents => _loggerFactory.LogEvents;

    /// <summary>
    /// Gets all exceptions caught while handling messages
    /// </summary>
    public IEnumerable<HandlerException> HandlerExceptions => _exceptionCollector.CaughtExceptions;

    /// <summary>
    /// Delivers the given message to the saga handler. This is how you would normally deliver a message to the saga.
    /// </summary>
    public void Deliver(object message, Dictionary<string, string> optionalHeaders = null, int deliveryTimeoutSeconds = 5)
    {
        if (message == null) throw new ArgumentNullException(nameof(message));

        _bus.Advanced.SyncBus.SendLocal(message, optionalHeaders);

        if (!_lockStepper.WaitOne(TimeSpan.FromSeconds(deliveryTimeoutSeconds)))
        {
            throw new TimeoutException($"Message {message} did not seem to have been processed withing {deliveryTimeoutSeconds} s timeout");
        }
    }

    /// <summary>
    /// Sets up a saga update conflict for all saga data instances matching the given <paramref name="predicate"/>.
    /// The <paramref name="createConflictingSagaData"/> callback will be invoked with a clone of each found instance, which can then be mutated into
    /// the conflicting instance.
    /// </summary>
    public void PrepareConflict<TSagaData>(Func<TSagaData, bool> predicate, Action<TSagaData> createConflictingSagaData) where TSagaData : ISagaData
    {
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));
        if (createConflictingSagaData == null) throw new ArgumentNullException(nameof(createConflictingSagaData));

        var sagaDataIds = Data.OfType<TSagaData>().Where(predicate).Select(s => s.Id).ToList();

        foreach (var id in sagaDataIds)
        {
            PrepareConflict(id, createConflictingSagaData);
        }
    }

    /// <summary>
    /// Sets up a saga update conflict for all saga data instances matching the given <paramref name="predicate"/>.
    /// The <paramref name="getConflictingSagaData"/> callback will be invoked with a clone of each found instance, which should then return a
    /// new and/or mutated instance that will be the conflicting instance.
    /// </summary>
    public void PrepareConflict<TSagaData>(Func<TSagaData, bool> predicate, Func<TSagaData, TSagaData> getConflictingSagaData) where TSagaData : ISagaData
    {
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));
        if (getConflictingSagaData == null) throw new ArgumentNullException(nameof(getConflictingSagaData));

        var sagaDataIds = Data.OfType<TSagaData>().Where(predicate).Select(s => s.Id).ToList();

        foreach (var id in sagaDataIds)
        {
            PrepareConflict(id, getConflictingSagaData);
        }
    }

    /// <summary>
    /// Sets up a saga update conflict for the saga data instance matching the given <paramref name="sagaDataId"/>.
    /// The <paramref name="createConflictingSagaData"/> callback will be invoked with a clone of the instance, which can then be mutated into
    /// the conflicting instance.
    /// </summary>
    public void PrepareConflict<TSagaData>(Guid sagaDataId, Action<TSagaData> createConflictingSagaData) where TSagaData : ISagaData
    {
        PrepareConflict<TSagaData>(sagaDataId, existing =>
        {
            var clone = (TSagaData)_inMemorySagaStorage.Clone(existing);
            createConflictingSagaData(clone);
            return clone;
        });
    }

    /// <summary>
    /// Sets up a saga update conflict for the saga data instance matching the given <paramref name="sagaDataId"/>.
    /// The <paramref name="getConflictingSagaData"/> callback will be invoked with a clone of the instance, which should then return a
    /// new and/or mutated instance that will be the conflicting instance.
    /// </summary>
    public void PrepareConflict<TSagaData>(Guid sagaDataId, Func<TSagaData, TSagaData> getConflictingSagaData) where TSagaData : ISagaData
    {
        if (getConflictingSagaData == null) throw new ArgumentNullException(nameof(getConflictingSagaData));

        var existing = Data.OfType<TSagaData>().FirstOrDefault(s => s.Id == sagaDataId);

        if (existing == null)
        {
            throw new ArgumentException(
                $@"Could not find an existing saga data of type {typeof(TSagaData)} with ID {sagaDataId}. 

To simulate a conflict, you'll need to ensure that a saga data instance with the relevant type/ID is available, and THEN prepare the conflict.

You can create a saga data instance by setting it up manually:

    fixture.Add(sagaDataInstance);

or you can do it 'naturally', by delivering the relevant initiation message to the saga:

    fixture.Deliver(new SomethingThatInitiatesTheSaga());

which then requires that you snatch batch the ID of the resulting saga data:");

        }

        PrepareConflictInternal(existing, getConflictingSagaData);
    }

    void PrepareConflictInternal<TSagaData>(TSagaData existing, Func<TSagaData, TSagaData> getConflictingSagaData) where TSagaData : ISagaData
    {
        var conflicting = getConflictingSagaData((TSagaData)_inMemorySagaStorage.Clone(existing));

        if (conflicting.Id != existing.Id)
        {
            throw new InvalidOperationException(
                "It's not possible to change the ID of the saga data when preparing the conflict, because that would change the meaning of it all.");
        }

        if (conflicting.Revision != existing.Revision)
        {
            throw new InvalidOperationException(
                "It's not possible to change the Revision of the saga data when preparing the conflict – the revision number of the conflicting saga data will automatically be incremented so that it causes a conflict on the next update attempt.");
        }

        _inMemorySagaStorage.PrepareConflict(conflicting);
    }

    /// <summary>
    /// Delivers the message as a 2nd level delivery to the saga handler, i.e. the message will be immediately
    /// dispatched inside an <see cref="IFailed{TMessage}"/> with the passed-in <paramref name="exception"/>
    /// </summary>
    public void DeliverFailed(object message, Exception exception, Dictionary<string, string> optionalHeaders = null, int deliveryTimeoutSeconds = 5)
    {
        if (message == null) throw new ArgumentNullException(nameof(message));
        if (exception == null) throw new ArgumentNullException(nameof(exception));

        var headers = optionalHeaders?.Clone() ?? new Dictionary<string, string>();

        var exceptionId = _secondLevelDispatcher.PrepareException(exception);

        headers[SecondLevelDispatcher.SecondLevelDispatchExceptionId] = exceptionId;

        _bus.Advanced.SyncBus.SendLocal(message, headers);

        if (!_lockStepper.WaitOne(TimeSpan.FromSeconds(deliveryTimeoutSeconds)))
        {
            throw new TimeoutException($"Message {message} did not seem to have been processed withing {deliveryTimeoutSeconds} s timeout");
        }
    }

    /// <summary>
    /// Adds the given saga data to the available saga data in the saga fixture. If the saga data is not provided
    /// with an ID, a new guid will automatically be assigned internally.
    /// </summary>
    public void Add(ISagaData sagaDataInstance)
    {
        _inMemorySagaStorage.AddInstance(sagaDataInstance);
    }

    /// <summary>
    /// Adds the given saga data instances to the available saga data in the fixture. If the saga data instances have not been provided
    /// with an ID, a new guid will automatically be assigned internally.
    /// </summary>
    public void AddRange(IEnumerable<ISagaData> sagaDataInstances)
    {
        foreach (var sagaDataInstance in sagaDataInstances)
        {
            Add(sagaDataInstance);
        }
    }

    /// <summary>
    /// Shuts down the in-mem bus that holds the saga handler
    /// </summary>
    public void Dispose()
    {
        if (_disposed) return;

        try
        {
            _bus.Dispose();
            _lockStepper?.Dispose();

            Disposed?.Invoke();
        }
        finally
        {
            _disposed = true;
        }
    }
}