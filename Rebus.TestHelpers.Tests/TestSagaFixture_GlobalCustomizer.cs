using System;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using NUnit.Framework;
using Rebus.Bus;
using Rebus.Handlers;
using Rebus.Sagas;
using Rebus.Serialization.Json;
using Rebus.TestHelpers.Tests.Extensions;
using ManualResetEvent = System.Threading.ManualResetEvent;

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously

namespace Rebus.TestHelpers.Tests;

[TestFixture]
public class TestSagaFixture_GlobalCustomizer : FixtureBase
{
    protected override void SetUp()
    {
        base.SetUp();

        Using(new DisposableCallback(SagaFixture.ClearGlobalCustomizer));
    }

    [Test]
    [Description("This only works with Newtonsoft JSON.NET, because the [JsonConstructor] attribute points to the right ctor")]
    public void CanCustomizeSomething()
    {
        using var completed = new ManualResetEvent(initialState: false);

        SagaFixture.SetGlobalCustomizer(configure => configure
            .Serialization(s => s.UseNewtonsoftJson()));

        using var fixture = SagaFixture.For(bus => new MySaga(bus, completed));

        fixture.Deliver(new MyInitiator(Guid.NewGuid()));

        try
        {
            completed.WaitOrDie(TimeSpan.FromSeconds(1));
        }
        finally
        {
            Console.WriteLine(string.Join(Environment.NewLine, fixture.LogEvents));
        }
    }

    class MySaga(IBus bus, ManualResetEvent gotInitiatorMessage) : Saga<MySagaData>, IAmInitiatedBy<MyInitiator>, IHandleMessages<MyFinisher>
    {
        protected override void CorrelateMessages(ICorrelationConfig<MySagaData> config)
        {
            config.Correlate<MyInitiator>(m => m.Id, s => s.Id);
            config.Correlate<MyFinisher>(m => m.Id, s => s.Id);
        }

        public async Task Handle(MyInitiator message)
        {
            await bus.SendLocal(new MyFinisher(message.Id));
        }

        public async Task Handle(MyFinisher message)
        {
            gotInitiatorMessage.Set();
        }
    }

    class MySagaData : SagaData;

    class MyInitiator
    {
        public Guid Id { get; }

        public MyInitiator()
        {
        }

        [JsonConstructor]
        public MyInitiator(Guid id)
        {
            Id = id;
        }
    }

    record MyFinisher(Guid Id);
}