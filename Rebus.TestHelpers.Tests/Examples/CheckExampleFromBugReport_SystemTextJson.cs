using System;
using System.Linq;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using NUnit.Framework;
using Rebus.Handlers;
using Rebus.Sagas;

namespace Rebus.TestHelpers.Tests.Examples;

[TestFixture]
[Description("Demonstrates how [JsonConstructor] attribute can work with the default serializer (which is System Text Json)")]
public class CheckExampleFromBugReport_SystemTextJson
{
    [Test]
    public void When_TriggeringMySaga_Then_ASagaIsStartedWithCorrectData()
    {
        // arrange
        var correlationId = Guid.NewGuid().ToString();
        var message = new TriggerMySagaMessage(correlationId);

        // act
        using var fixture = SagaFixture.For<MySaga>();

        fixture.Deliver(message);

        Console.WriteLine(string.Join(Environment.NewLine, fixture.LogEvents));

        var data = fixture.Data.OfType<MySagaData>().First();

        // assert
        Assert.That(data, Is.Not.Null);
        Assert.That(data.CorrelationId, Is.EqualTo(correlationId));
    }

    public class MySaga : Saga<MySagaData>,
        IAmInitiatedBy<TriggerMySagaMessage>,
        IHandleMessages<UpdateMySagaMessage>
    {
        protected override void CorrelateMessages(ICorrelationConfig<MySagaData> config)
        {
            config.Correlate<TriggerMySagaMessage>(m => m.CorrelationId, d => d.CorrelationId);
        }

        public Task Handle(TriggerMySagaMessage message)
        {
            //if (IsNew) Data.CorrelationId = message.CorrelationId;
            return Task.CompletedTask;
        }

        public Task Handle(UpdateMySagaMessage message)
        {
            Data.CorrelationId = message.CorrelationId;
            return Task.CompletedTask;
        }
    }

    public class TriggerMySagaMessage
    {
        [JsonConstructor]
        public TriggerMySagaMessage(string correlationId, string data)
        {
            CorrelationId = correlationId;
            Data = data;
        }

        public TriggerMySagaMessage(string correlationId)
        {
            CorrelationId = correlationId;
        }

        public string CorrelationId { get; set; }
        public string Data { get; }
    }

    public class UpdateMySagaMessage(string correlationId)
    {
        public string CorrelationId { get; set; } = correlationId;
    }

    public class MySagaData : ISagaData
    {
        public Guid Id { get; set; }

        public int Revision { get; set; }

        public string CorrelationId { get; set; }
    }
}