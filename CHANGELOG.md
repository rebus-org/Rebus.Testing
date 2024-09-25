# Changelog

## 5.0.0
* Initial version
* Add fake implementations of everything, including lots of new event types
* Collect handler exceptions in saga fixture

## 5.1.0
* Add `DeliverFailed` method to `SagaFixture<>`, making is possible to simulate 2nd level dispatch (i.e. dispatching as `IFailed<TMessage>`)

## 5.2.0
* Add `PrepareConflict<>` method to `SagaFixture<>`, enabling controlled simulation of saga data update conflicts

## 5.2.1
* Fix type of event recorded when a message is deferred and explicitly routed at the same time - thanks [rsivanov]

## 5.2.2
* Add `FakeMessageContext` to make it easier to simulate message headers in tests

## 5.2.3
* Make properties of `FakeMessageContext` settable so they can be replaced after its creation

## 6.0.0
* Update to Rebus 6 - thanks [rsivanov]

## 6.0.1
* Add full .NET type information (including full assembly name) in JSON-serialized saga data in `SagaFixture` to work better with dynamically generated types - thanks [fishie]

## 6.1.1
* Add version constraint to Rebus dep

## 6.1.2
* Add `ThrowIfNotEmpty` extension for `IEnumerable<HandlerException>` to make it easy to verify that no exceptions were thrown - thanks [mclausen]

## 6.1.3
* Do not enable 2nd level retries in saga fixture by default. Can be enabled by setting `secondLevelRetriesEnabled: true` when creating the saga fixture.

## 7.0.0
* Change API of `FakeMessageContext` to contain an `IncomingStepContext`, which makes it possible to mock more stuff (notably, the `CancellationToken` can be added)

## 7.1.0
* Add ability to customize saga serializer - thanks [hdrachmann]

## 7.1.1
* Expose parameters with default arguments on the generic saga fixture factory method

## 7.2.0
* Change how saga conflicts are simulated to enable staging conflicting data, thus making it possible to test conflicts in a much more realisting fashion

## 8.0.0
* Update Rebus dependency to 7 - thanks [mclausen]

## 9.0.0
* Update Rebus dependency to 8

## 9.1.0
* Add `FakeMessageContextScope` to make it easier to fake a message context so that `MessageContext.Current` will return something

## 9.2.0
* Add ability to configure a global customizer on `SagaFixture` by calling `SetGlobalCustomizer`. The customizer is a normal `RebusConfigurer` callback, which makes it easy to call own extension methods, set the message serializer, etc.

[fishie]: https://github.com/fishie
[hdrachmann]: https://github.com/hdrachmann
[mclausen]: https://github.com/mclausen
[rsivanov]: https://github.com/rsivanov