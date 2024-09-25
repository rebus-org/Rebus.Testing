using System;

namespace Rebus.TestHelpers.Tests;

class DisposableCallback(Action dispose) : IDisposable
{
    public void Dispose() => dispose();
}