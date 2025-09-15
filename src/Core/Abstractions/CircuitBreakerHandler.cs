using System;

namespace Kafka.Ksql.Linq.Core.Abstractions;

internal class CircuitBreakerHandler
{
    private readonly int _failureThreshold;
    private readonly TimeSpan _recoveryInterval;
    private int _failureCount = 0;
    private DateTime _lastFailureTime = DateTime.MinValue;
    private bool _isOpen = false;

    public CircuitBreakerHandler(int failureThreshold, TimeSpan recoveryInterval)
    {
        _failureThreshold = failureThreshold;
        _recoveryInterval = recoveryInterval;
    }

    public bool Handle(ErrorContext errorContext, object originalMessage)
    {
        var now = DateTime.UtcNow;

        // If the circuit is open and the recovery interval has elapsed, go half-open
        if (_isOpen && now - _lastFailureTime > _recoveryInterval)
        {
            _isOpen = false;
            _failureCount = 0;
        }

        // Skip processing when the circuit is open
        if (_isOpen)
        {
            return false; // Skip processing
        }

        // Increment failure count
        _failureCount++;
        _lastFailureTime = now;

        // Open the circuit when the threshold is exceeded
        if (_failureCount >= _failureThreshold)
        {
            _isOpen = true;
        }

        return false; // Skip this message
    }
}
