using System;

namespace Kafka.Ksql.Linq.Query.Builders.Common;

/// <summary>
/// Stream processing exception
/// </summary>
internal class StreamProcessingException : Exception
{
    public StreamProcessingException(string message) : base(message) { }

    public StreamProcessingException(string message, Exception innerException) : base(message, innerException) { }
}
