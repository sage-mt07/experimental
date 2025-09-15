using Xunit;

namespace Kafka.Ksql.Linq.Tests.Integration
{
    public class CheckTest1
    {
        private readonly Xunit.Abstractions.ITestOutputHelper _output;

        public CheckTest1(Xunit.Abstractions.ITestOutputHelper output)
        {
            _output = output;
        }

        [Fact]
        public void TestShouldRun()
        {
            _output.WriteLine("Test start");
            Assert.True(true);
            _output.WriteLine("Test end");
        }
    }
}
