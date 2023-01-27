using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace KafkaConsumer
{
    internal class Program
    {
        public const string TOPIC = "test-p2";

        async static Task Main(string[] args)
        {
            var config = new ConsumerConfig
            {
                GroupId = Guid.NewGuid().ToString(),
                BootstrapServers = "localhost:9093",
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };

            using var c = new ConsumerBuilder<string, string>(config).Build();
            c.Subscribe(TOPIC);
            CancellationTokenSource cts = new CancellationTokenSource();

            var counter = 0;
            var previousCounter = 0;
            Timer timer = new Timer(new TimerCallback((o) =>
            {
                var diff = counter - previousCounter;
                previousCounter = counter;
                Console.Write($"\r{counter} {diff} msg/s");
            }), null, 1000, 1000);

            try
            {
                while (true)
                {
                    var cr = c.Consume(cts.Token);
                    if (cr.IsPartitionEOF)
                        continue;
                    counter++;
                    await Task.CompletedTask;
                }
            }
            catch
            {
                c.Close();
            }
        }
    }
}
