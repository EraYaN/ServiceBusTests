using Rebus.Activation;
using Rebus.Config;
using Rebus.Handlers;
using Rebus.Persistence.FileSystem;
using Rebus.Routing.TypeBased;
using Rebus.Routing.TransportMessages;
using Rebus.Transport.InMem;
using System;
using System.Threading.Tasks;
using System.Timers;
using Rebus.Bus;
using Rebus.Logging;
using System.Text;

namespace ServiceBusTests.Rebus
{
    class Program
    {
        private static LogLevel UsedLogLevel = LogLevel.Debug;
        private static string InputQueueName = "test";

        static void Main(string[] args)
        {
            Console.WriteLine("Starting...");

            var network = new InMemNetwork(false);

            using (var activator = new BuiltinHandlerActivator()) // This one would be provided by the IoC container
            using (var timer = new Timer())
            using (var timer2 = new Timer())
            using (var stats_timer = new Timer())
            {
                // Add the "Input" OneWay bus
                var bus = Configure.With(activator)
                    .Logging(l => l.ColoredConsole(minLevel: UsedLogLevel))
                    .Transport(t => t.UseInMemoryTransportAsOneWayClient(network))
                    //.Sagas(s => s.UseFilesystem("processes"))
                    .Routing(r => r.TypeBased()
                        .Map<CurrentTimeMessage>(InputQueueName + "ctm")
                        .Map<TestMessage>(InputQueueName + "tm"))
                        //.MapFallback(InputQueueName + "unknownmessage"))
                    .Start();
                // Add the busses to receive the messages and dispatch the workers.
                var bus1 = ConfigureBus<CurrentTimeMessage>(ConfigureActivator<PrintDateTime>(), network, InputQueueName + "ctm");
                var bus2 = ConfigureBus<TestMessage>(ConfigureActivator<PrintTest>(), network, InputQueueName + "tm");

                timer.Elapsed += delegate { bus.Send(new CurrentTimeMessage(DateTimeOffset.Now)).Wait(); };
                timer.Interval = 1000;
                timer.Start();

                timer2.Elapsed += delegate { bus.Send(new TestMessage()).Wait(); };
                timer2.Interval = 2500;
                timer2.Start();

                stats_timer.Elapsed += delegate {
                    var sb = new StringBuilder("Stats: ");
                    foreach (var q in network.Queues)
                    {
                        sb.AppendFormat("{0}: {1}; ", q, network.GetCount(q));
                    }
                    Console.WriteLine(sb.ToString());
                };
                stats_timer.Interval = 5000;
                stats_timer.Start();


                Console.WriteLine("Press enter to quit");
                Console.ReadLine();
            }
        }

        static IHandlerActivator ConfigureActivator<T>() where T : IHandleMessages, new()
        {
            var activator = new BuiltinHandlerActivator(); // This one would be provided by the IoC container
            activator.Register(() => new T());
            return activator;
        }

        static IBus ConfigureBus<T>(IHandlerActivator activator, InMemNetwork network, string queue)
        {
            return Configure.With(activator)
                    .Logging(l => l.ColoredConsole(minLevel: UsedLogLevel))
                    .Transport(t => t.UseInMemoryTransport(network, queue))
                     .Options(o =>
                     {
                         o.SetMaxParallelism(1); o.SetNumberOfWorkers(1);
                     })
                     //.Subscriptions(s => s.UseJsonFile("subs.json"))
                    //.Sagas(s => s.UseFilesystem("processes"))
                    .Routing(r => r.TypeBased()
                        .Map<T>(queue))
                    .Start();
        }
    }
    class PrintDateTime : IHandleMessages<CurrentTimeMessage>
    {
        public async Task Handle(CurrentTimeMessage message)
        {
            Console.WriteLine("The time is {0}", message.Time);
        }
    }

    class PrintTest : IHandleMessages<TestMessage>
    {
        public PrintTest()
        {

        }

        public async Task Handle(TestMessage message)
        {
            Console.WriteLine("The is a test start");
            await Task.Delay(5000);
            Console.WriteLine("The is a test end");
        }
    }

    public class CurrentTimeMessage
    {
        public DateTimeOffset Time { get; }

        public CurrentTimeMessage(DateTimeOffset time)
        {
            Time = time;
        }
    }

    public class TestMessage
    {
        public TestMessage()
        { }
    }
}
