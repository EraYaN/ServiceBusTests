using Rebus.Activation;
using Rebus.Config;
using Rebus.Handlers;
using Rebus.Logging;
using Rebus.Persistence.FileSystem;
using Rebus.Routing.TypeBased;
using Rebus.Routing.TransportMessages;
using Rebus.Transport.InMem;
using System;
using System.Threading.Tasks;
using System.Timers;
using Rebus.Bus;
using Rebus.Pipeline;
using Rebus.Pipeline.Send;
using Rebus.Pipeline.Receive;
using System.Collections.Generic;
using System.Collections.Concurrent;
using Rebus.Messages;
using System.Text;
using Rebus.Transport;
using RabbitMQ.Client;

namespace ServiceBusTests.Rebus.RabbitMQ
{
    class Program
    {
        private static string RabbitMqConnectionString = "amqp://guest:guest@localhost:5672";

        private static LogLevel UsedLogLevel = LogLevel.Info;
        private static string InputQueueName = "test";

        private static ConnectionFactory factory;
        private static IConnection connection;
        private static IModel channel;

        private static string[] queues = new[] { InputQueueName+"tm", InputQueueName+"ctm" };

        private static int perConsumerPrefetch = 1;

        private static ConcurrentDictionary<string, int> stats = new ConcurrentDictionary<string, int>();

        static void Main(string[] args)
        {
            Console.WriteLine("Starting...");

            factory = new ConnectionFactory() { Uri = new Uri(RabbitMqConnectionString) };
            connection = factory.CreateConnection("Stats Connection");
            channel = connection.CreateModel();

            // You should do this before starting any consumers. Otherwise there are no "Ready" messages.
            foreach(var q in queues)
            {
                stats[q] = GetMessageCount(q);
            }

            using (var activator = new BuiltinHandlerActivator()) // This one would be provided by the IoC container
            using (var timer = new Timer())
            using (var timer2 = new Timer())
            using (var stats_timer = new Timer())
            {
                // Add the "Input" OneWay bus
                var bus = Configure.With(activator)
                    .Logging(l => l.ColoredConsole(minLevel: UsedLogLevel))
                    .Transport(t => t.UseRabbitMqAsOneWayClient(RabbitMqConnectionString).ClientConnectionName("Main Connection"))
                    //.Sagas(s => s.UseFilesystem("processes"))
                    .Routing(r => r.TypeBased()
                        .Map<CurrentTimeMessage>(InputQueueName + "ctm")
                        .Map<TestMessage>(InputQueueName + "tm"))
                        //.MapFallback(InputQueueName + "unknownmessage"))
                    .Options(
                      x => x.Decorate<IPipeline>(res => {
                          var pipeline = res.Get<IPipeline>();
                          var injector = new PipelineStepInjector(pipeline).OnSend(new CustomOutgoingMetricStep(stats), PipelineRelativePosition.Before, typeof(SerializeOutgoingMessageStep));
                          return injector;
                      })
                    )
                    .Start();

                // Add the busses to receive the messages and dispatch the workers.
                var bus1 = ConfigureBus<CurrentTimeMessage>(ConfigureActivator<PrintDateTime>(), InputQueueName + "ctm", perConsumerPrefetch);
                var bus2 = ConfigureBus<TestMessage>(ConfigureActivator<PrintTest>(), InputQueueName + "tm", perConsumerPrefetch, 4);

                timer.Elapsed += delegate { bus.Send(new CurrentTimeMessage(DateTimeOffset.Now)).Wait(); };
                timer.Interval = 1000;
                timer.Start();

                timer2.Elapsed += delegate { bus.Send(new TestMessage()).Wait(); };
                timer2.Interval = 2500;
                timer2.Start();

                stats_timer.Elapsed += delegate {
                    var sb = new StringBuilder("Stats: ");
                    foreach (var q in stats)
                    {
                        sb.AppendFormat("{0}: {1} (consumers: {2}); ", q.Key, q.Value, GetConsumerCount(q.Key));
                    }
                    Console.WriteLine(sb.ToString());
                };
                stats_timer.Interval = 5000;
                stats_timer.Start();

                Console.WriteLine("Press enter to quit");
                Console.ReadLine();
                bus.Dispose();
                bus1.Dispose();
                bus2.Dispose();
            }
        }

        static IHandlerActivator ConfigureActivator<T>() where T : IHandleMessages, new()
        {
            var activator = new BuiltinHandlerActivator(); // This one would be provided by the IoC container
            activator.Register(() => new T());
            return activator;
        }

        static IBus ConfigureBus<T>(IHandlerActivator activator, string queue, int prefetch, int workers = 1)
        {
            return ConfigureBus<T>(activator, queue, prefetch, workers, workers);
        }

        static IBus ConfigureBus<T>(IHandlerActivator activator, string queue, int prefetch, int workers, int parallelism)
        {
            return Configure.With(activator)
                    .Logging(l => l.ColoredConsole(minLevel: UsedLogLevel))
                    .Transport(t => t.UseRabbitMq(RabbitMqConnectionString, queue).Prefetch(prefetch).ClientConnectionName(string.Format("Consumer Connection {0}",queue)))
                     .Options(o =>
                     {
                         o.SetMaxParallelism(parallelism); o.SetNumberOfWorkers(workers);
                         o.Decorate<IPipeline>(res =>
                         {
                             var pipeline = res.Get<IPipeline>();
                             var injector = new PipelineStepInjector(pipeline).OnReceive(new CustomIncomingMetricStep(stats, queue), PipelineRelativePosition.After, typeof(DeserializeIncomingMessageStep));
                             return injector;
                         });
                     })
                    //.Sagas(s => s.UseFilesystem("processes"))
                    .Routing(r => r.TypeBased()
                        .Map<T>(queue))
                    .Start();
        }

        static int GetMessageCount(string queueName)
        {
            return (int)channel.MessageCount(queueName);
        }
        static int GetConsumerCount(string queueName)
        {
            return (int)channel.ConsumerCount(queueName);
        }
    }
    class PrintDateTime : IHandleMessages<CurrentTimeMessage>
    {
        public async Task Handle(CurrentTimeMessage message)
        {
            Console.WriteLine("The time is {0} received at {0}", message.Time, DateTimeOffset.Now);
        }
    }

    class PrintTest : IHandleMessages<TestMessage>
    {
        public PrintTest()
        {

        }

        public async Task Handle(TestMessage message)
        {
            var id = MessageContext.Current.Message.GetMessageId();
            Console.WriteLine("This is the test start for {0}", id);
            await Task.Delay(5000);
            Console.WriteLine("This is the test end for {0}", id);
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

    public class CustomIncomingMetricStep : IIncomingStep
    {
        private readonly ConcurrentDictionary<string, int> _stats;
        private readonly string _address;
        public CustomIncomingMetricStep(ConcurrentDictionary<string, int> stats, string address)
        {
            _stats = stats;
            _address = address;
        }

        public async Task Process(IncomingStepContext context, Func<Task> next)
        {
            // Message about to be processed. Log details of it to some kind of monitoring system, perf counters, etc
            try
            {
                if (_stats.ContainsKey(_address))
                    _stats[_address]--;
                else
                    _stats[_address] = 0;

                if(_stats[_address] < 0)
                {
                    _stats[_address] = 0;
                }
                await next();
                // Message succeeded.
            }
            catch (Exception ex)
            {
                // Log failure to monitoring but still throw so Rebus can retry
                throw;
            }
        }        
    }

    public class CustomOutgoingMetricStep : IOutgoingStep
    {
        private readonly ConcurrentDictionary<string, int> _stats;
        public CustomOutgoingMetricStep(ConcurrentDictionary<string, int> stats)
        {
            _stats = stats;
        }        

        public async Task Process(OutgoingStepContext context, Func<Task> next)
        {
            var addresses = context.Load<DestinationAddresses>();
            foreach (var a in addresses)
            {
                if (_stats.ContainsKey(a))
                    _stats[a]++;
                else
                    _stats[a] = 1;
            }
            await next();
            // Message sent! Log details of it to some kind of monitoring system; performance counters, whatever.
        }
    }
}
