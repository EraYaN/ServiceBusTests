using System;
using System.Threading.Tasks;
using System.Timers;

namespace ServiceBusTests.Naive
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            using (var timer = new Timer())
            using (var timer2 = new Timer())
            using (var stats_timer = new Timer())
            {
                var q1 = new MultiThreadQueue<CurrentTimeMessage>(TaskHandlers.HandleTime, 1);
                var q2 = new MultiThreadQueue<TestMessage>(TaskHandlers.HandleTest, 1);

                timer.Elapsed += delegate { q1.Enqueue(new CurrentTimeMessage(DateTimeOffset.Now)); };
                timer.Interval = 1000;
                timer.Start();

                timer2.Elapsed += delegate { q2.Enqueue(new TestMessage()); };
                timer2.Interval = 2500;
                timer2.Start();

                timer2.Elapsed += delegate { Console.WriteLine("Stats: q1: {0} q2: {1}", q1.Count, q2.Count); };
                timer2.Interval = 2500;
                timer2.Start();

                Console.WriteLine("Press enter to quit");
                Console.ReadLine();
                q1.Stop();
                q2.Stop();
            }
        }
    }

    static class TaskHandlers
    {
        public static void HandleTime(CurrentTimeMessage message)
        {
            Console.WriteLine("The time is {0}", message.Time);
        }

        public static void HandleTest(TestMessage message)
        {
            Console.WriteLine("The is a test start");
            Task.Delay(5000).Wait();
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
