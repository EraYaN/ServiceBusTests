using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

namespace ServiceBusTests.Naive
{
    public class MultiThreadQueue<T>
    {
        BlockingCollection<T> _jobs = new BlockingCollection<T>();
        Action<T> _handler;

        public int Count => _jobs.Count;

        public int BoundedCapacity => _jobs.BoundedCapacity;
        public MultiThreadQueue(Action<T> handler, int numThreads)
        {
            _handler = handler;
            for (int i = 0; i < numThreads; i++)
            {
                var thread = new Thread(OnHandlerStart)
                { IsBackground = true };//Mark 'false' if you want to prevent program exit until jobs finish
                thread.Start();
            }
        }

        public void Enqueue(T job, [CallerMemberName] string callerName = "")
        {
            if (!_jobs.IsAddingCompleted)
            {
                Console.WriteLine("Adding job to queue from {0}.", callerName);
                _jobs.Add(job);
            }
        }

        public void Stop()
        {
            //This will cause '_jobs.GetConsumingEnumerable' to stop blocking and exit when it's empty
            _jobs.CompleteAdding();
        }

        private void OnHandlerStart()
        {
            foreach (var job in _jobs.GetConsumingEnumerable(CancellationToken.None))
            {
                _handler(job);
            }
        }
    }
}
