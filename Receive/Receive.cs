using System;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Threading;

namespace Receive
{
    public class Receive
    {
        public static void Main(string[] args)
        {
//            Tutorial1();
            Tutorial2();
        }

        public static void Tutorial2()
        {
            var factory = new ConnectionFactory() {HostName = "localhost"};
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.QueueDeclare(queue: "task_queue",
                        durable: false,
                        exclusive: false,
                        autoDelete: false,
                        arguments: null);

                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body;
                        var message = Encoding.UTF8.GetString(body);

                        int dots = message.Split(".").Length - 1;
                        Thread.Sleep(dots * 1000);
                        
                        Console.WriteLine($" [x] received {message}. Waited for {dots} seconds");
                    };

                    channel.BasicConsume(queue: "task_queue",
                        autoAck: false,
                        consumer: consumer);

                    Console.WriteLine(" Press [Enter] to exit");
                    Console.ReadLine();
                }
            }
        }

        private static void Tutorial1()
        {
            var factory = new ConnectionFactory() {HostName = "localhost"};
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.QueueDeclare(queue: "hello",
                        durable: false,
                        exclusive: false,
                        autoDelete: false,
                        arguments: null);

                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body;
                        var message = Encoding.UTF8.GetString(body);
                        Console.WriteLine($" [x] received {message}");
                    };

                    channel.BasicConsume(queue: "hello",
                        autoAck: true,
                        consumer: consumer);

                    Console.WriteLine(" Press [Enter] to exit");
                    Console.ReadLine();
                }
            }
        }
    }
}
