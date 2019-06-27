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
            Tutorial2_Worker();
            Tutorial3_ReceiveLogs();
        }

        public static void Tutorial3_ReceiveLogs()
        {
            var factory = new ConnectionFactory { HostName = "localhost"};
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.ExchangeDeclare("logs", "fanout");
                    var queueName = channel.QueueDeclare().QueueName;
                    channel.QueueBind(queueName, "logs", "");

                    Console.WriteLine($" [*] waiting for logs");
                    
                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body;
                        var message = Encoding.UTF8.GetString(body);
                        Console.WriteLine($" [x] {message}");
                    };

                    channel.BasicConsume(queueName, true, consumer);

                    Console.WriteLine(" Press [Enter] to exit");
                    Console.ReadLine();
                }
            }
        }
        
        public static void Tutorial2_Worker()
        {
            var factory = new ConnectionFactory() {HostName = "localhost"};
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.QueueDeclare(queue: "task_queue",
                        durable: true,
                        exclusive: false,
                        autoDelete: false,
                        arguments: null);

                    channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
                    Console.WriteLine(" [*] Waiting for messages!");
                    
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
