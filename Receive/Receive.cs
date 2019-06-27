using System;
using System.Linq;
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
//            Tutorial2_Worker();
//            Tutorial3_ReceiveLogs();
//            Tutorial4_ReceiveLogsDirect(args);
            Tutorial5_ReceiveLogsTopic(args);
        }

        private static void Tutorial5_ReceiveLogsTopic(string[] args)
        {
            var factory = new ConnectionFactory {HostName = "localhost"};
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.ExchangeDeclare("topic_logs", "topic");
                    var queueName = channel.QueueDeclare().QueueName;
                    if (args.Length < 1)
                    {
                        Console.Error.WriteLine($"Usage: {Environment.GetCommandLineArgs()[0]} binding_key...");
                        Console.WriteLine(" Press [Enter] to exit");
                        Console.ReadLine();
                        Environment.ExitCode = 1;
                        return;
                    }

                    foreach (var bindingKey in args)
                    {
                        channel.QueueBind(queueName, "topic_logs", bindingKey);
                    }

                    Console.WriteLine(" [*] waiting for messages. To exit press Ctrl+C");
                    
                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body;
                        var message = Encoding.UTF8.GetString(body);
                        var routingKey = ea.RoutingKey;
                        Console.WriteLine($" [x] received {routingKey}: {message}");
                    };
                    channel.BasicConsume(queueName, true, consumer);
                    Console.WriteLine(" Press [enter] to exit");
                    Console.ReadLine();
                }
            }
        }

        public static void Tutorial4_ReceiveLogsDirect(string[] args)
        {
            var factory = new ConnectionFactory{HostName = "localhost"};
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.ExchangeDeclare("direct_logs", "direct");
                    var queueName = channel.QueueDeclare().QueueName;

                    if (args.Length < 1)
                    {
                        Console.Error.WriteLine($"Usage: {Environment.GetCommandLineArgs()[0]} [info] [warning] [error]");
                        Console.WriteLine(" Press [Enter] to exit");
                        Console.ReadLine();
                        Environment.ExitCode = 1;
                        return;
                    }

                    foreach (var severity in args)
                    {
                        channel.QueueBind(queueName, "direct_logs", severity);
                    }

                    Console.WriteLine(" [*] waiting for messages");
                    
                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body;
                        var message = Encoding.UTF8.GetString(body);
                        var routingKey = ea.RoutingKey;
                        Console.WriteLine($" [x] received {routingKey}: {message}");
                    };

                    channel.BasicConsume(queueName, true, consumer);

                    Console.WriteLine(" Press [Enter] to exit");
                    Console.ReadLine();
                }
            }
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
