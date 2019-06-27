using System;
using System.Linq;
using RabbitMQ.Client;
using System.Text;
using System.Threading;

namespace Send
{
    public class Send
    {
        public static void Main(string[] args)
        {
//            Tutorial1();
//            Tutorial2_NewTask(args);
//            Tutorial3_EmitLogs(args);
            Tutorial4_Routing(args);
        }

        public static void Tutorial4_Routing(string[] args)
        {
            var factory = new ConnectionFactory {HostName = "localhost"};
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.ExchangeDeclare("direct_logs", "direct");

                    var severity = args.Length > 0 ? args[0] : "info";
                    var message = args.Length > 1
                        ? string.Join(" ", args.Skip(1).ToArray())
                        : "Hello world!";
                    var body = Encoding.UTF8.GetBytes(message);

                    channel.BasicPublish("direct_logs", severity, null, body);

                    Console.WriteLine($" [x[ sent {severity}: {message}");
                }
            }

            Console.WriteLine(" Press [Enter] to exit");
            Console.ReadLine();
        }
        
        public static void Tutorial3_EmitLogs(string[] args)
        {
            var factory = new ConnectionFactory()
            {
                HostName = "localhost"
            };
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.ExchangeDeclare(exchange: "logs", type: "fanout");

                    var message = GetMessage(args);
                    var body = Encoding.UTF8.GetBytes((message));
                    channel.BasicPublish(exchange: "logs",
                        routingKey: "",
                        basicProperties: null,
                        body: body);
                    Console.WriteLine($" [x] sent {message}");
                }
            }
            Console.WriteLine(" Press [Enter] to exit");
            Console.ReadLine();
        }

        public static void Tutorial2_NewTask(string[] args)
        {
            var factory = new ConnectionFactory() {HostName = "localhost"};
            using (var connection = factory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.QueueDeclare(queue: "hello",
                        // setting durable to true stores the message if Rabbit crashes
                        durable: true,
                        exclusive: false,
                        autoDelete: false,
                        arguments: null);

                    var message = GetMessage(args);
                    var body = Encoding.UTF8.GetBytes(message);
                    var properites = channel.CreateBasicProperties();
                    properites.Persistent = true;

                    channel.BasicPublish(exchange: "",
                        routingKey: "task_queue",
                        basicProperties: properites,
                        body: body);

                    Console.WriteLine($" [x] sent {message}");
                }
            }

            Console.WriteLine("Press [Enter] to exit");
            Console.ReadLine();
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

                    string message = "Hello World!";
                    var body = Encoding.UTF8.GetBytes(message);

                    channel.BasicPublish(exchange: "",
                        routingKey: "hello",
                        basicProperties: null,
                        body: body);

                    Console.WriteLine($" [x] sent {message}");
                }
            }

            Console.WriteLine("Press [Enter] to exit");
            Console.ReadLine();
        }
        
        private static string GetMessage(string[] args)
        {
            return ((args.Length > 0) ? string.Join(" ", args) : "Hello World");
        }
    }
}
