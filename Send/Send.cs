using System;
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
            Tutorial2(args);
        }

        public static void Tutorial2(string[] args)
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

//            Console.WriteLine("Press [Enter] to exit");
//            Console.ReadLine();
        }

        private static string GetMessage(string[] args)
        {
            return ((args.Length > 0) ? string.Join(" ", args) : "Hello World");
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
    }
}
