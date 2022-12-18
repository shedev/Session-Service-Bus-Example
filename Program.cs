
using System.Text;
using Azure.Messaging.ServiceBus;

namespace Playground
{

    class Program
    {
        static async Task Main(string[] args)
        {
            await SendMessageToQueueAsync();
            await ReceieveMessageFromQueueAsync();
        }
        static async Task MessageHandler(ProcessSessionMessageEventArgs args)
        {
            var body = args.Message.Body.ToString();

            // we can evaluate application logic and use that to determine how to settle the message.
            await args.CompleteMessageAsync(args.Message);
            Console.WriteLine($"processing {args.Message.SessionId} message");
            // we can also set arbitrary session state using this receiver
            // the state is specific to the session, and not any particular message
            await args.SetSessionStateAsync(new BinaryData("some state"));
        }
        static Task ErrorHandler(ProcessErrorEventArgs args)
        {
            // the error source tells me at what point in the processing an error occurred
            Console.WriteLine(args.ErrorSource);
            // the fully qualified namespace is available
            Console.WriteLine(args.FullyQualifiedNamespace);
            // as well as the entity path
            Console.WriteLine(args.EntityPath);
            Console.WriteLine(args.Exception.ToString());
            return Task.CompletedTask;
        }
        private static async Task ReceieveMessageFromQueueAsync()
        {
            var connectionString = "sas key";
            string queueName = "queue name";
            var client = new ServiceBusClient(connectionString);

            var options = new ServiceBusSessionProcessorOptions
            {
                // By default after the message handler returns, the processor will complete the message
                // If I want more fine-grained control over settlement, I can set this to false.
                AutoCompleteMessages = false,

                // I can also allow for processing multiple sessions
                MaxConcurrentSessions = 5,

                // By default or when AutoCompleteMessages is set to true, the processor will complete the message after executing the message handler
                // Set AutoCompleteMessages to false to [settle messages](https://docs.microsoft.com/en-us/azure/service-bus-messaging/message-transfers-locks-settlement#peeklock) on your own.
                // In both cases, if the message handler throws an exception without settling the message, the processor will abandon the message.
                MaxConcurrentCallsPerSession = 2,

                // Processing can be optionally limited to a subset of session Ids.
                SessionIds = { "session-1", "session-2" },
            };

            await using ServiceBusSessionProcessor processor = client.CreateSessionProcessor(queueName, options);
            // configure the message and error handler to use
            processor.ProcessMessageAsync += MessageHandler;
            processor.ProcessErrorAsync += ErrorHandler;
            await processor.StartProcessingAsync();
            // since the processing happens in the background, we add a Conole.ReadKey to allow the processing to continue until a key is pressed.
         Console.ReadKey(true);
        }

        private static async Task SendMessageToQueueAsync()
        {
            var connectionString = "sas key";
            string queueName = "queue name";
            var items = new List<string>{
           "CUSTOMER A-CREATE",
           "CUSTOMER B-CREATE",
           "CUSTOMER B-UPDATE",
           "CUSTOMER C-CREATE",
           "CUSTOMER A-UPDATE",
           "CUSTOMER B-UPDATE",
           "CUSTOMER B-UPDATE",
           "CUSTOMER B-UPDATE",
           "CUSTOMER C-UPDATE",
           "CUSTOMER B-UPDATE",
           "CUSTOMER A-UPDATE",
           "CUSTOMER D-CREATE"
                   };
            var client = new ServiceBusClient(connectionString);
            var sender = client.CreateSender(queueName);
            foreach (var item in items)
            {
                var message = new ServiceBusMessage(Encoding.UTF8.GetBytes($"{item}"))
                {
                    SessionId = $"{GetSessionFromMessage(item)}"
                };
                await sender.SendMessageAsync(message);
                Console.WriteLine($"sending {item} message");
            }
        }

        private static string GetSessionFromMessage(string customer)
        {

            if (customer.Contains("CUSTOMER A"))
                return "session-1";
            if (customer.Contains("CUSTOMER B"))
                return "session-2";
            if (customer.Contains("CUSTOMER C"))
                return "session-3";
            if (customer.Contains("CUSTOMER D"))
                return "session-4";

            // Default session
            return "session-4";
        }
    }

}


