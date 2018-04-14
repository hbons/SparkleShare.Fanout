//  Copyright (c) 2015 Hylke Bons
//
//  Permission is hereby granted, free of charge, to any person obtaining a copy
//  of this software and associated documentation files (the "Software"), to deal
//  in the Software without restriction, including without limitation the rights
//  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//  copies of the Software, and to permit persons to whom the Software is
//  furnished to do so, subject to the following conditions:
//
//  The above copyright notice and this permission notice shall be included in
//  all copies or substantial portions of the Software.
//
//  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
//  THE SOFTWARE.


using System;
using System.Threading.Tasks;

using NetMQ;
using NetMQ.Sockets;

namespace SparkleShare.Fanout {

    public class Fanout {

        // Configuration
        static string Address    = "127.0.0.1";
        static int ResponsePort  = 5555;
        static int PublisherPort = 5556;

        // Set to false in production
        static bool TestMode = true;


        static void Main (string [] args)
        {
            using (NetMQContext context = NetMQContext.Create ())
            {
                Task server_task = Task.Factory.StartNew (() => Server (context));

                if (TestMode) {
                    Task client_task     = Task.Factory.StartNew (() => Client (context));
                    Task subscriber_task = Task.Factory.StartNew (() => Subscriber (context));

                    Task.WaitAll (server_task, client_task, subscriber_task);
                
                } else {
                    Task.WaitAll (server_task);
                }
            }
        }


        static void Server (NetMQContext context)
        {
            using (NetMQSocket response_socket  = context.CreateResponseSocket ())
            using (NetMQSocket publisher_socket = context.CreatePublisherSocket ())
            {
                response_socket.Bind ("tcp://" + Address + ":" + ResponsePort);
                publisher_socket.Bind ("tcp://" + Address + ":" + PublisherPort);

                while (true) {
                    string message = response_socket.ReceiveString ();

                    if (message.Equals ("ping")) {
                        Console.WriteLine ("[response_socket] Received: {0}", message);

                        int timestamp = (int) DateTime.UtcNow.Subtract (new DateTime (1970, 1, 1)).TotalSeconds;
                        response_socket.Send (timestamp.ToString ());

                        Console.WriteLine ("[response_socket] Sent: {0}", timestamp);

                    } else if (message.StartsWith ("announce ")) {
                        string topic = "", content = "";

                        try {
                            string body     = message.Substring (9);
                            string [] parts = body.Split ("!".ToCharArray ());

                            topic   = parts [0];
                            content = parts [1];

                            Console.WriteLine("[response_socket] Received: topic: {0}, content: {1}", topic, content);

                        } catch (Exception) {
                            Console.WriteLine ("[response_socket] Invalid request: {0}", message);
                        }

                        if (!string.IsNullOrEmpty (topic) && !string.IsNullOrEmpty (content)) {
                            response_socket.Send ("OK");
                            Console.WriteLine ("[response_socket] Sent: OK");

                            publisher_socket.SendMore (topic).Send (content);
                            Console.WriteLine ("[publisher_socket] Sent: topic: {0}, content: {1}", topic, content);
                        }
                    
                    } else {
                        Console.WriteLine ("[response_socket] Invalid request: {0}", message);
                        response_socket.Send ("Huh?");
                    }
                }
            }      
        }


        static void Client (NetMQContext context)
        {
            using (NetMQSocket request_socket = context.CreateRequestSocket ())
            {
                request_socket.Connect ("tcp://" + Address + ":" + ResponsePort);

                while (true) {
                    Console.WriteLine("[request_socket] Type message to send: ");

                    string message = Console.ReadLine ();
                    request_socket.Send (message);

                    message = request_socket.ReceiveString ();
                    Console.WriteLine ("[request_socket] Received: {0}", message);
                }
            }
        }


        static void Subscriber (NetMQContext context)
        {
            using (NetMQSocket subscriber_socket = context.CreateSubscriberSocket ())
            {
                subscriber_socket.Connect ("tcp://" + Address + ":" + PublisherPort);

                (subscriber_socket as SubscriberSocket).Subscribe ("kittens");
                (subscriber_socket as SubscriberSocket).Subscribe ("puppies");

                while (true) {
                    string topic   = subscriber_socket.ReceiveString ();
                    string content = subscriber_socket.ReceiveString ();

                    Console.WriteLine ("[subcriber_socket] Received: topic: {0}, content: {1}", topic, content);
                }
            }
        }
    }
}
