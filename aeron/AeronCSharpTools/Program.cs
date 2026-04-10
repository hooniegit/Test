using System;
using Tools;
using System.Threading;

namespace AeronCSharpTools
{
    internal class Program
    {

        private static Random random = new Random();

        static void Main(string[] args)
        {
            //DataPublisher publisher = new DataPublisher(streamId:11);
            //publisher.Connect();

            //while (true)
            //{
            //    string timestamp = DateTime.Now.ToString();
            //    int id = random.Next();
            //    var value = "Rand_" + id;
            //    publisher.PublishSingleDataMessage(id, value, timestamp);

            //}

            byte[] sharedBuffer = new byte[1024 * 1024]; // 어플리케이션이 사용할 공유 메모리
            int delay = 3000; // Connect() 재시도 delay
            bool keepRunning = true; // false 설정 시 반복문 종료
            while (keepRunning)
            {
                try
                {
                    Console.WriteLine("\n[System] 미디어 드라이버 연결을 시도합니다...");
                    using (var subscriber = new DataSubscriber(streamId: 10, sharedBuffer: sharedBuffer))
                    {
                        // 데이터 수신 콜백
                        subscriber.OnListDataReceived = (data) =>
                        {
                            var entries = data.Entries;
                            int entryCount = 0;

                            while (entries.HasNext)
                            {
                                entries.Next();
                                entryCount++;

                                // 이하에서 데이터 처리
                                int id = entries.Id;
                                double value = entries.Value;
                            }
                            
                            // 이하에서 데이터 처리
                            string timestamp = data.GetTimestamp();
                            Console.WriteLine(timestamp); // TEST
                        };

                        subscriber.OnDisconnected = () =>
                        {
                            Console.WriteLine("[System] 연결 끊김이 감지되었습니다. 수신을 중지하고 재연결을 준비합니다."); // TEST
                            subscriber.Stop(); // _isRunning = false
                        };

                        // 연결 및 데이터 수신
                        subscriber.Connect();
                        subscriber.Start();
                    }
                }
                catch (Exception ex)
                {
                    // Connect() 실패
                    Console.WriteLine($"[Error] 미디어 드라이버 연결/수신 중 예외 발생: {ex.Message}"); // TEST
                }

                // Time Delay
                Console.WriteLine("[System] 3초 후 재연결을 시도합니다..."); // TEST
                Thread.Sleep(delay);
            }

        }
    }
}
