using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Com.Hooniegit.Sbe;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona.Concurrent;
using Tools;

namespace AeronCSharpSubscriber
{
    internal class Program
    {
        static void Main(string[] args)
        {

            using (var subscriber = new DataSubscriber(streamId:10))
            {
                // Media Driver 연결
                subscriber.Connect();

                // ListDataMessage 데이터 수신 Callback 정의
                subscriber.OnListDataReceived = (data) =>
                {
                    var entries = data.Entries;
                    int entryCount = 0;

                    while (entries.HasNext)
                    {
                        entries.Next(); // (필수) 반드시 호출: 데이터 포인트 이동 목적
                        entryCount++;

                        int currentId = entries.Id;
                        double currentValue = entries.Value;

                        // 이하에서 currentId, currentValue 값으로 작업 정의
                        // ...
                    }

                    string timestamp = data.GetTimestamp();

                    // 이하에서 timestamp 값으로 작업 정의
                    Console.WriteLine("entry: " + entryCount);
                    Console.WriteLine("timestamp: " + timestamp);

                };

                // (무한 루프) Subscriber 시작
                subscriber.Start();
            }

        }
    }
}
