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

            using (var subscriber = new DataSubscriber())
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

            // TEST

            //string aeronDir = System.IO.Path.GetTempPath() + "aeron-sbe-ipc";
            //var ctx = new Aeron.Context().AeronDirectoryName(aeronDir);

            //using (var aeron = Aeron.Connect(ctx))
            //using (var subscription = aeron.AddSubscription("aeron:ipc", 10))
            //{
            //    var sbeBuffer = new Org.SbeTool.Sbe.Dll.DirectBuffer(new byte[0]);

            //    // 1. 기존의 FragmentHandler 로직을 일반 변수로 분리합니다.
            //    // 이 핸들러는 조각난 데이터가 모두 합쳐진 '완전한 상태'일 때만 호출됩니다.
            //    var myDataHandler = new FragmentHandler((buffer, offset, length, aeronHeader) =>
            //    {
            //        // 1. [핵심 수정] 엉뚱한 메모리를 읽지 않도록 정확한 길이만큼 배열에 복사합니다.
            //        // 수십 바이트 수준의 복사이므로 성능 저하는 마이크로초(µs) 미만입니다.
            //        byte[] payload = new byte[length];
            //        buffer.GetBytes(offset, payload, 0, length);

            //        var sbeBuffer = new Org.SbeTool.Sbe.Dll.DirectBuffer(payload);

            //        // 2. [핵심 수정] 새 배열을 만들었으므로 오프셋 파라미터는 무조건 '0'이 되어야 합니다!
            //        var msgHeader = new MessageHeader();
            //        msgHeader.Wrap(sbeBuffer, 0, 0);

            //        int templateId = msgHeader.TemplateId;
            //        int blockLength = msgHeader.BlockLength;
            //        int version = msgHeader.Version;

            //        // 헤더 이후의 데이터 시작점도 0 + Header.Size 로 계산됩니다.
            //        int messageOffset = MessageHeader.Size;

            //        if (templateId == SingleDataMessage.TemplateId)
            //        {
            //            var singleMsg = new SingleDataMessage();
            //            singleMsg.WrapForDecode(sbeBuffer, messageOffset, blockLength, version);

            //            // Console.WriteLine 작업은 테스트 용도로 사용하고, 현장 배포 부분에서 제외
            //            Console.WriteLine("=== [수신: SingleDataMessage] ===");
            //            Console.WriteLine($"ID: {singleMsg.Id}, Value: {singleMsg.Value}");
            //            Console.WriteLine($"Timestamp: {singleMsg.GetTimestamp()}");
            //        }
            //        else if (templateId == ListDataMessage.TemplateId)
            //        {
            //            var listMsg = new ListDataMessage();
            //            listMsg.WrapForDecode(sbeBuffer, messageOffset, blockLength, version);

            //            // Console.WriteLine 작업은 테스트 용도로 사용하고, 현장 배포 부분에서 제외
            //            Console.WriteLine("=== [수신: ListDataMessage] ===");

            //            var entries = listMsg.Entries;
            //            int entryIndex = 1;

            //            // 데이터 포인터 넘기기
            //            // Entries.Next() 메서드를 반드시 전부 호출해 데이터 잔여량이 발생하는 현상 방지
            //            while (entries.HasNext)
            //            {
            //                entries.Next(); // 이 메서드가 호출될 때마다 내부 포인터가 다음 항목으로 전진합니다.
            //                //Console.WriteLine($"  - Entry {entryIndex}: ID={entries.Id}, Value={entries.Value}");
            //                entryIndex++;
            //            }

            //            // Console.WriteLine 작업은 테스트 용도로 사용하고, 현장 배포 부분에서 제외
            //            Console.WriteLine($"Timestamp: {listMsg.GetTimestamp()}");
            //        }
            //    });

            //    // 2. [핵심] 기존 핸들러를 FragmentAssembler로 감싸줍니다(Wrapping).
            //    // Assembler가 내부적으로 조각난 메시지를 모아두었다가, 완성이 되면 myDataHandler를 호출해 줍니다.
            //    // Aeron의 기본 통신 단위인 MTU(4~8kb) 크기를 넘는 데이터를 전송할 경우 FragmentAssembler가 반드시 필요합니다.
            //    var assembler = new FragmentAssembler(myDataHandler);

            //    var idleStrategy = new SleepingIdleStrategy(1); // 데이터가 없을 때 1ms 휴식
            //    //var idleStrategy = new BusySpinIdleStrategy(); // CPU 코어 하나를 100% 점유하여 무한 루프 (고성능)

            //    while (true)
            //    {
            //        int pollCount = 1024; // 1회 루프를 돌 때마다 읽는 최대 메세지 개수 (기본은 10)

            //        // 3. Poll 메서드에는 직접 만든 핸들러가 아니라, assembler의 핸들러를 넘겨줍니다.
            //        int fragmentsRead = subscription.Poll(assembler.OnFragment, pollCount);
            //        idleStrategy.Idle(fragmentsRead);
            //    }
            //}

        }
    }
}
